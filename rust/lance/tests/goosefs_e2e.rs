//! GooseFS Lance Dataset E2E tests (Stage 4).
//! Run: cargo test -p lance --features goosefs --test goosefs_e2e -- --ignored --nocapture --test-threads=1
#![cfg(feature = "goosefs")]

use std::sync::Arc;

use arrow::array::{FixedSizeListArray, Float32Array, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::RecordBatchIterator;
use futures::{StreamExt, TryStreamExt};
use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use lance_arrow::FixedSizeListArrayExt;

// ── Storage option keys (mirrors goosefs_sdk::config constants) ──
const STORAGE_OPT_MASTER_ADDR: &str = "goosefs_master_addr";
const STORAGE_OPT_WRITE_TYPE: &str = "goosefs_write_type";
const STORAGE_OPT_AUTH_TYPE: &str = "goosefs_auth_type";
const STORAGE_OPT_AUTH_USERNAME: &str = "goosefs_auth_username";

// ── WriteType values (mirrors goosefs_sdk::config::WriteType variants) ──
const WRITE_TYPE_CACHE_THROUGH: &str = "cache_through";
const WRITE_TYPE_THROUGH: &str = "through";

fn goosefs_uri(suffix: &str) -> String {
    let addr = std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".into());
    format!("goosefs://{}/lance-test/datasets/{}", addr, suffix)
}

// ============================================================
// Test 4.1: Basic write + read
// ============================================================

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_dataset_write_read() {
    let uri = goosefs_uri("basic.lance");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "alice", "bob", "charlie", "david", "eve",
            ])),
            Arc::new(Float32Array::from(vec![95.5, 87.3, 91.0, 78.8, 99.1])),
        ],
    )
    .unwrap();

    // Write Dataset
    let write_params = WriteParams {
        mode: WriteMode::Overwrite,
        ..Default::default()
    };
    let batches = RecordBatchIterator::new([Ok(batch.clone())], schema.clone());
    Dataset::write(batches, &uri, Some(write_params))
        .await
        .unwrap();

    // Open + Verify row count
    let dataset = Dataset::open(&uri).await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 5);

    // Scan + Verify data
    let mut stream = dataset.scan().try_into_stream().await.unwrap();
    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        total_rows += batch.num_rows();
    }
    assert_eq!(total_rows, 5);

    // Schema verification
    assert_eq!(dataset.schema().fields.len(), 3);
    println!("test_lance_dataset_write_read: 5 rows, 3 fields ✅");
}

// ============================================================
// Test 4.2: Append
// ============================================================

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_dataset_append() {
    let uri = goosefs_uri("append.lance");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Float32, false),
    ]));

    // Initial write
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0])),
        ],
    )
    .unwrap();

    let batches = RecordBatchIterator::new([Ok(batch1)], schema.clone());
    let mut dataset = Dataset::write(
        batches,
        &uri,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 3);

    // Append
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5])),
            Arc::new(Float32Array::from(vec![4.0, 5.0])),
        ],
    )
    .unwrap();

    let batches2 = RecordBatchIterator::new([Ok(batch2)], schema.clone());
    match dataset.append(batches2, None).await {
        Ok(_) => println!("append succeeded"),
        Err(e) => println!("append FAILED: {:?}", e),
    }

    // Verify
    let dataset = Dataset::open(&uri).await.unwrap();
    let count = dataset.count_rows(None).await.unwrap();
    println!(
        "After append: count = {}, version = {}",
        count,
        dataset.version().version
    );
    assert_eq!(count, 5);
    println!("test_lance_dataset_append: 3 → 5 rows ✅");
}

// ============================================================
// Test 4.3: Filter + Projection
// ============================================================

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_dataset_scan_with_filter() {
    let uri = goosefs_uri("filter.lance");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
    ]));

    let ids: Vec<i32> = (0..100).collect();
    let categories: Vec<String> = (0..100)
        .map(|i| {
            if i % 3 == 0 {
                "A".into()
            } else if i % 3 == 1 {
                "B".into()
            } else {
                "C".into()
            }
        })
        .collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(categories)),
        ],
    )
    .unwrap();

    let batches = RecordBatchIterator::new([Ok(batch)], schema.clone());
    Dataset::write(
        batches,
        &uri,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // count_rows with filter
    let dataset = Dataset::open(&uri).await.unwrap();
    let count_a = dataset
        .count_rows(Some("category = 'A'".into()))
        .await
        .unwrap();
    println!("count_rows(category='A') = {}", count_a);
    assert_eq!(count_a, 34); // 0,3,6,...,99 → ceil(100/3)=34

    // Scan with projection
    let mut scanner = dataset.scan();
    scanner.project(&["id"]).unwrap();
    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches[0].num_columns(), 1);
    println!("test_lance_dataset_scan_with_filter: 34 A's, projection OK ✅");
}

// ============================================================
// Test 4.4: Large write (10K rows × 128-dim vectors)
// ============================================================

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_dataset_large_write() {
    let uri = goosefs_uri("large.lance");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 128),
            false,
        ),
    ]));

    let num_rows: i32 = 10_000;
    let ids: Vec<i32> = (0..num_rows).collect();
    let embeddings: Vec<f32> = (0..num_rows * 128).map(|i| (i as f32) * 0.001).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(
                FixedSizeListArray::try_new_from_values(Float32Array::from(embeddings), 128)
                    .unwrap(),
            ),
        ],
    )
    .unwrap();

    let start = std::time::Instant::now();
    let batches = RecordBatchIterator::new([Ok(batch)], schema.clone());
    Dataset::write(
        batches,
        &uri,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    let write_time = start.elapsed();

    let start = std::time::Instant::now();
    let dataset = Dataset::open(&uri).await.unwrap();
    let count = dataset.count_rows(None).await.unwrap();
    let read_time = start.elapsed();

    println!(
        "Large write: {} rows, write={:?}, open+count={:?}",
        count, write_time, read_time
    );
    assert_eq!(count, num_rows as usize);
}

// ============================================================
// Test 4.5: Versioning
// ============================================================

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_dataset_versioning() {
    let uri = goosefs_uri("versioned.lance");

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

    // Version 1: write 3 rows
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let batches = RecordBatchIterator::new([Ok(batch1)], schema.clone());
    Dataset::write(
        batches,
        &uri,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let ds1 = Dataset::open(&uri).await.unwrap();
    assert_eq!(ds1.count_rows(None).await.unwrap(), 3);
    let v1 = ds1.version().version;
    println!("v1 = {}, rows = 3", v1);

    // Version 2: append 2 rows
    let batch2 =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![4, 5]))]).unwrap();
    let batches2 = RecordBatchIterator::new([Ok(batch2)], schema.clone());
    let mut ds_append = Dataset::open(&uri).await.unwrap();
    ds_append.append(batches2, None).await.unwrap();

    let ds2 = Dataset::open(&uri).await.unwrap();
    assert_eq!(ds2.count_rows(None).await.unwrap(), 5);
    let v2 = ds2.version().version;
    println!("v2 = {}, rows = 5", v2);
    assert!(v2 > v1);

    // Checkout v1
    let ds_v1 = ds2.checkout_version(v1).await.unwrap();
    assert_eq!(ds_v1.count_rows(None).await.unwrap(), 3);
    println!("checkout v1: rows = 3 ✅");
}

// ============================================================
// Test 4.6: storage_options
// ============================================================

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_dataset_with_storage_options() {
    use lance::dataset::builder::DatasetBuilder;

    let uri = "goosefs://127.0.0.1:9200/lance-test/datasets/opts.lance";

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
    )
    .unwrap();

    // Write
    let batches = RecordBatchIterator::new([Ok(batch)], schema.clone());
    Dataset::write(
        batches,
        uri,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Read with explicit storage_options
    let dataset = DatasetBuilder::from_uri(uri)
        .with_storage_option(STORAGE_OPT_MASTER_ADDR, "127.0.0.1:9200")
        .load()
        .await
        .unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 3);
    println!("test_lance_dataset_with_storage_options: 3 rows ✅");
}

// ============================================================
// Test 4.7: Persisted write with CACHE_THROUGH
// ============================================================

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_dataset_write_cache_through() {
    use lance::dataset::builder::DatasetBuilder;
    use lance_io::object_store::ObjectStoreParams;
    use lance_io::object_store::StorageOptionsAccessor;
    use std::collections::HashMap;

    let uri = "goosefs://127.0.0.1:9200/lance-test/datasets/cache_through.lance";

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "alice", "bob", "charlie", "david", "eve",
            ])),
            Arc::new(Float32Array::from(vec![95.5, 87.3, 91.0, 78.8, 99.1])),
        ],
    )
    .unwrap();

    // Build storage_options with write_type = CACHE_THROUGH
    let mut options = HashMap::new();
    options.insert(
        STORAGE_OPT_WRITE_TYPE.to_string(),
        WRITE_TYPE_CACHE_THROUGH.to_string(),
    );
    let accessor = Arc::new(StorageOptionsAccessor::with_static_options(options));
    let store_params = ObjectStoreParams {
        storage_options_accessor: Some(accessor),
        ..Default::default()
    };

    // Write with CACHE_THROUGH (data is written to both cache and UFS)
    let batches = RecordBatchIterator::new([Ok(batch.clone())], schema.clone());
    Dataset::write(
        batches,
        uri,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            store_params: Some(store_params.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Verify row count
    let dataset = DatasetBuilder::from_uri(uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WRITE_TYPE_CACHE_THROUGH)
        .load()
        .await
        .unwrap();
    let count = dataset.count_rows(None).await.unwrap();
    assert_eq!(count, 5);
    println!(
        "test_lance_dataset_write_cache_through: {} rows, CACHE_THROUGH persisted ✅",
        count
    );

    // Verify data round-trip via scan
    let mut stream = dataset.scan().try_into_stream().await.unwrap();
    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        total_rows += batch.num_rows();
    }
    assert_eq!(total_rows, 5);
    println!(
        "test_lance_dataset_write_cache_through: scan verified {} rows ✅",
        total_rows
    );
}

// ============================================================
// Test 4.8: Persisted write with THROUGH (direct UFS, skip cache)
// ============================================================

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_dataset_write_through() {
    use lance::dataset::builder::DatasetBuilder;
    use lance_io::object_store::ObjectStoreParams;
    use lance_io::object_store::StorageOptionsAccessor;
    use std::collections::HashMap;

    let uri = "goosefs://127.0.0.1:9200/lance-test/datasets/through.lance";

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Float32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(Float32Array::from(vec![1.1, 2.2, 3.3])),
        ],
    )
    .unwrap();

    // Build storage_options with write_type = THROUGH
    let mut options = HashMap::new();
    options.insert(
        STORAGE_OPT_WRITE_TYPE.to_string(),
        WRITE_TYPE_THROUGH.to_string(),
    );
    let accessor = Arc::new(StorageOptionsAccessor::with_static_options(options));
    let store_params = ObjectStoreParams {
        storage_options_accessor: Some(accessor),
        ..Default::default()
    };

    // Write with THROUGH (data goes directly to UFS, bypassing cache)
    let batches = RecordBatchIterator::new([Ok(batch.clone())], schema.clone());
    Dataset::write(
        batches,
        uri,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            store_params: Some(store_params.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Verify row count
    let dataset = DatasetBuilder::from_uri(uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WRITE_TYPE_THROUGH)
        .load()
        .await
        .unwrap();
    let count = dataset.count_rows(None).await.unwrap();
    assert_eq!(count, 3);
    println!(
        "test_lance_dataset_write_through: {} rows, THROUGH persisted ✅",
        count
    );
}

// ============================================================
// Test 4.9: Authentication with SIMPLE mode
// ============================================================

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_dataset_with_auth() {
    use lance::dataset::builder::DatasetBuilder;
    use lance_io::object_store::ObjectStoreParams;
    use lance_io::object_store::StorageOptionsAccessor;
    use std::collections::HashMap;

    let uri = "goosefs://127.0.0.1:9200/lance-test/datasets/auth.lance";

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])),
        ],
    )
    .unwrap();

    // Build storage_options with auth_type = simple and auth_username
    let mut options = HashMap::new();
    options.insert(STORAGE_OPT_AUTH_TYPE.to_string(), "simple".to_string());
    options.insert(
        STORAGE_OPT_AUTH_USERNAME.to_string(),
        "testuser".to_string(),
    );
    let accessor = Arc::new(StorageOptionsAccessor::with_static_options(options));
    let store_params = ObjectStoreParams {
        storage_options_accessor: Some(accessor),
        ..Default::default()
    };

    // Write with SIMPLE authentication
    let batches = RecordBatchIterator::new([Ok(batch.clone())], schema.clone());
    Dataset::write(
        batches,
        uri,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            store_params: Some(store_params.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Read with auth storage_options
    let dataset = DatasetBuilder::from_uri(uri)
        .with_storage_option(STORAGE_OPT_AUTH_TYPE, "simple")
        .with_storage_option(STORAGE_OPT_AUTH_USERNAME, "testuser")
        .load()
        .await
        .unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 3);
    println!("test_lance_dataset_with_auth: 3 rows, SIMPLE auth ✅");
}

// ============================================================
// Test 4.10: Persisted write with CACHE_THROUGH + append + versioning
// ============================================================

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_dataset_persisted_append_versioning() {
    use lance::dataset::builder::DatasetBuilder;
    use lance_io::object_store::ObjectStoreParams;
    use lance_io::object_store::StorageOptionsAccessor;
    use std::collections::HashMap;

    let uri = "goosefs://127.0.0.1:9200/lance-test/datasets/persist_append.lance";

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Utf8, false),
    ]));

    // Build storage_options with write_type = CACHE_THROUGH
    let mut options = HashMap::new();
    options.insert(
        STORAGE_OPT_WRITE_TYPE.to_string(),
        WRITE_TYPE_CACHE_THROUGH.to_string(),
    );
    let accessor = Arc::new(StorageOptionsAccessor::with_static_options(options.clone()));
    let store_params = ObjectStoreParams {
        storage_options_accessor: Some(accessor),
        ..Default::default()
    };

    // Version 1: write 3 rows with CACHE_THROUGH
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
        ],
    )
    .unwrap();

    let batches = RecordBatchIterator::new([Ok(batch1)], schema.clone());
    Dataset::write(
        batches,
        uri,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            store_params: Some(store_params.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let ds1 = DatasetBuilder::from_uri(uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WRITE_TYPE_CACHE_THROUGH)
        .load()
        .await
        .unwrap();
    assert_eq!(ds1.count_rows(None).await.unwrap(), 3);
    let v1 = ds1.version().version;
    println!("v1 = {}, rows = 3, CACHE_THROUGH ✅", v1);

    // Version 2: append 2 more rows with CACHE_THROUGH
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5])),
            Arc::new(StringArray::from(vec!["qux", "quux"])),
        ],
    )
    .unwrap();

    let mut ds_append = DatasetBuilder::from_uri(uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WRITE_TYPE_CACHE_THROUGH)
        .load()
        .await
        .unwrap();
    let batches2 = RecordBatchIterator::new([Ok(batch2)], schema.clone());
    ds_append.append(batches2, None).await.unwrap();

    // Verify v2
    let ds2 = DatasetBuilder::from_uri(uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WRITE_TYPE_CACHE_THROUGH)
        .load()
        .await
        .unwrap();
    assert_eq!(ds2.count_rows(None).await.unwrap(), 5);
    let v2 = ds2.version().version;
    println!("v2 = {}, rows = 5, CACHE_THROUGH ✅", v2);
    assert!(v2 > v1);

    // Checkout v1 to verify versioning still works with persisted data
    let ds_v1 = ds2.checkout_version(v1).await.unwrap();
    assert_eq!(ds_v1.count_rows(None).await.unwrap(), 3);
    println!(
        "test_lance_dataset_persisted_append_versioning: v1=3, v2=5, checkout v1=3, CACHE_THROUGH ✅"
    );
}
