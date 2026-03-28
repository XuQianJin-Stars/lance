//! GooseFS UFS-COSN Persistence E2E Tests (Stage 6).
//!
//! Verifies that data written through GooseFS with CACHE_THROUGH / THROUGH
//! write types is actually persisted to the underlying COS (cosn://b-1253121517).
//!
//! These tests write Lance Datasets via the goosefs:// URI, then verify:
//!   1. Data is readable back through GooseFS.
//!   2. Files are marked as PERSISTED (not NOT_PERSISTED) in GooseFS metadata,
//!      confirming UFS write-through to COSN occurred.
//!
//! Prerequisites:
//!   - GooseFS cluster running (master + worker + job_master + job_worker)
//!   - GooseFS UFS root: cosn://b-1253121517 (configured in goosefs-site.properties)
//!   - COS credentials configured in core-site.xml
//!   - Default: master at 127.0.0.1:9200; override with GOOSEFS_MASTER_ADDR env var
//!
//! Run:
//!   cargo test -p lance --features goosefs --test goosefs_ufs_cosn_e2e -- --ignored --nocapture --test-threads=1
#![cfg(feature = "goosefs")]

use std::sync::Arc;

use arrow::array::{Float32Array, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::RecordBatchIterator;
use futures::{StreamExt, TryStreamExt};
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::{WriteMode, WriteParams};
use lance_io::object_store::ObjectStoreParams;
use lance_io::object_store::StorageOptionsAccessor;
use std::collections::HashMap;

// ── Storage option keys ──
const STORAGE_OPT_WRITE_TYPE: &str = "goosefs_write_type";
const WRITE_TYPE_CACHE_THROUGH: &str = "cache_through";
const WRITE_TYPE_THROUGH: &str = "through";

/// Generate a unique GooseFS URI for each test run (timestamp-based).
fn goosefs_ufs_uri(suffix: &str) -> String {
    let addr = std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".into());
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("goosefs://{}/lance-test/ufs-cosn/{}_{}", addr, suffix, ts)
}

/// Build ObjectStoreParams with the specified write_type.
fn store_params_with_write_type(write_type: &str) -> ObjectStoreParams {
    let mut options = HashMap::new();
    options.insert(STORAGE_OPT_WRITE_TYPE.to_string(), write_type.to_string());
    let accessor = Arc::new(StorageOptionsAccessor::with_static_options(options));
    ObjectStoreParams {
        storage_options_accessor: Some(accessor),
        ..Default::default()
    }
}

/// Create a sample schema: (id: Int32, name: Utf8, score: Float32).
fn sample_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float32, false),
    ]))
}

/// Create a sample RecordBatch with `n` rows.
fn sample_batch(schema: &Arc<Schema>, start_id: i32, n: usize) -> RecordBatch {
    let ids: Vec<i32> = (start_id..start_id + n as i32).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("item_{}", i)).collect();
    let scores: Vec<f32> = ids.iter().map(|i| *i as f32 * 1.5).collect();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float32Array::from(scores)),
        ],
    )
    .unwrap()
}

/// Check GooseFS file persistence status via CLI.
/// Returns true if the path shows "PERSISTED" (not "NOT_PERSISTED").
fn check_persisted_via_cli(goosefs_path: &str) -> bool {
    let goosefs_home =
        std::env::var("GOOSEFS_HOME").unwrap_or_else(|_| "/opt/sourcecode/cos/goosefs".into());
    let output = std::process::Command::new(format!("{}/bin/goosefs", goosefs_home))
        .args(["fs", "ls", "-R", goosefs_path])
        .output();

    match output {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            let stderr = String::from_utf8_lossy(&out.stderr);
            if !out.status.success() {
                println!(
                    "[UFS-check] CLI failed for '{}': stderr={}",
                    goosefs_path, stderr
                );
                return false;
            }
            // Check if any file lines contain "PERSISTED" (without "NOT_PERSISTED")
            let has_persisted = stdout.lines().any(|line| {
                // A line with PERSISTED but not NOT_PERSISTED
                line.contains("PERSISTED") && !line.contains("NOT_PERSISTED")
            });
            let has_not_persisted = stdout.lines().any(|line| line.contains("NOT_PERSISTED"));
            println!(
                "[UFS-check] path='{}': has_persisted={}, has_not_persisted={}",
                goosefs_path, has_persisted, has_not_persisted
            );
            if has_not_persisted {
                // Print the NOT_PERSISTED lines for debugging
                for line in stdout.lines() {
                    if line.contains("NOT_PERSISTED") {
                        println!("[UFS-check]   NOT_PERSISTED: {}", line.trim());
                    }
                }
            }
            has_persisted
        }
        Err(e) => {
            println!("[UFS-check] Failed to run goosefs CLI: {}", e);
            false
        }
    }
}

/// Extract the GooseFS path from a goosefs:// URI.
/// e.g., "goosefs://127.0.0.1:9200/lance-test/foo" → "/lance-test/foo"
fn uri_to_goosefs_path(uri: &str) -> String {
    let url = url::Url::parse(uri).expect("Invalid URI");
    url.path().to_string()
}

// ============================================================
// Test 6.1: CACHE_THROUGH write → verify PERSISTED in UFS (COSN)
// ============================================================

#[ignore = "Requires GooseFS cluster with COSN UFS"]
#[tokio::test]
async fn test_6_1_cache_through_write_persisted_to_cosn() {
    let uri = goosefs_ufs_uri("cache_through_basic.lance");
    let schema = sample_schema();
    let batch = sample_batch(&schema, 1, 10);
    let store_params = store_params_with_write_type(WRITE_TYPE_CACHE_THROUGH);

    println!("[6.1] Writing 10 rows with CACHE_THROUGH to: {}", uri);

    // Write with CACHE_THROUGH
    let batches = RecordBatchIterator::new([Ok(batch)], schema.clone());
    Dataset::write(
        batches,
        &uri,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            store_params: Some(store_params.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Read back and verify
    let dataset = DatasetBuilder::from_uri(&uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WRITE_TYPE_CACHE_THROUGH)
        .load()
        .await
        .unwrap();
    let count = dataset.count_rows(None).await.unwrap();
    assert_eq!(count, 10, "Expected 10 rows");
    println!("[6.1] Read back {} rows ✅", count);

    // Verify PERSISTED status via GooseFS CLI
    let goosefs_path = uri_to_goosefs_path(&uri);
    let is_persisted = check_persisted_via_cli(&goosefs_path);
    assert!(
        is_persisted,
        "Files should be PERSISTED (written to COSN UFS) with CACHE_THROUGH mode"
    );
    println!("[6.1] Files are PERSISTED to COSN UFS ✅");

    println!("test_6_1_cache_through_write_persisted_to_cosn: PASSED ✅");
}

// ============================================================
// Test 6.2: THROUGH write → verify PERSISTED in UFS (COSN)
// ============================================================

#[ignore = "Requires GooseFS cluster with COSN UFS"]
#[tokio::test]
async fn test_6_2_through_write_persisted_to_cosn() {
    let uri = goosefs_ufs_uri("through_basic.lance");
    let schema = sample_schema();
    let batch = sample_batch(&schema, 1, 5);
    let store_params = store_params_with_write_type(WRITE_TYPE_THROUGH);

    println!("[6.2] Writing 5 rows with THROUGH to: {}", uri);

    // Write with THROUGH (directly to UFS, bypassing cache)
    let batches = RecordBatchIterator::new([Ok(batch)], schema.clone());
    Dataset::write(
        batches,
        &uri,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            store_params: Some(store_params.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Read back and verify
    let dataset = DatasetBuilder::from_uri(&uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WRITE_TYPE_THROUGH)
        .load()
        .await
        .unwrap();
    let count = dataset.count_rows(None).await.unwrap();
    assert_eq!(count, 5, "Expected 5 rows");
    println!("[6.2] Read back {} rows ✅", count);

    // Verify PERSISTED status
    let goosefs_path = uri_to_goosefs_path(&uri);
    let is_persisted = check_persisted_via_cli(&goosefs_path);
    assert!(
        is_persisted,
        "Files should be PERSISTED (written to COSN UFS) with THROUGH mode"
    );
    println!("[6.2] Files are PERSISTED to COSN UFS ✅");

    println!("test_6_2_through_write_persisted_to_cosn: PASSED ✅");
}

// ============================================================
// Test 6.3: CACHE_THROUGH append → both versions persisted to COSN
// ============================================================

#[ignore = "Requires GooseFS cluster with COSN UFS"]
#[tokio::test]
async fn test_6_3_cache_through_append_persisted() {
    let uri = goosefs_ufs_uri("ct_append.lance");
    let schema = sample_schema();
    let store_params = store_params_with_write_type(WRITE_TYPE_CACHE_THROUGH);

    println!("[6.3] Writing initial 5 rows with CACHE_THROUGH: {}", uri);

    // Version 1: write 5 rows
    let batch1 = sample_batch(&schema, 1, 5);
    let batches = RecordBatchIterator::new([Ok(batch1)], schema.clone());
    Dataset::write(
        batches,
        &uri,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            store_params: Some(store_params.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let ds1 = DatasetBuilder::from_uri(&uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WRITE_TYPE_CACHE_THROUGH)
        .load()
        .await
        .unwrap();
    assert_eq!(ds1.count_rows(None).await.unwrap(), 5);
    let v1 = ds1.version().version;
    println!("[6.3] v1={}, 5 rows ✅", v1);

    // Version 2: append 3 rows
    let batch2 = sample_batch(&schema, 100, 3);
    let batches2 = RecordBatchIterator::new([Ok(batch2)], schema.clone());
    let mut ds_append = DatasetBuilder::from_uri(&uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WRITE_TYPE_CACHE_THROUGH)
        .load()
        .await
        .unwrap();
    ds_append.append(batches2, None).await.unwrap();

    let ds2 = DatasetBuilder::from_uri(&uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WRITE_TYPE_CACHE_THROUGH)
        .load()
        .await
        .unwrap();
    assert_eq!(ds2.count_rows(None).await.unwrap(), 8);
    let v2 = ds2.version().version;
    println!("[6.3] v2={}, 8 rows ✅", v2);
    assert!(v2 > v1);

    // Verify PERSISTED
    let goosefs_path = uri_to_goosefs_path(&uri);
    let is_persisted = check_persisted_via_cli(&goosefs_path);
    assert!(
        is_persisted,
        "Files should be PERSISTED after CACHE_THROUGH append"
    );
    println!("[6.3] Append data PERSISTED to COSN UFS ✅");

    println!("test_6_3_cache_through_append_persisted: PASSED ✅");
}

// ============================================================
// Test 6.4: Data scan integrity after CACHE_THROUGH write
// ============================================================

#[ignore = "Requires GooseFS cluster with COSN UFS"]
#[tokio::test]
async fn test_6_4_cache_through_scan_integrity() {
    let uri = goosefs_ufs_uri("ct_scan.lance");
    let schema = sample_schema();
    let store_params = store_params_with_write_type(WRITE_TYPE_CACHE_THROUGH);

    // Write 20 rows
    let batch = sample_batch(&schema, 1, 20);
    let batches = RecordBatchIterator::new([Ok(batch)], schema.clone());
    Dataset::write(
        batches,
        &uri,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            store_params: Some(store_params.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    println!("[6.4] Wrote 20 rows with CACHE_THROUGH to: {}", uri);

    // Full scan and verify each row
    let dataset = DatasetBuilder::from_uri(&uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WRITE_TYPE_CACHE_THROUGH)
        .load()
        .await
        .unwrap();

    let mut stream = dataset.scan().try_into_stream().await.unwrap();
    let mut total_rows = 0;
    let mut all_ids: Vec<i32> = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        total_rows += batch.num_rows();
        let id_col = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..id_col.len() {
            all_ids.push(id_col.value(i));
        }
    }
    assert_eq!(total_rows, 20, "Expected 20 rows from scan");
    all_ids.sort();
    let expected_ids: Vec<i32> = (1..=20).collect();
    assert_eq!(all_ids, expected_ids, "IDs should match 1..=20");
    println!("[6.4] Scan verified 20 rows with correct IDs ✅");

    // Verify persisted
    let goosefs_path = uri_to_goosefs_path(&uri);
    let is_persisted = check_persisted_via_cli(&goosefs_path);
    assert!(is_persisted, "Data should be PERSISTED to COSN");
    println!("[6.4] Data PERSISTED to COSN UFS ✅");

    println!("test_6_4_cache_through_scan_integrity: PASSED ✅");
}

// ============================================================
// Test 6.5: Filter query after CACHE_THROUGH write
// ============================================================

#[ignore = "Requires GooseFS cluster with COSN UFS"]
#[tokio::test]
async fn test_6_5_cache_through_filter_query() {
    let uri = goosefs_ufs_uri("ct_filter.lance");
    let schema = sample_schema();
    let store_params = store_params_with_write_type(WRITE_TYPE_CACHE_THROUGH);

    // Write 50 rows with varying scores
    let n = 50;
    let ids: Vec<i32> = (1..=n).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("user_{}", i)).collect();
    let scores: Vec<f32> = ids.iter().map(|i| *i as f32 * 2.0).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float32Array::from(scores)),
        ],
    )
    .unwrap();

    let batches = RecordBatchIterator::new([Ok(batch)], schema.clone());
    Dataset::write(
        batches,
        &uri,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            store_params: Some(store_params.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    println!("[6.5] Wrote 50 rows with CACHE_THROUGH to: {}", uri);

    // Filter: score > 50.0 → ids 26..50 → 25 rows
    let dataset = DatasetBuilder::from_uri(&uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WRITE_TYPE_CACHE_THROUGH)
        .load()
        .await
        .unwrap();
    let count = dataset
        .count_rows(Some("score > 50.0".into()))
        .await
        .unwrap();
    println!("[6.5] count_rows(score > 50.0) = {}", count);
    assert_eq!(count, 25, "Expected 25 rows with score > 50.0");

    // Projection: only id column
    let mut scanner = dataset.scan();
    scanner.project(&["id"]).unwrap();
    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        batches[0].num_columns(),
        1,
        "Projection should yield 1 column"
    );
    println!("[6.5] Filter + projection on CACHE_THROUGH data ✅");

    println!("test_6_5_cache_through_filter_query: PASSED ✅");
}

// ============================================================
// Test 6.6: Versioning with CACHE_THROUGH persisted to COSN
// ============================================================

#[ignore = "Requires GooseFS cluster with COSN UFS"]
#[tokio::test]
async fn test_6_6_cache_through_versioning_cosn() {
    let uri = goosefs_ufs_uri("ct_versioning.lance");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let store_params = store_params_with_write_type(WRITE_TYPE_CACHE_THROUGH);

    // Version 1: 3 rows
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
            store_params: Some(store_params.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let ds1 = DatasetBuilder::from_uri(&uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WRITE_TYPE_CACHE_THROUGH)
        .load()
        .await
        .unwrap();
    assert_eq!(ds1.count_rows(None).await.unwrap(), 3);
    let v1 = ds1.version().version;
    println!("[6.6] v1={}, 3 rows", v1);

    // Version 2: append 2 rows
    let batch2 =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![4, 5]))]).unwrap();
    let batches2 = RecordBatchIterator::new([Ok(batch2)], schema.clone());
    let mut ds_append = DatasetBuilder::from_uri(&uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WRITE_TYPE_CACHE_THROUGH)
        .load()
        .await
        .unwrap();
    ds_append.append(batches2, None).await.unwrap();

    let ds2 = DatasetBuilder::from_uri(&uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WRITE_TYPE_CACHE_THROUGH)
        .load()
        .await
        .unwrap();
    assert_eq!(ds2.count_rows(None).await.unwrap(), 5);
    let v2 = ds2.version().version;
    println!("[6.6] v2={}, 5 rows", v2);
    assert!(v2 > v1);

    // Checkout v1 — should still read 3 rows
    let ds_v1 = ds2.checkout_version(v1).await.unwrap();
    assert_eq!(ds_v1.count_rows(None).await.unwrap(), 3);
    println!("[6.6] checkout v1: 3 rows ✅");

    // Verify persisted
    let goosefs_path = uri_to_goosefs_path(&uri);
    let is_persisted = check_persisted_via_cli(&goosefs_path);
    assert!(is_persisted, "Both versions should be PERSISTED to COSN");
    println!("[6.6] Versioned data PERSISTED to COSN UFS ✅");

    println!("test_6_6_cache_through_versioning_cosn: PASSED ✅");
}

// ============================================================
// Test 6.7: Large dataset CACHE_THROUGH write to COSN
// ============================================================

#[ignore = "Requires GooseFS cluster with COSN UFS"]
#[tokio::test]
async fn test_6_7_large_dataset_cache_through_cosn() {
    let uri = goosefs_ufs_uri("ct_large.lance");
    let schema = sample_schema();
    let store_params = store_params_with_write_type(WRITE_TYPE_CACHE_THROUGH);

    let num_rows = 1000;
    let ids: Vec<i32> = (0..num_rows).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("record_{}", i)).collect();
    let scores: Vec<f32> = ids.iter().map(|i| *i as f32 * 0.1).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float32Array::from(scores)),
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
            store_params: Some(store_params.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    let write_time = start.elapsed();
    println!(
        "[6.7] Wrote {} rows with CACHE_THROUGH in {:?}",
        num_rows, write_time
    );

    // Read back
    let start = std::time::Instant::now();
    let dataset = DatasetBuilder::from_uri(&uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WRITE_TYPE_CACHE_THROUGH)
        .load()
        .await
        .unwrap();
    let count = dataset.count_rows(None).await.unwrap();
    let read_time = start.elapsed();
    assert_eq!(count, num_rows as usize);
    println!("[6.7] Read {} rows in {:?} ✅", count, read_time);

    // Verify persisted
    let goosefs_path = uri_to_goosefs_path(&uri);
    let is_persisted = check_persisted_via_cli(&goosefs_path);
    assert!(is_persisted, "Large dataset should be PERSISTED to COSN");
    println!("[6.7] 1000-row dataset PERSISTED to COSN UFS ✅");

    println!("test_6_7_large_dataset_cache_through_cosn: PASSED ✅");
}

// ============================================================
// Test 6.8: Overwrite with CACHE_THROUGH → new data persisted
// ============================================================

#[ignore = "Requires GooseFS cluster with COSN UFS"]
#[tokio::test]
async fn test_6_8_overwrite_cache_through_cosn() {
    let uri = goosefs_ufs_uri("ct_overwrite.lance");
    let schema = sample_schema();
    let store_params = store_params_with_write_type(WRITE_TYPE_CACHE_THROUGH);

    // First write: 5 rows
    let batch1 = sample_batch(&schema, 1, 5);
    let batches1 = RecordBatchIterator::new([Ok(batch1)], schema.clone());
    Dataset::write(
        batches1,
        &uri,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            store_params: Some(store_params.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let ds1 = DatasetBuilder::from_uri(&uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WRITE_TYPE_CACHE_THROUGH)
        .load()
        .await
        .unwrap();
    assert_eq!(ds1.count_rows(None).await.unwrap(), 5);
    println!("[6.8] First write: 5 rows");

    // Overwrite with 10 rows
    let batch2 = sample_batch(&schema, 100, 10);
    let batches2 = RecordBatchIterator::new([Ok(batch2)], schema.clone());
    Dataset::write(
        batches2,
        &uri,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            store_params: Some(store_params.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let ds2 = DatasetBuilder::from_uri(&uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WRITE_TYPE_CACHE_THROUGH)
        .load()
        .await
        .unwrap();
    let count = ds2.count_rows(None).await.unwrap();
    assert_eq!(count, 10, "After overwrite should have 10 rows");
    println!("[6.8] Overwrite: 10 rows ✅");

    // Verify IDs are from the second batch (100..110)
    let mut stream = ds2.scan().try_into_stream().await.unwrap();
    let mut all_ids: Vec<i32> = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        let id_col = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..id_col.len() {
            all_ids.push(id_col.value(i));
        }
    }
    all_ids.sort();
    let expected_ids: Vec<i32> = (100..110).collect();
    assert_eq!(all_ids, expected_ids, "IDs should be 100..110");
    println!("[6.8] Overwrite data verified: IDs 100..110 ✅");

    // Verify persisted
    let goosefs_path = uri_to_goosefs_path(&uri);
    let is_persisted = check_persisted_via_cli(&goosefs_path);
    assert!(is_persisted, "Overwritten data should be PERSISTED to COSN");
    println!("[6.8] Overwritten data PERSISTED to COSN UFS ✅");

    println!("test_6_8_overwrite_cache_through_cosn: PASSED ✅");
}

// ============================================================
// Test 6.9: MUST_CACHE vs CACHE_THROUGH comparison (NOT_PERSISTED vs PERSISTED)
// ============================================================

#[ignore = "Requires GooseFS cluster with COSN UFS"]
#[tokio::test]
async fn test_6_9_must_cache_vs_cache_through() {
    let schema = sample_schema();

    // Write with default (no write_type → MUST_CACHE)
    let uri_must_cache = goosefs_ufs_uri("must_cache.lance");
    let batch_mc = sample_batch(&schema, 1, 3);
    let batches_mc = RecordBatchIterator::new([Ok(batch_mc)], schema.clone());
    Dataset::write(
        batches_mc,
        &uri_must_cache,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    println!(
        "[6.9] Wrote 3 rows with default (MUST_CACHE) to: {}",
        uri_must_cache
    );

    // Write with CACHE_THROUGH
    let uri_cache_through = goosefs_ufs_uri("cache_through.lance");
    let batch_ct = sample_batch(&schema, 1, 3);
    let store_params_ct = store_params_with_write_type(WRITE_TYPE_CACHE_THROUGH);
    let batches_ct = RecordBatchIterator::new([Ok(batch_ct)], schema.clone());
    Dataset::write(
        batches_ct,
        &uri_cache_through,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            store_params: Some(store_params_ct.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    println!(
        "[6.9] Wrote 3 rows with CACHE_THROUGH to: {}",
        uri_cache_through
    );

    // Both should be readable
    let ds_mc = Dataset::open(&uri_must_cache).await.unwrap();
    assert_eq!(ds_mc.count_rows(None).await.unwrap(), 3);
    let ds_ct = DatasetBuilder::from_uri(&uri_cache_through)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WRITE_TYPE_CACHE_THROUGH)
        .load()
        .await
        .unwrap();
    assert_eq!(ds_ct.count_rows(None).await.unwrap(), 3);
    println!("[6.9] Both datasets readable with 3 rows ✅");

    // MUST_CACHE should show NOT_PERSISTED
    let mc_path = uri_to_goosefs_path(&uri_must_cache);
    let mc_persisted = check_persisted_via_cli(&mc_path);
    println!(
        "[6.9] MUST_CACHE persisted={} (expected: false)",
        mc_persisted
    );

    // CACHE_THROUGH should show PERSISTED
    let ct_path = uri_to_goosefs_path(&uri_cache_through);
    let ct_persisted = check_persisted_via_cli(&ct_path);
    println!(
        "[6.9] CACHE_THROUGH persisted={} (expected: true)",
        ct_persisted
    );
    assert!(ct_persisted, "CACHE_THROUGH data must be PERSISTED to COSN");
    // MUST_CACHE should NOT be persisted (only in cache)
    assert!(
        !mc_persisted,
        "MUST_CACHE data should NOT be persisted to COSN"
    );

    println!("test_6_9_must_cache_vs_cache_through: PASSED ✅");
}

// ============================================================
// Test 6.10: End-to-end UFS verification — write, persist check, read from fresh connection
// ============================================================

#[ignore = "Requires GooseFS cluster with COSN UFS"]
#[tokio::test]
async fn test_6_10_full_ufs_cosn_roundtrip() {
    let uri = goosefs_ufs_uri("full_roundtrip.lance");
    let schema = sample_schema();
    let store_params = store_params_with_write_type(WRITE_TYPE_CACHE_THROUGH);

    // Step 1: Write data with CACHE_THROUGH
    let batch = sample_batch(&schema, 1, 15);
    let batches = RecordBatchIterator::new([Ok(batch)], schema.clone());
    Dataset::write(
        batches,
        &uri,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            store_params: Some(store_params.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    println!("[6.10] Step 1: Wrote 15 rows with CACHE_THROUGH");

    // Step 2: Verify persisted
    let goosefs_path = uri_to_goosefs_path(&uri);
    let is_persisted = check_persisted_via_cli(&goosefs_path);
    assert!(is_persisted, "Data should be PERSISTED to COSN");
    println!("[6.10] Step 2: PERSISTED check ✅");

    // Step 3: Open from a fresh DatasetBuilder (simulates a new client session)
    let dataset = DatasetBuilder::from_uri(&uri)
        .with_storage_option(STORAGE_OPT_WRITE_TYPE, WRITE_TYPE_CACHE_THROUGH)
        .load()
        .await
        .unwrap();
    let count = dataset.count_rows(None).await.unwrap();
    assert_eq!(count, 15);
    println!("[6.10] Step 3: Fresh read: {} rows ✅", count);

    // Step 4: Full scan verification
    let mut stream = dataset.scan().try_into_stream().await.unwrap();
    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        total_rows += batch.num_rows();
        // Verify schema has 3 fields
        assert_eq!(
            batch.num_columns(),
            3,
            "Expected 3 columns (id, name, score)"
        );
    }
    assert_eq!(total_rows, 15);
    println!(
        "[6.10] Step 4: Scan verified {} rows, 3 columns ✅",
        total_rows
    );

    // Step 5: List files in the GooseFS path and print persistence status
    let goosefs_home =
        std::env::var("GOOSEFS_HOME").unwrap_or_else(|_| "/opt/sourcecode/cos/goosefs".into());
    let output = std::process::Command::new(format!("{}/bin/goosefs", goosefs_home))
        .args(["fs", "ls", "-R", &goosefs_path])
        .output();
    if let Ok(out) = output {
        let stdout = String::from_utf8_lossy(&out.stdout);
        println!("[6.10] Step 5: File listing:");
        for line in stdout.lines().take(20) {
            println!("  {}", line);
        }
    }

    println!("test_6_10_full_ufs_cosn_roundtrip: PASSED ✅");
}
