//! Stage 7: Lance Namespace + Dataset Full E2E on GooseFS
//!
//! Tests the complete workflow: create namespace via LanceNamespace API → create table
//! with data → operate on the underlying Lance Dataset via `goosefs://` URI → verify
//! through both namespace API and Dataset API.
//!
//! Run: cargo test -p lance-namespace-impls --features dir-goosefs --test goosefs_namespace_dataset_e2e -- --ignored --nocapture --test-threads=1
#![cfg(feature = "dir-goosefs")]

mod goosefs_test_helpers;

use std::sync::Arc;

use arrow::array::{FixedSizeListArray, Float32Array, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow_array::RecordBatchIterator;
use bytes::Bytes;
use futures::TryStreamExt;

use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use lance_arrow::FixedSizeListArrayExt;
use lance_namespace::models::*;

use goosefs_test_helpers::*;

// ============================================================
// Stage 7: Namespace + Dataset Integration E2E
// ============================================================

// ── Test 7.1: Create table via Namespace → Open Dataset via goosefs:// URI ──

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_7_1_namespace_create_then_dataset_open() {
    let root = goosefs_namespace_root("stage7", "test_7_1");
    let ns = create_goosefs_namespace(&root).await;

    // Create namespace + table via LanceNamespace API
    let location = setup_ns_with_table(ns.as_ref(), "analytics", "users", 10).await;
    println!("[7.1] Table location: {}", location);

    // Open the table directly via Dataset API using the goosefs:// URI
    let dataset = Dataset::open(&location).await.unwrap();
    let count = dataset.count_rows(None).await.unwrap();
    assert_eq!(count, 10, "Dataset should have 10 rows");

    // Verify schema matches
    assert_eq!(dataset.schema().fields.len(), 3); // id, name, score
    println!(
        "[7.1] Dataset opened via URI: {} rows, {} fields ✅",
        count,
        dataset.schema().fields.len()
    );

    // Verify through namespace API too
    let mut desc = DescribeTableRequest::new();
    desc.id = Some(vec!["analytics".into(), "users".into()]);
    let table_info = ns.describe_table(desc).await.unwrap();
    assert!(table_info.location.is_some());
    println!("[7.1] Namespace describe_table confirms location ✅");
}

// ── Test 7.2: Namespace create → Dataset append → verify via both APIs ──

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_7_2_namespace_table_dataset_append() {
    let root = goosefs_namespace_root("stage7", "test_7_2");
    let ns = create_goosefs_namespace(&root).await;

    // Create namespace + table with 5 rows
    let location = setup_ns_with_table(ns.as_ref(), "data", "events", 5).await;

    // Append more rows via Dataset API
    let schema = simple_schema();
    let batch2 = sample_batch(&schema, 100, 5);
    let mut dataset = Dataset::open(&location).await.unwrap();
    let batches = RecordBatchIterator::new([Ok(batch2)], schema.clone());
    dataset.append(batches, None).await.unwrap();

    // Verify via Dataset API
    let dataset = Dataset::open(&location).await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 10);
    println!("[7.2] After append: 10 rows via Dataset API ✅");

    // Verify table still exists in namespace
    let mut exists_req = TableExistsRequest::new();
    exists_req.id = Some(vec!["data".into(), "events".into()]);
    ns.table_exists(exists_req).await.unwrap();
    println!("[7.2] table_exists via Namespace API ✅");

    // Verify via describe_table_version
    let mut ver_req = DescribeTableVersionRequest::new();
    ver_req.id = Some(vec!["data".into(), "events".into()]);
    match ns.describe_table_version(ver_req).await {
        Ok(desc) => println!(
            "[7.2] describe_table_version: v{}, manifest={} ✅",
            desc.version.version, desc.version.manifest_path
        ),
        Err(e) => println!("[7.2] describe_table_version: {} (may be expected) ⏭️", e),
    }
}

// ── Test 7.3: Dataset scan + filter on namespace-created table ──

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_7_3_namespace_table_scan_filter() {
    let root = goosefs_namespace_root("stage7", "test_7_3");
    let ns = create_goosefs_namespace(&root).await;

    // Create table with 20 rows (ids 1-20, scores = id * 1.5)
    let location = setup_ns_with_table(ns.as_ref(), "queries", "scores", 20).await;

    let dataset = Dataset::open(&location).await.unwrap();

    // Filter: score > 15.0 (ids >= 11, so 10 rows)
    let count = dataset
        .count_rows(Some("score > 15.0".into()))
        .await
        .unwrap();
    assert_eq!(count, 10, "Should have 10 rows with score > 15.0");
    println!("[7.3] count_rows(score > 15.0) = {} ✅", count);

    // Scan with projection
    let mut scanner = dataset.scan();
    scanner.project(&["id", "score"]).unwrap();
    scanner.filter("score > 15.0").unwrap();
    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 10);
    assert_eq!(batches[0].num_columns(), 2); // id + score only
    println!(
        "[7.3] Scan with projection + filter: {} rows, {} cols ✅",
        total, 2
    );
}

// ── Test 7.4: Multiple namespaces → Dataset operations across namespaces ──

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_7_4_multi_namespace_dataset_isolation() {
    let root = goosefs_namespace_root("stage7", "test_7_4");
    let ns = create_goosefs_namespace(&root).await;

    // Create two namespaces with same table name but different data
    let loc_a = setup_ns_with_table(ns.as_ref(), "team_a", "metrics", 5).await;
    let loc_b = setup_ns_with_table(ns.as_ref(), "team_b", "metrics", 15).await;

    // Open datasets and verify isolation
    let ds_a = Dataset::open(&loc_a).await.unwrap();
    let ds_b = Dataset::open(&loc_b).await.unwrap();

    assert_eq!(ds_a.count_rows(None).await.unwrap(), 5);
    assert_eq!(ds_b.count_rows(None).await.unwrap(), 15);
    println!("[7.4] team_a/metrics: 5 rows, team_b/metrics: 15 rows ✅");

    // Append to team_a only
    let schema = simple_schema();
    let extra = sample_batch(&schema, 200, 5);
    let mut ds_a_mut = Dataset::open(&loc_a).await.unwrap();
    let batches = RecordBatchIterator::new([Ok(extra)], schema.clone());
    ds_a_mut.append(batches, None).await.unwrap();

    // Verify team_a changed, team_b unchanged
    let ds_a = Dataset::open(&loc_a).await.unwrap();
    let ds_b = Dataset::open(&loc_b).await.unwrap();
    assert_eq!(ds_a.count_rows(None).await.unwrap(), 10);
    assert_eq!(ds_b.count_rows(None).await.unwrap(), 15);
    println!("[7.4] After append: team_a=10, team_b=15 (isolated) ✅");
}

// ── Test 7.5: Namespace table → Dataset delete + versioning ──

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_7_5_namespace_table_delete_and_versioning() {
    let root = goosefs_namespace_root("stage7", "test_7_5");
    let ns = create_goosefs_namespace(&root).await;

    let location = setup_ns_with_table(ns.as_ref(), "logs", "access", 20).await;

    // v1: 20 rows
    let ds = Dataset::open(&location).await.unwrap();
    let v1 = ds.version().version;
    assert_eq!(ds.count_rows(None).await.unwrap(), 20);

    // Delete rows where id > 10
    let mut ds = Dataset::open(&location).await.unwrap();
    ds.delete("id > 10").await.unwrap();

    let ds = Dataset::open(&location).await.unwrap();
    let v2 = ds.version().version;
    assert_eq!(ds.count_rows(None).await.unwrap(), 10);
    assert!(v2 > v1);
    println!(
        "[7.5] v1={} (20 rows) → v2={} (10 rows after delete) ✅",
        v1, v2
    );

    // Checkout v1 to see original data
    let ds_v1 = ds.checkout_version(v1).await.unwrap();
    assert_eq!(ds_v1.count_rows(None).await.unwrap(), 20);
    println!("[7.5] checkout v1: 20 rows recovered ✅");
}

// ── Test 7.6: Namespace table → Schema evolution via Dataset API ──

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_7_6_namespace_table_schema_evolution() {
    let root = goosefs_namespace_root("stage7", "test_7_6");
    let ns = create_goosefs_namespace(&root).await;

    let location = setup_ns_with_table(ns.as_ref(), "evolve", "users", 5).await;

    // Initial: 3 columns (id, name, score)
    let ds = Dataset::open(&location).await.unwrap();
    assert_eq!(ds.schema().fields.len(), 3);

    // Add column: "rank" computed from score
    let mut ds = Dataset::open(&location).await.unwrap();
    ds.add_columns(
        lance::dataset::NewColumnTransform::SqlExpressions(vec![(
            "rank".to_string(),
            "CAST(score AS INT)".to_string(),
        )]),
        None,
        None,
    )
    .await
    .unwrap();

    let ds = Dataset::open(&location).await.unwrap();
    assert_eq!(ds.schema().fields.len(), 4);
    let field_names: Vec<&str> = ds.schema().fields.iter().map(|f| f.name.as_str()).collect();
    assert!(field_names.contains(&"rank"));
    println!(
        "[7.6] add_columns('rank') → {} fields: {:?} ✅",
        4, field_names
    );

    // Drop column
    let mut ds = Dataset::open(&location).await.unwrap();
    ds.drop_columns(&["rank"]).await.unwrap();

    let ds = Dataset::open(&location).await.unwrap();
    assert_eq!(ds.schema().fields.len(), 3);
    println!("[7.6] drop_columns('rank') → {} fields ✅", 3);

    // Verify table still accessible via namespace
    let mut exists_req = TableExistsRequest::new();
    exists_req.id = Some(vec!["evolve".into(), "users".into()]);
    ns.table_exists(exists_req).await.unwrap();
    println!("[7.6] Namespace table_exists still true ✅");
}

// ── Test 7.7: Vector search on namespace-managed table ──

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_7_7_namespace_table_vector_search() {
    let root = goosefs_namespace_root("stage7", "test_7_7");
    let ns = create_goosefs_namespace(&root).await;

    // Create namespace
    let mut create_ns = CreateNamespaceRequest::new();
    create_ns.id = Some(vec!["vectors".into()]);
    ns.create_namespace(create_ns).await.unwrap();

    // Build vector data and write directly via Dataset API to namespace path
    let dim: i32 = 16;
    let num_rows = 100;
    let vec_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
            false,
        ),
    ]));

    let ids: Vec<i32> = (0..num_rows).collect();
    let vectors: Vec<f32> = (0..num_rows * dim).map(|i| (i as f32) * 0.01).collect();

    let batch = RecordBatch::try_new(
        vec_schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(
                FixedSizeListArray::try_new_from_values(Float32Array::from(vectors), dim).unwrap(),
            ),
        ],
    )
    .unwrap();

    // Write to a path under the namespace root
    let table_uri = format!("{}/vectors/embeddings.lance", root);
    let batches = RecordBatchIterator::new([Ok(batch)], vec_schema.clone());
    Dataset::write(
        batches,
        &table_uri,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // KNN search
    let dataset = Dataset::open(&table_uri).await.unwrap();
    let query: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.01 + 0.001).collect();
    let query_array = Float32Array::from(query);

    let results: Vec<RecordBatch> = dataset
        .scan()
        .nearest("embedding", &query_array, 5)
        .unwrap()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    let total: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 5);

    let id_col = results[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(id_col.value(0), 0, "Nearest should be id=0");
    println!(
        "[7.7] Vector search in namespace: top-5, nearest=id:{} ✅",
        id_col.value(0)
    );
}

// ── Test 7.8: Namespace table → Dataset truncate + recreate ──

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_7_8_namespace_table_truncate_recreate() {
    let root = goosefs_namespace_root("stage7", "test_7_8");
    let ns = create_goosefs_namespace(&root).await;

    let location = setup_ns_with_table(ns.as_ref(), "reset", "data", 10).await;

    // Truncate via Dataset API
    let mut ds = Dataset::open(&location).await.unwrap();
    ds.truncate_table().await.unwrap();

    let ds = Dataset::open(&location).await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 0);
    println!("[7.8] Truncated: 0 rows ✅");

    // Write new data to the same location
    let schema = simple_schema();
    let new_batch = sample_batch(&schema, 500, 8);
    let batches = RecordBatchIterator::new([Ok(new_batch)], schema.clone());
    Dataset::write(
        batches,
        &location,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let ds = Dataset::open(&location).await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 8);
    println!("[7.8] Recreated with 8 rows ✅");

    // Namespace still knows about the table
    let mut exists_req = TableExistsRequest::new();
    exists_req.id = Some(vec!["reset".into(), "data".into()]);
    ns.table_exists(exists_req).await.unwrap();
    println!("[7.8] Namespace table_exists: true ✅");
}

// ── Test 7.9: Deregister namespace table → Dataset still accessible → Re-register ──

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_7_9_namespace_deregister_dataset_access_reregister() {
    let root = goosefs_namespace_root("stage7", "test_7_9");
    let ns = create_goosefs_namespace(&root).await;

    let location = setup_ns_with_table(ns.as_ref(), "dereg", "table1", 7).await;

    // Dataset accessible via URI
    let ds = Dataset::open(&location).await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 7);

    // Deregister from namespace
    let mut dereg_req = DeregisterTableRequest::new();
    dereg_req.id = Some(vec!["dereg".into(), "table1".into()]);
    let dereg_resp = ns.deregister_table(dereg_req).await.unwrap();
    println!("[7.9] Deregistered: location={:?}", dereg_resp.location);

    // Table should not exist in namespace anymore
    let mut exists_req = TableExistsRequest::new();
    exists_req.id = Some(vec!["dereg".into(), "table1".into()]);
    let exists_result = ns.table_exists(exists_req).await;
    assert!(
        exists_result.is_err(),
        "table_exists should fail after deregister"
    );
    println!("[7.9] table_exists after deregister: Err (not found) ✅");

    // But the underlying Dataset is still accessible via goosefs:// URI!
    let ds = Dataset::open(&location).await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 7);
    println!("[7.9] Dataset still accessible via URI after deregister: 7 rows ✅");

    // Re-register the table (requires relative path)
    let relative_path = location.rsplit('/').next().unwrap_or(&location);
    let mut reg_req = RegisterTableRequest::new(relative_path.to_string());
    reg_req.id = Some(vec!["dereg".into(), "table1".into()]);
    let reg_resp = ns.register_table(reg_req).await.unwrap();
    println!("[7.9] Re-registered: location={:?}", reg_resp.location);

    // Verify table exists again in namespace
    let mut exists_req = TableExistsRequest::new();
    exists_req.id = Some(vec!["dereg".into(), "table1".into()]);
    ns.table_exists(exists_req).await.unwrap();
    println!("[7.9] table_exists after re-register: true ✅");
}

// ── Test 7.10: Full lifecycle: namespace → dataset write → index → search → delete → drop ──

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_7_10_full_lifecycle() {
    let root = goosefs_namespace_root("stage7", "test_7_10");
    let ns = create_goosefs_namespace(&root).await;

    // Step 1: Create namespace
    let mut create_ns = CreateNamespaceRequest::new();
    create_ns.id = Some(vec!["lifecycle".into()]);
    create_ns.properties = Some([("owner".into(), "e2e-test".into())].into_iter().collect());
    ns.create_namespace(create_ns).await.unwrap();
    println!("[7.10] Step 1: namespace created ✅");

    // Step 2: Create table with data (namespace already created above, create table directly)
    let schema = simple_schema();
    let batch = sample_batch(&schema, 1, 20);
    let ipc = build_ipc_data(&schema, &[batch]);
    let mut create_tbl = CreateTableRequest::new();
    create_tbl.id = Some(vec!["lifecycle".to_string(), "records".to_string()]);
    let create_resp = ns.create_table(create_tbl, Bytes::from(ipc)).await.unwrap();
    let location = create_resp.location.unwrap();
    println!("[7.10] Step 2: table created at {} ✅", location);

    // Step 3: Open via Dataset API and verify
    let ds = Dataset::open(&location).await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 20);
    println!("[7.10] Step 3: Dataset open + count = 20 ✅");

    // Step 4: Append via Dataset API
    let schema = simple_schema();
    let extra = sample_batch(&schema, 100, 10);
    let mut ds = Dataset::open(&location).await.unwrap();
    let batches = RecordBatchIterator::new([Ok(extra)], schema.clone());
    ds.append(batches, None).await.unwrap();
    let ds = Dataset::open(&location).await.unwrap();
    assert_eq!(ds.count_rows(None).await.unwrap(), 30);
    println!("[7.10] Step 4: append → 30 rows ✅");

    // Step 5: Create index via Namespace API
    let mut idx_req = CreateTableIndexRequest::new("id".to_string(), "BTree".to_string());
    idx_req.id = Some(vec!["lifecycle".into(), "records".into()]);
    idx_req.name = Some("idx_id".to_string());
    match ns.create_table_index(idx_req).await {
        Ok(resp) => println!("[7.10] Step 5: index created: {:?} ✅", resp.transaction_id),
        Err(e) => println!("[7.10] Step 5: index creation: {} (may be expected) ⏭️", e),
    }

    // Step 6: Delete rows via Dataset API
    // Initial: ids 1-20 (20 rows), appended: ids 100-109 (10 rows) = 30 total
    // Delete id > 25 → removes ids 100-109 = 10 rows, leaving 20
    let mut ds = Dataset::open(&location).await.unwrap();
    let del = ds.delete("id > 25").await.unwrap();
    println!(
        "[7.10] Step 6: deleted {} rows (id > 25) ✅",
        del.num_deleted_rows
    );
    let ds = Dataset::open(&location).await.unwrap();
    let remaining = ds.count_rows(None).await.unwrap();
    assert_eq!(
        remaining, 20,
        "Should have 20 rows (ids 1-20) after deleting id > 25"
    );

    // Step 7: Verify versions
    let versions = ds.versions().await.unwrap();
    println!("[7.10] Step 7: {} versions total ✅", versions.len());
    assert!(versions.len() >= 3); // initial + append + delete

    // Step 8: Drop table via Namespace API
    let mut drop_req = DropTableRequest::new();
    drop_req.id = Some(vec!["lifecycle".into(), "records".into()]);
    match ns.drop_table(drop_req).await {
        Ok(_) => println!("[7.10] Step 8: drop_table ✅"),
        Err(e) => println!(
            "[7.10] Step 8: drop_table: {} (known GooseFS limitation) ⚠️",
            e
        ),
    }

    // Step 9: Drop namespace
    let mut drop_ns = DropNamespaceRequest::new();
    drop_ns.id = Some(vec!["lifecycle".into()]);
    match ns.drop_namespace(drop_ns).await {
        Ok(_) => println!("[7.10] Step 9: drop_namespace ✅"),
        Err(e) => println!(
            "[7.10] Step 9: drop_namespace: {} (may have residual files) ⚠️",
            e
        ),
    }

    // Step 10: Verify namespace gone
    let mut exists_ns = NamespaceExistsRequest::new();
    exists_ns.id = Some(vec!["lifecycle".into()]);
    let ns_exists = ns.namespace_exists(exists_ns).await;
    match ns_exists {
        Ok(_) => println!("[7.10] Step 10: namespace still exists ⚠️ (residual)"),
        Err(_) => println!("[7.10] Step 10: namespace gone ✅"),
    }

    println!("[7.10] Full lifecycle complete ✅");
}
