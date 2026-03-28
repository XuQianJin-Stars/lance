//! Shared helper functions for GooseFS Namespace E2E tests (Stage 5 & Stage 6).
//!
//! Extracted from goosefs_namespace_e2e.rs and goosefs_namespace_e2e_stage6.rs
//! to reduce maintenance burden and ensure consistency.

use std::sync::Arc;

use arrow::array::{Float32Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;

use lance_namespace::LanceNamespace;
use lance_namespace::models::{CreateNamespaceRequest, CreateTableRequest};
use lance_namespace_impls::DirectoryNamespaceBuilder;

/// Generate a unique GooseFS URI root for each test run (timestamp-based).
///
/// The `prefix` determines the subdirectory under `/lance-test/` (e.g., "namespaces", "stage6").
/// The `suffix` provides test-specific identification (e.g., "test_5_1_crud").
pub fn goosefs_namespace_root(prefix: &str, suffix: &str) -> String {
    let addr = std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".into());
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("goosefs://{}/lance-test/{}/{}_{}", addr, prefix, suffix, ts)
}

/// Build IPC data from Arrow RecordBatch(es) for the `create_table` API.
///
/// Serializes the schema and batches into Arrow IPC stream format,
/// which is required by `LanceNamespace::create_table`.
pub fn build_ipc_data(schema: &Arc<ArrowSchema>, batches: &[RecordBatch]) -> Vec<u8> {
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, schema).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();
    }
    buffer
}

/// Create a simple schema: (id: Int32, name: Utf8, score: Float32).
pub fn simple_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float32, false),
    ]))
}

/// Create a sample RecordBatch with `n` rows, starting from `start_id`.
///
/// Generates rows like: (start_id, "item_{start_id}", start_id * 1.5), ...
pub fn sample_batch(schema: &Arc<ArrowSchema>, start_id: i32, n: usize) -> RecordBatch {
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

/// Create a DirectoryNamespace backed by GooseFS with manifest enabled.
pub async fn create_goosefs_namespace(root: &str) -> Box<dyn LanceNamespace> {
    let ns = DirectoryNamespaceBuilder::new(root)
        .manifest_enabled(true)
        .build()
        .await
        .unwrap_or_else(|e| panic!("Failed to create namespace at {}: {}", root, e));
    Box::new(ns)
}

/// Helper: create a namespace and a table with data inside it, returning the table location.
pub async fn setup_ns_with_table(
    ns: &dyn LanceNamespace,
    ns_name: &str,
    table_name: &str,
    rows: usize,
) -> String {
    let mut create_ns = CreateNamespaceRequest::new();
    create_ns.id = Some(vec![ns_name.to_string()]);
    ns.create_namespace(create_ns).await.unwrap();

    let schema = simple_schema();
    let batch = sample_batch(&schema, 1, rows);
    let ipc = build_ipc_data(&schema, &[batch]);
    let mut create_tbl = CreateTableRequest::new();
    create_tbl.id = Some(vec![ns_name.to_string(), table_name.to_string()]);
    let resp = ns.create_table(create_tbl, Bytes::from(ipc)).await.unwrap();

    resp.location.unwrap_or_default()
}
