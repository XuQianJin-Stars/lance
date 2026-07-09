//! GooseFS client local page cache benchmark via Lance.
//!
//! Creates a Lance dataset on GooseFS, then benchmarks **scan** (sequential
//! read) and **random access** (take by row index) throughput. The GooseFS
//! client cache backend (io_uring vs tokio::fs vs none) is controlled by
//! environment variables — Lance is completely transparent to the cache
//! choice.
//!
//! ## Usage
//!
//! ```bash
//! # 1. No cache (baseline)
//! GOOSEFS_USER_CLIENT_CACHE_ENABLED=false \
//!   cargo run --release --example goosefs_cache_bench
//!
//! # 2. Cache + tokio::fs backend
//! GOOSEFS_USER_CLIENT_CACHE_ENABLED=true \
//!   GOOSEFS_USER_CLIENT_CACHE_URING_ENABLED=false \
//!   cargo run --release --example goosefs_cache_bench
//!
//! # 3. Cache + io_uring backend (Linux 5.1+)
//! GOOSEFS_USER_CLIENT_CACHE_ENABLED=true \
//!   GOOSEFS_USER_CLIENT_CACHE_URING_ENABLED=true \
//!   cargo run --release --example goosefs_cache_bench
//! ```
//!
//! ## Env knobs
//!
//! | Variable | Default | Description |
//! |----------|---------|-------------|
//! | `GOOSEFS_MASTER_ADDR` | `127.0.0.1:9200` | GooseFS master address |
//! | `GOOSEFS_AUTH_TYPE` | `nosasl` | Auth type (`nosasl`/`simple`) |
//! | `GOOSEFS_USER_CLIENT_CACHE_ENABLED` | `false` | Enable page cache |
//! | `GOOSEFS_USER_CLIENT_CACHE_URING_ENABLED` | `true`(Linux) | io_uring backend |
//! | `BENCH_ROWS` | `100000` | Number of rows in test dataset |
//! | `BENCH_DIM` | `128` | Vector dimension |
//! | `BENCH_SCAN_ROUNDS` | `5` | Scan iterations |
//! | `BENCH_TAKE_ROUNDS` | `1000` | Random take iterations |
//! | `BENCH_CONCURRENCY` | `32` | Concurrent take tasks |

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Float32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchIterator};
use futures::StreamExt;
use lance::dataset::builder::DatasetBuilder;
use lance::Dataset;

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn master_addr() -> String {
    std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string())
}

fn auth_type() -> String {
    std::env::var("GOOSEFS_AUTH_TYPE").unwrap_or_else(|_| "nosasl".to_string())
}

fn cache_status() -> &'static str {
    let enabled = std::env::var("GOOSEFS_USER_CLIENT_CACHE_ENABLED")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(false);
    if !enabled {
        return "disabled";
    }
    let uring = std::env::var("GOOSEFS_USER_CLIENT_CACHE_URING_ENABLED")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(cfg!(target_os = "linux"));
    if uring {
        "enabled (io_uring)"
    } else {
        "enabled (tokio::fs)"
    }
}

fn storage_options() -> HashMap<String, String> {
    let mut opts = HashMap::new();
    opts.insert("goosefs_master_addr".to_string(), master_addr());
    opts.insert("goosefs_auth_type".to_string(), auth_type());
    opts
}

fn dataset_uri(path: &str) -> String {
    format!("goosefs://{}/{}", master_addr(), path)
}

/// Create a test dataset with `rows` rows. Each row has a Float32 `value`
/// column (simulating vector data) and a UInt64 `id` column.
async fn create_dataset(uri: &str, rows: u64, dim: usize) -> Result<(), Box<dyn std::error::Error>> {
    println!("Creating dataset: {uri} ({rows} rows × {dim}-dim float32 values)");

    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Float32, false),
        Field::new("id", DataType::UInt64, false),
    ]));

    let batch_size = 1024usize;
    let mut batches = Vec::new();
    let mut offset = 0u64;
    while offset < rows {
        let n = (rows - offset).min(batch_size as u64) as usize;
        // Simulate dim-dimensional vector data as a flat float32 array
        let value_data: Vec<f32> = (0..(n * dim)).map(|i| (i as f32) * 0.001).collect();
        let id_data: Vec<u64> = (offset..offset + n as u64).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float32Array::from(value_data)),
                Arc::new(UInt64Array::from(id_data)),
            ],
        )?;
        batches.push(Ok(batch));
        offset += n as u64;
    }

    let reader = RecordBatchIterator::new(batches, schema.clone());
    Dataset::write(reader, uri, None).await?;
    println!("Dataset created.");
    Ok(())
}

/// Benchmark: full table scan.
async fn bench_scan(dataset: &Dataset, rounds: usize, dim: usize) -> Vec<f64> {
    let bytes_per_row = (dim * 4 + 8) as f64; // dim float32 + u64 id
    let mut throughputs = Vec::with_capacity(rounds);
    for i in 0..rounds {
        let start = Instant::now();
        let mut total_rows = 0u64;
        let mut stream = dataset.scan().try_into_stream().await.unwrap();
        while let Some(batch) = stream.next().await {
            total_rows += batch.unwrap().num_rows() as u64;
        }
        let elapsed = start.elapsed();
        let mib_s = (total_rows as f64 * bytes_per_row / (1024.0 * 1024.0))
            / elapsed.as_secs_f64().max(1e-9);
        println!(
            "  scan round {}: {} rows in {:.2}s → {:.1} MiB/s",
            i + 1,
            total_rows,
            elapsed.as_secs_f64(),
            mib_s
        );
        throughputs.push(mib_s);
    }
    throughputs
}

/// Benchmark: random row access via `take()`.
async fn bench_take(dataset: &Dataset, rounds: usize, num_rows: u64) {
    use rand::Rng;
    let mut rng = rand::rng();
    let mut latencies = Vec::with_capacity(rounds);

    for _ in 0..rounds {
        let idx = rng.random_range(0..num_rows);
        let start = Instant::now();
        let _ = dataset.take(&[idx], dataset.schema().clone()).await.unwrap();
        latencies.push(start.elapsed().as_micros() as u64);
    }

    latencies.sort_unstable();
    println!(
        "  take: {} rounds, p50={}µs, p99={}µs, avg={}µs",
        rounds,
        latencies[latencies.len() / 2],
        latencies[latencies.len() * 99 / 100],
        latencies.iter().sum::<u64>() / latencies.len() as u64,
    );
}

/// Benchmark: concurrent random access.
async fn bench_take_concurrent(
    dataset: &Dataset,
    concurrency: usize,
    rounds_per_task: usize,
    num_rows: u64,
) {
    use rand::Rng;
    use rand::SeedableRng;
    let mut handles = Vec::with_capacity(concurrency);
    for task_id in 0..concurrency {
        let ds = dataset.clone();
        let mut rng = rand::rngs::StdRng::seed_from_u64(task_id as u64);
        handles.push(tokio::spawn(async move {
            let mut latencies = Vec::with_capacity(rounds_per_task);
            for _ in 0..rounds_per_task {
                let idx = rng.random_range(0..num_rows);
                let start = Instant::now();
                let _ = ds.take(&[idx], ds.schema().clone()).await.unwrap();
                latencies.push(start.elapsed().as_micros() as u64);
            }
            latencies
        }));
    }

    let mut all = Vec::with_capacity(concurrency * rounds_per_task);
    for h in handles {
        all.extend(h.await.unwrap());
    }
    all.sort_unstable();
    let total = all.len();
    let total_time_us: u64 = all.iter().sum();
    let ops_per_sec = total as f64 / (total_time_us as f64 / 1e6).max(1e-9);
    println!(
        "  take concurrent ({}×{}): {} ops total, p50={}µs, p99={}µs, ≈{:.0} ops/s",
        concurrency,
        rounds_per_task,
        total,
        all[all.len() / 2],
        all[all.len() * 99 / 100],
        ops_per_sec,
    );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let rows: u64 = env_or("BENCH_ROWS", 100_000);
    let dim: usize = env_or("BENCH_DIM", 128);
    let scan_rounds: usize = env_or("BENCH_SCAN_ROUNDS", 5);
    let take_rounds: usize = env_or("BENCH_TAKE_ROUNDS", 1000);
    let concurrency: usize = env_or("BENCH_CONCURRENCY", 32);
    let take_rounds_conc: usize = (take_rounds / concurrency).max(10);

    let ds_path = format!("/lance-bench/cache_bench_{}", std::process::id());
    let uri = dataset_uri(&ds_path);

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Lance + GooseFS Page Cache Benchmark                       ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("  master={}  auth={}", master_addr(), auth_type());
    println!("  cache: {}", cache_status());
    println!("  dataset: {rows} rows × {dim}-dim vectors");
    println!("  scan_rounds={scan_rounds}  take_rounds={take_rounds}  concurrency={concurrency}");

    // ── Create test dataset ────────────────────────────────────
    create_dataset(&uri, rows, dim).await?;

    // ── Open dataset ───────────────────────────────────────────
    let dataset = DatasetBuilder::from_uri(&uri)
        .with_storage_options(storage_options())
        .load()
        .await?;

    let num_rows = dataset.count_rows(None).await?;
    println!("  opened dataset: {num_rows} rows");

    // ── Warm up (fill page cache) ──────────────────────────────
    println!("\n── Warm-up scan ──────────────────────────────────────────────");
    let _ = bench_scan(&dataset, 1, dim).await;

    // ── Scan benchmark ─────────────────────────────────────────
    println!("\n── Scan throughput ────────────────────────────────────────────");
    let scan_tp = bench_scan(&dataset, scan_rounds, dim).await;
    let avg_scan = scan_tp.iter().sum::<f64>() / scan_tp.len() as f64;
    println!("  → avg scan throughput: {avg_scan:.1} MiB/s");

    // ── Random access benchmark ────────────────────────────────
    println!("\n── Random access (take) ───────────────────────────────────────");
    bench_take(&dataset, take_rounds, num_rows as u64).await;

    // ── Concurrent random access ───────────────────────────────
    println!("\n── Concurrent random access ───────────────────────────────────");
    bench_take_concurrent(&dataset, concurrency, take_rounds_conc, num_rows as u64).await;

    // ── Summary ────────────────────────────────────────────────
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("  Summary: cache={}", cache_status());
    println!("  scan: {avg_scan:.1} MiB/s avg ({scan_rounds} rounds)");
    println!("═══════════════════════════════════════════════════════════════");

    // ── Cleanup ────────────────────────────────────────────────
    // Best-effort cleanup; the dataset is under /lance-bench/ which can be
    // cleaned up manually if needed.
    println!("\nDataset: {uri}");
    println!("Cleanup: goosefs fs -rm -r {}", ds_path);

    Ok(())
}
