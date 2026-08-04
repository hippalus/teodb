//! Benchmark: WAL append throughput under concurrency (D1).
//!
//! Appends are fsync-bound. Group commit (A3) shares one fsync across all
//! frames queued behind the in-flight write, so appends/sec should scale
//! with concurrency instead of staying flat at ~1/fsync-latency.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use teodb_storage::wal::{WalConfig, WalHeader, WalManager, WalOp, WalRecord};

const APPENDS_PER_TASK: usize = 8;
const ROWS_PER_BATCH: usize = 64;

fn make_record() -> WalRecord {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ids: Vec<i64> = (0..ROWS_PER_BATCH as i64).collect();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids))]).unwrap();
    WalRecord {
        header: WalHeader {
            protocol_version: teodb_core::write_protocol::WRITE_PROTOCOL_VERSION,
            table_uuid: Some(uuid::Uuid::from_u128(1)),
            batch_id: uuid::Uuid::new_v4(),
            table: teodb_core::ident::TableIdent::new("bench", "events"),
            schema_id: 0,
            generation: 1,
            created_at_ms: 0,
            idempotency_key: None,
            row_count: ROWS_PER_BATCH as u64,
            byte_count: 0,
            op: WalOp::Append,
        },
        batch,
    }
}

fn bench_wal_append(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("wal_append");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(4));

    // Keep WAL dirs alive until the group finishes.
    let mut dirs = Vec::new();

    for concurrency in [1usize, 8, 64] {
        let dir = tempfile::tempdir().unwrap();
        let wal = rt.block_on(async {
            Arc::new(
                WalManager::open(WalConfig {
                    root_dir: dir.path().to_path_buf(),
                    fsync_on_append: true,
                    ..Default::default()
                })
                .await
                .unwrap(),
            )
        });
        dirs.push(dir);

        group.throughput(Throughput::Elements((concurrency * APPENDS_PER_TASK) as u64));
        group.bench_with_input(
            BenchmarkId::new("concurrency", concurrency),
            &concurrency,
            |b, &tasks| {
                b.to_async(&rt).iter(|| {
                    let wal = wal.clone();
                    async move {
                        let mut handles = Vec::with_capacity(tasks);
                        for _ in 0..tasks {
                            let wal = wal.clone();
                            handles.push(tokio::spawn(async move {
                                let record = make_record();
                                for _ in 0..APPENDS_PER_TASK {
                                    wal.append(&record).await.unwrap();
                                }
                            }));
                        }
                        for h in handles {
                            h.await.unwrap();
                        }
                    }
                });
            },
        );
    }

    group.finish();
    drop(dirs);
}

criterion_group!(benches, bench_wal_append);
criterion_main!(benches);
