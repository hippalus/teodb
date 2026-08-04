use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput};
use futures::future::join_all;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog as IcebergCatalog, CatalogBuilder as _};
use teodb_catalog::{
    CatalogCommitOutcome, CatalogObserver, CatalogStatusCheckOutcome, IcebergCatalogAdapter, IcebergCatalogConfig,
};
use teodb_core::error::TeoDBError;
use teodb_core::file::{DataContent, DataFile, FileFormat};
use teodb_core::ident::TableIdent;
use teodb_core::location::ObjectLocation;
use teodb_core::traits::catalog::{Catalog, CommitAppend};
use teodb_core::write_protocol::{
    AppendCommitIdentity, CommitId, GenerationRange, WriterCheckpoint, WriterEpoch, WriterId, writer_checkpoint_key,
};

const WRITER_COUNTS: [usize; 4] = [1, 5, 16, 32];
const LOAD_WRITER_COUNTS: [usize; 3] = [2, 5, 16];
const PROTOCOL_BUDGET_WAVES_PER_SCENARIO: usize = 5;

fn schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .expect("benchmark schema")
}

fn checkpoint_properties(count: usize) -> HashMap<String, String> {
    (0..count)
        .map(|index| {
            let writer_id = WriterId::from_uuid(uuid::Uuid::from_u128(index as u128 + 1));
            let checkpoint = WriterCheckpoint::new(
                WriterEpoch::new(1),
                1,
                CommitId::from_uuid(uuid::Uuid::from_u128(index as u128 + 10_000)),
                1,
            )
            .encode()
            .expect("checkpoint encoding");
            (writer_checkpoint_key(writer_id), checkpoint)
        })
        .collect()
}

struct BenchCatalog {
    _warehouse: tempfile::TempDir,
    adapter: Arc<IcebergCatalogAdapter>,
    tables: Vec<(TableIdent, uuid::Uuid, String)>,
}

async fn setup_catalog(table_count: usize, checkpoint_count: usize, max_writers: usize) -> BenchCatalog {
    setup_catalog_with_observer(table_count, checkpoint_count, max_writers, None).await
}

async fn setup_catalog_with_observer(
    table_count: usize,
    checkpoint_count: usize,
    max_writers: usize,
    observer: Option<Arc<dyn CatalogObserver>>,
) -> BenchCatalog {
    let warehouse = tempfile::tempdir().expect("benchmark warehouse");
    let memory = Arc::new(
        iceberg::memory::MemoryCatalogBuilder::default()
            .load(
                "multi-writer-bench",
                HashMap::from([(
                    iceberg::memory::MEMORY_CATALOG_WAREHOUSE.into(),
                    warehouse.path().to_string_lossy().into_owned(),
                )]),
            )
            .await
            .expect("memory catalog"),
    );
    let namespace = iceberg::NamespaceIdent::new("bench".into());
    memory
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("benchmark namespace");
    let mut tables = Vec::with_capacity(table_count);
    for index in 0..table_count {
        let name = format!("events_{index}");
        let table = memory
            .create_table(
                &namespace,
                iceberg::TableCreation::builder()
                    .name(name.clone())
                    .schema(schema())
                    .properties(checkpoint_properties(checkpoint_count))
                    .build(),
            )
            .await
            .expect("benchmark table");
        tables.push((
            TableIdent::new("bench", name),
            table.metadata().uuid(),
            table
                .metadata()
                .location()
                .trim_end_matches('/')
                .to_owned(),
        ));
    }
    let mut adapter = IcebergCatalogAdapter::from_catalog(
        memory,
        IcebergCatalogConfig {
            max_writer_checkpoints_per_table: max_writers,
            ..IcebergCatalogConfig::default()
        },
    );
    if let Some(observer) = observer {
        adapter = adapter.with_observer(observer);
    }
    BenchCatalog {
        _warehouse: warehouse,
        adapter: Arc::new(adapter),
        tables,
    }
}

#[derive(Default)]
struct BudgetObserver {
    committed: AtomicU64,
    conflicts: AtomicU64,
    rebases: AtomicU64,
}

impl CatalogObserver for BudgetObserver {
    fn on_append_commit(&self, outcome: CatalogCommitOutcome, _duration: Duration) {
        match outcome {
            CatalogCommitOutcome::Committed => {
                self.committed.fetch_add(1, Ordering::Relaxed);
            }
            CatalogCommitOutcome::Conflict => {
                self.conflicts.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn on_append_rebase(&self, rebases: u32) {
        self.rebases
            .fetch_add(u64::from(rebases), Ordering::Relaxed);
    }

    fn on_status_check(&self, _outcome: CatalogStatusCheckOutcome, _duration: Duration) {}

    fn on_writer_checkpoint_parse_failure(&self) {}

    fn on_writer_checkpoint_count(&self, _count: usize) {}
}

struct ScenarioMeasurement {
    median_micros: u64,
    successful_commits: u64,
    rebases: u64,
    conflicts: u64,
    manifest_files: u64,
}

async fn measure_scenario(writer_count: usize, same_table: bool, waves: usize) -> ScenarioMeasurement {
    let table_count = if same_table { 1 } else { writer_count };
    let observer = Arc::new(BudgetObserver::default());
    let state = setup_catalog_with_observer(table_count, 0, writer_count.max(32), Some(observer.clone())).await;
    let writer_ids: Vec<_> = (0..writer_count)
        .map(|index| WriterId::from_uuid(uuid::Uuid::from_u128(index as u128 + 50_000)))
        .collect();
    let generations: Vec<_> = (0..writer_count)
        .map(|_| AtomicU64::new(0))
        .collect();
    let mut durations = Vec::with_capacity(waves);

    for _ in 0..waves {
        let requests: Vec<_> = writer_ids
            .iter()
            .enumerate()
            .map(|(index, writer_id)| {
                let table = if same_table {
                    &state.tables[0]
                } else {
                    &state.tables[index]
                };
                append_request(table, *writer_id, generations[index].load(Ordering::Relaxed) + 1)
            })
            .collect();
        let started = Instant::now();
        let results = join_all(
            requests
                .into_iter()
                .map(|request| state.adapter.commit_append(request)),
        )
        .await;
        durations.push(started.elapsed().as_micros().max(1) as u64);
        for (index, result) in results.into_iter().enumerate() {
            match result {
                Ok(_) => {
                    generations[index].fetch_add(1, Ordering::Relaxed);
                }
                Err(TeoDBError::Conflict { .. }) => {}
                Err(error) => {
                    panic!("unexpected protocol-budget append error: {error}")
                }
            }
        }
    }

    durations.sort_unstable();
    let manifest_files = if same_table {
        state
            .adapter
            .load_live_files(&state.tables[0].0)
            .await
            .expect("load budget manifests")
            .len() as u64
    } else {
        let mut files = 0u64;
        for table in &state.tables {
            files += state
                .adapter
                .load_live_files(&table.0)
                .await
                .expect("load budget manifests")
                .len() as u64;
        }
        files
    };
    ScenarioMeasurement {
        median_micros: durations[durations.len() / 2],
        successful_commits: observer.committed.load(Ordering::Relaxed),
        rebases: observer.rebases.load(Ordering::Relaxed),
        conflicts: observer.conflicts.load(Ordering::Relaxed),
        manifest_files,
    }
}

async fn protocol_budget_snapshot() -> serde_json::Value {
    let structural = setup_catalog(1, 32, 64).await;
    let metadata = structural
        .adapter
        .load_table(&structural.tables[0].0)
        .await
        .expect("load 32-writer metadata");
    let metadata_bytes = serde_json::to_vec(metadata.as_ref())
        .expect("serialize 32-writer metadata")
        .len() as u64;
    let commit_payload_bytes = serde_json::to_vec(&serde_json::json!({
        "identifier": {"namespace": ["bench"], "name": "events_0"},
        "requirements": [{
            "type": "assert-table-uuid",
            "uuid": metadata.table_uuid,
        }],
        "updates": [{
            "action": "set-properties",
            "updates": checkpoint_properties(32),
        }],
    }))
    .expect("serialize 32-writer commit payload")
    .len() as u64;

    let same_two = measure_scenario(2, true, PROTOCOL_BUDGET_WAVES_PER_SCENARIO).await;
    let same_sixteen = measure_scenario(16, true, PROTOCOL_BUDGET_WAVES_PER_SCENARIO).await;
    let different_sixteen = measure_scenario(16, false, PROTOCOL_BUDGET_WAVES_PER_SCENARIO).await;
    let ratio = |numerator: u64, denominator: u64| numerator as f64 / denominator.max(1) as f64;

    serde_json::json!({
        "schema_version": 1,
        "measurement": {
            "waves_per_scenario": PROTOCOL_BUDGET_WAVES_PER_SCENARIO,
        },
        "structural": {
            "metadata_bytes_32_writers": metadata_bytes,
            "commit_payload_bytes_32_writers": commit_payload_bytes,
            "same_table_16_successful_commits": same_sixteen.successful_commits,
            "same_table_16_rebases": same_sixteen.rebases,
            "same_table_16_conflicts": same_sixteen.conflicts,
            "same_table_16_manifest_files": same_sixteen.manifest_files,
        },
        "ratios": {
            "same_table_16_to_2_latency": ratio(
                same_sixteen.median_micros,
                same_two.median_micros,
            ),
            "same_to_different_table_16_latency": ratio(
                same_sixteen.median_micros,
                different_sixteen.median_micros,
            ),
        },
        "diagnostics": {
            "same_table_2_median_micros": same_two.median_micros,
            "same_table_16_median_micros": same_sixteen.median_micros,
            "different_table_16_median_micros": different_sixteen.median_micros,
            "different_table_16_successful_commits": different_sixteen.successful_commits,
        },
    })
}

fn write_protocol_budget(path: &str) {
    let runtime = tokio::runtime::Runtime::new().expect("protocol budget runtime");
    let snapshot = runtime.block_on(protocol_budget_snapshot());
    let bytes = serde_json::to_vec_pretty(&snapshot).expect("serialize budget artifact");
    let requested = std::path::Path::new(path);
    let output = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(requested)
    };
    if let Some(parent) = output.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).expect("create protocol budget output directory");
    }
    std::fs::write(&output, bytes).expect("write protocol budget artifact");
    println!("multi-writer protocol budget written to {}", output.display());
}

fn data_file(path: String) -> DataFile {
    let canonical_path = if path.contains("://") {
        path
    } else {
        format!("file://{path}")
    };
    DataFile {
        content: DataContent::Data,
        path: ObjectLocation::parse(&canonical_path).expect("benchmark data file location"),
        format: FileFormat::Parquet,
        partition_spec_id: 0,
        sort_order_id: None,
        schema_id: 0,
        partition_values: HashMap::new(),
        record_count: 1,
        file_size_bytes: 1,
        column_sizes: HashMap::new(),
        value_counts: HashMap::new(),
        null_value_counts: HashMap::new(),
        nan_value_counts: HashMap::new(),
        lower_bounds: HashMap::new(),
        upper_bounds: HashMap::new(),
        split_offsets: Vec::new(),
        equality_ids: Vec::new(),
        key_metadata: None,
    }
}

fn append_request(table: &(TableIdent, uuid::Uuid, String), writer_id: WriterId, generation: u64) -> CommitAppend {
    let commit_id = CommitId::now_v7();
    CommitAppend {
        table: table.0.clone(),
        table_uuid: table.1,
        identity: AppendCommitIdentity {
            writer_id,
            writer_epoch: WriterEpoch::new(1),
            commit_id,
            generations: GenerationRange::new(generation, generation).expect("generation"),
        },
        base_snapshot_id: None,
        added_data_files: vec![data_file(format!(
            "{}/data/{writer_id}/{}-f0000.parquet",
            table.2, commit_id
        ))],
        properties: HashMap::new(),
    }
}

fn metadata_overhead(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime");
    let mut group = c.benchmark_group("catalog_metadata_overhead");

    for writer_count in WRITER_COUNTS {
        let state = runtime.block_on(setup_catalog(1, writer_count, 64));
        let metadata = runtime
            .block_on(state.adapter.load_table(&state.tables[0].0))
            .expect("load benchmark metadata");
        let metadata_bytes = serde_json::to_vec(metadata.as_ref()).expect("serialize metadata");
        let commit_update = iceberg::TableUpdate::SetProperties {
            updates: checkpoint_properties(writer_count),
        };
        let commit_payload = serde_json::to_vec(&serde_json::json!({
            "identifier": {
                "namespace": ["bench"],
                "name": "events_0",
            },
            "requirements": [{
                "type": "assert-table-uuid",
                "uuid": metadata.table_uuid,
            }],
            "updates": [commit_update],
        }))
        .expect("serialize representative REST commit payload");

        group.throughput(Throughput::Bytes(metadata_bytes.len() as u64));
        group.bench_with_input(
            BenchmarkId::new(
                "metadata_json",
                format!("{writer_count}-writers-{}-bytes", metadata_bytes.len()),
            ),
            &metadata,
            |bench, metadata| {
                bench.iter(|| black_box(serde_json::to_vec(black_box(metadata.as_ref())).unwrap()));
            },
        );

        group.throughput(Throughput::Bytes(commit_payload.len() as u64));
        group.bench_with_input(
            BenchmarkId::new(
                "commit_payload_json",
                format!("{writer_count}-writers-{}-bytes", commit_payload.len()),
            ),
            &writer_count,
            |bench, _| {
                let update = iceberg::TableUpdate::SetProperties {
                    updates: checkpoint_properties(writer_count),
                };
                bench.iter(|| {
                    black_box(
                        serde_json::to_vec(&serde_json::json!({
                            "identifier": {"namespace": ["bench"], "name": "events_0"},
                            "requirements": [{
                                "type": "assert-table-uuid",
                                "uuid": metadata.table_uuid,
                            }],
                            "updates": [update.clone()],
                        }))
                        .unwrap(),
                    )
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("catalog_load", format!("{writer_count}-writers")),
            &writer_count,
            |bench, _| {
                bench.to_async(&runtime).iter(|| async {
                    black_box(
                        state
                            .adapter
                            .load_table(&state.tables[0].0)
                            .await
                            .expect("catalog load"),
                    )
                });
            },
        );
    }
    group.finish();
}

fn conflict_amplification(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime");
    let mut group = c.benchmark_group("catalog_conflict_amplification");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));

    for writer_count in LOAD_WRITER_COUNTS {
        for same_table in [true, false] {
            let table_count = if same_table { 1 } else { writer_count };
            let state = runtime.block_on(setup_catalog(table_count, 0, writer_count));
            let writer_ids: Vec<_> = (0..writer_count)
                .map(|index| WriterId::from_uuid(uuid::Uuid::from_u128(index as u128 + 1_000)))
                .collect();
            let generations: Vec<_> = (0..writer_count)
                .map(|_| AtomicU64::new(0))
                .collect();
            let scenario = if same_table {
                "same_table_default_duplicate_check"
            } else {
                "different_tables"
            };

            group.bench_function(BenchmarkId::new(scenario, writer_count), |bench| {
                bench.to_async(&runtime).iter(|| {
                    let requests: Vec<_> = writer_ids
                        .iter()
                        .enumerate()
                        .map(|(index, writer_id)| {
                            let table = if same_table {
                                &state.tables[0]
                            } else {
                                &state.tables[index]
                            };
                            let next_generation = generations[index].load(Ordering::Relaxed) + 1;
                            append_request(table, *writer_id, next_generation)
                        })
                        .collect();
                    async {
                        let results = join_all(
                            requests
                                .into_iter()
                                .map(|request| state.adapter.commit_append(request)),
                        )
                        .await;
                        let mut committed = 0usize;
                        let mut conflicts = 0usize;
                        for (index, result) in results.into_iter().enumerate() {
                            match result {
                                Ok(_) => {
                                    generations[index].fetch_add(1, Ordering::Relaxed);
                                    committed += 1;
                                }
                                Err(TeoDBError::Conflict { .. }) => conflicts += 1,
                                Err(error) => panic!("unexpected benchmark append error: {error}"),
                            }
                        }
                        black_box((committed, conflicts))
                    }
                });
            });
        }
    }
    group.finish();
}

fn main() {
    if let Ok(path) = std::env::var("TEODB_PROTOCOL_BUDGET_OUT") {
        write_protocol_budget(&path);
        if std::env::var_os("TEODB_PROTOCOL_BUDGET_ONLY").is_some() {
            return;
        }
    }

    let mut criterion = Criterion::default().configure_from_args();
    metadata_overhead(&mut criterion);
    conflict_amplification(&mut criterion);
    criterion.final_summary();
}
