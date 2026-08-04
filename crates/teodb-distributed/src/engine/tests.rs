use super::*;
use std::collections::HashMap;

use teodb_core::error::TeoDBResult;
use teodb_core::file::{DataFile, TableMetadata};
use teodb_core::traits::catalog::{
    Catalog, CommitAppend, CommitReplace, CommitStatus, CreateTableRequest, RetainedFileSet,
};
use teodb_core::{ident::TableIdent, location::ObjectLocation};
use teodb_test_support::{MockCatalog, stub_storage_factory, table_metadata_with_snapshot};

fn test_session_factory_from_catalog(
    catalog: Arc<dyn Catalog>,
    runtime_config: teodb_query::DataFusionRuntimeConfig,
    config: teodb_query::DataFusionSessionConfig,
) -> Arc<teodb_query::DataFusionSessionFactory> {
    Arc::new(
        teodb_query::DataFusionSessionFactory::new(
            catalog,
            stub_storage_factory(),
            teodb_query::DataFusionRuntime::try_new(&runtime_config).unwrap(),
            config,
        )
        .unwrap(),
    )
}

fn test_session_factory(
    catalog: MockCatalog,
    runtime_config: teodb_query::DataFusionRuntimeConfig,
    config: teodb_query::DataFusionSessionConfig,
) -> Arc<teodb_query::DataFusionSessionFactory> {
    test_session_factory_from_catalog(Arc::new(catalog), runtime_config, config)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_engine_executes_sql() {
    let tmp = tempfile::tempdir().unwrap();
    let runtime_config = teodb_query::DataFusionRuntimeConfig {
        memory_pool_bytes: 64 * 1024 * 1024,
        spill_dir: tmp.path().to_path_buf(),
    };
    let config = teodb_query::DataFusionSessionConfig {
        batch_size: 1024,
        target_partitions: 1,
        ..Default::default()
    };
    let factory = test_session_factory(MockCatalog::empty(), runtime_config, config);
    let engine = BallistaQueryEngineBuilder::standalone(factory, 1).build();

    let req = QueryRequest {
        sql: "SELECT 1 AS one".into(),
        principal: teodb_core::traits::authz::Principal {
            subject: "standalone-test".into(),
            roles: vec![],
            claims: HashMap::new(),
        },
        query_id: QueryId::new(),
        limit: None,
    };

    let handle = engine.prepare(req).await.unwrap();
    assert_eq!(handle.schema.fields().len(), 1);

    let mut stream = engine.execute_stream(handle).await.unwrap();
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch.unwrap().num_rows();
    }
    assert_eq!(rows, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_status_registry_stays_bounded() {
    let engine = BallistaQueryEngineBuilder::standalone(stub_factory(), 1)
        .status_retention(3, Duration::from_secs(60))
        .build();
    let mut query_ids = Vec::new();

    for _ in 0..50 {
        let query_id = QueryId::new();
        engine
            .queries
            .set(query_id, QueryStatus::Completed)
            .await;
        query_ids.push(query_id);
    }
    engine.queries.run_pending_tasks().await;

    assert!(
        engine.queries.entry_count() <= 3,
        "status cache must not grow past configured capacity"
    );
    let mut retained = 0;
    for query_id in query_ids {
        if engine.status(&query_id).await.is_ok() {
            retained += 1;
        }
    }
    assert!(retained <= 3, "evicted query statuses must not remain readable");
}

#[test]
fn remote_engine_normalizes_scheduler_endpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let factory = test_session_factory(
        MockCatalog::empty(),
        teodb_query::DataFusionRuntimeConfig {
            spill_dir: tmp.path().to_path_buf(),
            ..Default::default()
        },
        teodb_query::DataFusionSessionConfig { ..Default::default() },
    );

    let engine = BallistaQueryEngineBuilder::remote(factory, "scheduler:50050")
        .unwrap()
        .build();

    match &engine.mode {
        BallistaMode::Remote { scheduler_url } => {
            assert_eq!(scheduler_url, "http://scheduler:50050");
        }
        _ => panic!("expected remote mode"),
    }
}

#[test]
fn classify_table_not_found_strips_prefix() {
    use datafusion::error::DataFusionError;
    let e = DataFusionError::Plan("table 'datafusion.perf.events' not found".into());
    let err = classify_planning_error(e);
    match &err {
        TeoDBError::NotFound { resource } => {
            assert!(
                !resource.contains("datafusion."),
                "should strip 'datafusion.' prefix, got: {resource}"
            );
            assert!(resource.contains("perf.events"));
        }
        other => panic!("expected NotFound, got: {other:?}"),
    }
}

#[test]
fn classify_generic_planning_error() {
    use datafusion::error::DataFusionError;
    let e = DataFusionError::Plan("column 'x' not in schema".into());
    let err = classify_planning_error(e);
    assert!(
        matches!(err, TeoDBError::QueryExecution(_)),
        "non-table-not-found should be QueryExecution"
    );
}

// Snapshot pinning

use teodb_core::snapshot_pin::InMemorySnapshotRegistry;

/// TeoDB-native table metadata with a current snapshot and no files.
fn teo_metadata_with_snapshot(snapshot_id: i64) -> teodb_core::file::TableMetadata {
    teodb_core::file::TableMetadata {
        table_uuid: uuid::Uuid::nil(),
        namespace: "ns".into(),
        table_name: "events".into(),
        table_location: ObjectLocation {
            scheme: teodb_core::location::StorageScheme::Local,
            bucket: None,
            key: "data/events".into(),
        },
        current_snapshot_id: Some(snapshot_id),
        current_schema_id: 0,
        current_partition_spec_id: 0,
        current_sort_order_id: 0,
        schemas: vec![teodb_core::schema::SchemaDefinition {
            schema_id: 0,
            columns: vec![teodb_core::schema::ColumnMeta {
                id: 1,
                name: "id".into(),
                data_type: teodb_core::schema::TeoDataType::Int64,
                nullable: false,
                doc: None,
            }],
            identifier_field_ids: vec![1],
        }],
        partition_specs: vec![teodb_core::schema::PartitionSpec {
            spec_id: 0,
            fields: vec![],
        }],
        sort_orders: vec![teodb_core::schema::SortOrder {
            order_id: 0,
            fields: vec![],
        }],
        snapshots: vec![],
        current_snapshot: Some(teodb_core::file::Snapshot {
            snapshot_id,
            parent_snapshot_id: None,
            sequence_number: 1,
            timestamp_ms: 0,
            operation: teodb_core::file::SnapshotOperation::Append,
            data_files: vec![],
            delete_files: vec![],
            summary: Default::default(),
        }),
        properties: HashMap::new(),
    }
}

fn scan_plan_over_snapshot(snapshot_id: i64) -> LogicalPlan {
    let provider = teodb_query::TeoTableProvider::try_new(
        TableIdent::new("ns", "events"),
        Arc::new(teo_metadata_with_snapshot(snapshot_id)),
        Arc::new(MockCatalog::empty()),
        stub_storage_factory(),
    )
    .expect("provider");

    datafusion::logical_expr::LogicalPlanBuilder::scan(
        "events",
        datafusion::datasource::provider_as_source(Arc::new(provider)),
        None,
    )
    .expect("scan")
    .build()
    .expect("plan")
}

fn stub_factory() -> Arc<teodb_query::DataFusionSessionFactory> {
    let tmp = tempfile::tempdir().unwrap();
    test_session_factory(
        MockCatalog::empty(),
        teodb_query::DataFusionRuntimeConfig {
            memory_pool_bytes: 64 * 1024 * 1024,
            spill_dir: tmp.path().to_path_buf(),
        },
        teodb_query::DataFusionSessionConfig {
            batch_size: 1024,
            target_partitions: 1,
            ..Default::default()
        },
    )
}

async fn wait_released(registry: &Arc<InMemorySnapshotRegistry>, table: &TableIdent) {
    // SnapshotPin releases via a spawned task — poll briefly.
    for _ in 0..100 {
        if registry
            .active_snapshots(table)
            .await
            .unwrap()
            .is_empty()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("snapshot pins were not released");
}

#[test]
fn collect_scan_targets_finds_teodb_scans() {
    let plan = scan_plan_over_snapshot(42);
    let targets = collect_scan_targets(&plan);
    assert_eq!(targets, vec![(TableIdent::new("ns", "events"), 42)]);
}

#[tokio::test]
async fn pins_released_on_releaser_drop() {
    let registry = Arc::new(InMemorySnapshotRegistry::new());
    let engine = BallistaQueryEngineBuilder::standalone(stub_factory(), 1)
        .snapshot_registry(registry.clone() as Arc<dyn teodb_core::snapshot_pin::ActiveSnapshotRegistry>)
        .build();
    let table = TableIdent::new("ns", "events");

    let qid = QueryId::new();
    let releaser = engine
        .pin_scanned_snapshots(qid, &scan_plan_over_snapshot(42))
        .await
        .expect("pins created");
    assert_eq!(registry.active_snapshots(&table).await.unwrap(), vec![42]);

    drop(releaser);
    wait_released(&registry, &table).await;
}

#[tokio::test]
async fn pins_released_on_cancel() {
    let registry = Arc::new(InMemorySnapshotRegistry::new());
    let engine = BallistaQueryEngineBuilder::standalone(stub_factory(), 1)
        .snapshot_registry(registry.clone() as Arc<dyn teodb_core::snapshot_pin::ActiveSnapshotRegistry>)
        .build();
    let table = TableIdent::new("ns", "events");

    let qid = QueryId::new();
    let _releaser = engine
        .pin_scanned_snapshots(qid, &scan_plan_over_snapshot(42))
        .await
        .expect("pins created");
    assert_eq!(registry.active_snapshots(&table).await.unwrap(), vec![42]);

    engine.cancel(&qid).await.unwrap();
    wait_released(&registry, &table).await;
}

// Scheduler-outage local fallback

#[test]
fn scheduler_unreachable_classification() {
    use datafusion::error::DataFusionError;
    // tonic transport failures arrive as Execution with debug text.
    for msg in [
        "tonic::transport::Error(Transport, ConnectError(ConnectError(\"tcp connect error\", Os { code: 61, kind: ConnectionRefused, message: \"Connection refused\" })))",
        "transport error",
        "dns error: failed to lookup address",
    ] {
        assert!(
            is_scheduler_unreachable(&DataFusionError::Execution(msg.into())),
            "should classify as unreachable: {msg}"
        );
    }
    // Scheduler-side query failures must not trigger fallback.
    for msg in [
        "Fail to execute query due to JobFailed(\"divide by zero\")",
        "table 'ns.missing' not found",
    ] {
        assert!(
            !is_scheduler_unreachable(&DataFusionError::Execution(msg.into())),
            "should NOT classify as unreachable: {msg}"
        );
    }
}

struct CountingObserver {
    fallbacks: std::sync::atomic::AtomicU64,
}

impl EngineEventObserver for CountingObserver {
    fn on_local_fallback(&self, _query_id: &QueryId, _error: &str) {
        self.fallbacks
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

struct OneShotCatalog {
    metadata: Arc<TableMetadata>,
    load_table_calls: std::sync::atomic::AtomicUsize,
}

impl OneShotCatalog {
    fn new(metadata: Arc<TableMetadata>) -> Self {
        Self {
            metadata,
            load_table_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn load_table_calls(&self) -> usize {
        self.load_table_calls
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl Catalog for OneShotCatalog {
    async fn list_namespaces(&self) -> TeoDBResult<Vec<String>> {
        Ok(vec!["ns".into()])
    }

    async fn create_namespace(&self, _namespace: &str, _properties: HashMap<String, String>) -> TeoDBResult<()> {
        Ok(())
    }

    async fn drop_namespace(&self, _namespace: &str) -> TeoDBResult<()> {
        Ok(())
    }

    async fn list_tables(&self, _namespace: &str) -> TeoDBResult<Vec<TableIdent>> {
        Ok(vec![TableIdent::new("ns", "events")])
    }

    async fn load_table(&self, ident: &TableIdent) -> TeoDBResult<Arc<TableMetadata>> {
        let call = self
            .load_table_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if call == 0 {
            Ok(self.metadata.clone())
        } else {
            Err(TeoDBError::NotFound {
                resource: ident.to_string(),
            })
        }
    }

    async fn create_table(&self, _req: CreateTableRequest) -> TeoDBResult<Arc<TableMetadata>> {
        Err(TeoDBError::Internal("not used".into()))
    }

    async fn drop_table(&self, _ident: &TableIdent) -> TeoDBResult<()> {
        Ok(())
    }

    async fn load_live_files(&self, _ident: &TableIdent) -> TeoDBResult<Vec<DataFile>> {
        Ok(vec![])
    }

    async fn load_all_referenced_file_paths(
        &self,
        _ident: &TableIdent,
    ) -> TeoDBResult<std::collections::HashSet<String>> {
        Ok(std::collections::HashSet::new())
    }

    async fn load_retained_file_paths(
        &self,
        _ident: &TableIdent,
        _retention: &teodb_core::snapshot_retention::SnapshotRetention,
        _protected: &std::collections::HashSet<teodb_core::ident::SnapshotId>,
    ) -> TeoDBResult<RetainedFileSet> {
        Ok(RetainedFileSet::default())
    }

    async fn commit_append(&self, _req: CommitAppend) -> TeoDBResult<Arc<TableMetadata>> {
        Err(TeoDBError::Internal("not used".into()))
    }

    async fn check_append_status(&self, _req: &CommitAppend) -> TeoDBResult<CommitStatus> {
        Ok(CommitStatus::NotCommitted)
    }

    async fn commit_replace(&self, _req: CommitReplace) -> TeoDBResult<Arc<TableMetadata>> {
        Err(TeoDBError::Internal("not used".into()))
    }

    async fn update_table_properties(
        &self,
        _ident: &TableIdent,
        _expected: HashMap<String, String>,
        _updates: HashMap<String, String>,
        _removals: Vec<String>,
    ) -> TeoDBResult<Arc<TableMetadata>> {
        Err(TeoDBError::Internal("not used".into()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_engine_falls_back_locally_when_scheduler_unreachable() {
    // Port 1 on localhost: connection refused, immediately.
    let observer = Arc::new(CountingObserver {
        fallbacks: std::sync::atomic::AtomicU64::new(0),
    });
    let engine = BallistaQueryEngineBuilder::remote(stub_factory(), "127.0.0.1:1")
        .unwrap()
        .event_observer(observer.clone() as Arc<dyn EngineEventObserver>)
        .build();

    let req = QueryRequest {
        sql: "SELECT 1 AS one".into(),
        principal: teodb_core::traits::authz::Principal {
            subject: "fallback-test".into(),
            roles: vec![],
            claims: HashMap::new(),
        },
        query_id: QueryId::new(),
        limit: None,
    };
    let query_id = req.query_id;

    let handle = engine.prepare(req).await.unwrap();
    let mut stream = engine.execute_stream(handle).await.unwrap();

    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch.unwrap().num_rows();
    }
    assert_eq!(rows, 1, "fallback must produce the query result locally");
    assert_eq!(
        observer
            .fallbacks
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "observer must record exactly one fallback"
    );
    assert!(matches!(
        engine.status(&query_id).await.unwrap(),
        QueryStatus::Completed
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_fallback_uses_prepared_plan_not_live_catalog() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = Arc::new(OneShotCatalog::new(metadata_with_snapshot()));
    let factory = test_session_factory_from_catalog(
        catalog.clone() as Arc<dyn Catalog>,
        teodb_query::DataFusionRuntimeConfig {
            memory_pool_bytes: 64 * 1024 * 1024,
            spill_dir: tmp.path().to_path_buf(),
        },
        teodb_query::DataFusionSessionConfig {
            batch_size: 1024,
            target_partitions: 1,
            ..Default::default()
        },
    );
    let engine = BallistaQueryEngineBuilder::remote(factory, "127.0.0.1:1")
        .unwrap()
        .build();

    let req = QueryRequest {
        sql: "SELECT id FROM ns.events".into(),
        principal: teodb_core::traits::authz::Principal {
            subject: "fallback-snapshot-test".into(),
            roles: vec![],
            claims: HashMap::new(),
        },
        query_id: QueryId::new(),
        limit: None,
    };

    let handle = engine.prepare(req).await.unwrap();
    assert_eq!(catalog.load_table_calls(), 1);

    let mut stream = engine.execute_stream(handle).await.unwrap();
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch.unwrap().num_rows();
    }

    assert_eq!(rows, 0, "the prepared empty snapshot should execute locally");
    assert_eq!(
        catalog.load_table_calls(),
        1,
        "fallback must not re-resolve the table from the live catalog"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_engine_fails_without_fallback_when_disabled() {
    let engine = BallistaQueryEngineBuilder::remote(stub_factory(), "127.0.0.1:1")
        .unwrap()
        .local_fallback(false)
        .build();

    let req = QueryRequest {
        sql: "SELECT 1 AS one".into(),
        principal: teodb_core::traits::authz::Principal {
            subject: "no-fallback-test".into(),
            roles: vec![],
            claims: HashMap::new(),
        },
        query_id: QueryId::new(),
        limit: None,
    };

    let handle = engine.prepare(req).await.unwrap();
    let mut stream = engine.execute_stream(handle).await.unwrap();
    let first = stream.next().await;
    assert!(
        matches!(first, Some(Err(_))),
        "without fallback the connectivity error must surface, got: {first:?}"
    );
}

fn metadata_with_snapshot() -> Arc<TableMetadata> {
    table_metadata_with_snapshot("file:///data/events", 7)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_pins_scanned_snapshot_and_handle_drop_releases() {
    let tmp = tempfile::tempdir().unwrap();
    let factory = test_session_factory(
        MockCatalog::builder()
            .namespaces(["ns"])
            .tables([TableIdent::new("ns", "events")])
            .serves("events", metadata_with_snapshot())
            .build(),
        teodb_query::DataFusionRuntimeConfig {
            memory_pool_bytes: 64 * 1024 * 1024,
            spill_dir: tmp.path().to_path_buf(),
        },
        teodb_query::DataFusionSessionConfig {
            batch_size: 1024,
            target_partitions: 1,
            ..Default::default()
        },
    );
    let registry = Arc::new(InMemorySnapshotRegistry::new());
    let engine = BallistaQueryEngineBuilder::standalone(factory, 1)
        .snapshot_registry(registry.clone() as Arc<dyn teodb_core::snapshot_pin::ActiveSnapshotRegistry>)
        .build();
    let table = TableIdent::new("ns", "events");

    let req = QueryRequest {
        sql: "SELECT id FROM ns.events".into(),
        principal: teodb_core::traits::authz::Principal {
            subject: "pin-test".into(),
            roles: vec![],
            claims: HashMap::new(),
        },
        query_id: QueryId::new(),
        limit: None,
    };

    let handle = engine.prepare(req).await.unwrap();
    assert_eq!(
        registry.active_snapshots(&table).await.unwrap(),
        vec![7],
        "prepare must pin the scanned table's current snapshot"
    );

    // A handle dropped without execution must release its pins.
    drop(handle);
    wait_released(&registry, &table).await;
}
