//! Real Iceberg REST + RustFS multi-writer release-gate coverage.
//!
//! The real-stack tests are ignored by default because they require Docker.
//! They use configured endpoints or a Testcontainers-managed stack locally;
//! CI uses the pinned Compose stack and runs this binary serially through
//! `scripts/ci/multi-writer-release-gate.sh`.

use std::sync::Arc;

use teodb_core::error::TeoDBError;
use teodb_core::ident::TableIdent;
use teodb_core::traits::catalog::Catalog;
use teodb_core::traits::storage::Storage;
use teodb_core::write_protocol::{ClusterId, parse_writer_checkpoint, writer_checkpoint_count};
use teodb_ingest::flush::{FlushOutcome, Flusher};
use teodb_ingest::service::{IngestOutcome, IngestService};
use teodb_query::ddl::{PartitionFieldDef, PartitionTransformDef};
use teodb_test_support::{FaultInjectingStorage, StorageFault, StorageOperation};

mod support;

use support::catalog_proxy::CatalogProxy;
use support::rustfs::{
    TestEnv, assert_live_parquet, create_table, id_column, objects_under, purge_table, string_column, table_plan,
};
use support::writer::WriterHarness;

async fn ingest(service: &IngestService, ident: &TableIdent, id: i64, key: Option<&str>) -> IngestOutcome {
    service
        .ingest_rows(ident, &[serde_json::json!({ "id": id })], key)
        .await
        .expect("durable ingest")
}

async fn flush_together(writer_a: &Flusher, writer_b: &Flusher, ident: &TableIdent) -> (FlushOutcome, FlushOutcome) {
    let start = Arc::new(tokio::sync::Barrier::new(3));
    let run_a = {
        let start = start.clone();
        async move {
            start.wait().await;
            writer_a.flush_table(ident).await
        }
    };
    let run_b = {
        let start = start.clone();
        async move {
            start.wait().await;
            writer_b.flush_table(ident).await
        }
    };
    let release = async move {
        start.wait().await;
    };
    let (a, b, ()) = tokio::join!(run_a, run_b, release);
    (a.expect("writer A flush"), b.expect("writer B flush"))
}

fn assert_one_row_commit(outcome: FlushOutcome) {
    assert!(matches!(outcome, FlushOutcome::Committed { record_count: 1, .. }));
}

#[tokio::test]
#[ignore = "requires Docker for RustFS + Iceberg REST"]
async fn real_rest_rustfs_two_writers_survive_restart_and_preserve_all_appends() {
    let env = TestEnv::resolve().await;
    let catalog = env.catalog().await;
    let backend = env.backend();
    let factory = env.factory(backend.clone());
    let namespace = env.unique_namespace("teodb_mw_restart");
    let ident = create_table(
        &env,
        catalog.clone(),
        factory.clone(),
        table_plan(&namespace, "events", vec![id_column()], vec![]),
    )
    .await;
    let cluster_id = ClusterId::from_uuid(uuid::Uuid::now_v7());
    let mut writer_a = WriterHarness::new(
        catalog.clone(),
        factory.clone(),
        env.warehouse.clone(),
        cluster_id,
        "writer-a",
    )
    .await
    .expect("start writer A");
    let writer_b = WriterHarness::new(
        catalog.clone(),
        factory.clone(),
        env.warehouse.clone(),
        cluster_id,
        "writer-b",
    )
    .await
    .expect("start writer B");

    ingest(&writer_a.runtime().ingest, &ident, 1, None).await;
    ingest(&writer_b.runtime().ingest, &ident, 2, None).await;
    let (first_a, first_b) = flush_together(&writer_a.runtime().flusher, &writer_b.runtime().flusher, &ident).await;
    assert_one_row_commit(first_a);
    assert_one_row_commit(first_b);

    let first_metadata = catalog
        .load_table(&ident)
        .await
        .expect("load first-wave metadata");
    assert_eq!(writer_checkpoint_count(&first_metadata.properties), 2);
    assert_live_parquet(catalog.as_ref(), backend.as_ref(), &ident, 2, 2).await;

    ingest(&writer_a.runtime().ingest, &ident, 3, None).await;
    let before_restart = writer_a.identity();
    writer_a.crash();
    assert!(!writer_a.is_ready());
    writer_a
        .restart()
        .await
        .expect("replay writer A WAL");
    let after_restart = writer_a.identity();
    assert_eq!(
        after_restart.writer_id, before_restart.writer_id,
        "writer ID is stable across process restart"
    );
    assert!(
        after_restart.writer_epoch > before_restart.writer_epoch,
        "restart must fence the old process with a higher epoch"
    );
    let replayed = writer_a
        .runtime()
        .buffers
        .get(&ident)
        .expect("uncommitted generation replayed")
        .snapshot_for_query();
    assert_eq!(replayed.committed_high_water, 1);
    assert_eq!(replayed.batches.len(), 1);
    assert_eq!(replayed.batches[0].generation, 2);
    assert_eq!(replayed.batches[0].batch.num_rows(), 1);

    ingest(&writer_b.runtime().ingest, &ident, 4, None).await;
    let (second_a, second_b) = flush_together(&writer_a.runtime().flusher, &writer_b.runtime().flusher, &ident).await;
    assert_one_row_commit(second_a);
    assert_one_row_commit(second_b);

    let metadata = catalog
        .load_table(&ident)
        .await
        .expect("load final metadata");
    assert_eq!(writer_checkpoint_count(&metadata.properties), 2);
    for (identity, expected_epoch) in [
        (writer_a.identity(), after_restart.writer_epoch),
        (writer_b.identity(), writer_b.identity().writer_epoch),
    ] {
        let checkpoint = parse_writer_checkpoint(&ident, &metadata.properties, identity.writer_id)
            .expect("parse writer checkpoint")
            .expect("writer checkpoint exists");
        assert_eq!(checkpoint.generation, 2);
        assert_eq!(checkpoint.epoch, expected_epoch);
    }

    let locations = assert_live_parquet(catalog.as_ref(), backend.as_ref(), &ident, 4, 4).await;
    let writer_ids = [
        writer_a.identity().writer_id.to_string(),
        writer_b.identity().writer_id.to_string(),
    ];
    for location in locations {
        assert!(
            writer_ids
                .iter()
                .any(|writer_id| location.key.contains(&format!("/{writer_id}/"))),
            "data path must carry one of the stable writer IDs: {}",
            location.key
        );
    }
    writer_a.assert_clean().await;
    writer_b.assert_clean().await;
    purge_table(&env, catalog, factory, ident).await;
}

#[tokio::test]
#[ignore = "requires Docker for RustFS + Iceberg REST"]
async fn real_two_nodes_with_one_idempotency_key_are_accepted_twice() {
    let env = TestEnv::resolve().await;
    let catalog = env.catalog().await;
    let backend = env.backend();
    let factory = env.factory(backend.clone());
    let namespace = env.unique_namespace("teodb_mw_idempotency");
    let ident = create_table(
        &env,
        catalog.clone(),
        factory.clone(),
        table_plan(&namespace, "events", vec![id_column()], vec![]),
    )
    .await;
    let cluster_id = ClusterId::from_uuid(uuid::Uuid::now_v7());
    let writer_a = WriterHarness::new(
        catalog.clone(),
        factory.clone(),
        env.warehouse.clone(),
        cluster_id,
        "writer-a",
    )
    .await
    .expect("start writer A");
    let writer_b = WriterHarness::new(
        catalog.clone(),
        factory.clone(),
        env.warehouse.clone(),
        cluster_id,
        "writer-b",
    )
    .await
    .expect("start writer B");

    let key = "shared-cross-node-key";
    let outcome_a = ingest(&writer_a.runtime().ingest, &ident, 10, Some(key)).await;
    let outcome_b = ingest(&writer_b.runtime().ingest, &ident, 20, Some(key)).await;
    assert!(
        matches!(outcome_a, IngestOutcome::Accepted(_)) && matches!(outcome_b, IngestOutcome::Accepted(_)),
        "§13.1: idempotency is writer-local; TeoDB does not provide external exactly-once across nodes"
    );

    let (flush_a, flush_b) = flush_together(&writer_a.runtime().flusher, &writer_b.runtime().flusher, &ident).await;
    assert_one_row_commit(flush_a);
    assert_one_row_commit(flush_b);
    let metadata = catalog
        .load_table(&ident)
        .await
        .expect("load idempotency-boundary metadata");
    assert_eq!(writer_checkpoint_count(&metadata.properties), 2);
    assert_live_parquet(catalog.as_ref(), backend.as_ref(), &ident, 2, 2).await;
    purge_table(&env, catalog, factory, ident).await;
}

#[tokio::test]
#[ignore = "requires Docker for RustFS + Iceberg REST"]
async fn real_mw_t9_catalog_outage_aborts_restart_before_admission() {
    let env = TestEnv::resolve().await;
    let proxy = CatalogProxy::start(&env.catalog_uri)
        .await
        .expect("start catalog proxy");
    let catalog = env.catalog_at(proxy.uri()).await;
    let backend = env.backend();
    let factory = env.factory(backend.clone());
    let namespace = env.unique_namespace("teodb_real_mw_t9");
    let ident = create_table(
        &env,
        catalog.clone(),
        factory.clone(),
        table_plan(&namespace, "events", vec![id_column()], vec![]),
    )
    .await;
    let mut writer = WriterHarness::new(
        catalog.clone(),
        factory.clone(),
        env.warehouse.clone(),
        ClusterId::from_uuid(uuid::Uuid::now_v7()),
        "writer-a",
    )
    .await
    .expect("start writer");
    ingest(&writer.runtime().ingest, &ident, 1, None).await;
    writer.crash();

    proxy.cut();
    let error = writer
        .restart()
        .await
        .expect_err("catalog outage must abort replay");
    assert!(
        matches!(
            error,
            TeoDBError::Catalog(_) | TeoDBError::Unavailable(_) | TeoDBError::ExternalRetryable(_)
        ),
        "unexpected outage error: {error}"
    );
    assert!(!writer.is_ready(), "failed replay must not expose write admission");
    assert!(
        catalog.load_live_files(&ident).await.is_err(),
        "cut proxy must also prevent an authoritative read"
    );

    proxy.restore();
    writer
        .restart()
        .await
        .expect("same WAL root recovers after catalog restoration");
    assert!(writer.is_ready());
    assert_eq!(
        writer
            .runtime()
            .buffers
            .get(&ident)
            .expect("replayed buffer")
            .snapshot_for_query()
            .batches
            .len(),
        1
    );
    assert_one_row_commit(
        writer
            .runtime()
            .flusher
            .flush_table(&ident)
            .await
            .expect("flush recovered row"),
    );
    assert_live_parquet(catalog.as_ref(), backend.as_ref(), &ident, 1, 1).await;
    writer.assert_clean().await;
    purge_table(&env, catalog, factory, ident).await;
}

#[tokio::test]
#[ignore = "requires Docker for RustFS + Iceberg REST"]
async fn real_mw_t10_unreadable_wal_aborts_restart_before_catalog_injection() {
    let env = TestEnv::resolve().await;
    let catalog = env.catalog().await;
    let backend = env.backend();
    let factory = env.factory(backend.clone());
    let namespace = env.unique_namespace("teodb_real_mw_t10");
    let ident = create_table(
        &env,
        catalog.clone(),
        factory.clone(),
        table_plan(&namespace, "events", vec![id_column()], vec![]),
    )
    .await;
    let mut writer = WriterHarness::new(
        catalog.clone(),
        factory.clone(),
        env.warehouse.clone(),
        ClusterId::from_uuid(uuid::Uuid::now_v7()),
        "writer-a",
    )
    .await
    .expect("start writer");
    ingest(&writer.runtime().ingest, &ident, 1, None).await;
    writer.crash();
    std::fs::create_dir(writer.wal_root().join("99999999999999999999.wal"))
        .expect("create portable unreadable WAL fixture");

    let error = writer
        .restart()
        .await
        .expect_err("unreadable WAL must abort replay");
    assert!(matches!(error, TeoDBError::Wal { .. }));
    assert!(!writer.is_ready());
    assert_live_parquet(catalog.as_ref(), backend.as_ref(), &ident, 0, 0).await;
    purge_table(&env, catalog, factory, ident).await;
}

struct FaultCase {
    env: TestEnv,
    catalog: Arc<dyn Catalog>,
    backend: Arc<dyn Storage>,
    fault_storage: Arc<FaultInjectingStorage>,
    factory: Arc<dyn teodb_core::traits::storage::StorageFactory>,
    writer: WriterHarness,
    ident: TableIdent,
}

impl FaultCase {
    async fn unpartitioned(prefix: &str) -> Self {
        let env = TestEnv::resolve().await;
        let catalog = env.catalog().await;
        let concrete_backend = env.backend();
        let backend: Arc<dyn Storage> = concrete_backend;
        let fault_storage = Arc::new(FaultInjectingStorage::new(backend.clone()));
        let factory = env.factory(fault_storage.clone());
        let namespace = env.unique_namespace(prefix);
        let ident = create_table(
            &env,
            catalog.clone(),
            factory.clone(),
            table_plan(&namespace, "events", vec![id_column()], vec![]),
        )
        .await;
        let writer = WriterHarness::new(
            catalog.clone(),
            factory.clone(),
            env.warehouse.clone(),
            ClusterId::from_uuid(uuid::Uuid::now_v7()),
            "writer-a",
        )
        .await
        .expect("start fault-case writer");
        Self {
            env,
            catalog,
            backend,
            fault_storage,
            factory,
            writer,
            ident,
        }
    }

    fn data_prefix(&self) -> String {
        format!("{}/{}/data/", self.ident.namespace, self.ident.name)
    }

    async fn assert_pending_without_sidecar(&self, expected_batches: usize) {
        let buffer = self
            .writer
            .runtime()
            .buffers
            .get(&self.ident)
            .expect("faulted table buffer");
        assert_eq!(buffer.snapshot_for_query().batches.len(), expected_batches);
        self.writer.assert_clean().await;
    }

    async fn cleanup(self) {
        purge_table(&self.env, self.catalog, self.factory, self.ident).await;
    }
}

#[tokio::test]
#[ignore = "requires Docker for RustFS + Iceberg REST"]
async fn real_rustfs_timeout_before_upload_retries_without_loss() {
    let case = FaultCase::unpartitioned("teodb_s3_timeout").await;
    ingest(&case.writer.runtime().ingest, &case.ident, 1, None).await;
    case.fault_storage.push(StorageFault::fail_next(
        StorageOperation::Put,
        "injected S3 request timeout",
    ));

    let error = case
        .writer
        .runtime()
        .flusher
        .flush_table(&case.ident)
        .await
        .expect_err("timeout must fail the first flush");
    assert!(error.is_retryable());
    case.assert_pending_without_sidecar(1).await;
    assert!(
        objects_under(case.backend.as_ref(), &case.data_prefix())
            .await
            .is_empty()
    );
    assert_live_parquet(case.catalog.as_ref(), case.backend.as_ref(), &case.ident, 0, 0).await;

    assert_one_row_commit(
        case.writer
            .runtime()
            .flusher
            .flush_table(&case.ident)
            .await
            .expect("fault-free retry"),
    );
    assert_live_parquet(case.catalog.as_ref(), case.backend.as_ref(), &case.ident, 1, 1).await;
    case.writer.assert_clean().await;
    case.cleanup().await;
}

#[tokio::test]
#[ignore = "requires Docker for RustFS + Iceberg REST"]
async fn real_rustfs_throttle_before_upload_retries_without_loss() {
    let case = FaultCase::unpartitioned("teodb_s3_throttle").await;
    ingest(&case.writer.runtime().ingest, &case.ident, 1, None).await;
    case.fault_storage.push(StorageFault::fail_next(
        StorageOperation::Put,
        "injected S3 SlowDown throttle",
    ));

    let error = case
        .writer
        .runtime()
        .flusher
        .flush_table(&case.ident)
        .await
        .expect_err("throttle must fail the first flush");
    assert!(error.is_retryable());
    assert_eq!(case.fault_storage.pending_faults(), 0);
    case.assert_pending_without_sidecar(1).await;
    assert_one_row_commit(
        case.writer
            .runtime()
            .flusher
            .flush_table(&case.ident)
            .await
            .expect("retry throttled flush"),
    );
    assert_live_parquet(case.catalog.as_ref(), case.backend.as_ref(), &case.ident, 1, 1).await;
    case.cleanup().await;
}

#[tokio::test]
#[ignore = "requires Docker for RustFS + Iceberg REST"]
async fn real_rustfs_lost_put_response_leaves_only_an_orphan_candidate() {
    let case = FaultCase::unpartitioned("teodb_s3_lost_response").await;
    ingest(&case.writer.runtime().ingest, &case.ident, 1, None).await;
    case.fault_storage
        .push(StorageFault::lose_next_response(
            StorageOperation::Put,
            "injected response loss after S3 accepted PUT",
        ));

    let error = case
        .writer
        .runtime()
        .flusher
        .flush_table(&case.ident)
        .await
        .expect_err("lost response must leave publication unstarted");
    assert!(error.is_retryable());
    case.assert_pending_without_sidecar(1).await;
    assert_eq!(
        objects_under(case.backend.as_ref(), &case.data_prefix())
            .await
            .len(),
        1,
        "delegated PUT leaves one physical orphan candidate"
    );
    assert_live_parquet(case.catalog.as_ref(), case.backend.as_ref(), &case.ident, 0, 0).await;

    assert_one_row_commit(
        case.writer
            .runtime()
            .flusher
            .flush_table(&case.ident)
            .await
            .expect("retry after response loss"),
    );
    assert_live_parquet(case.catalog.as_ref(), case.backend.as_ref(), &case.ident, 1, 1).await;
    assert_eq!(
        objects_under(case.backend.as_ref(), &case.data_prefix())
            .await
            .len(),
        2,
        "one live object and one orphan candidate remain physically"
    );
    case.cleanup().await;
}

#[tokio::test]
#[ignore = "requires Docker for RustFS + Iceberg REST"]
async fn real_rustfs_partial_partition_upload_never_reaches_catalog() {
    let env = TestEnv::resolve().await;
    let catalog = env.catalog().await;
    let concrete_backend = env.backend();
    let backend: Arc<dyn Storage> = concrete_backend;
    let fault_storage = Arc::new(FaultInjectingStorage::new(backend.clone()));
    let factory = env.factory(fault_storage.clone());
    let namespace = env.unique_namespace("teodb_s3_partial");
    let ident = create_table(
        &env,
        catalog.clone(),
        factory.clone(),
        table_plan(
            &namespace,
            "events",
            vec![id_column(), string_column(2, "region")],
            vec![PartitionFieldDef {
                column_name: "region".into(),
                transform: PartitionTransformDef::Identity,
            }],
        ),
    )
    .await;
    let writer = WriterHarness::new(
        catalog.clone(),
        factory.clone(),
        env.warehouse.clone(),
        ClusterId::from_uuid(uuid::Uuid::now_v7()),
        "writer-a",
    )
    .await
    .expect("start partitioned writer");
    writer
        .runtime()
        .ingest
        .ingest_rows(
            &ident,
            &[
                serde_json::json!({ "id": 1, "region": "eu" }),
                serde_json::json!({ "id": 2, "region": "us" }),
            ],
            None,
        )
        .await
        .expect("ingest two partitions");
    fault_storage.push(StorageFault::fail_nth(
        StorageOperation::Put,
        2,
        "injected failure after first partition upload",
    ));

    let error = writer
        .runtime()
        .flusher
        .flush_table(&ident)
        .await
        .expect_err("second partition upload must fail");
    assert!(error.is_retryable());
    writer.assert_clean().await;
    let data_prefix = format!("{namespace}/events/data/");
    assert_eq!(
        objects_under(backend.as_ref(), &data_prefix)
            .await
            .len(),
        1,
        "exactly one partition object uploaded before the failure"
    );
    assert_live_parquet(catalog.as_ref(), backend.as_ref(), &ident, 0, 0).await;
    assert_eq!(
        writer
            .runtime()
            .buffers
            .get(&ident)
            .expect("partitioned buffer")
            .snapshot_for_query()
            .batches
            .len(),
        1
    );

    let retry = writer
        .runtime()
        .flusher
        .flush_table(&ident)
        .await
        .expect("retry complete partition set");
    assert!(matches!(retry, FlushOutcome::Committed { record_count: 2, .. }));
    assert_live_parquet(catalog.as_ref(), backend.as_ref(), &ident, 2, 2).await;
    assert_eq!(
        objects_under(backend.as_ref(), &data_prefix)
            .await
            .len(),
        3,
        "two live files plus the first attempt's orphan candidate"
    );
    writer.assert_clean().await;
    purge_table(&env, catalog, factory, ident).await;
}

#[test]
fn pinned_compose_harness_is_checked_in() {
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    assert!(
        workspace
            .join("deploy/docker/docker-compose.rustfs.yaml")
            .is_file()
    );
    assert!(
        workspace
            .join("deploy/docker/docker-compose.rustfs.ci.yaml")
            .is_file()
    );
}
