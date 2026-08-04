//! Crash-recovery integration suite.
//!
//! Simulates crashes by dropping the WAL manager *without* releasing its
//! lease, then drives the full startup recovery path
//! (`teodb_ingest::replay::Replayer`: catalog seeding → WAL replay →
//! buffer re-injection → idempotency rebuild → GC) and asserts on what a
//! client would observe:
//! - every ACKed ingest replays exactly once (I1);
//! - a torn tail (crash mid-append) is tolerated;
//! - a corrupt mid-segment frame aborts startup in `fail` mode and
//!   quarantines in `salvage` mode;
//! - flushed-and-committed data is not re-replayed, even when the local
//!   committed checkpoint was lost (catalog is authoritative).

use std::collections::HashMap;
use std::sync::Arc;

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::file::{DataFile, TableMetadata};
use teodb_core::ident::TableIdent;
use teodb_core::location::ObjectLocation;
use teodb_core::schema::{ColumnMeta, PartitionSpec, SchemaDefinition, SortOrder, TeoDataType};
use teodb_core::traits::catalog::{Catalog, CommitAppend, CommitReplace, CommitStatus, CreateTableRequest};
use teodb_ingest::buffer::BufferRegistry;
use teodb_ingest::idempotency::IdempotencyIndex;
use teodb_ingest::replay::Replayer;
use teodb_ingest::service::{IngestOutcome, IngestService};
use teodb_storage::wal::{FrameDecode, WalConfig, WalManager, WalRecoveryMode, decode_frame};

// Catalog stub

const TABLE_UUID: uuid::Uuid = uuid::Uuid::from_u128(0x1111);

/// Catalog that knows one stable incarnation of `default.events` and can
/// expose the current writer's durable checkpoint.
struct StubCatalog {
    table_uuid: uuid::Uuid,
    checkpoint: Option<(
        teodb_core::write_protocol::WriterId,
        teodb_core::write_protocol::WriterCheckpoint,
    )>,
    additional_properties: HashMap<String, String>,
    unavailable: bool,
    stale_commit_epoch: Option<teodb_core::write_protocol::WriterEpoch>,
}

impl StubCatalog {
    fn new() -> Self {
        Self {
            table_uuid: TABLE_UUID,
            checkpoint: None,
            additional_properties: HashMap::new(),
            unavailable: false,
            stale_commit_epoch: None,
        }
    }

    fn with_checkpoint(identity: &teodb_core::write_protocol::ResolvedIdentity, generation: u64) -> Self {
        Self::with_checkpoint_epoch(identity, generation, identity.writer_epoch)
    }

    fn with_checkpoint_epoch(
        identity: &teodb_core::write_protocol::ResolvedIdentity,
        generation: u64,
        epoch: teodb_core::write_protocol::WriterEpoch,
    ) -> Self {
        Self::with_exact_checkpoint(
            identity,
            generation,
            epoch,
            teodb_core::write_protocol::CommitId::from_uuid(uuid::Uuid::from_u128(0x2222)),
        )
    }

    fn with_exact_checkpoint(
        identity: &teodb_core::write_protocol::ResolvedIdentity,
        generation: u64,
        epoch: teodb_core::write_protocol::WriterEpoch,
        commit_id: teodb_core::write_protocol::CommitId,
    ) -> Self {
        Self {
            table_uuid: TABLE_UUID,
            checkpoint: Some((
                identity.writer_id,
                teodb_core::write_protocol::WriterCheckpoint::new(
                    epoch,
                    generation,
                    commit_id,
                    chrono::Utc::now().timestamp_millis(),
                ),
            )),
            additional_properties: HashMap::new(),
            unavailable: false,
            stale_commit_epoch: None,
        }
    }

    fn with_foreign_checkpoint(generation: u64) -> Self {
        let writer_id = teodb_core::write_protocol::WriterId::from_uuid(uuid::Uuid::from_u128(0x9999));
        let checkpoint = teodb_core::write_protocol::WriterCheckpoint::new(
            teodb_core::write_protocol::WriterEpoch::new(3),
            generation,
            teodb_core::write_protocol::CommitId::from_uuid(uuid::Uuid::from_u128(0x8888)),
            1,
        );
        Self {
            additional_properties: HashMap::from([(
                teodb_core::write_protocol::writer_checkpoint_key(writer_id),
                checkpoint.encode().unwrap(),
            )]),
            ..Self::new()
        }
    }

    fn with_malformed_foreign_checkpoint() -> Self {
        let writer_id = teodb_core::write_protocol::WriterId::from_uuid(uuid::Uuid::from_u128(0x9999));
        Self {
            additional_properties: HashMap::from([(
                teodb_core::write_protocol::writer_checkpoint_key(writer_id),
                "{not-json".into(),
            )]),
            ..Self::new()
        }
    }

    fn wrong_incarnation() -> Self {
        Self {
            table_uuid: uuid::Uuid::from_u128(0xdead),
            ..Self::new()
        }
    }

    fn unavailable() -> Self {
        Self {
            unavailable: true,
            ..Self::new()
        }
    }

    fn with_stale_commit_rejection(
        identity: &teodb_core::write_protocol::ResolvedIdentity,
        committed_generation: u64,
        current_epoch: teodb_core::write_protocol::WriterEpoch,
    ) -> Self {
        Self {
            stale_commit_epoch: Some(current_epoch),
            ..Self::with_checkpoint_epoch(identity, committed_generation, current_epoch)
        }
    }
}

fn metadata_with_properties(properties: HashMap<String, String>) -> TableMetadata {
    metadata_with_uuid_properties(TABLE_UUID, properties)
}

fn metadata_with_uuid_properties(table_uuid: uuid::Uuid, properties: HashMap<String, String>) -> TableMetadata {
    TableMetadata {
        table_uuid,
        namespace: "default".into(),
        table_name: "events".into(),
        table_location: ObjectLocation::parse("s3://warehouse/default/events").expect("test table location"),
        current_snapshot_id: None,
        current_schema_id: 0,
        current_partition_spec_id: 0,
        current_sort_order_id: 0,
        schemas: vec![SchemaDefinition {
            schema_id: 0,
            columns: vec![ColumnMeta {
                id: 1,
                name: "id".into(),
                data_type: TeoDataType::Int64,
                nullable: false,
                doc: None,
            }],
            identifier_field_ids: vec![1],
        }],
        partition_specs: vec![PartitionSpec {
            spec_id: 0,
            fields: vec![],
        }],
        sort_orders: vec![SortOrder {
            order_id: 0,
            fields: vec![],
        }],
        snapshots: vec![],
        current_snapshot: None,
        properties,
    }
}

fn base_metadata() -> TableMetadata {
    metadata_with_properties(HashMap::new())
}

#[async_trait::async_trait]
impl Catalog for StubCatalog {
    async fn list_namespaces(&self) -> TeoDBResult<Vec<String>> {
        Ok(vec!["default".into()])
    }

    async fn create_namespace(&self, _namespace: &str, _properties: HashMap<String, String>) -> TeoDBResult<()> {
        Ok(())
    }

    async fn drop_namespace(&self, _namespace: &str) -> TeoDBResult<()> {
        Ok(())
    }

    async fn list_tables(&self, ns: &str) -> TeoDBResult<Vec<TableIdent>> {
        if ns == "default" {
            Ok(vec![TableIdent::new("default", "events")])
        } else {
            Ok(vec![])
        }
    }

    async fn load_table(&self, ident: &TableIdent) -> TeoDBResult<Arc<TableMetadata>> {
        if self.unavailable {
            return Err(TeoDBError::Unavailable("catalog unavailable during recovery".into()));
        }
        if ident.namespace == "default" && ident.name == "events" {
            let mut properties = self.additional_properties.clone();
            if let Some((writer_id, checkpoint)) = self.checkpoint.as_ref() {
                properties.insert(
                    teodb_core::write_protocol::writer_checkpoint_key(*writer_id),
                    checkpoint.encode()?,
                );
            }
            Ok(Arc::new(metadata_with_uuid_properties(self.table_uuid, properties)))
        } else {
            Err(TeoDBError::NotFound {
                resource: format!("table {}.{}", ident.namespace, ident.name),
            })
        }
    }

    async fn create_table(&self, _req: CreateTableRequest) -> TeoDBResult<Arc<TableMetadata>> {
        Ok(Arc::new(base_metadata()))
    }

    async fn drop_table(&self, _ident: &TableIdent) -> TeoDBResult<()> {
        Ok(())
    }

    async fn load_live_files(&self, _ident: &TableIdent) -> TeoDBResult<Vec<DataFile>> {
        Ok(vec![])
    }

    async fn commit_append(&self, req: CommitAppend) -> TeoDBResult<Arc<TableMetadata>> {
        if let Some(current_epoch) = self.stale_commit_epoch {
            return Err(TeoDBError::StaleWriterEpoch {
                table: req.table,
                writer_id: req.identity.writer_id,
                request_epoch: req.identity.writer_epoch,
                current_epoch,
            });
        }
        Ok(Arc::new(base_metadata()))
    }

    async fn check_append_status(&self, _req: &CommitAppend) -> TeoDBResult<CommitStatus> {
        Ok(CommitStatus::NotCommitted)
    }

    async fn commit_replace(&self, _req: CommitReplace) -> TeoDBResult<Arc<TableMetadata>> {
        Ok(Arc::new(base_metadata()))
    }

    async fn update_table_properties(
        &self,
        _ident: &TableIdent,
        _expected: HashMap<String, String>,
        _updates: HashMap<String, String>,
        _removals: Vec<String>,
    ) -> TeoDBResult<Arc<TableMetadata>> {
        Ok(Arc::new(base_metadata()))
    }
}

// Fixture

fn wal_config(dir: &std::path::Path, mode: WalRecoveryMode) -> WalConfig {
    WalConfig {
        root_dir: dir.to_path_buf(),
        fsync_on_append: false,
        recovery_mode: mode,
        ..Default::default()
    }
}

struct CrashNode {
    ingest: IngestService,
}

struct CrashServices {
    wal: Arc<WalManager>,
}

struct CrashState {
    services: CrashServices,
}

/// Build the transport-independent ingest domain over a real WAL.
async fn build_node(wal_dir: &std::path::Path) -> (CrashNode, CrashState) {
    let catalog: Arc<dyn teodb_core::traits::catalog::Catalog> = Arc::new(StubCatalog::new());
    let wal = Arc::new(
        WalManager::open(wal_config(wal_dir, WalRecoveryMode::Fail))
            .await
            .expect("open WAL"),
    );
    let buffers = Arc::new(BufferRegistry::new(wal.clone(), 64 * 1024 * 1024, 48 * 1024 * 1024));
    let idempotency = Arc::new(IdempotencyIndex::new(std::time::Duration::from_secs(60), 1000));
    let ingest = IngestService::new(catalog, buffers, wal.clone(), idempotency, Arc::from("s3://warehouse"));
    (
        CrashNode { ingest },
        CrashState {
            services: CrashServices { wal },
        },
    )
}

/// "Restart" after a crash: reopen the WAL (the stale lease is reclaimed)
/// and run the full recovery path into fresh buffers.
async fn recover(
    wal_dir: &std::path::Path,
    mode: WalRecoveryMode,
    catalog: Arc<dyn Catalog>,
) -> (
    TeoDBResult<()>,
    Arc<WalManager>,
    Arc<BufferRegistry>,
    Arc<IdempotencyIndex>,
) {
    let wal = WalManager::open(wal_config(wal_dir, mode))
        .await
        .expect("reopen WAL after crash");
    let wal = Arc::new(wal);
    let buffers = Arc::new(BufferRegistry::new(wal.clone(), 64 * 1024 * 1024, 48 * 1024 * 1024));
    let idempotency = Arc::new(IdempotencyIndex::new(std::time::Duration::from_secs(60), 1000));

    let result = Replayer::new(
        Arc::clone(&wal),
        Arc::clone(&buffers),
        catalog,
        Arc::clone(&idempotency),
    )
    .replay_wal(None)
    .await;
    (result, wal, buffers, idempotency)
}

/// Ingest one row through the domain service, returning the durable ACK.
async fn ingest_row(node: &CrashNode, id: i64) -> (String, u64) {
    ingest_row_with_key(node, id, None).await
}

async fn ingest_row_with_key(node: &CrashNode, id: i64, idempotency_key: Option<&str>) -> (String, u64) {
    let rows = [serde_json::json!({ "id": id })];
    let outcome = node
        .ingest
        .ingest_rows(&TableIdent::new("default", "events"), &rows, idempotency_key)
        .await
        .expect("ingest must be ACKed");
    let receipt = match outcome {
        IngestOutcome::Accepted(receipt) | IngestOutcome::Deduplicated(receipt) => receipt,
    };
    (receipt.batch_id.to_string(), receipt.generation)
}

/// Locate the WAL segment holding the two ingest frames and return the byte
/// offset where the *second* frame starts.
///
/// Robust by construction: it sorts the segment files (so the choice is not at
/// the mercy of `read_dir` ordering) and returns only a segment that actually
/// holds a second complete frame — so the caller's `data[offset + N]` is always
/// in bounds. If no segment holds two complete frames it panics with the
/// segment names and sizes rather than letting the caller index out of bounds,
/// which turns any unexpected layout (e.g. an unforeseen rotation) into a
/// debuggable failure instead of an opaque `index out of bounds`.
fn segment_and_second_frame_offset(wal_dir: &std::path::Path) -> (std::path::PathBuf, usize) {
    let mut segments: Vec<std::path::PathBuf> = std::fs::read_dir(wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.to_string_lossy().ends_with(".wal"))
        .collect();
    segments.sort();
    assert!(!segments.is_empty(), "expected at least one WAL segment");

    for segment in &segments {
        let data = std::fs::read(segment).unwrap();
        let FrameDecode::Complete(_, first) = decode_frame(&data) else {
            continue;
        };
        if matches!(decode_frame(&data[first..]), FrameDecode::Complete(_, _)) {
            return (segment.clone(), first);
        }
    }

    let sizes: Vec<u64> = segments
        .iter()
        .map(|s| std::fs::metadata(s).map(|m| m.len()).unwrap_or(0))
        .collect();
    panic!("no WAL segment held two complete frames; segments={segments:?} sizes={sizes:?}");
}

// Tests

#[tokio::test]
async fn acked_ingests_replay_exactly_once_after_crash() {
    let dir = tempfile::tempdir().unwrap();
    let table = TableIdent::new("default", "events");

    let acked = {
        let (app, state) = build_node(dir.path()).await;
        let mut acked = Vec::new();
        for id in 1..=3 {
            acked.push(ingest_row(&app, id).await);
        }
        // Crash: drop the node without flushing or releasing the WAL lease.
        drop(app);
        drop(state);
        acked
    };

    let (result, _wal, buffers, _idx) = recover(dir.path(), WalRecoveryMode::Fail, Arc::new(StubCatalog::new())).await;
    result.expect("recovery succeeds");

    let buffer = buffers.get(&table).expect("buffer rebuilt");
    let snapshot = buffer.snapshot_for_query();
    assert_eq!(
        snapshot.batches.len(),
        acked.len(),
        "every ACKed batch replays exactly once"
    );

    let mut replayed: Vec<(String, u64)> = snapshot
        .batches
        .iter()
        .map(|e| (e.batch_id.to_string(), e.generation))
        .collect();
    replayed.sort_by_key(|(_, generation)| *generation);
    assert_eq!(replayed, acked, "batch ids and generations match the ACKs");
    assert!(
        snapshot
            .batches
            .iter()
            .all(|e| e.batch.num_rows() == 1),
        "payloads survive intact"
    );
}

#[tokio::test]
async fn replay_orders_out_of_order_generations_before_prefix_flushing() {
    let source_dir = tempfile::tempdir().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let table = TableIdent::new("default", "events");

    {
        let (app, state) = build_node(source_dir.path()).await;
        for id in 1..=3 {
            ingest_row(&app, id).await;
        }
        drop(app);
        drop(state);
    }

    let source_wal = WalManager::open(wal_config(source_dir.path(), WalRecoveryMode::Fail))
        .await
        .expect("reopen source WAL after crash");
    let records = source_wal
        .prepare_replay_all()
        .await
        .expect("prepare source WAL replay")
        .collect()
        .await
        .expect("inspect source WAL records");
    assert_eq!(records.len(), 3);

    // Recreate the production race deterministically: generation reservation
    // occurs before asynchronous WAL persistence, so generation 2 can land
    // physically before generation 1 for the same table.
    let target_wal = WalManager::open(wal_config(dir.path(), WalRecoveryMode::Fail))
        .await
        .expect("open out-of-order WAL");
    target_wal.append(&records[1]).await.unwrap();
    target_wal.append(&records[0]).await.unwrap();
    target_wal.append(&records[2]).await.unwrap();
    drop(target_wal);

    let wal = Arc::new(
        WalManager::open(wal_config(dir.path(), WalRecoveryMode::Fail))
            .await
            .expect("reopen out-of-order WAL after crash"),
    );
    let batch_bytes: Vec<u64> = records
        .iter()
        .map(|record| {
            record
                .batch
                .columns()
                .iter()
                .map(|column| column.get_buffer_memory_size() as u64)
                .sum()
        })
        .collect();
    assert!(
        batch_bytes
            .iter()
            .all(|bytes| *bytes == batch_bytes[0])
    );
    let max_bytes = batch_bytes[0];
    drop(records);

    let catalog: Arc<dyn Catalog> = Arc::new(StubCatalog::new());
    let buffers = Arc::new(BufferRegistry::new(wal.clone(), max_bytes, max_bytes));
    let idempotency = Arc::new(IdempotencyIndex::new(std::time::Duration::from_secs(60), 1000));
    let flusher = teodb_ingest::flush::Flusher::new(
        buffers.clone(),
        catalog.clone(),
        teodb_test_support::single_backend_factory(teodb_test_support::in_memory_backend()),
        wal.clone(),
    );

    Replayer::new(wal.clone(), buffers.clone(), catalog, idempotency)
        .with_recovery_flusher(flusher)
        .replay_wal(None)
        .await
        .expect("replay flushes a committed prefix and continues");

    let buffer = buffers.get(&table).expect("buffer rebuilt");
    let stats = buffer.buffer_stats();
    assert!(stats.pending_bytes + stats.in_flight_bytes <= max_bytes);
    let committed = wal
        .committed_generation(&teodb_core::write_protocol::WalTableKey::new(TABLE_UUID, table))
        .await
        .expect("recovery flush advances the durable checkpoint");
    assert_eq!(committed, 2, "only the contiguous generations 1..=2 flush");
    let buffered_generations: Vec<_> = buffer
        .snapshot_for_query()
        .batches
        .iter()
        .map(|entry| entry.generation)
        .collect();
    assert_eq!(buffered_generations, vec![3]);
}

#[tokio::test]
async fn torn_tail_is_tolerated_and_acked_prefix_replays() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (app, _state) = build_node(dir.path()).await;
        ingest_row(&app, 1).await;
        ingest_row(&app, 2).await;
    }

    // Crash mid-append: cut the second frame in half.
    let (segment, second_frame) = segment_and_second_frame_offset(dir.path());
    let mut data = std::fs::read(&segment).unwrap();
    let torn_len = second_frame + (data.len() - second_frame) / 2;
    data.truncate(torn_len);
    std::fs::write(&segment, &data).unwrap();

    let (result, _wal, buffers, _idx) = recover(dir.path(), WalRecoveryMode::Fail, Arc::new(StubCatalog::new())).await;
    result.expect("torn tail is benign even in fail mode");

    let buffer = buffers
        .get(&TableIdent::new("default", "events"))
        .expect("buffer rebuilt");
    assert_eq!(
        buffer.snapshot_for_query().batches.len(),
        1,
        "the fully persisted record replays; the torn one was never durable"
    );
}

#[tokio::test]
async fn corrupt_mid_frame_aborts_startup_in_fail_mode() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (app, _state) = build_node(dir.path()).await;
        ingest_row(&app, 1).await;
        ingest_row(&app, 2).await;
    }

    // Flip a payload byte inside the second frame (CRC mismatch).
    let (segment, second_frame) = segment_and_second_frame_offset(dir.path());
    let mut data = std::fs::read(&segment).unwrap();
    data[second_frame + 10] ^= 0xFF;
    std::fs::write(&segment, &data).unwrap();

    let (result, _wal, buffers, _idx) = recover(dir.path(), WalRecoveryMode::Fail, Arc::new(StubCatalog::new())).await;
    let err = result.expect_err("fail mode must abort startup on corruption");
    assert!(err.to_string().contains("corrupt WAL frame"), "unexpected error: {err}");
    assert_eq!(buffers.table_count(), 0, "nothing was injected");
}

#[tokio::test]
async fn corrupt_mid_frame_salvages_prefix_in_salvage_mode() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (app, _state) = build_node(dir.path()).await;
        ingest_row(&app, 1).await;
        ingest_row(&app, 2).await;
    }

    let (segment, second_frame) = segment_and_second_frame_offset(dir.path());
    let mut data = std::fs::read(&segment).unwrap();
    data[second_frame + 10] ^= 0xFF;
    std::fs::write(&segment, &data).unwrap();

    let (result, _wal, buffers, _idx) =
        recover(dir.path(), WalRecoveryMode::Salvage, Arc::new(StubCatalog::new())).await;
    result.expect("salvage mode continues startup");

    let buffer = buffers
        .get(&TableIdent::new("default", "events"))
        .expect("buffer rebuilt");
    assert_eq!(
        buffer.snapshot_for_query().batches.len(),
        1,
        "frames before the corruption are preserved"
    );

    let names: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        names.iter().any(|n| n.ends_with(".wal.corrupt")),
        "corrupt segment quarantined: {names:?}"
    );
}

#[tokio::test]
async fn mw_t10_unreadable_wal_segment_aborts_recovery_before_injection() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (app, state) = build_node(dir.path()).await;
        ingest_row(&app, 1).await;
        drop(app);
        drop(state);
    }

    // A directory with a segment-shaped name is discoverable but cannot be
    // read as bytes on any supported platform. This pins the I/O-failure path
    // without permission semantics that differ for root/container users.
    std::fs::create_dir(dir.path().join("99999999999999999999.wal")).unwrap();

    let (result, _wal, buffers, _idx) = recover(dir.path(), WalRecoveryMode::Fail, Arc::new(StubCatalog::new())).await;
    let error = result.expect_err("an unreadable segment must fail closed");
    assert!(matches!(error, TeoDBError::Wal { .. }), "unexpected error: {error}");
    assert_eq!(
        buffers.table_count(),
        0,
        "startup failure must happen before any replay injection"
    );
}

#[tokio::test]
async fn flushed_then_crash_does_not_rereplay_committed_records() {
    let dir = tempfile::tempdir().unwrap();
    let table = TableIdent::new("default", "events");

    let identity = {
        let (app, state) = build_node(dir.path()).await;
        ingest_row(&app, 1).await; // generation 1 — flushed below
        ingest_row(&app, 2).await; // generation 2 — flushed below
        ingest_row(&app, 3).await; // generation 3 — ACKed but unflushed
        let identity = state.services.wal.writer_identity();
        drop(app);
        drop(state);
        identity
    };
    let checkpoint = dir.path().join("committed.json");
    if checkpoint.exists() {
        std::fs::remove_file(&checkpoint).unwrap();
    }

    let catalog: Arc<dyn Catalog> = Arc::new(StubCatalog::with_checkpoint(&identity, 2));
    let (result, _wal, buffers, _idx) = recover(dir.path(), WalRecoveryMode::Fail, catalog).await;
    result.expect("recovery succeeds");

    let buffer = buffers.get(&table).expect("buffer rebuilt");
    let snapshot = buffer.snapshot_for_query();
    assert_eq!(snapshot.batches.len(), 1, "only the unflushed record replays");
    assert_eq!(snapshot.batches[0].generation, 3);
    assert_eq!(
        buffer.committed_high_water(),
        2,
        "committed cutoff seeded from the catalog snapshot"
    );
}

#[tokio::test]
async fn idempotent_retry_after_crash_is_deduplicated() {
    let dir = tempfile::tempdir().unwrap();
    let table = TableIdent::new("default", "events");

    let (first_batch_id, _) = {
        let (app, _state) = build_node(dir.path()).await;
        let (batch_id, _) = ingest_row_with_key(&app, 1, Some("k-crash")).await;
        (batch_id, ())
    };

    let (result, _wal, buffers, idempotency) =
        recover(dir.path(), WalRecoveryMode::Fail, Arc::new(StubCatalog::new())).await;
    result.expect("recovery succeeds");
    assert!(buffers.get(&table).is_some());

    // The client retries the same logical batch after the restart.
    match idempotency.claim(&table, "k-crash") {
        teodb_ingest::idempotency::Claim::Duplicate(receipt) => {
            assert_eq!(
                receipt.batch_id.to_string(),
                first_batch_id,
                "original receipt survives the crash"
            );
        }
        other => panic!("expected Duplicate after recovery, got {other:?}"),
    }
}

#[tokio::test]
async fn mw_t2_foreign_writer_checkpoint_does_not_suppress_local_wal() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (app, state) = build_node(dir.path()).await;
        ingest_row(&app, 1).await;
        drop(app);
        drop(state);
    }

    let (result, _wal, buffers, _idx) = recover(
        dir.path(),
        WalRecoveryMode::Fail,
        Arc::new(StubCatalog::with_foreign_checkpoint(100)),
    )
    .await;
    result.expect("foreign high water is not local progress");
    let buffer = buffers
        .get(&TableIdent::new("default", "events"))
        .expect("local WAL batch replayed");
    assert_eq!(buffer.snapshot_for_query().batches.len(), 1);
    assert_eq!(buffer.committed_high_water(), 0);
}

#[tokio::test]
async fn mw_t8_recovery_rejects_recreated_table_incarnation_before_injection() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (app, state) = build_node(dir.path()).await;
        ingest_row(&app, 1).await;
        drop(app);
        drop(state);
    }

    let (result, _wal, buffers, _idx) = recover(
        dir.path(),
        WalRecoveryMode::Fail,
        Arc::new(StubCatalog::wrong_incarnation()),
    )
    .await;
    assert!(matches!(result, Err(TeoDBError::TableIncarnationMismatch { .. })));
    assert_eq!(buffers.table_count(), 0);
}

#[tokio::test]
async fn mw_t9_catalog_unavailability_aborts_recovery_before_injection() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (app, state) = build_node(dir.path()).await;
        ingest_row(&app, 1).await;
        drop(app);
        drop(state);
    }

    let (result, _wal, buffers, _idx) =
        recover(dir.path(), WalRecoveryMode::Fail, Arc::new(StubCatalog::unavailable())).await;
    assert!(matches!(result, Err(TeoDBError::Unavailable(_))));
    assert_eq!(buffers.table_count(), 0);
}

#[tokio::test]
async fn malformed_foreign_checkpoint_aborts_recovery() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (app, state) = build_node(dir.path()).await;
        ingest_row(&app, 1).await;
        drop(app);
        drop(state);
    }

    let (result, _wal, buffers, _idx) = recover(
        dir.path(),
        WalRecoveryMode::Fail,
        Arc::new(StubCatalog::with_malformed_foreign_checkpoint()),
    )
    .await;
    assert!(matches!(result, Err(TeoDBError::MetadataCorruption { .. })));
    assert_eq!(buffers.table_count(), 0);
}

#[tokio::test]
async fn recovery_fences_itself_above_catalog_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let identity = {
        let (app, state) = build_node(dir.path()).await;
        ingest_row(&app, 1).await;
        ingest_row(&app, 2).await;
        let identity = state.services.wal.writer_identity();
        drop(app);
        drop(state);
        identity
    };
    let observed_epoch = teodb_core::write_protocol::WriterEpoch::new(10);
    let catalog: Arc<dyn Catalog> = Arc::new(StubCatalog::with_checkpoint_epoch(&identity, 1, observed_epoch));
    let (result, wal, buffers, _idx) = recover(dir.path(), WalRecoveryMode::Fail, catalog).await;
    result.expect("epoch reconciliation succeeds");

    assert!(
        wal.writer_identity().writer_epoch > observed_epoch,
        "the restarted process must fence itself above catalog state"
    );
    let buffer = buffers
        .get(&TableIdent::new("default", "events"))
        .expect("generation two remains");
    assert_eq!(buffer.snapshot_for_query().batches.len(), 1);
    assert_eq!(buffer.snapshot_for_query().batches[0].generation, 2);
}

#[tokio::test]
async fn mw_t4_crash_after_catalog_commit_resolves_sidecar_without_replaying_range() {
    let dir = tempfile::tempdir().unwrap();
    let (identity, commit_id) = {
        let (app, state) = build_node(dir.path()).await;
        let (_, generation) = ingest_row(&app, 1).await;
        let identity = state.services.wal.writer_identity();
        let commit_id = teodb_core::write_protocol::CommitId::now_v7();
        let prepared = teodb_storage::wal::PreparedFlush::new(
            TableIdent::new("default", "events"),
            TABLE_UUID,
            identity.writer_id,
            identity.writer_epoch,
            commit_id,
            teodb_core::write_protocol::GenerationRange::new(generation, generation).unwrap(),
            1,
            chrono::Utc::now().timestamp_millis(),
            vec![teodb_core::file::DataFile {
                content: teodb_core::file::DataContent::Data,
                path: teodb_core::location::ObjectLocation {
                    scheme: teodb_core::location::StorageScheme::S3,
                    bucket: Some("warehouse".into()),
                    key: format!("default/events/data/{}/{commit_id}-f0000.parquet", identity.writer_id),
                },
                format: teodb_core::file::FileFormat::Parquet,
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
            }],
            None,
        );
        state
            .services
            .wal
            .persist_prepared(&prepared)
            .await
            .unwrap();
        drop(app);
        drop(state);
        (identity, commit_id)
    };

    let catalog: Arc<dyn Catalog> = Arc::new(StubCatalog::with_exact_checkpoint(
        &identity,
        1,
        identity.writer_epoch,
        commit_id,
    ));
    let (result, wal, buffers, _idx) = recover(dir.path(), WalRecoveryMode::Fail, catalog).await;
    result.expect("exact catalog proof completes the durable sidecar");

    let buffer = buffers
        .get(&TableIdent::new("default", "events"))
        .expect("checkpoint buffer initialized");
    assert!(buffer.snapshot_for_query().batches.is_empty());
    assert_eq!(buffer.committed_high_water(), 1);
    assert!(wal.list_prepared().await.unwrap().is_empty());
}

#[tokio::test]
async fn stale_sidecar_is_contained_without_blocking_startup() {
    let dir = tempfile::tempdir().unwrap();
    let (identity, commit_id) = {
        let (app, state) = build_node(dir.path()).await;
        ingest_row(&app, 1).await;
        let (_, generation) = ingest_row(&app, 2).await;
        let identity = state.services.wal.writer_identity();
        let commit_id = teodb_core::write_protocol::CommitId::now_v7();
        let prepared = teodb_storage::wal::PreparedFlush::new(
            TableIdent::new("default", "events"),
            TABLE_UUID,
            identity.writer_id,
            identity.writer_epoch,
            commit_id,
            teodb_core::write_protocol::GenerationRange::new(generation, generation).unwrap(),
            1,
            chrono::Utc::now().timestamp_millis(),
            vec![teodb_core::file::DataFile {
                content: teodb_core::file::DataContent::Data,
                path: teodb_core::location::ObjectLocation {
                    scheme: teodb_core::location::StorageScheme::S3,
                    bucket: Some("warehouse".into()),
                    key: format!("default/events/data/{}/{commit_id}-f0000.parquet", identity.writer_id),
                },
                format: teodb_core::file::FileFormat::Parquet,
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
            }],
            None,
        );
        state
            .services
            .wal
            .persist_prepared(&prepared)
            .await
            .unwrap();
        drop(app);
        drop(state);
        (identity, commit_id)
    };

    let current_epoch = teodb_core::write_protocol::WriterEpoch::new(identity.writer_epoch.get() + 7);
    let catalog: Arc<dyn Catalog> = Arc::new(StubCatalog::with_stale_commit_rejection(&identity, 1, current_epoch));
    let (result, wal, buffers, _idx) = recover(dir.path(), WalRecoveryMode::Fail, catalog).await;
    result.expect("a valid but stale sidecar must degrade only its table");

    let buffer = buffers
        .get(&TableIdent::new("default", "events"))
        .expect("blocked table remains registered");
    let blocked = buffer
        .blocked_flush()
        .expect("stale intent is operator-visible");
    assert_eq!(blocked.prepared.commit_id, commit_id);
    assert_eq!(blocked.last_error_class, "StaleWriterEpoch");
    assert_eq!(blocked.status_check_attempts, 1);
    assert_eq!(buffer.snapshot_for_query().batches.len(), 1);
    assert_eq!(wal.list_prepared().await.unwrap().len(), 1);
}
