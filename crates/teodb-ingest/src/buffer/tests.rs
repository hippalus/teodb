use super::*;
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use teodb_core::error::TeoDBError;
use teodb_core::file::TableMetadata;
use teodb_core::ident::TableIdent;
use teodb_core::location::{ObjectLocation, StorageScheme};
use teodb_core::schema::*;
use teodb_core::write_protocol::{ClusterId, CommitId, GenerationRange, WriterEpoch, WriterId, WriterSlot};
use teodb_storage::wal::PreparedFlush;

fn test_metadata() -> Arc<TableMetadata> {
    Arc::new(TableMetadata {
        table_uuid: uuid::Uuid::nil(),
        namespace: "test".into(),
        table_name: "buf_test".into(),
        table_location: ObjectLocation {
            scheme: StorageScheme::Local,
            bucket: None,
            key: "test/buf_test".into(),
        },
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
        properties: HashMap::new(),
    })
}

fn test_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap()
}

fn test_batch_byte_size(batch: &RecordBatch) -> u64 {
    batch
        .columns()
        .iter()
        .map(|c| c.get_buffer_memory_size() as u64)
        .sum()
}

fn test_prepared(buffer: &TableBuffer, generation: u64, commit_id: CommitId) -> PreparedFlush {
    let writer_id = WriterId::derive(
        ClusterId::from_uuid(uuid::Uuid::nil()),
        &WriterSlot::new("buffer-test").unwrap(),
    );
    PreparedFlush::new(
        buffer.ident().clone(),
        buffer.table_uuid(),
        writer_id,
        WriterEpoch::new(1),
        commit_id,
        GenerationRange::new(generation, generation).unwrap(),
        3,
        1,
        Vec::new(),
        None,
    )
}

async fn test_registry() -> (tempfile::TempDir, BufferRegistry) {
    let directory = tempfile::tempdir().unwrap();
    let wal = Arc::new(
        teodb_storage::wal::WalManager::open(teodb_storage::wal::WalConfig {
            root_dir: directory.path().to_path_buf(),
            fsync_on_append: false,
            ..Default::default()
        })
        .await
        .unwrap(),
    );
    let registry = BufferRegistry::new(wal, 1024 * 1024, 512 * 1024);
    (directory, registry)
}

#[test]
fn insert_and_snapshot() {
    let meta = test_metadata();
    let buf = TableBuffer::new(TableIdent::new("test", "buf_test"), meta, 0, 1024 * 1024, 512 * 1024);

    let ok1 = buf
        .insert(uuid::Uuid::now_v7(), test_batch())
        .unwrap();
    assert_eq!(ok1.generation, 1);
    assert!(!ok1.backpressure_signal);

    let ok2 = buf
        .insert(uuid::Uuid::now_v7(), test_batch())
        .unwrap();
    assert_eq!(ok2.generation, 2);

    let snap = buf.snapshot_for_query();
    assert_eq!(snap.committed_high_water, 0);
    assert_eq!(snap.batches.len(), 2);
}

#[test]
fn drain_and_commit() {
    let meta = test_metadata();
    let buf = TableBuffer::new(
        TableIdent::new("test", "buf_test"),
        meta.clone(),
        0,
        1024 * 1024,
        512 * 1024,
    );

    buf.insert(uuid::Uuid::now_v7(), test_batch())
        .unwrap();
    buf.insert(uuid::Uuid::now_v7(), test_batch())
        .unwrap();

    let in_flight = buf.drain_pending_to_in_flight();
    assert_eq!(in_flight.len(), 2);

    // Snapshot still sees in-flight entries
    let snap = buf.snapshot_for_query();
    assert_eq!(snap.batches.len(), 2);

    // Commit
    buf.mark_committed(2, meta).unwrap();

    assert_eq!(buf.committed_high_water(), 2);
    let snap = buf.snapshot_for_query();
    assert_eq!(snap.batches.len(), 0);
    assert!(!buf.has_pending());
}

#[test]
fn committed_high_water_never_regresses() {
    let meta = test_metadata();
    let buf = TableBuffer::new(
        TableIdent::new("test", "buf_test"),
        meta.clone(),
        10,
        1024 * 1024,
        512 * 1024,
    );
    buf.mark_committed(5, meta).unwrap();
    assert_eq!(buf.committed_high_water(), 10);
}

#[test]
fn exhausted_generation_space_fails_closed() {
    let meta = test_metadata();
    let buf = TableBuffer::new(
        TableIdent::new("test", "buf_test"),
        meta,
        u64::MAX,
        1024 * 1024,
        512 * 1024,
    );
    assert!(matches!(
        buf.reserve(&test_batch()),
        Err(TeoDBError::WriteProtocol { .. })
    ));
    assert!(matches!(
        buf.insert(uuid::Uuid::now_v7(), test_batch()),
        Err(TeoDBError::WriteProtocol { .. })
    ));
}

#[test]
fn flush_failure_merges_back() {
    let meta = test_metadata();
    let buf = TableBuffer::new(TableIdent::new("test", "buf_test"), meta, 0, 1024 * 1024, 512 * 1024);

    buf.insert(uuid::Uuid::now_v7(), test_batch())
        .unwrap();
    let in_flight = buf.drain_pending_to_in_flight();
    assert_eq!(in_flight.len(), 1);

    // Insert another while flushing
    buf.insert(uuid::Uuid::now_v7(), test_batch())
        .unwrap();

    // Flush fails
    buf.rollback_unprepared_flush().unwrap();

    // Both entries back in pending
    let snap = buf.snapshot_for_query();
    assert_eq!(snap.batches.len(), 2);
    assert!(buf.has_pending());
}

#[test]
fn unprepared_rollback_refuses_prepared_and_blocked_owners() {
    let buffer = TableBuffer::new(
        TableIdent::new("test", "buf_test"),
        test_metadata(),
        0,
        1024 * 1024,
        512 * 1024,
    );
    let inserted = buffer
        .insert(uuid::Uuid::now_v7(), test_batch())
        .unwrap();
    buffer.drain_pending_to_in_flight();
    let prepared = test_prepared(&buffer, inserted.generation, CommitId::now_v7());
    buffer.set_prepared(prepared.clone()).unwrap();

    assert!(matches!(
        buffer.rollback_unprepared_flush(),
        Err(TeoDBError::WriteProtocol { .. })
    ));
    assert_eq!(buffer.prepared_flush().unwrap().commit_id, prepared.commit_id);

    buffer
        .mark_flush_blocked(&prepared, "unknown".into(), 1)
        .unwrap();
    assert!(matches!(
        buffer.rollback_unprepared_flush(),
        Err(TeoDBError::WriteProtocol { .. })
    ));
    assert_eq!(buffer.blocked_flush().unwrap().prepared.commit_id, prepared.commit_id);
}

#[test]
fn prepared_rollback_requires_exact_owner() {
    let buffer = TableBuffer::new(
        TableIdent::new("test", "buf_test"),
        test_metadata(),
        0,
        1024 * 1024,
        512 * 1024,
    );
    let inserted = buffer
        .insert(uuid::Uuid::now_v7(), test_batch())
        .unwrap();
    buffer.drain_pending_to_in_flight();
    let prepared = test_prepared(&buffer, inserted.generation, CommitId::now_v7());
    let wrong = test_prepared(&buffer, inserted.generation, CommitId::now_v7());
    buffer.set_prepared(prepared.clone()).unwrap();

    assert!(matches!(
        buffer.mark_flush_failed(&wrong),
        Err(TeoDBError::WriteProtocol { .. })
    ));
    assert_eq!(buffer.prepared_flush().unwrap().commit_id, prepared.commit_id);
    assert_eq!(buffer.snapshot_for_query().batches.len(), 1);

    buffer.mark_flush_failed(&prepared).unwrap();
    assert!(buffer.prepared_flush().is_none());
    assert_eq!(buffer.drain_pending_to_in_flight().len(), 1);
}

#[test]
fn blocked_rollback_requires_exact_owner() {
    let buffer = TableBuffer::new(
        TableIdent::new("test", "buf_test"),
        test_metadata(),
        0,
        1024 * 1024,
        512 * 1024,
    );
    let inserted = buffer
        .insert(uuid::Uuid::now_v7(), test_batch())
        .unwrap();
    buffer.drain_pending_to_in_flight();
    let prepared = test_prepared(&buffer, inserted.generation, CommitId::now_v7());
    let wrong = test_prepared(&buffer, inserted.generation, CommitId::now_v7());
    buffer.set_prepared(prepared.clone()).unwrap();
    buffer
        .mark_flush_blocked(&prepared, "unknown".into(), 1)
        .unwrap();

    assert!(matches!(
        buffer.mark_flush_failed(&wrong),
        Err(TeoDBError::WriteProtocol { .. })
    ));
    assert_eq!(buffer.blocked_flush().unwrap().prepared.commit_id, prepared.commit_id);
    assert_eq!(buffer.snapshot_for_query().batches.len(), 1);

    buffer.mark_flush_failed(&prepared).unwrap();
    assert!(buffer.blocked_flush().is_none());
    assert_eq!(buffer.drain_pending_to_in_flight().len(), 1);
}

#[test]
fn backpressure_on_overflow() {
    let meta = test_metadata();
    // Tiny limits
    let buf = TableBuffer::new(TableIdent::new("test", "buf_test"), meta, 0, 100, 50);

    // First insert might fit
    let batch = test_batch();
    let byte_size: u64 = batch
        .columns()
        .iter()
        .map(|c| c.get_buffer_memory_size() as u64)
        .sum();

    if byte_size > 100 {
        // Batch is already too large for the tiny limit
        let result = buf.insert(uuid::Uuid::now_v7(), batch);
        assert!(result.is_err());
    } else {
        let _ = buf.insert(uuid::Uuid::now_v7(), batch).unwrap();
        // Second insert should fail
        let result = buf.insert(uuid::Uuid::now_v7(), test_batch());
        if let Err(e) = result {
            assert!(matches!(e, TeoDBError::Backpressure(_)));
        }
    }
}

#[test]
fn reservation_counts_against_capacity_and_can_be_released() {
    let meta = test_metadata();
    let batch = test_batch();
    let byte_size = test_batch_byte_size(&batch);
    let buf = TableBuffer::new(TableIdent::new("test", "buf_test"), meta, 0, byte_size, byte_size);

    let reservation = buf.reserve(&batch).unwrap();
    assert_eq!(reservation.generation, 1);
    assert!(
        matches!(buf.reserve(&batch), Err(TeoDBError::Backpressure(_))),
        "reserved bytes must count against max_bytes"
    );

    buf.release_reservation(reservation);
    assert!(buf.reserve(&batch).is_ok(), "released reservation frees capacity");
}

#[test]
fn reserved_generation_blocks_later_flush_until_inserted() {
    let meta = test_metadata();
    let batch = test_batch();
    let byte_size = test_batch_byte_size(&batch);
    let buf = TableBuffer::new(
        TableIdent::new("test", "buf_test"),
        meta,
        0,
        byte_size * 4,
        byte_size * 4,
    );

    let first = buf.reserve(&batch).unwrap();
    let second = buf.reserve(&batch).unwrap();
    assert_eq!(first.generation, 1);
    assert_eq!(second.generation, 2);

    buf.insert_reserved(uuid::Uuid::now_v7(), second, batch.clone());
    assert!(
        buf.drain_pending_to_in_flight().is_empty(),
        "generation 2 cannot flush while generation 1 is still reserved"
    );

    buf.insert_reserved(uuid::Uuid::now_v7(), first, batch);
    let drained = buf.drain_pending_to_in_flight();
    let mut generations: Vec<_> = drained
        .iter()
        .map(|entry| entry.generation)
        .collect();
    generations.sort_unstable();
    assert_eq!(generations, vec![1, 2]);
}

/// P1-12: a generation reserved-but-not-yet-inserted (A) must never be
/// skipped by `committed_high_water` when a later generation (B) is flushed.
/// Because the drain guard refuses to flush B while A is reserved, the
/// committed high water cannot advance past A, and a late-inserted A stays
/// visible and is flushed exactly once.
#[test]
fn reserved_generation_keeps_high_water_below_late_insert() {
    let meta = test_metadata();
    let batch = test_batch();
    let byte_size = test_batch_byte_size(&batch);
    let buf = TableBuffer::new(
        TableIdent::new("test", "buf_test"),
        meta.clone(),
        0,
        byte_size * 8,
        byte_size * 8,
    );

    let a = buf.reserve(&batch).unwrap(); // gen 1
    let b = buf.reserve(&batch).unwrap(); // gen 2
    buf.insert_reserved(uuid::Uuid::now_v7(), b, batch.clone());

    // B cannot drain while A is reserved, so no flush can advance the high
    // water past A.
    assert!(buf.drain_pending_to_in_flight().is_empty());
    assert!(
        buf.committed_high_water() < a.generation,
        "high water must stay below the still-reserved generation"
    );

    // Insert A late; now both drain together and are flushed exactly once.
    buf.insert_reserved(uuid::Uuid::now_v7(), a, batch);
    let drained = buf.drain_pending_to_in_flight();
    assert_eq!(drained.len(), 2, "A and B flush together, each exactly once");

    let mut snapshot = test_metadata();
    Arc::make_mut(&mut snapshot).current_snapshot_id = Some(1);
    buf.mark_committed(2, snapshot).unwrap();
    assert_eq!(buf.committed_high_water(), 2);
}

#[test]
fn explicit_generation_insert_advances_next_generation() {
    let meta = test_metadata();
    let buf = TableBuffer::new(TableIdent::new("test", "buf_test"), meta, 0, 1024 * 1024, 512 * 1024);

    buf.insert_with_generation(uuid::Uuid::now_v7(), 42, test_batch())
        .unwrap();
    let ok = buf
        .insert(uuid::Uuid::now_v7(), test_batch())
        .unwrap();
    assert_eq!(ok.generation, 43);
}

#[test]
fn drain_idempotent_during_in_flight() {
    let meta = test_metadata();
    let buf = TableBuffer::new(TableIdent::new("test", "buf_test"), meta, 0, 1024 * 1024, 512 * 1024);

    buf.insert(uuid::Uuid::now_v7(), test_batch())
        .unwrap();
    let first = buf.drain_pending_to_in_flight();
    let second = buf.drain_pending_to_in_flight();
    assert_eq!(first.len(), second.len());
}

#[tokio::test]
async fn remove_reports_discarded_rows_and_counts_them() {
    let (_directory, registry) = test_registry().await;
    let ident = TableIdent::new("test", "buf_test");
    let buf = Arc::new(TableBuffer::new(
        ident.clone(),
        test_metadata(),
        0,
        1024 * 1024,
        512 * 1024,
    ));
    buf.insert(uuid::Uuid::now_v7(), test_batch())
        .unwrap();
    buf.insert(uuid::Uuid::now_v7(), test_batch())
        .unwrap();
    // One entry in-flight, one pending — both count as unflushed.
    buf.drain_pending_to_in_flight();
    buf.insert(uuid::Uuid::now_v7(), test_batch())
        .unwrap();
    registry.insert_for_test(ident.clone(), buf);

    let stats = registry.remove(&ident).expect("buffer existed");
    assert_eq!(stats.rows, 9);
    assert_eq!(stats.entries, 3);
    assert!(stats.bytes > 0);
    assert_eq!(registry.evicted_rows_total(), 9);
    assert!(registry.remove(&ident).is_none(), "already removed");
}

#[tokio::test]
async fn remove_clean_buffer_reports_zero() {
    let (_directory, registry) = test_registry().await;
    let ident = TableIdent::new("test", "buf_test");
    let meta = test_metadata();
    let buf = Arc::new(TableBuffer::new(
        ident.clone(),
        meta.clone(),
        0,
        1024 * 1024,
        512 * 1024,
    ));
    buf.insert(uuid::Uuid::now_v7(), test_batch())
        .unwrap();
    buf.drain_pending_to_in_flight();
    buf.mark_committed(1, meta).unwrap();
    registry.insert_for_test(ident.clone(), buf);

    let stats = registry.remove(&ident).expect("buffer existed");
    assert_eq!(stats.rows, 0);
    assert_eq!(registry.evicted_rows_total(), 0);
}

#[test]
fn committed_grace_keeps_entries_visible_for_stale_readers() {
    let meta = test_metadata();
    let buf = TableBuffer::new(
        TableIdent::new("test", "buf_test"),
        meta.clone(),
        0,
        1024 * 1024,
        512 * 1024,
    )
    .with_committed_grace(Duration::from_secs(60));

    buf.insert(uuid::Uuid::now_v7(), test_batch())
        .unwrap();
    buf.drain_pending_to_in_flight();
    buf.mark_committed(1, meta).unwrap();

    // Committed entries stay in the snapshot for the grace window;
    // a fresh reader excludes them via the generation cutoff (<= 1),
    // a stale reader (cutoff 0) still sees them.
    let snap = buf.snapshot_for_query();
    assert_eq!(snap.committed_high_water, 1);
    assert_eq!(snap.batches.len(), 1, "grace keeps the committed entry visible");
    assert_eq!(snap.batches[0].generation, 1);
    assert!(!buf.has_pending(), "grace entries are not unflushed work");

    let stats = buf.buffer_stats();
    assert_eq!(
        stats.pending_entries + stats.in_flight_entries,
        0,
        "grace entries don't count as buffered work"
    );
    assert!(stats.recently_committed_bytes > 0);
}

#[test]
fn committed_grace_zero_drops_entries_immediately() {
    let meta = test_metadata();
    let buf = TableBuffer::new(
        TableIdent::new("test", "buf_test"),
        meta.clone(),
        0,
        1024 * 1024,
        512 * 1024,
    );

    buf.insert(uuid::Uuid::now_v7(), test_batch())
        .unwrap();
    buf.drain_pending_to_in_flight();
    buf.mark_committed(1, meta).unwrap();

    assert!(buf.snapshot_for_query().batches.is_empty());
}

#[test]
fn committed_grace_entries_expire() {
    let meta = test_metadata();
    let buf = TableBuffer::new(
        TableIdent::new("test", "buf_test"),
        meta.clone(),
        0,
        1024 * 1024,
        512 * 1024,
    )
    .with_committed_grace(Duration::from_millis(1));

    buf.insert(uuid::Uuid::now_v7(), test_batch())
        .unwrap();
    buf.drain_pending_to_in_flight();
    buf.mark_committed(1, meta).unwrap();
    std::thread::sleep(Duration::from_millis(10));

    assert!(
        buf.snapshot_for_query().batches.is_empty(),
        "expired grace entries are invisible"
    );
}
