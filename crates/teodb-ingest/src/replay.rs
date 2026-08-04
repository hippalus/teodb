//! Fail-closed, writer-scoped WAL and prepared-intent recovery.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::traits::catalog::{Catalog, CommitAppend, CommitStatus};
use teodb_core::write_protocol::{
    AppendCommitIdentity, WalTableKey, parse_writer_checkpoint, validate_writer_checkpoints,
};
use teodb_storage::wal::{PreparedFlush, WalManager};

use crate::buffer::{BufferRegistry, TableBuffer};
use crate::flush::{FlushOutcome, Flusher};
use crate::idempotency::{Claim, IdempotencyIndex, IngestReceipt};

pub trait ReplayObserver: Send + Sync {
    fn on_batch_replayed(&self, rows: u64);
    fn on_record(&self, outcome: &'static str);
    fn on_recovery_failure(&self, reason: &'static str);
    fn on_flush_blocked(&self, reason: &'static str);
    fn on_replay_complete(&self, records: usize, duration: Duration);
}

#[derive(Clone)]
pub struct Replayer {
    wal: Arc<WalManager>,
    buffers: Arc<BufferRegistry>,
    catalog: Arc<dyn Catalog>,
    idempotency: Arc<IdempotencyIndex>,
    recovery_flusher: Option<Flusher>,
}

impl Replayer {
    pub fn new(
        wal: Arc<WalManager>,
        buffers: Arc<BufferRegistry>,
        catalog: Arc<dyn Catalog>,
        idempotency: Arc<IdempotencyIndex>,
    ) -> Self {
        Self {
            wal,
            buffers,
            catalog,
            idempotency,
            recovery_flusher: None,
        }
    }

    /// Allow startup recovery to commit a replayed prefix when bounded buffer
    /// admission reports capacity pressure, then retry the rejected record.
    pub fn with_recovery_flusher(mut self, flusher: Flusher) -> Self {
        self.recovery_flusher = Some(flusher);
        self
    }

    #[tracing::instrument(name = "wal.replay", skip_all)]
    pub async fn replay_wal(&self, observer: Option<&dyn ReplayObserver>) -> TeoDBResult<()> {
        let result = self.replay_wal_inner(observer).await;
        if let (Err(error), Some(observer)) = (&result, observer) {
            observer.on_recovery_failure(error.code());
        }
        result
    }

    async fn replay_wal_inner(&self, observer: Option<&dyn ReplayObserver>) -> TeoDBResult<()> {
        let started = std::time::Instant::now();
        // Pass one validates every frame before catalog, buffer, checkpoint, or
        // prepared-intent state is mutated. The plan retains metadata only.
        let mut plan = self.wal.prepare_replay_all().await?;
        let records = plan.record_count();
        let prepared = self.wal.list_prepared().await?;
        let writer = self.wal.writer_identity();

        let keys = recovery_keys(plan.table_keys().cloned(), &prepared)?;
        let metadata = self
            .load_target_metadata(&keys, writer.writer_id)
            .await?;
        let dropped_tables: HashSet<_> = keys
            .iter()
            .filter(|key| !metadata.contains_key(*key))
            .map(|key| key.ident.clone())
            .collect();

        let max_observed_epoch = metadata
            .values()
            .filter_map(|(_, checkpoint)| checkpoint.as_ref())
            .map(|checkpoint| checkpoint.epoch)
            .max();
        if let Some(epoch) = max_observed_epoch {
            self.wal.observe_epoch_and_bump(epoch)?;
        }

        // Catalog checkpoints are authoritative. Seed every exact incarnation
        // before deciding which WAL records remain replayable.
        for (key, (_, checkpoint)) in &metadata {
            self.wal
                .seed_committed(
                    key.clone(),
                    checkpoint
                        .as_ref()
                        .map_or(0, |checkpoint| checkpoint.generation),
                )
                .await;
        }

        let mut prepared_by_uuid: HashMap<_, _> = prepared
            .iter()
            .cloned()
            .map(|prepared| (prepared.table_uuid, prepared))
            .collect();
        let mut replayed = 0usize;

        while let Some(record) = plan.next_record().await? {
            let key = record.header.table_key()?;
            let Some((_, checkpoint)) = metadata.get(&key) else {
                // A catalog-confirmed dropped table was tombstoned while
                // loading targeted metadata.
                if let Some(observer) = observer {
                    observer.on_record("dropped");
                }
                continue;
            };
            if checkpoint
                .as_ref()
                .is_some_and(|checkpoint| record.header.generation <= checkpoint.generation)
            {
                if let Some(observer) = observer {
                    observer.on_record("committed");
                }
                continue;
            }

            let buffer = self
                .buffers
                .get_or_load(&record.header.table, self.catalog.as_ref())
                .await?;
            let dedupe_claimed = if let Some(idempotency_key) = &record.header.idempotency_key {
                match self
                    .idempotency
                    .claim(&record.header.table, idempotency_key)
                {
                    Claim::Acquired => true,
                    Claim::Duplicate(_) | Claim::InProgress => {
                        if let Some(observer) = observer {
                            observer.on_record("deduplicated");
                        }
                        continue;
                    }
                }
            } else {
                false
            };

            let insert = || {
                buffer.insert_with_generation_at(
                    record.header.batch_id,
                    record.header.generation,
                    record.header.created_at_ms,
                    record.batch.clone(),
                )
            };
            let insert_result = match insert() {
                Ok(_) => Ok(()),
                Err(error @ TeoDBError::Backpressure(_)) => {
                    let Some(flusher) = &self.recovery_flusher else {
                        if dedupe_claimed {
                            self.idempotency.abort(
                                &record.header.table,
                                record
                                    .header
                                    .idempotency_key
                                    .as_deref()
                                    .expect("claimed key"),
                            );
                        }
                        return Err(error);
                    };
                    if prepared_by_uuid
                        .get(&key.table_uuid)
                        .is_some_and(|prepared| record.header.generation > prepared.generations.lo)
                    {
                        // A partial prepared range must never be drained into a
                        // new commit identity. With an unchanged buffer limit,
                        // the original range necessarily fits because it was
                        // produced from this same bounded buffer.
                        Err(error)
                    } else {
                        match flusher.flush_table(&record.header.table).await {
                            Ok(FlushOutcome::Empty) => Err(error),
                            Ok(_) => insert().map(|_| ()),
                            Err(flush_error) => Err(flush_error),
                        }
                    }
                }
                Err(error) => Err(error),
            };
            if let Err(error) = insert_result {
                if dedupe_claimed {
                    self.idempotency.abort(
                        &record.header.table,
                        record
                            .header
                            .idempotency_key
                            .as_deref()
                            .expect("claimed key"),
                    );
                }
                return Err(error);
            }
            replayed += 1;
            if let Some(idempotency_key) = &record.header.idempotency_key {
                self.idempotency.complete(
                    &record.header.table,
                    idempotency_key,
                    IngestReceipt {
                        batch_id: record.header.batch_id,
                        writer_id: writer.writer_id,
                        generation: record.header.generation,
                        accepted_rows: record.header.row_count,
                    },
                );
            }
            if let Some(observer) = observer {
                observer.on_batch_replayed(record.header.row_count);
                observer.on_record("replayed");
            }
            if let Some(prepared) = prepared_by_uuid
                .get(&key.table_uuid)
                .filter(|prepared| record.header.generation >= prepared.generations.hi)
                .cloned()
            {
                self.resolve_prepared(&buffer, &prepared, observer)
                    .await?;
                prepared_by_uuid.remove(&key.table_uuid);
            }
        }
        let peak_live_decoded_records = plan.peak_live_decoded_records();

        // Resolve sidecars whose WAL range was catalog-committed and therefore
        // skipped above; live ranges were resolved at their generation boundary.
        // A well-formed persistent Unknown becomes table-local degraded state;
        // malformed metadata or unavailable catalog has already failed startup.
        for prepared in prepared_by_uuid.values() {
            let buffer = self
                .buffers
                .get_or_load(&prepared.table, self.catalog.as_ref())
                .await?;
            self.resolve_prepared(&buffer, prepared, observer)
                .await?;
        }

        // Catalog-confirmed drops are made durable only after the immutable
        // replay snapshot has been fully consumed.
        for table in dropped_tables {
            self.wal.append_drop_tombstone(&table).await?;
        }

        if let Err(error) = self.wal.gc().await {
            warn!(%error, "WAL post-recovery GC failed (non-fatal)");
        }
        info!(
            records,
            replayed,
            prepared = prepared.len(),
            peak_live_decoded_records,
            "writer-scoped WAL recovery complete"
        );
        if let Some(observer) = observer {
            observer.on_replay_complete(records, started.elapsed());
        }
        Ok(())
    }

    async fn load_target_metadata(
        &self,
        keys: &HashSet<WalTableKey>,
        writer_id: teodb_core::write_protocol::WriterId,
    ) -> TeoDBResult<
        HashMap<
            WalTableKey,
            (
                Arc<teodb_core::file::TableMetadata>,
                Option<teodb_core::write_protocol::WriterCheckpoint>,
            ),
        >,
    > {
        let mut loaded = HashMap::with_capacity(keys.len());
        for key in keys {
            let metadata = match self.catalog.load_table(&key.ident).await {
                Ok(metadata) => metadata,
                Err(TeoDBError::NotFound { .. }) => {
                    // The caller persists a tombstone after consuming the
                    // immutable replay snapshot.
                    continue;
                }
                Err(error) => return Err(error),
            };
            if metadata.table_uuid != key.table_uuid {
                return Err(TeoDBError::TableIncarnationMismatch {
                    table: key.ident.clone(),
                    expected: key.table_uuid,
                    actual: metadata.table_uuid,
                });
            }
            validate_writer_checkpoints(&key.ident, &metadata.properties)?;
            let checkpoint = parse_writer_checkpoint(&key.ident, &metadata.properties, writer_id)?;
            loaded.insert(key.clone(), (metadata, checkpoint));
        }
        Ok(loaded)
    }

    async fn resolve_prepared(
        &self,
        buffer: &TableBuffer,
        prepared: &PreparedFlush,
        observer: Option<&dyn ReplayObserver>,
    ) -> TeoDBResult<()> {
        super::flush::validate_prepared_data_files(&buffer.metadata(), prepared)?;
        let request = prepared_request(prepared);
        // Once the sidecar has passed structural and semantic validation, it
        // becomes a table-local recovery concern. Reclaim its exact WAL range
        // before querying the catalog so every non-committed outcome can be
        // contained as FlushBlocked without losing the durable intent.
        buffer.restore_prepared(prepared.clone())?;
        match self.catalog.check_append_status(&request).await {
            Ok(CommitStatus::Committed(metadata)) => {
                // The catalog checkpoint may already have caused this range's
                // WAL frames to be skipped. Completion is monotonic and does
                // not require reconstructing an in-flight range in that case.
                self.complete_prepared(buffer, prepared, metadata)
                    .await
            }
            Ok(CommitStatus::NotCommitted) => match self.catalog.commit_append(request).await {
                Ok(metadata) => {
                    self.complete_prepared(buffer, prepared, metadata)
                        .await
                }
                Err(TeoDBError::CommitStateUnknown { message, .. }) => {
                    buffer.mark_flush_blocked(prepared, message, 1)?;
                    if let Some(observer) = observer {
                        observer.on_flush_blocked("CommitStateUnknown");
                    }
                    Ok(())
                }
                Err(error) => {
                    contain_prepared_error(buffer, prepared, &error, 1)?;
                    if let Some(observer) = observer {
                        observer.on_flush_blocked(error.code());
                    }
                    Ok(())
                }
            },
            Ok(CommitStatus::Unknown { message }) => {
                buffer.mark_flush_blocked(prepared, message, 1)?;
                if let Some(observer) = observer {
                    observer.on_flush_blocked("CommitStateUnknown");
                }
                Ok(())
            }
            Err(error) => {
                contain_prepared_error(buffer, prepared, &error, 1)?;
                if let Some(observer) = observer {
                    observer.on_flush_blocked(error.code());
                }
                Ok(())
            }
        }
    }

    async fn complete_prepared(
        &self,
        buffer: &TableBuffer,
        prepared: &PreparedFlush,
        metadata: Arc<teodb_core::file::TableMetadata>,
    ) -> TeoDBResult<()> {
        buffer.mark_committed(prepared.generations.hi, metadata)?;
        self.wal
            .mark_committed(
                WalTableKey::new(prepared.table_uuid, prepared.table.clone()),
                prepared.generations.hi,
            )
            .await;
        self.wal
            .remove_prepared(prepared.table_uuid)
            .await
    }
}

fn contain_prepared_error(
    buffer: &TableBuffer,
    prepared: &PreparedFlush,
    error: &TeoDBError,
    attempts: u32,
) -> TeoDBResult<()> {
    warn!(
        table = %buffer.ident(),
        commit_id = %prepared.commit_id,
        error_class = error.code(),
        %error,
        "prepared flush resolution was rejected; containing it as a table-local blocked flush"
    );
    buffer.mark_flush_blocked_with_class(prepared, error.to_string(), error.code(), attempts)
}

fn recovery_keys(
    records: impl IntoIterator<Item = WalTableKey>,
    prepared: &[PreparedFlush],
) -> TeoDBResult<HashSet<WalTableKey>> {
    let mut keys: HashSet<_> = records.into_iter().collect();
    for intent in prepared {
        let key = WalTableKey::new(intent.table_uuid, intent.table.clone());
        if let Some(existing) = keys.iter().find(|key| key.ident == intent.table)
            && existing.table_uuid != intent.table_uuid
        {
            return Err(TeoDBError::TableIncarnationMismatch {
                table: intent.table.clone(),
                expected: intent.table_uuid,
                actual: existing.table_uuid,
            });
        }
        keys.insert(key);
    }
    Ok(keys)
}

fn prepared_request(prepared: &PreparedFlush) -> CommitAppend {
    CommitAppend {
        table: prepared.table.clone(),
        table_uuid: prepared.table_uuid,
        identity: AppendCommitIdentity {
            writer_id: prepared.writer_id,
            writer_epoch: prepared.writer_epoch,
            commit_id: prepared.commit_id,
            generations: prepared.generations,
        },
        base_snapshot_id: prepared.base_snapshot_id,
        added_data_files: prepared.data_files.clone(),
        properties: HashMap::new(),
    }
}
