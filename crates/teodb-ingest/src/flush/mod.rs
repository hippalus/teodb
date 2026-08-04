use std::sync::Arc;
use std::time::Instant;

use tracing::{debug, error, warn};

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::TableIdent;
use teodb_core::traits::catalog::{Catalog, CommitStatus};
use teodb_core::traits::storage::StorageFactory;
use teodb_core::write_protocol::{CommitId, GenerationRange, ResolvedIdentity};
use teodb_storage::wal::{PreparedFlush, WalManager};

use crate::buffer::{BlockedFlush, BufferRegistry, TableBuffer};
use crate::config::CommitStatusCheckConfig;

mod commit;
mod r#loop;
mod partitioning;
mod write;

pub use r#loop::{FlushLoopConfig, flush_loop};

/// Observer for flush operations, used to wire metrics without coupling to Prometheus.
pub trait FlushObserver: Send + Sync + 'static {
    fn on_flush_complete(
        &self,
        table: &TableIdent,
        rows: u64,
        oldest_committed_created_at_ms: Option<i64>,
        duration: std::time::Duration,
    );
    fn on_flush_empty(&self, duration: std::time::Duration);
    fn on_flush_error(&self);
    fn on_data_file_write(&self, duration: std::time::Duration);
    fn on_flush_lock_wait(&self, duration: std::time::Duration);
    fn on_flush_blocked(&self, reason: &'static str);
    fn on_blocked_resolution(&self, outcome: &'static str);
}

#[derive(Debug)]
pub enum FlushOutcome {
    Empty,
    Committed {
        gen_lo: u64,
        gen_hi: u64,
        record_count: u64,
    },
}

#[derive(Clone)]
pub struct Flusher {
    registry: Arc<BufferRegistry>,
    catalog: Arc<dyn Catalog>,
    storage_factory: Arc<dyn StorageFactory>,
    wal: Arc<WalManager>,
    status_check: CommitStatusCheckConfig,
    blocked_recheck_limit: Arc<tokio::sync::Semaphore>,
    observer: Option<Arc<dyn FlushObserver>>,
}

impl Flusher {
    pub fn new(
        registry: Arc<BufferRegistry>,
        catalog: Arc<dyn Catalog>,
        storage_factory: Arc<dyn StorageFactory>,
        wal: Arc<WalManager>,
    ) -> Self {
        Self {
            registry,
            catalog,
            storage_factory,
            wal,
            status_check: CommitStatusCheckConfig::default(),
            blocked_recheck_limit: Arc::new(tokio::sync::Semaphore::new(
                CommitStatusCheckConfig::default().max_concurrent_blocked_rechecks,
            )),
            observer: None,
        }
    }

    pub fn with_status_check_config(mut self, config: CommitStatusCheckConfig) -> Self {
        self.blocked_recheck_limit = Arc::new(tokio::sync::Semaphore::new(config.max_concurrent_blocked_rechecks));
        self.status_check = config;
        self
    }

    pub fn with_observer(mut self, observer: Arc<dyn FlushObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    #[tracing::instrument(name = "ingest.flush", skip_all, fields(table = %buffer.ident()))]
    pub async fn flush_once(&self, buffer: &TableBuffer) -> TeoDBResult<FlushOutcome> {
        let started = Instant::now();
        let oldest_committed_created_at_ms = buffer
            .buffer_stats()
            .oldest_uncommitted_created_at_ms;
        let result = self.flush_once_inner(buffer).await;
        if let Some(observer) = &self.observer {
            match &result {
                Ok(FlushOutcome::Committed { record_count, .. }) => {
                    observer.on_flush_complete(
                        buffer.ident(),
                        *record_count,
                        oldest_committed_created_at_ms,
                        started.elapsed(),
                    );
                }
                Ok(FlushOutcome::Empty) => observer.on_flush_empty(started.elapsed()),
                Err(_) => observer.on_flush_error(),
            }
        }
        result
    }

    async fn flush_once_inner(&self, buffer: &TableBuffer) -> TeoDBResult<FlushOutcome> {
        // Lock before looking at pending/in-flight state. A periodic flush and
        // an explicit flush must never write or commit the same in-flight set.
        let lock_started = Instant::now();
        let _flush_guard = buffer.lock_flush().await;
        if let Some(observer) = &self.observer {
            observer.on_flush_lock_wait(lock_started.elapsed());
        }
        if let Some(blocked) = buffer.blocked_flush() {
            return Err(TeoDBError::FlushBlocked {
                table: buffer.ident().clone(),
                commit_id: blocked.prepared.commit_id,
            });
        }

        if let Some(prepared) = buffer.prepared_flush() {
            return self.commit_prepared(buffer, &prepared).await;
        }

        let in_flight = buffer.drain_pending_to_in_flight();
        if in_flight.is_empty() {
            return Ok(FlushOutcome::Empty);
        }

        let gen_lo = in_flight
            .iter()
            .map(|e| e.generation)
            .min()
            .unwrap_or(0);
        let gen_hi = in_flight
            .iter()
            .map(|e| e.generation)
            .max()
            .unwrap_or(0);
        let record_count: u64 = in_flight
            .iter()
            .map(|e| e.batch.num_rows() as u64)
            .sum();

        debug!(
            table = %buffer.ident(),
            gen_lo, gen_hi, record_count,
            "flushing buffer"
        );

        let metadata = buffer.metadata();
        let identity = self.writer_identity();
        let commit_id = CommitId::now_v7();
        let generations = match GenerationRange::new(gen_lo, gen_hi) {
            Ok(generations) => generations,
            Err(error) => {
                buffer.rollback_unprepared_flush()?;
                return Err(error);
            }
        };
        let batches = in_flight.into_iter().map(|e| e.batch).collect();
        let write_started = Instant::now();
        let data_files = match write::write_flush_data_files(
            self.storage_factory.as_ref(),
            &metadata,
            batches,
            write::FlushWriteContext {
                writer_id: identity.writer_id,
                commit_id,
                generations,
            },
        )
        .await
        {
            Ok(files) => {
                if let Some(observer) = &self.observer {
                    observer.on_data_file_write(write_started.elapsed());
                }
                files
            }
            Err(error) => {
                if let Some(observer) = &self.observer {
                    observer.on_data_file_write(write_started.elapsed());
                }
                // No catalog request has started; returning the in-flight
                // range to pending is safe. Uploaded partial rolls are
                // guarded-age orphan candidates.
                buffer.rollback_unprepared_flush()?;
                return Err(error);
            }
        };
        let prepared = PreparedFlush::new(
            buffer.ident().clone(),
            metadata.table_uuid,
            identity.writer_id,
            identity.writer_epoch,
            commit_id,
            generations,
            record_count,
            chrono::Utc::now().timestamp_millis(),
            data_files,
            metadata.current_snapshot_id,
        );
        validate_prepared_or_rollback(buffer, &metadata, &prepared)?;
        if let Err(error) = buffer.set_prepared(prepared.clone()) {
            buffer.rollback_unprepared_flush()?;
            return Err(error);
        }
        self.commit_prepared(buffer, &prepared).await
    }

    fn writer_identity(&self) -> ResolvedIdentity {
        self.wal.writer_identity()
    }

    async fn commit_prepared(&self, buffer: &TableBuffer, prepared: &PreparedFlush) -> TeoDBResult<FlushOutcome> {
        // Persist on every attempt before contacting the catalog. Besides
        // being idempotent, this closes the cancellation window where a
        // blocking sidecar write finishes after its awaiting flush future
        // is dropped: the next owner re-awaits the same exact intent.
        self.wal.persist_prepared(prepared).await?;
        match commit::commit_flush(commit::FlushCommit {
            catalog: self.catalog.as_ref(),
            wal: self.wal.as_ref(),
            buffer,
            prepared,
        })
        .await
        {
            Err(TeoDBError::CommitStateUnknown { message, .. }) => {
                self.resolve_unknown(buffer, prepared, message)
                    .await
            }
            other => other,
        }
    }

    async fn resolve_unknown(
        &self,
        buffer: &TableBuffer,
        prepared: &PreparedFlush,
        mut last_error: String,
    ) -> TeoDBResult<FlushOutcome> {
        debug!(
            table = %buffer.ident(),
            commit_id = %prepared.commit_id,
            error = %last_error,
            "catalog commit response was ambiguous; starting exact status checks"
        );
        let request = commit::build_commit_request(prepared);
        let started = tokio::time::Instant::now();
        let deadline = started + self.status_check.total_timeout;
        let mut wait = self.status_check.min_wait;
        let mut attempts = 0u32;
        let mut consecutive_not_committed = 0u8;

        loop {
            attempts = attempts.saturating_add(1);
            match self.catalog.check_append_status(&request).await {
                Ok(CommitStatus::Committed(metadata)) => {
                    return commit::complete_flush(self.wal.as_ref(), buffer, prepared, metadata).await;
                }
                Ok(CommitStatus::NotCommitted) => {
                    consecutive_not_committed = consecutive_not_committed.saturating_add(1);
                    last_error = "exact commit not visible in authoritative metadata".into();
                    if consecutive_not_committed >= 2 {
                        // Resume the same immutable intent. A delayed original
                        // request races safely because the rebase guard checks
                        // the same exact commit ID before every update attempt.
                        match commit::commit_flush(commit::FlushCommit {
                            catalog: self.catalog.as_ref(),
                            wal: self.wal.as_ref(),
                            buffer,
                            prepared,
                        })
                        .await
                        {
                            Err(TeoDBError::CommitStateUnknown { message, .. }) => {
                                last_error = message;
                                consecutive_not_committed = 0;
                            }
                            result => return result,
                        }
                    }
                }
                Ok(CommitStatus::Unknown { message }) => {
                    consecutive_not_committed = 0;
                    last_error = message;
                }
                Err(error) => {
                    if !error.is_retryable() {
                        buffer.mark_flush_blocked_with_class(prepared, error.to_string(), error.code(), attempts)?;
                        if let Some(observer) = &self.observer {
                            observer.on_flush_blocked(error.code());
                        }
                        return Err(error);
                    }
                    consecutive_not_committed = 0;
                    last_error = error.to_string();
                }
            }

            if attempts > self.status_check.num_retries || tokio::time::Instant::now() >= deadline {
                break;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            tokio::time::sleep(wait.min(remaining)).await;
            wait = wait
                .saturating_mul(2)
                .min(self.status_check.max_wait);
        }

        buffer.mark_flush_blocked(prepared, last_error, attempts)?;
        if let Some(observer) = &self.observer {
            observer.on_flush_blocked("CommitStatusUnknown");
        }
        Err(TeoDBError::FlushBlocked {
            table: buffer.ident().clone(),
            commit_id: prepared.commit_id,
        })
    }

    pub fn blocked_tables(&self) -> Vec<BlockedFlush> {
        self.registry
            .tables()
            .into_iter()
            .filter_map(|ident| self.registry.get(&ident))
            .filter_map(|buffer| buffer.blocked_flush())
            .collect()
    }

    /// Operator-authorized exact recheck. It never force-marks or discards.
    #[tracing::instrument(name = "ingest.flush_recheck", skip_all, fields(table = %ident))]
    pub async fn recheck_blocked(&self, ident: &TableIdent) -> TeoDBResult<FlushOutcome> {
        let Some(buffer) = self.registry.get(ident) else {
            return Err(TeoDBError::NotFound {
                resource: format!("blocked flush for {ident}"),
            });
        };
        let _permit = self
            .blocked_recheck_limit
            .acquire()
            .await
            .map_err(|_| TeoDBError::Unavailable("blocked resolver stopped".into()))?;
        let lock_started = Instant::now();
        let _guard = buffer.lock_flush().await;
        if let Some(observer) = &self.observer {
            observer.on_flush_lock_wait(lock_started.elapsed());
        }
        let Some(blocked) = buffer.blocked_flush() else {
            return Ok(FlushOutcome::Empty);
        };
        let request = commit::build_commit_request(&blocked.prepared);
        let status = match self.catalog.check_append_status(&request).await {
            Ok(status) => status,
            Err(error) => {
                if let Some(observer) = &self.observer {
                    observer.on_blocked_resolution("error");
                }
                return Err(error);
            }
        };
        match status {
            CommitStatus::Committed(metadata) => {
                let result = commit::complete_flush(self.wal.as_ref(), &buffer, &blocked.prepared, metadata).await;
                if let Some(observer) = &self.observer {
                    observer.on_blocked_resolution(if result.is_ok() { "committed" } else { "error" });
                }
                result
            }
            CommitStatus::NotCommitted => {
                let result = match commit::commit_flush(commit::FlushCommit {
                    catalog: self.catalog.as_ref(),
                    wal: self.wal.as_ref(),
                    buffer: &buffer,
                    prepared: &blocked.prepared,
                })
                .await
                {
                    Err(TeoDBError::CommitStateUnknown { message, .. }) => {
                        buffer.mark_blocked_recheck(message, blocked.status_check_attempts.saturating_add(1));
                        Err(TeoDBError::FlushBlocked {
                            table: ident.clone(),
                            commit_id: blocked.prepared.commit_id,
                        })
                    }
                    result => result,
                };
                if let Some(observer) = &self.observer {
                    let outcome = match &result {
                        Ok(_) => "recommitted",
                        Err(TeoDBError::FlushBlocked { .. }) => "still_unknown",
                        Err(_) => "error",
                    };
                    observer.on_blocked_resolution(outcome);
                }
                result
            }
            CommitStatus::Unknown { message } => {
                buffer.mark_blocked_recheck(message, blocked.status_check_attempts.saturating_add(1));
                if let Some(observer) = &self.observer {
                    observer.on_blocked_resolution("still_unknown");
                }
                Err(TeoDBError::FlushBlocked {
                    table: ident.clone(),
                    commit_id: blocked.prepared.commit_id,
                })
            }
        }
    }

    pub(crate) async fn flush_all_tables(&self) {
        let tables = self.registry.tables();
        for ident in tables {
            if let Some(buffer) = self.registry.get(&ident)
                && buffer.has_pending()
            {
                if let Some(blocked) = buffer.blocked_flush() {
                    let elapsed_ms = chrono::Utc::now()
                        .timestamp_millis()
                        .saturating_sub(blocked.last_recheck_ms);
                    let recheck_delay = blocked_recheck_delay(
                        self.status_check.blocked_recheck_interval,
                        self.status_check.blocked_recheck_jitter_percent,
                        blocked.prepared.commit_id,
                    );
                    if elapsed_ms
                        >= recheck_delay
                            .as_millis()
                            .try_into()
                            .unwrap_or(i64::MAX)
                    {
                        let _ = self.recheck_blocked(&ident).await;
                    }
                    continue;
                }
                match self.flush_once(&buffer).await {
                    Ok(FlushOutcome::Committed { .. }) => {}
                    Ok(FlushOutcome::Empty) => {}
                    Err(e) => {
                        if matches!(&e, teodb_core::error::TeoDBError::Conflict { .. }) {
                            warn!(table = %ident, error = %e, "flush conflict, will retry next tick");
                        } else {
                            error!(table = %ident, error = %e, "flush failed");
                        }
                    }
                }
            }
        }
    }

    pub async fn flush_table(&self, ident: &TableIdent) -> TeoDBResult<FlushOutcome> {
        let Some(buffer) = self.registry.get(ident) else {
            self.catalog.load_table(ident).await?;
            return Ok(FlushOutcome::Empty);
        };

        self.flush_once(&buffer).await
    }
}

fn blocked_recheck_delay(base: std::time::Duration, jitter_percent: u8, commit_id: CommitId) -> std::time::Duration {
    let percent = u64::from(jitter_percent.min(100));
    if percent == 0 || base.is_zero() {
        return base;
    }
    let bytes = commit_id.as_uuid().as_bytes();
    let hash = u64::from_le_bytes(
        bytes[..8]
            .try_into()
            .expect("UUID prefix is eight bytes"),
    ) ^ u64::from_le_bytes(
        bytes[8..]
            .try_into()
            .expect("UUID suffix is eight bytes"),
    );
    let signed_percent = i128::from(hash % (percent.saturating_mul(2).saturating_add(1))) - i128::from(percent);
    let base_ms = i128::try_from(base.as_millis()).unwrap_or(i128::MAX);
    let jittered_ms = base_ms
        .saturating_add(base_ms.saturating_mul(signed_percent) / 100)
        .max(0);
    std::time::Duration::from_millis(u64::try_from(jittered_ms).unwrap_or(u64::MAX))
}

pub(crate) fn validate_prepared_data_files(
    metadata: &teodb_core::file::TableMetadata,
    prepared: &PreparedFlush,
) -> TeoDBResult<()> {
    let table = TableIdent::new(metadata.namespace.clone(), metadata.table_name.clone());
    if prepared.table_uuid != metadata.table_uuid || prepared.table != table {
        return Err(TeoDBError::WriteProtocol {
            table,
            message: "prepared intent does not match the loaded table incarnation".into(),
        });
    }
    if prepared.data_files.is_empty() {
        return Err(TeoDBError::WriteProtocol {
            table,
            message: "prepared intent contains no data files".into(),
        });
    }
    let data_prefix = format!("{}/data/", metadata.table_location.key.trim_end_matches('/'),);
    let commit_prefix = format!("{}-", prepared.commit_id);
    for data_file in &prepared.data_files {
        let key = &data_file.path.key;
        let schema = metadata
            .schemas
            .iter()
            .find(|schema| schema.schema_id == data_file.schema_id)
            .ok_or_else(|| TeoDBError::WriteProtocol {
                table: table.clone(),
                message: format!("prepared data file references unknown schema {}", data_file.schema_id),
            })?;
        let partition_spec = metadata
            .partition_specs
            .iter()
            .find(|spec| spec.spec_id == data_file.partition_spec_id)
            .ok_or_else(|| TeoDBError::WriteProtocol {
                table: table.clone(),
                message: format!(
                    "prepared data file references unknown partition spec {}",
                    data_file.partition_spec_id
                ),
            })?;
        let prefix = if partition_spec.fields.is_empty() {
            format!("{data_prefix}{}/", prepared.writer_id)
        } else {
            let partition_path =
                teodb_catalog::iceberg_partition_path(schema, partition_spec, &data_file.partition_values)?;
            format!("{data_prefix}{partition_path}/{}/", prepared.writer_id)
        };

        let relative_name = key.strip_prefix(&prefix);
        if relative_name.is_none_or(|name| name.is_empty() || name.contains('/'))
            || key.starts_with('/')
            || key.split('/').any(|component| component == "..")
            || data_file.path.scheme != metadata.table_location.scheme
            || data_file.path.bucket != metadata.table_location.bucket
            || data_file.content != teodb_core::file::DataContent::Data
            || !relative_name.is_some_and(|name| name.starts_with(&commit_prefix))
        {
            return Err(TeoDBError::WriteProtocol {
                table: table.clone(),
                message: format!(
                    "data-file path '{key}' is not an exact member of commit {} under '{prefix}'",
                    prepared.commit_id
                ),
            });
        }
    }
    Ok(())
}

fn validate_prepared_or_rollback(
    buffer: &TableBuffer,
    metadata: &teodb_core::file::TableMetadata,
    prepared: &PreparedFlush,
) -> TeoDBResult<()> {
    if let Err(error) = validate_prepared_data_files(metadata, prepared) {
        buffer.rollback_unprepared_flush()?;
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;
    use teodb_core::file::TableMetadata;
    use teodb_core::location::{ObjectLocation, StorageScheme};
    use teodb_core::schema::*;
    use teodb_core::write_protocol::{ClusterId, WriterEpoch, WriterId, WriterSlot};
    use teodb_test_support::{MockAppendOutcome, MockCatalog, MockCommitStatus, table_metadata};

    fn test_metadata() -> Arc<TableMetadata> {
        Arc::new(TableMetadata {
            table_uuid: uuid::Uuid::nil(),
            namespace: "test".into(),
            table_name: "flush_test".into(),
            table_location: ObjectLocation {
                scheme: StorageScheme::Local,
                bucket: None,
                key: "test/flush_test".into(),
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

    fn test_batch() -> arrow::array::RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        arrow::array::RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap()
    }

    #[tokio::test]
    async fn flush_empty_buffer() {
        let meta = test_metadata();
        let buf = TableBuffer::new(TableIdent::new("test", "flush_test"), meta, 0, 1024 * 1024, 512 * 1024);

        let in_flight = buf.drain_pending_to_in_flight();
        assert!(in_flight.is_empty());
    }

    #[test]
    fn drain_returns_entries() {
        let meta = test_metadata();
        let buf = TableBuffer::new(TableIdent::new("test", "flush_test"), meta, 0, 1024 * 1024, 512 * 1024);
        buf.insert(uuid::Uuid::now_v7(), test_batch())
            .unwrap();

        let entries = buf.drain_pending_to_in_flight();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].batch.num_rows(), 3);
    }

    #[test]
    fn prepared_validation_failure_rolls_in_flight_back_to_pending() {
        let metadata = test_metadata();
        let buffer = TableBuffer::new(
            TableIdent::new("test", "flush_test"),
            metadata.clone(),
            0,
            1024 * 1024,
            512 * 1024,
        );
        let inserted = buffer
            .insert(uuid::Uuid::now_v7(), test_batch())
            .unwrap();
        buffer.drain_pending_to_in_flight();
        let cluster_id = ClusterId::from_uuid(uuid::Uuid::nil());
        let writer_id = WriterId::derive(cluster_id, &WriterSlot::new("validation-rollback").unwrap());
        let prepared = PreparedFlush::new(
            buffer.ident().clone(),
            buffer.table_uuid(),
            writer_id,
            WriterEpoch::new(1),
            CommitId::now_v7(),
            GenerationRange::new(inserted.generation, inserted.generation).unwrap(),
            3,
            chrono::Utc::now().timestamp_millis(),
            Vec::new(),
            None,
        );

        let error = validate_prepared_or_rollback(&buffer, &metadata, &prepared).unwrap_err();
        assert!(matches!(error, TeoDBError::WriteProtocol { .. }));
        assert!(buffer.prepared_flush().is_none());

        let retried = buffer.drain_pending_to_in_flight();
        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0].generation, inserted.generation);
    }

    #[tokio::test]
    async fn mw_t6_concurrent_periodic_and_manual_flush_publish_one_commit() {
        let iceberg = table_metadata("s3://warehouse/test/flush_test");
        let catalog = Arc::new(
            MockCatalog::builder()
                .serves_any(iceberg.clone())
                .commit_result(iceberg.clone())
                .build(),
        );
        let (_directory, flusher, ident, _buffer) = flusher_with_catalog(catalog.clone(), &iceberg).await;
        let flusher = Arc::new(flusher);

        let (left, right) = tokio::join!(flusher.flush_table(&ident), flusher.flush_table(&ident),);
        let outcomes = [left.unwrap(), right.unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, FlushOutcome::Committed { .. }))
                .count(),
            1
        );
        assert_eq!(catalog.commit_append_calls(), 1);
    }

    async fn flusher_with_catalog(
        catalog: Arc<MockCatalog>,
        metadata: &Arc<TableMetadata>,
    ) -> (tempfile::TempDir, Flusher, TableIdent, Arc<TableBuffer>) {
        let ident = TableIdent::new("test", "flush_test");
        let buffer = Arc::new(TableBuffer::new(
            ident.clone(),
            metadata.clone(),
            0,
            1024 * 1024,
            512 * 1024,
        ));
        buffer
            .insert(uuid::Uuid::now_v7(), test_batch())
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let wal = Arc::new(
            WalManager::open(teodb_storage::wal::WalConfig {
                root_dir: directory.path().to_path_buf(),
                fsync_on_append: false,
                ..Default::default()
            })
            .await
            .unwrap(),
        );
        let registry = Arc::new(BufferRegistry::new(wal.clone(), 1024 * 1024, 512 * 1024));
        registry.insert_for_test(ident.clone(), buffer.clone());
        let storage = teodb_test_support::single_backend_factory(teodb_test_support::in_memory_backend());
        (directory, Flusher::new(registry, catalog, storage, wal), ident, buffer)
    }

    #[tokio::test]
    async fn mw_t3_ambiguous_success_is_completed_from_exact_status_without_reappend() {
        let iceberg = table_metadata("s3://warehouse/test/flush_test");
        let catalog = Arc::new(
            MockCatalog::builder()
                .append_outcomes([MockAppendOutcome::StateUnknown("response lost after apply".into())])
                .status_outcomes([MockCommitStatus::Committed(iceberg.clone())])
                .build(),
        );
        let (_directory, flusher, ident, buffer) = flusher_with_catalog(catalog.clone(), &iceberg).await;

        assert!(matches!(
            flusher.flush_table(&ident).await.unwrap(),
            FlushOutcome::Committed { .. }
        ));
        assert_eq!(catalog.commit_append_calls(), 1);
        assert!(buffer.blocked_flush().is_none());
        assert!(!buffer.has_pending());
    }

    #[tokio::test]
    async fn mw_t5_ambiguous_failure_reuses_the_exact_commit_identity() {
        let iceberg = table_metadata("s3://warehouse/test/flush_test");
        let catalog = Arc::new(
            MockCatalog::builder()
                .append_outcomes([
                    MockAppendOutcome::StateUnknown("request failed before apply but outcome was unknown".into()),
                    MockAppendOutcome::Success(iceberg.clone()),
                ])
                .status_outcomes([MockCommitStatus::NotCommitted, MockCommitStatus::NotCommitted])
                .build(),
        );
        let (_directory, mut flusher, ident, _buffer) = flusher_with_catalog(catalog.clone(), &iceberg).await;
        let config = CommitStatusCheckConfig {
            min_wait: std::time::Duration::ZERO,
            max_wait: std::time::Duration::ZERO,
            total_timeout: std::time::Duration::from_secs(1),
            ..CommitStatusCheckConfig::default()
        };
        flusher = flusher.with_status_check_config(config);

        assert!(matches!(
            flusher.flush_table(&ident).await.unwrap(),
            FlushOutcome::Committed { .. }
        ));
        let requests = catalog.append_requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].identity.commit_id, requests[1].identity.commit_id);
        assert_eq!(requests[0].identity.generations, requests[1].identity.generations);
    }

    #[tokio::test]
    async fn mw_t15_persistent_unknown_blocks_only_after_bounded_checks() {
        let iceberg = table_metadata("s3://warehouse/test/flush_test");
        let catalog = Arc::new(
            MockCatalog::builder()
                .append_outcomes([MockAppendOutcome::StateUnknown("catalog response unavailable".into())])
                .status_outcomes([
                    MockCommitStatus::Unknown("unavailable-1".into()),
                    MockCommitStatus::Unknown("unavailable-2".into()),
                    MockCommitStatus::Unknown("unavailable-3".into()),
                ])
                .build(),
        );
        let (_directory, mut flusher, ident, buffer) = flusher_with_catalog(catalog, &iceberg).await;
        let config = CommitStatusCheckConfig {
            num_retries: 2,
            min_wait: std::time::Duration::ZERO,
            max_wait: std::time::Duration::ZERO,
            total_timeout: std::time::Duration::from_secs(1),
            ..CommitStatusCheckConfig::default()
        };
        flusher = flusher.with_status_check_config(config);

        assert!(matches!(
            flusher.flush_table(&ident).await,
            Err(TeoDBError::FlushBlocked { .. })
        ));
        let blocked = buffer
            .blocked_flush()
            .expect("table is contained");
        assert_eq!(blocked.status_check_attempts, 3);
        assert_eq!(blocked.last_error_class, "commit_status_unknown");
        assert!(matches!(
            buffer.reserve(&test_batch()),
            Err(TeoDBError::FlushBlocked { .. })
        ));
        let second = tokio::time::timeout(std::time::Duration::from_millis(100), flusher.flush_table(&ident))
            .await
            .expect("flush mutex was released");
        assert!(matches!(second, Err(TeoDBError::FlushBlocked { .. })));
    }

    #[test]
    fn blocked_flush_contains_only_its_table() {
        let ident = TableIdent::new("test", "blocked");
        let buffer = TableBuffer::new(ident.clone(), test_metadata(), 0, 1024 * 1024, 512 * 1024);
        buffer
            .insert(uuid::Uuid::now_v7(), test_batch())
            .unwrap();
        buffer.drain_pending_to_in_flight();
        let cluster_id = ClusterId::from_uuid(uuid::Uuid::nil());
        let slot = WriterSlot::new("blocked-test").unwrap();
        let prepared = PreparedFlush::new(
            ident.clone(),
            buffer.table_uuid(),
            WriterId::derive(cluster_id, &slot),
            WriterEpoch::new(1),
            CommitId::now_v7(),
            GenerationRange::new(1, 1).unwrap(),
            3,
            1,
            Vec::new(),
            None,
        );
        buffer.set_prepared(prepared.clone()).unwrap();
        buffer
            .mark_flush_blocked(&prepared, "catalog unavailable".into(), 2)
            .unwrap();
        assert!(matches!(
            buffer.reserve(&test_batch()),
            Err(TeoDBError::FlushBlocked { .. })
        ));

        let healthy = TableBuffer::new(
            TableIdent::new("test", "healthy"),
            test_metadata(),
            0,
            1024 * 1024,
            512 * 1024,
        );
        assert!(healthy.reserve(&test_batch()).is_ok());
    }

    #[test]
    fn blocked_recheck_jitter_is_deterministic_and_bounded() {
        let commit_id = CommitId::now_v7();
        let base = std::time::Duration::from_secs(100);
        let first = blocked_recheck_delay(base, 15, commit_id);
        let second = blocked_recheck_delay(base, 15, commit_id);
        assert_eq!(first, second);
        assert!(first >= std::time::Duration::from_secs(85));
        assert!(first <= std::time::Duration::from_secs(115));
        assert_eq!(blocked_recheck_delay(base, 0, commit_id), base);
    }
}
