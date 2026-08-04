//! Snapshot commit paths.
//!
//! Append conflict retry/rebase is owned by Iceberg's transaction. TeoDB adds
//! exact commit identity, a writer checkpoint in the same transaction, and a
//! catalog decorator that revalidates writer state on every rebase attempt.

use std::sync::Arc;

use backon::Retryable;
use tracing::debug;

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::traits::catalog::{CommitAppend, CommitReplace};

use super::IcebergCatalogAdapter;
use super::append_attempt_guard::{AppendAttemptGuard, GuardRejection};
use super::commit_error::{CommitAttemptError, classify};
use super::commit_metadata::{checkpoint_property, snapshot_properties};
use super::idents::make_table_ident;
use crate::error::map_iceberg_error;

impl IcebergCatalogAdapter {
    /// Commit an append. Iceberg owns conflict retries; there is deliberately
    /// no broad TeoDB retry loop around an ambiguous catalog response.
    pub(super) async fn commit_append_with_retry(
        &self,
        req: CommitAppend,
    ) -> TeoDBResult<Arc<iceberg::spec::TableMetadata>> {
        let started = std::time::Instant::now();
        let result = self.try_commit_append(&req).await;
        if let Some(observer) = &self.observer {
            observer.on_append_commit(commit_outcome(&result), started.elapsed());
        }
        result
    }

    /// Commit a replace, retrying transient failures.
    pub(super) async fn commit_replace_with_retry(
        &self,
        req: CommitReplace,
    ) -> TeoDBResult<Arc<iceberg::spec::TableMetadata>> {
        let table = req.table.clone();
        let mut attempt = 0u32;

        (|| async { self.try_commit_replace(&req).await })
            .retry(self.cfg.retry.backoff_builder())
            .when(is_retryable)
            .notify(|error, backoff| {
                debug!(
                    attempt,
                    ?backoff,
                    table = %table,
                    %error,
                    "replace failed, retrying"
                );
                attempt += 1;
            })
            .await
    }

    async fn try_commit_append(&self, req: &CommitAppend) -> TeoDBResult<Arc<iceberg::spec::TableMetadata>> {
        let iceberg_ident = make_table_ident(&req.table);
        let guard = AppendAttemptGuard::new(
            self.inner.clone(),
            req.clone(),
            self.cfg.max_writer_checkpoints_per_table,
            self.observer.clone(),
        );
        // Transaction::commit always reloads through the guard before applying
        // actions. The bootstrap load is only needed to construct Transaction;
        // validating it here as well would double-count one logical attempt.
        let table = self
            .inner
            .load_table(&iceberg_ident)
            .await
            .map_err(map_iceberg_error)?;
        let tx = build_append_transaction(req, &table)?;
        let commit_result = tx.commit(&guard).await;
        let attempts = guard.invocation_count();
        if attempts > 1
            && let Some(observer) = &self.observer
        {
            observer.on_append_rebase(attempts - 1);
        }
        let updated = match commit_result {
            Ok(updated) => updated,
            Err(error) => return guard_result(&guard, req, error),
        };

        Ok(updated.metadata_ref())
    }

    async fn try_commit_replace(&self, req: &CommitReplace) -> TeoDBResult<Arc<iceberg::spec::TableMetadata>> {
        // iceberg-rust 0.10.x's public `Transaction` API exposes no
        // overwrite/replace/delete action — `fast_append` is the only public
        // snapshot-producing action; the overwrite machinery
        // (`SnapshotProducer` / `SnapshotProduceOperation` with `delete_entries`
        // and `Operation::Overwrite`) is `pub(crate)`. So a replace cannot
        // remove the inputs it supersedes at commit time.
        //
        // INTERIM (correct for reads): we `fast_append` the new files and record
        // the superseded paths in the `teodb.removed_data_files` snapshot
        // property. Every data-file read path reconciles that marker along the
        // current-snapshot lineage in `super::manifests` (`live_data_files` /
        // `removed_data_file_paths`), so queries count each row exactly once even
        // though the old files are still physically referenced by the manifest.
        // Because the commit is not a real overwrite, background compaction stays
        // OFF by default (`cluster.compaction_enabled = false`).
        //
        // TO UNBLOCK: bump iceberg-rust to a version that exposes an overwrite
        // action (mind the arrow/datafusion/ballista lockstep in
        // docs/VERSIONING.md), then remove the inputs here and drop the
        // read-time reconciliation.
        let iceberg_ident = make_table_ident(&req.table);
        let table = self
            .inner
            .load_table(&iceberg_ident)
            .await
            .map_err(map_iceberg_error)?;

        // CAS check: replace always requires base_snapshot_id to match.
        let actual_snap = table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id());
        if actual_snap != Some(req.base_snapshot_id) {
            return Err(TeoDBError::Conflict {
                resource: format!("table {}", req.table),
                expected: req.base_snapshot_id.to_string(),
                actual: actual_snap
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "None".into()),
            });
        }

        let mut props = req.properties.clone();
        let removed_json = serde_json::to_string(&req.removed_data_files)
            .map_err(|e| TeoDBError::Catalog(format!("failed to serialize removed_data_files: {e}")))?;
        props.insert(super::manifests::REMOVED_DATA_FILES_PROP.into(), removed_json);
        let added_data_files = convert_data_files(&req.table, table.metadata(), &req.added_data_files)?;

        let tx = iceberg::transaction::Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(added_data_files)
            .set_snapshot_properties(props);

        let tx = iceberg::transaction::ApplyTransactionAction::apply(action, tx).map_err(map_iceberg_error)?;
        let updated = tx
            .commit(&*self.inner)
            .await
            .map_err(map_iceberg_error)?;

        Ok(updated.metadata_ref())
    }
}

fn commit_outcome(result: &TeoDBResult<Arc<iceberg::spec::TableMetadata>>) -> crate::CatalogCommitOutcome {
    match result {
        Ok(_) => crate::CatalogCommitOutcome::Committed,
        Err(TeoDBError::Conflict { .. }) => crate::CatalogCommitOutcome::Conflict,
        Err(TeoDBError::CommitStateUnknown { .. }) => crate::CatalogCommitOutcome::StateUnknown,
        Err(TeoDBError::StaleWriterEpoch { .. }) => crate::CatalogCommitOutcome::StaleWriterEpoch,
        Err(TeoDBError::WriterRegistryFull { .. }) => crate::CatalogCommitOutcome::WriterRegistryFull,
        Err(TeoDBError::TableIncarnationMismatch { .. }) => crate::CatalogCommitOutcome::TableIncarnationMismatch,
        Err(TeoDBError::MetadataCorruption { .. }) => crate::CatalogCommitOutcome::MetadataCorruption,
        Err(TeoDBError::WriteProtocol { .. }) => crate::CatalogCommitOutcome::ProtocolError,
        Err(TeoDBError::ExternalRetryable(_) | TeoDBError::Unavailable(_)) => {
            crate::CatalogCommitOutcome::RetryableError
        }
        Err(_) => crate::CatalogCommitOutcome::FatalError,
    }
}

pub(super) fn build_append_transaction(
    request: &CommitAppend,
    table: &iceberg::table::Table,
) -> TeoDBResult<iceberg::transaction::Transaction> {
    let properties = snapshot_properties(request)?;
    let (checkpoint_key, checkpoint_value) = checkpoint_property(request)?;
    let added_data_files = convert_data_files(&request.table, table.metadata(), &request.added_data_files)?;
    let transaction = iceberg::transaction::Transaction::new(table);
    let append = transaction
        .fast_append()
        .set_commit_uuid(request.identity.commit_id.into_uuid())
        .add_data_files(added_data_files)
        .set_snapshot_properties(properties);
    let transaction =
        iceberg::transaction::ApplyTransactionAction::apply(append, transaction).map_err(map_iceberg_error)?;
    let checkpoint = transaction
        .update_table_properties()
        .set(checkpoint_key, checkpoint_value);
    iceberg::transaction::ApplyTransactionAction::apply(checkpoint, transaction).map_err(map_iceberg_error)
}

fn convert_data_files(
    table: &teodb_core::ident::TableIdent,
    metadata: &iceberg::spec::TableMetadata,
    files: &[teodb_core::file::DataFile],
) -> TeoDBResult<Vec<iceberg::spec::DataFile>> {
    files
        .iter()
        .map(|file| {
            let partition_spec = metadata
                .partition_spec_by_id(file.partition_spec_id)
                .ok_or_else(|| TeoDBError::MetadataCorruption {
                    table: table.clone(),
                    message: format!("partition spec {} is missing", file.partition_spec_id),
                })?;
            crate::convert::teodb_data_file_to_iceberg(file, partition_spec)
        })
        .collect()
}

fn guard_result(
    guard: &AppendAttemptGuard,
    request: &CommitAppend,
    error: iceberg::Error,
) -> TeoDBResult<Arc<iceberg::spec::TableMetadata>> {
    if let Some(rejection) = guard.take_rejection() {
        return match rejection {
            GuardRejection::AlreadyCommitted(metadata) => Ok(metadata),
            other => Err(other.into_teodb_error(request, guard.max_writer_checkpoints())),
        };
    }

    match classify(error) {
        CommitAttemptError::Conflict(error) => Err(map_iceberg_error(error)),
        CommitAttemptError::StateUnknown(error) => Err(TeoDBError::CommitStateUnknown {
            table: request.table.clone(),
            commit_id: request.identity.commit_id,
            message: error.to_string(),
        }),
        CommitAttemptError::RetryableBeforeCommit(error) => Err(TeoDBError::ExternalRetryable(error.to_string())),
        CommitAttemptError::Fatal(error) => Err(map_iceberg_error(error)),
    }
}

/// Returns true if the error is transient and worth retrying.
///
/// A `Conflict` is NOT retryable here: it means the CAS base snapshot moved, so
/// re-running with the same `base_snapshot_id` would fail identically and just
/// burn the backoff budget. The caller (e.g. the flush loop) must re-resolve the
/// base snapshot and rebuild the commit before trying again.
fn is_retryable(e: &TeoDBError) -> bool {
    matches!(e, TeoDBError::Catalog(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::{Catalog as _, CatalogBuilder as _};
    use teodb_core::file::{DataContent, DataFile, FileFormat};
    use teodb_core::ident::TableIdent;
    use teodb_core::location::{ObjectLocation, StorageScheme};
    use teodb_core::traits::catalog::{Catalog as TeoCatalog, CommitStatus};
    use teodb_core::write_protocol::{
        AppendCommitIdentity, COMMIT_ID_PROPERTY, CommitId, GenerationRange, WriterEpoch, WriterId,
        parse_writer_checkpoint,
    };

    fn schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap()
    }

    fn data_file(path: String) -> DataFile {
        DataFile {
            content: DataContent::Data,
            path: crate::convert::iceberg_location_to_teodb(&path).expect("test data file location"),
            format: FileFormat::Parquet,
            partition_spec_id: 0,
            sort_order_id: None,
            schema_id: 0,
            partition_values: HashMap::new(),
            record_count: 1,
            file_size_bytes: 10,
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

    fn request(table_uuid: uuid::Uuid, writer_id: WriterId, path: String) -> CommitAppend {
        CommitAppend {
            table: TableIdent::new("test", "events"),
            table_uuid,
            identity: AppendCommitIdentity {
                writer_id,
                writer_epoch: WriterEpoch::new(1),
                commit_id: CommitId::now_v7(),
                generations: GenerationRange::new(1, 1).unwrap(),
            },
            base_snapshot_id: None,
            added_data_files: vec![data_file(path)],
            properties: HashMap::new(),
        }
    }

    async fn memory_adapter(test_name: &str) -> (tempfile::TempDir, IcebergCatalogAdapter, iceberg::table::Table) {
        let warehouse = tempfile::tempdir().unwrap();
        let memory = iceberg::memory::MemoryCatalogBuilder::default()
            .load(
                test_name,
                HashMap::from([(
                    iceberg::memory::MEMORY_CATALOG_WAREHOUSE.into(),
                    warehouse.path().to_string_lossy().into_owned(),
                )]),
            )
            .await
            .unwrap();
        let namespace = iceberg::NamespaceIdent::new("test".into());
        memory
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let table = memory
            .create_table(
                &namespace,
                iceberg::TableCreation::builder()
                    .name("events".into())
                    .schema(schema())
                    .build(),
            )
            .await
            .unwrap();
        let adapter = IcebergCatalogAdapter::from_catalog(
            Arc::new(memory),
            super::super::config::IcebergCatalogConfig::default(),
        );
        (warehouse, adapter, table)
    }

    #[tokio::test]
    async fn mw_t1_two_writers_rebase_preserves_files_and_checkpoints() {
        let warehouse = tempfile::tempdir().unwrap();
        let memory = iceberg::memory::MemoryCatalogBuilder::default()
            .load(
                "multi-writer-test",
                HashMap::from([(
                    iceberg::memory::MEMORY_CATALOG_WAREHOUSE.into(),
                    warehouse.path().to_string_lossy().into_owned(),
                )]),
            )
            .await
            .unwrap();
        let namespace = iceberg::NamespaceIdent::new("test".into());
        memory
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let table = memory
            .create_table(
                &namespace,
                iceberg::TableCreation::builder()
                    .name("events".into())
                    .schema(schema())
                    .build(),
            )
            .await
            .unwrap();
        let table_uuid = table.metadata().uuid();
        let location = table.metadata().location().trim_end_matches('/');
        let writer_a = WriterId::from_uuid(uuid::Uuid::now_v7());
        let writer_b = WriterId::from_uuid(uuid::Uuid::now_v7());
        let mut request_a = request(table_uuid, writer_a, format!("{location}/data/{writer_a}/a.parquet"));
        request_a
            .properties
            .insert("test.writer".into(), "a".into());
        let mut request_b = request(table_uuid, writer_b, format!("{location}/data/{writer_b}/b.parquet"));
        request_b
            .properties
            .insert("test.writer".into(), "b".into());
        let adapter = IcebergCatalogAdapter::from_catalog(
            Arc::new(memory),
            super::super::config::IcebergCatalogConfig::default(),
        );
        let writer_b_guard = AppendAttemptGuard::new(
            adapter.inner.clone(),
            request_b.clone(),
            adapter.cfg.max_writer_checkpoints_per_table,
            None,
        );
        let (writer_b_guard, writer_b_validated, resume_writer_b) = writer_b_guard.pause_after_first_validation();
        let writer_b_tx = build_append_transaction(&request_b, &table).unwrap();
        let (result_b, result_a) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(writer_b_tx.commit(&writer_b_guard), async {
                writer_b_validated
                    .await
                    .expect("writer B reaches its initial guarded attempt");
                let result = TeoCatalog::commit_append(&adapter, request_a.clone()).await;
                resume_writer_b.notify_one();
                result
            })
        })
        .await
        .expect("barrier-driven two-writer rebase completes");
        result_a.unwrap();
        result_b.unwrap();
        assert_eq!(
            writer_b_guard.invocation_count(),
            2,
            "writer B must revalidate after writer A advances the table"
        );

        let mut next_a = request(
            table_uuid,
            writer_a,
            format!("{location}/data/{writer_a}/a-next.parquet"),
        );
        next_a.identity.writer_epoch = WriterEpoch::new(2);
        next_a.identity.generations = GenerationRange::new(2, 2).unwrap();
        TeoCatalog::commit_append(&adapter, next_a)
            .await
            .unwrap();

        // An older epoch remains idempotently successful when its exact
        // commit is in snapshot history, even though the current checkpoint
        // has advanced to epoch 2.
        TeoCatalog::commit_append(&adapter, request_a.clone())
            .await
            .unwrap();

        let mut stale_a = request(
            table_uuid,
            writer_a,
            format!("{location}/data/{writer_a}/stale.parquet"),
        );
        stale_a.identity.generations = GenerationRange::new(3, 3).unwrap();
        let stale_error = TeoCatalog::commit_append(&adapter, stale_a)
            .await
            .expect_err("an unproven old epoch must be fenced");
        assert!(matches!(stale_error, TeoDBError::StaleWriterEpoch { .. }));

        let latest = TeoCatalog::load_table(&adapter, &request_a.table)
            .await
            .unwrap();
        assert_eq!(latest.snapshots.len(), 3);
        let writer_markers: std::collections::HashSet<_> = latest
            .snapshots
            .iter()
            .filter_map(|snapshot| snapshot.summary.get("test.writer"))
            .map(String::as_str)
            .collect();
        assert_eq!(writer_markers, std::collections::HashSet::from(["a", "b"]));
        let live_paths: std::collections::HashSet<_> = TeoCatalog::load_live_files(&adapter, &request_a.table)
            .await
            .unwrap()
            .into_iter()
            .map(|file| file.path.to_uri())
            .collect();
        assert!(live_paths.contains(&request_a.added_data_files[0].path.to_uri()));
        assert!(live_paths.contains(&request_b.added_data_files[0].path.to_uri()));
        assert_eq!(
            parse_writer_checkpoint(&request_a.table, &latest.properties, writer_a)
                .unwrap()
                .unwrap()
                .generation,
            2
        );
        assert_eq!(
            parse_writer_checkpoint(&request_b.table, &latest.properties, writer_b)
                .unwrap()
                .unwrap()
                .generation,
            1
        );
        assert!(matches!(
            TeoCatalog::check_append_status(&adapter, &request_a)
                .await
                .unwrap(),
            CommitStatus::Committed(_)
        ));
        assert!(matches!(
            TeoCatalog::check_append_status(&adapter, &request_b)
                .await
                .unwrap(),
            CommitStatus::Committed(_)
        ));
    }

    #[tokio::test]
    async fn mw_t7_disjoint_ranges_are_monotonic_and_stale_completion_fails_closed() {
        let (_warehouse, adapter, table) = memory_adapter("mw-t7").await;
        let table_uuid = table.metadata().uuid();
        let location = table.metadata().location().trim_end_matches('/');
        let writer = WriterId::from_uuid(uuid::Uuid::now_v7());

        let mut committed = request(
            table_uuid,
            writer,
            format!("{location}/data/{writer}/range-6-10.parquet"),
        );
        committed.identity.generations = GenerationRange::new(6, 10).unwrap();
        TeoCatalog::commit_append(&adapter, committed.clone())
            .await
            .expect("newer range commits first");

        let mut delayed = request(
            table_uuid,
            writer,
            format!("{location}/data/{writer}/range-1-5.parquet"),
        );
        delayed.identity.generations = GenerationRange::new(1, 5).unwrap();
        assert!(matches!(
            TeoCatalog::commit_append(&adapter, delayed).await,
            Err(TeoDBError::WriteProtocol { .. })
        ));

        TeoCatalog::commit_append(&adapter, committed.clone())
            .await
            .expect("the exact already-committed range is idempotent");
        let latest = TeoCatalog::load_table(&adapter, &committed.table)
            .await
            .unwrap();
        assert_eq!(
            parse_writer_checkpoint(&committed.table, &latest.properties, writer)
                .unwrap()
                .unwrap()
                .generation,
            10
        );
        assert_eq!(latest.snapshots.len(), 1);
    }

    #[tokio::test]
    async fn mw_t14_exact_commit_is_found_after_snapshot_history_advances() {
        let (_warehouse, adapter, table) = memory_adapter("mw-t14").await;
        let table_uuid = table.metadata().uuid();
        let location = table.metadata().location().trim_end_matches('/');
        let mut commits = Vec::new();

        for label in ["a", "b", "c"] {
            let writer = WriterId::from_uuid(uuid::Uuid::now_v7());
            let request = request(table_uuid, writer, format!("{location}/data/{writer}/{label}.parquet"));
            TeoCatalog::commit_append(&adapter, request.clone())
                .await
                .unwrap();
            commits.push(request);
        }

        let latest = TeoCatalog::load_table(&adapter, &commits[0].table)
            .await
            .unwrap();
        assert_eq!(latest.snapshots.len(), 3);
        assert_eq!(
            latest
                .current_snapshot
                .as_ref()
                .unwrap()
                .summary
                .get(COMMIT_ID_PROPERTY),
            Some(&commits[2].identity.commit_id.to_string())
        );
        assert!(matches!(
            TeoCatalog::check_append_status(&adapter, &commits[0])
                .await
                .unwrap(),
            CommitStatus::Committed(_)
        ));
    }

    #[tokio::test]
    async fn exact_status_resolves_dropped_table_as_not_committed() {
        let (_warehouse, adapter, table) = memory_adapter("status-after-drop").await;
        let table_uuid = table.metadata().uuid();
        let writer = WriterId::from_uuid(uuid::Uuid::now_v7());
        let request = request(
            table_uuid,
            writer,
            format!("{}/data/{writer}/pending.parquet", table.metadata().location()),
        );

        TeoCatalog::drop_table(&adapter, &request.table)
            .await
            .unwrap();

        assert!(matches!(
            TeoCatalog::check_append_status(&adapter, &request)
                .await
                .unwrap(),
            CommitStatus::NotCommitted
        ));
    }

    #[tokio::test]
    async fn mw_t18_restart_races_inflight_request_with_one_logical_append() {
        let (_warehouse, adapter, table) = memory_adapter("mw-t18").await;
        let wal_root = tempfile::tempdir().unwrap();
        let wal_config = teodb_storage::wal::WalConfig {
            root_dir: wal_root.path().to_path_buf(),
            fsync_on_append: true,
            ..Default::default()
        };
        let wal = teodb_storage::wal::WalManager::open(wal_config.clone())
            .await
            .unwrap();
        let identity = wal.writer_identity();
        let table_uuid = table.metadata().uuid();
        let mut request = request(
            table_uuid,
            identity.writer_id,
            format!("s3://warehouse/test/events/data/{}/pending.parquet", identity.writer_id),
        );
        request.identity.writer_epoch = identity.writer_epoch;
        let prepared_file = teodb_core::file::DataFile {
            content: teodb_core::file::DataContent::Data,
            path: ObjectLocation {
                scheme: StorageScheme::S3,
                bucket: Some("warehouse".into()),
                key: format!(
                    "test/events/data/{}/{}-f0000.parquet",
                    identity.writer_id, request.identity.commit_id
                ),
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
        };
        request.added_data_files = vec![data_file(prepared_file.path.to_uri())];
        let prepared = teodb_storage::wal::PreparedFlush::new(
            request.table.clone(),
            table_uuid,
            identity.writer_id,
            identity.writer_epoch,
            request.identity.commit_id,
            request.identity.generations,
            1,
            0,
            vec![prepared_file],
            None,
        );
        wal.persist_prepared(&prepared).await.unwrap();
        wal.release_lease().await;
        drop(wal);

        let guard = AppendAttemptGuard::new(
            adapter.inner.clone(),
            request.clone(),
            adapter.cfg.max_writer_checkpoints_per_table,
            None,
        );
        let (guard, old_request_validated, resume_old_request) = guard.pause_after_first_validation();
        let old_transaction = build_append_transaction(&request, &table).unwrap();
        let request_for_restart = request.clone();

        let (old_result, restart_result) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(
                async {
                    match old_transaction.commit(&guard).await {
                        Ok(table) => Ok(table.metadata_ref()),
                        Err(error) => guard_result(&guard, &request, error),
                    }
                },
                async {
                    old_request_validated
                        .await
                        .expect("old request reached the upstream attempt barrier");
                    let restarted = teodb_storage::wal::WalManager::open(wal_config)
                        .await
                        .expect("restart reopens the same WAL");
                    let sidecars = restarted.list_prepared().await.unwrap();
                    assert_eq!(sidecars, vec![prepared.clone()]);

                    let result = TeoCatalog::commit_append(&adapter, request_for_restart).await;
                    if result.is_ok() {
                        restarted
                            .remove_prepared(table_uuid)
                            .await
                            .unwrap();
                    }
                    resume_old_request.notify_one();
                    assert!(
                        restarted
                            .list_prepared()
                            .await
                            .unwrap()
                            .is_empty(),
                        "successful exact resolution removes the durable sidecar"
                    );
                    result
                }
            )
        })
        .await
        .expect("barrier-driven in-flight crash race completes");

        old_result.expect("delayed old request resolves idempotently");
        restart_result.expect("restart publishes the exact sidecar intent");
        assert_eq!(guard.invocation_count(), 2);

        let latest = TeoCatalog::load_table(&adapter, &request.table)
            .await
            .unwrap();
        assert_eq!(latest.snapshots.len(), 1);
        let checkpoint = parse_writer_checkpoint(&request.table, &latest.properties, identity.writer_id)
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint.commit_id, request.identity.commit_id);
        assert_eq!(checkpoint.generation, request.identity.generations.hi);
    }
}
