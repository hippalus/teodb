//! Catalog decorator that revalidates a TeoDB append on every Iceberg
//! transaction attempt, including automatic conflict rebase reloads.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use teodb_core::error::TeoDBError;
use teodb_core::traits::catalog::CommitAppend;
use teodb_core::write_protocol::{
    parse_writer_checkpoint, snapshot_matches_append_identity, validate_writer_checkpoints, writer_checkpoint_key,
};

#[derive(Debug, Clone)]
pub(super) enum GuardRejection {
    AlreadyCommitted(Arc<iceberg::spec::TableMetadata>),
    StaleWriterEpoch {
        current: teodb_core::write_protocol::WriterEpoch,
    },
    WriterRegistryFull,
    TableIncarnationMismatch {
        actual: uuid::Uuid,
    },
    MetadataCorruption(String),
    ProtocolViolation(String),
}

impl GuardRejection {
    pub(super) fn into_teodb_error(self, request: &CommitAppend, limit: usize) -> TeoDBError {
        match self {
            Self::AlreadyCommitted(_) => TeoDBError::WriteProtocol {
                table: request.table.clone(),
                message: "already committed guard outcome consumed as an error".into(),
            },
            Self::StaleWriterEpoch { current } => TeoDBError::StaleWriterEpoch {
                table: request.table.clone(),
                writer_id: request.identity.writer_id,
                request_epoch: request.identity.writer_epoch,
                current_epoch: current,
            },
            Self::WriterRegistryFull => TeoDBError::WriterRegistryFull {
                table: request.table.clone(),
                limit,
            },
            Self::TableIncarnationMismatch { actual } => TeoDBError::TableIncarnationMismatch {
                table: request.table.clone(),
                expected: request.table_uuid,
                actual,
            },
            Self::MetadataCorruption(message) => TeoDBError::MetadataCorruption {
                table: request.table.clone(),
                message,
            },
            Self::ProtocolViolation(message) => TeoDBError::WriteProtocol {
                table: request.table.clone(),
                message,
            },
        }
    }
}

#[derive(Default)]
struct GuardState {
    rejection: Mutex<Option<GuardRejection>>,
    invocations: AtomicU32,
}

pub(super) struct AppendAttemptGuard {
    inner: Arc<dyn iceberg::Catalog>,
    request: CommitAppend,
    max_writer_checkpoints: usize,
    observer: Option<Arc<dyn crate::CatalogObserver>>,
    state: Arc<GuardState>,
    #[cfg(test)]
    pause_after_first_validation: Option<Arc<ValidationPause>>,
}

#[cfg(test)]
struct ValidationPause {
    reached: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    resume: Arc<tokio::sync::Notify>,
}

impl AppendAttemptGuard {
    pub(super) fn new(
        inner: Arc<dyn iceberg::Catalog>,
        request: CommitAppend,
        max_writer_checkpoints: usize,
        observer: Option<Arc<dyn crate::CatalogObserver>>,
    ) -> Self {
        Self {
            inner,
            request,
            max_writer_checkpoints,
            observer,
            state: Arc::new(GuardState::default()),
            #[cfg(test)]
            pause_after_first_validation: None,
        }
    }

    pub(super) fn take_rejection(&self) -> Option<GuardRejection> {
        self.state
            .rejection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    pub(super) fn max_writer_checkpoints(&self) -> usize {
        self.max_writer_checkpoints
    }

    pub(super) fn invocation_count(&self) -> u32 {
        self.state.invocations.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(super) fn pause_after_first_validation(
        mut self,
    ) -> (Self, tokio::sync::oneshot::Receiver<()>, Arc<tokio::sync::Notify>) {
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let resume = Arc::new(tokio::sync::Notify::new());
        self.pause_after_first_validation = Some(Arc::new(ValidationPause {
            reached: Mutex::new(Some(reached_tx)),
            resume: resume.clone(),
        }));
        (self, reached_rx, resume)
    }

    fn reject(&self, rejection: GuardRejection) -> iceberg::Error {
        *self
            .state
            .rejection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(rejection);
        iceberg::Error::new(
            iceberg::ErrorKind::PreconditionFailed,
            "append rejected by typed TeoDB attempt guard",
        )
        .with_retryable(false)
    }

    fn validate(&self, metadata: &iceberg::spec::TableMetadata) -> Result<(), iceberg::Error> {
        self.state
            .invocations
            .fetch_add(1, Ordering::Relaxed);

        self.request
            .identity
            .validate(&self.request.table, self.request.table_uuid)
            .map_err(|error| self.reject(GuardRejection::ProtocolViolation(error.to_string())))?;
        if metadata.uuid() != self.request.table_uuid {
            return Err(self.reject(GuardRejection::TableIncarnationMismatch {
                actual: metadata.uuid(),
            }));
        }

        let checkpoint_count =
            validate_writer_checkpoints(&self.request.table, metadata.properties()).map_err(|error| {
                if let Some(observer) = &self.observer {
                    observer.on_writer_checkpoint_parse_failure();
                }
                self.reject(GuardRejection::MetadataCorruption(error.to_string()))
            })?;
        if let Some(observer) = &self.observer {
            observer.on_writer_checkpoint_count(checkpoint_count);
        }
        let own_checkpoint = parse_writer_checkpoint(
            &self.request.table,
            metadata.properties(),
            self.request.identity.writer_id,
        )
        .map_err(|error| self.reject(GuardRejection::MetadataCorruption(error.to_string())))?;

        let mut exact_snapshot = false;
        for snapshot in metadata.snapshots() {
            let matches = snapshot_matches_append_identity(
                &self.request.table,
                self.request.table_uuid,
                &self.request.identity,
                snapshot.snapshot_id(),
                &snapshot.summary().additional_properties,
            )
            .map_err(|error| self.reject(GuardRejection::MetadataCorruption(error.to_string())))?;
            exact_snapshot |= matches;
        }
        let exact_checkpoint = if let Some(checkpoint) = own_checkpoint.as_ref() {
            if checkpoint.commit_id == self.request.identity.commit_id {
                if checkpoint.epoch != self.request.identity.writer_epoch
                    || checkpoint.generation != self.request.identity.generations.hi
                {
                    return Err(self.reject(GuardRejection::MetadataCorruption(format!(
                        "writer checkpoint reuses commit ID {} with a mismatched epoch or generation",
                        self.request.identity.commit_id
                    ))));
                }
                true
            } else {
                false
            }
        } else {
            false
        };
        if exact_snapshot || exact_checkpoint {
            return Err(self.reject(GuardRejection::AlreadyCommitted(Arc::new(metadata.clone()))));
        }

        let own_key = writer_checkpoint_key(self.request.identity.writer_id);
        if !metadata.properties().contains_key(&own_key) && checkpoint_count >= self.max_writer_checkpoints {
            return Err(self.reject(GuardRejection::WriterRegistryFull));
        }

        if let Some(checkpoint) = own_checkpoint {
            if self.request.identity.writer_epoch < checkpoint.epoch {
                return Err(self.reject(GuardRejection::StaleWriterEpoch {
                    current: checkpoint.epoch,
                }));
            }
            if self.request.identity.generations.lo <= checkpoint.generation {
                return Err(self.reject(GuardRejection::ProtocolViolation(format!(
                    "generation range {}-{} overlaps committed generation {} with a different commit ID",
                    self.request.identity.generations.lo, self.request.identity.generations.hi, checkpoint.generation
                ))));
            }
        }

        Ok(())
    }
}

impl fmt::Debug for AppendAttemptGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AppendAttemptGuard")
            .field("table", &self.request.table)
            .field("commit_id", &self.request.identity.commit_id)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl iceberg::Catalog for AppendAttemptGuard {
    async fn list_namespaces(
        &self,
        parent: Option<&iceberg::NamespaceIdent>,
    ) -> iceberg::Result<Vec<iceberg::NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &iceberg::NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<iceberg::Namespace> {
        self.inner
            .create_namespace(namespace, properties)
            .await
    }

    async fn get_namespace(&self, namespace: &iceberg::NamespaceIdent) -> iceberg::Result<iceberg::Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &iceberg::NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &iceberg::NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner
            .update_namespace(namespace, properties)
            .await
    }

    async fn drop_namespace(&self, namespace: &iceberg::NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &iceberg::NamespaceIdent) -> iceberg::Result<Vec<iceberg::TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &iceberg::NamespaceIdent,
        creation: iceberg::TableCreation,
    ) -> iceberg::Result<iceberg::table::Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &iceberg::TableIdent) -> iceberg::Result<iceberg::table::Table> {
        let loaded = self.inner.load_table(table).await?;
        if table == loaded.identifier() && table == &super::idents::make_table_ident(&self.request.table) {
            self.validate(loaded.metadata())?;
            #[cfg(test)]
            if let Some(pause) = &self.pause_after_first_validation {
                let reached = pause
                    .reached
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                if let Some(reached) = reached {
                    let _ = reached.send(());
                    pause.resume.notified().await;
                }
            }
        }
        Ok(loaded)
    }

    async fn drop_table(&self, table: &iceberg::TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(table).await
    }

    async fn purge_table(&self, table: &iceberg::TableIdent) -> iceberg::Result<()> {
        self.inner.purge_table(table).await
    }

    async fn table_exists(&self, table: &iceberg::TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &iceberg::TableIdent, dest: &iceberg::TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        table: &iceberg::TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<iceberg::table::Table> {
        self.inner
            .register_table(table, metadata_location)
            .await
    }

    async fn update_table(&self, commit: iceberg::TableCommit) -> iceberg::Result<iceberg::table::Table> {
        self.inner.update_table(commit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use iceberg::spec::{
        FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadata, TableMetadataBuilder, Type,
        UnboundPartitionSpec,
    };
    use iceberg::{Catalog as _, CatalogBuilder as _};
    use teodb_core::file::{DataContent, DataFile, FileFormat};
    use teodb_core::ident::TableIdent;
    use teodb_core::write_protocol::{
        AppendCommitIdentity, CommitId, GenerationRange, WriterCheckpoint, WriterEpoch, WriterId, writer_checkpoint_key,
    };

    fn metadata() -> TableMetadata {
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        TableMetadataBuilder::new(
            schema,
            UnboundPartitionSpec::builder()
                .with_spec_id(0)
                .build(),
            SortOrder {
                order_id: 0,
                fields: Vec::new(),
            },
            "memory://warehouse/test/events".into(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    fn with_properties(metadata: &TableMetadata, properties: HashMap<String, String>) -> TableMetadata {
        metadata
            .clone()
            .into_builder(None)
            .set_properties(properties)
            .unwrap()
            .build()
            .unwrap()
            .metadata
    }

    fn request(metadata: &TableMetadata, writer_id: WriterId) -> CommitAppend {
        CommitAppend {
            table: TableIdent::new("test", "events"),
            table_uuid: metadata.uuid(),
            identity: AppendCommitIdentity {
                writer_id,
                writer_epoch: WriterEpoch::new(7),
                commit_id: CommitId::now_v7(),
                generations: GenerationRange::new(1, 2).unwrap(),
            },
            base_snapshot_id: None,
            added_data_files: Vec::new(),
            properties: HashMap::new(),
        }
    }

    async fn guard(request: CommitAppend, limit: usize) -> AppendAttemptGuard {
        let warehouse = tempfile::tempdir().unwrap();
        let catalog = iceberg::memory::MemoryCatalogBuilder::default()
            .load(
                "guard-test",
                HashMap::from([(
                    iceberg::memory::MEMORY_CATALOG_WAREHOUSE.into(),
                    warehouse.path().to_string_lossy().into_owned(),
                )]),
            )
            .await
            .unwrap();
        AppendAttemptGuard::new(Arc::new(catalog), request, limit, None)
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

    #[tokio::test]
    async fn mw_t16_transaction_retry_revalidates_and_fences_stale_epoch() {
        let warehouse = tempfile::tempdir().unwrap();
        let catalog = Arc::new(
            iceberg::memory::MemoryCatalogBuilder::default()
                .load(
                    "mw-t16",
                    HashMap::from([(
                        iceberg::memory::MEMORY_CATALOG_WAREHOUSE.into(),
                        warehouse.path().to_string_lossy().into_owned(),
                    )]),
                )
                .await
                .unwrap(),
        );
        let namespace = iceberg::NamespaceIdent::new("test".into());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let table = catalog
            .create_table(
                &namespace,
                iceberg::TableCreation::builder()
                    .name("events".into())
                    .schema(metadata().current_schema().as_ref().clone())
                    .properties([
                        ("commit.retry.min-wait-ms".into(), "0".into()),
                        ("commit.retry.max-wait-ms".into(), "0".into()),
                        ("commit.retry.total-timeout-ms".into(), "1000".into()),
                        ("commit.retry.num-retries".into(), "4".into()),
                    ])
                    .build(),
            )
            .await
            .unwrap();
        let base = table.metadata();
        let writer_id = WriterId::from_uuid(uuid::Uuid::now_v7());
        let location = base.location().trim_end_matches('/');
        let mut stale = request(base, writer_id);
        stale.added_data_files = vec![data_file(format!("{location}/data/{writer_id}/epoch-7.parquet"))];
        let mut newer = request(base, writer_id);
        newer.identity.writer_epoch = WriterEpoch::new(8);
        newer.identity.commit_id = CommitId::now_v7();
        newer.identity.generations = GenerationRange::new(3, 3).unwrap();
        newer.added_data_files = vec![data_file(format!("{location}/data/{writer_id}/epoch-8.parquet"))];

        let guard = AppendAttemptGuard::new(catalog.clone(), stale.clone(), 8, None);
        let (guard, first_validation_reached, resume_stale) = guard.pause_after_first_validation();
        let stale_tx = super::super::commit::build_append_transaction(&stale, &table).unwrap();
        let newer_adapter = super::super::IcebergCatalogAdapter::from_catalog(
            catalog.clone(),
            super::super::config::IcebergCatalogConfig::default(),
        );

        let (stale_result, newer_result) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(stale_tx.commit(&guard), async {
                first_validation_reached
                    .await
                    .expect("epoch-7 attempt reaches the guarded load");
                let result = teodb_core::traits::catalog::Catalog::commit_append(&newer_adapter, newer.clone()).await;
                resume_stale.notify_one();
                result
            })
        })
        .await
        .expect("barrier-driven retry completes without timing assumptions");

        newer_result.expect("epoch 8 commits while epoch 7 is paused");
        stale_result.expect_err("epoch 7 must be rejected during conflict rebase");
        assert_eq!(
            guard.invocation_count(),
            2,
            "the guard runs once for the initial transaction attempt and once for its rebase"
        );
        let rejection = guard
            .take_rejection()
            .expect("rebase records a typed rejection");
        let error = rejection.into_teodb_error(&stale, 8);
        assert!(matches!(
            error,
            TeoDBError::StaleWriterEpoch { current_epoch, .. }
                if current_epoch == WriterEpoch::new(8)
        ));

        let latest = catalog
            .load_table(table.identifier())
            .await
            .unwrap();
        let checkpoint = parse_writer_checkpoint(&stale.table, latest.metadata().properties(), writer_id)
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint.epoch, WriterEpoch::new(8));
        assert_eq!(checkpoint.generation, 3);
        assert_eq!(latest.metadata().snapshots().count(), 1);
    }

    #[tokio::test]
    async fn exact_commit_id_with_mismatched_epoch_is_corruption() {
        let base = metadata();
        let writer_id = WriterId::from_uuid(uuid::Uuid::now_v7());
        let request = request(&base, writer_id);
        let checkpoint = WriterCheckpoint::new(
            WriterEpoch::new(8),
            request.identity.generations.hi,
            request.identity.commit_id,
            1,
        )
        .encode()
        .unwrap();
        let metadata = with_properties(&base, HashMap::from([(writer_checkpoint_key(writer_id), checkpoint)]));
        let guard = guard(request, 8).await;

        guard
            .validate(&metadata)
            .expect_err("commit ID reuse must fail closed");
        assert!(matches!(
            guard.take_rejection(),
            Some(GuardRejection::MetadataCorruption(_))
        ));
    }

    #[tokio::test]
    async fn malformed_foreign_checkpoint_fails_closed() {
        let base = metadata();
        let writer_id = WriterId::from_uuid(uuid::Uuid::now_v7());
        let foreign = WriterId::from_uuid(uuid::Uuid::now_v7());
        let malformed = with_properties(
            &base,
            HashMap::from([(writer_checkpoint_key(foreign), "{not-json".into())]),
        );
        let malformed_guard = guard(request(&base, writer_id), 1).await;
        malformed_guard
            .validate(&malformed)
            .expect_err("foreign corruption must not be ignored");
        assert!(matches!(
            malformed_guard.take_rejection(),
            Some(GuardRejection::MetadataCorruption(_))
        ));
    }

    #[tokio::test]
    async fn mw_t11_writer_registry_bound_allows_existing_and_rejects_new() {
        let base = metadata();
        let writer_id = WriterId::from_uuid(uuid::Uuid::now_v7());
        let foreign = WriterId::from_uuid(uuid::Uuid::now_v7());
        let full = with_properties(
            &base,
            HashMap::from([(
                writer_checkpoint_key(foreign),
                WriterCheckpoint::new(WriterEpoch::new(1), 1, CommitId::now_v7(), 1)
                    .encode()
                    .unwrap(),
            )]),
        );
        let full_guard = guard(request(&base, writer_id), 1).await;
        full_guard
            .validate(&full)
            .expect_err("new writer must respect the registry bound");
        assert!(matches!(
            full_guard.take_rejection(),
            Some(GuardRejection::WriterRegistryFull)
        ));

        let mut existing_request = request(&base, foreign);
        existing_request.identity.generations = GenerationRange::new(2, 2).unwrap();
        let existing_guard = guard(existing_request, 1).await;
        existing_guard
            .validate(&full)
            .expect("an existing writer may advance at the registry bound");
    }
}
