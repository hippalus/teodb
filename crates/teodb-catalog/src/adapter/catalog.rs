//! Iceberg implementation of TeoDB's catalog boundary.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use reqwest::Client;
use tracing::{debug, instrument};

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::file::{DataFile, TableMetadata};
use teodb_core::ident::TableIdent;
use teodb_core::traits::catalog::{Catalog, CommitAppend, CommitReplace, CommitStatus, CreateTableRequest};

use super::config::{IcebergCatalogConfig, IcebergCredentials};
use super::idents::{make_namespace, make_table_ident};
use super::manifests;
use crate::error::map_iceberg_error;

/// Implements the TeoDB `Catalog` trait by delegating to an
/// `iceberg_catalog_rest::RestCatalog`.
pub struct IcebergCatalogAdapter {
    pub(super) inner: Arc<dyn iceberg::Catalog>,
    pub(super) cfg: IcebergCatalogConfig,
    pub(super) observer: Option<Arc<dyn crate::CatalogObserver>>,
}

impl IcebergCatalogAdapter {
    /// Open a connection to an Iceberg REST catalog.
    pub async fn open(cfg: IcebergCatalogConfig) -> TeoDBResult<Self> {
        use iceberg::CatalogBuilder;
        use iceberg_catalog_rest::RestCatalogBuilder;

        let mut props = HashMap::from([
            ("uri".to_string(), cfg.uri.clone()),
            ("warehouse".to_string(), cfg.warehouse.clone()),
        ]);

        match &cfg.credentials {
            IcebergCredentials::None => {}
            IcebergCredentials::Bearer { token } => {
                props.insert("token".to_string(), token.clone());
            }
            IcebergCredentials::OAuth2 {
                credential,
                scope,
                oauth_server_uri,
            } => {
                props.insert("credential".to_string(), credential.clone());
                if let Some(s) = scope {
                    props.insert("scope".to_string(), s.clone());
                }
                if let Some(uri) = oauth_server_uri {
                    props.insert("oauth2-server-uri".to_string(), uri.clone());
                }
            }
        }

        debug!(uri = %cfg.uri, warehouse = %cfg.warehouse, "opening Iceberg REST catalog");

        // Merge S3 storage properties so OpenDAL picks up credentials.
        props.extend(cfg.s3_props.clone());

        let storage_factory = iceberg_storage_opendal::OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        };
        let client = Client::builder()
            .timeout(cfg.request_timeout)
            .build()
            .map_err(|error| TeoDBError::Catalog(format!("failed to build REST catalog HTTP client: {error}")))?;

        let catalog = RestCatalogBuilder::default()
            .with_client(client)
            .with_storage_factory(Arc::new(storage_factory))
            .load("teodb", props)
            .await
            .map_err(map_iceberg_error)?;

        Ok(Self::from_catalog(Arc::new(catalog), cfg))
    }

    /// Construct the adapter over an already-open Iceberg catalog.
    ///
    /// This dependency-injection boundary is also used by deterministic
    /// component tests and performance harnesses.
    pub fn from_catalog(inner: Arc<dyn iceberg::Catalog>, cfg: IcebergCatalogConfig) -> Self {
        Self {
            inner,
            cfg,
            observer: None,
        }
    }

    /// Attach a low-cardinality protocol observer.
    pub fn with_observer(mut self, observer: Arc<dyn crate::CatalogObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Load an iceberg table by TeoDB ident.
    pub(super) async fn load_iceberg_table(&self, ident: &TableIdent) -> TeoDBResult<iceberg::table::Table> {
        self.inner
            .load_table(&make_table_ident(ident))
            .await
            .map_err(map_iceberg_error)
    }

    fn domain_metadata(ident: &TableIdent, metadata: &iceberg::spec::TableMetadata) -> TeoDBResult<Arc<TableMetadata>> {
        crate::convert::iceberg_to_teo_metadata(ident, metadata, &[], &[]).map(Arc::new)
    }

    async fn resolve_append_status(&self, req: &CommitAppend) -> TeoDBResult<CommitStatus> {
        use teodb_core::write_protocol::{
            parse_writer_checkpoint, snapshot_matches_append_identity, validate_writer_checkpoints,
        };

        req.identity
            .validate(&req.table, req.table_uuid)?;
        let table = match self.load_iceberg_table(&req.table).await {
            Ok(table) => table,
            Err(TeoDBError::NotFound { .. }) => return Ok(CommitStatus::NotCommitted),
            Err(error) => {
                return Ok(CommitStatus::Unknown {
                    message: error.to_string(),
                });
            }
        };
        let metadata = table.metadata();
        if metadata.uuid() != req.table_uuid {
            return Err(TeoDBError::TableIncarnationMismatch {
                table: req.table.clone(),
                expected: req.table_uuid,
                actual: metadata.uuid(),
            });
        }

        let mut found_in_history = false;
        for snapshot in metadata.snapshots() {
            found_in_history |= snapshot_matches_append_identity(
                &req.table,
                req.table_uuid,
                &req.identity,
                snapshot.snapshot_id(),
                &snapshot.summary().additional_properties,
            )?;
        }
        validate_writer_checkpoints(&req.table, metadata.properties())?;
        let checkpoint = parse_writer_checkpoint(&req.table, metadata.properties(), req.identity.writer_id)?;
        let found_in_checkpoint = checkpoint.as_ref().is_some_and(|checkpoint| {
            checkpoint.commit_id == req.identity.commit_id
                && checkpoint.epoch == req.identity.writer_epoch
                && checkpoint.generation == req.identity.generations.hi
        });
        if let Some(checkpoint) = checkpoint
            && checkpoint.commit_id == req.identity.commit_id
            && !found_in_checkpoint
        {
            return Err(TeoDBError::MetadataCorruption {
                table: req.table.clone(),
                message: format!(
                    "writer checkpoint reuses commit ID {} with a mismatched epoch or generation",
                    req.identity.commit_id
                ),
            });
        }

        if found_in_history || found_in_checkpoint {
            Ok(CommitStatus::Committed(Self::domain_metadata(&req.table, metadata)?))
        } else {
            Ok(CommitStatus::NotCommitted)
        }
    }
}

#[async_trait]
impl Catalog for IcebergCatalogAdapter {
    #[instrument(name = "catalog.list_namespaces", skip_all)]
    async fn list_namespaces(&self) -> TeoDBResult<Vec<String>> {
        let namespaces = self
            .inner
            .list_namespaces(None)
            .await
            .map_err(map_iceberg_error)?;

        Ok(namespaces
            .into_iter()
            .map(|ns| ns.iter().cloned().collect::<Vec<_>>().join("."))
            .collect())
    }

    #[instrument(
        name = "catalog.create_namespace",
        skip_all,
        fields(namespace = %namespace, property_count = properties.len())
    )]
    async fn create_namespace(&self, namespace: &str, properties: HashMap<String, String>) -> TeoDBResult<()> {
        let ns = make_namespace(namespace);
        self.inner
            .create_namespace(&ns, properties)
            .await
            .map_err(map_iceberg_error)?;
        Ok(())
    }

    #[instrument(name = "catalog.drop_namespace", skip_all, fields(namespace = %namespace))]
    async fn drop_namespace(&self, namespace: &str) -> TeoDBResult<()> {
        let ns = make_namespace(namespace);
        self.inner
            .drop_namespace(&ns)
            .await
            .map_err(map_iceberg_error)
    }

    #[instrument(name = "catalog.list_tables", skip_all, fields(namespace = %namespace))]
    async fn list_tables(&self, namespace: &str) -> TeoDBResult<Vec<TableIdent>> {
        let ns = make_namespace(namespace);
        let tables = self
            .inner
            .list_tables(&ns)
            .await
            .map_err(map_iceberg_error)?;

        Ok(tables
            .into_iter()
            .map(|id| {
                let ns_str = id
                    .namespace()
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(".");
                TableIdent::new(ns_str, id.name())
            })
            .collect())
    }

    #[instrument(name = "catalog.load_table", skip_all, fields(table = %ident))]
    async fn load_table(&self, ident: &TableIdent) -> TeoDBResult<Arc<TableMetadata>> {
        let metadata = self
            .load_iceberg_table(ident)
            .await?
            .metadata_ref();
        match teodb_core::write_protocol::validate_writer_checkpoints(ident, metadata.properties()) {
            Ok(count) => {
                if let Some(observer) = &self.observer {
                    observer.on_writer_checkpoint_count(count);
                }
            }
            Err(error) => {
                if let Some(observer) = &self.observer {
                    observer.on_writer_checkpoint_parse_failure();
                }
                return Err(error);
            }
        }
        Self::domain_metadata(ident, &metadata)
    }

    #[instrument(
        name = "catalog.create_table",
        skip_all,
        fields(table = %req.ident, property_count = req.properties.len())
    )]
    async fn create_table(&self, req: CreateTableRequest) -> TeoDBResult<Arc<TableMetadata>> {
        let ns = make_namespace(&req.ident.namespace);
        let schema = crate::convert::teodb_schema_to_iceberg(&req.schema)?;
        let partition_spec = crate::convert::teodb_unbound_partition_spec_to_iceberg(&req.partition_spec)?;
        let sort_order = crate::convert::teodb_sort_order_to_iceberg(&req.sort_order);

        let creation = iceberg::TableCreation::builder()
            .name(req.ident.name.clone())
            .schema(schema)
            .partition_spec(partition_spec)
            .sort_order(sort_order)
            .location(req.location.to_uri())
            .properties(req.properties.clone())
            .build();

        let table = self
            .inner
            .create_table(&ns, creation)
            .await
            .map_err(map_iceberg_error)?;

        Self::domain_metadata(&req.ident, table.metadata())
    }

    #[instrument(name = "catalog.drop_table", skip_all, fields(table = %ident))]
    async fn drop_table(&self, ident: &TableIdent) -> TeoDBResult<()> {
        let iceberg_ident = make_table_ident(ident);
        self.inner
            .drop_table(&iceberg_ident)
            .await
            .map_err(map_iceberg_error)
    }

    #[instrument(name = "catalog.load_live_files", skip_all, fields(table = %ident))]
    async fn load_live_files(&self, ident: &TableIdent) -> TeoDBResult<Vec<DataFile>> {
        let table = self.load_iceberg_table(ident).await?;
        let files = manifests::ManifestReader::new(&table)
            .live_data_files()
            .await?;
        crate::convert::iceberg_data_files_to_teodb(&files)
    }

    #[instrument(name = "catalog.load_referenced_files", skip_all, fields(table = %ident))]
    async fn load_all_referenced_file_paths(&self, ident: &TableIdent) -> TeoDBResult<HashSet<String>> {
        let table = self.load_iceberg_table(ident).await?;
        manifests::ManifestReader::new(&table)
            .collect_referenced_paths(|_| true)
            .await
    }

    #[instrument(
        name = "catalog.load_retained_files",
        skip_all,
        fields(table = %ident, protected_snapshots = protected.len())
    )]
    async fn load_retained_file_paths(
        &self,
        ident: &TableIdent,
        retention: &teodb_core::snapshot_retention::SnapshotRetention,
        protected: &HashSet<teodb_core::ident::SnapshotId>,
    ) -> TeoDBResult<teodb_core::traits::catalog::RetainedFileSet> {
        let table = self.load_iceberg_table(ident).await?;
        manifests::ManifestReader::new(&table)
            .retained_file_set(ident, retention, protected)
            .await
    }

    #[instrument(
        name = "catalog.commit_append",
        skip_all,
        fields(
            table = %req.table,
            commit_id = %req.identity.commit_id,
            data_files = req.added_data_files.len()
        )
    )]
    async fn commit_append(&self, req: CommitAppend) -> TeoDBResult<Arc<TableMetadata>> {
        let ident = req.table.clone();
        let metadata = self.commit_append_with_retry(req).await?;
        Self::domain_metadata(&ident, &metadata)
    }

    #[instrument(
        name = "catalog.check_append_status",
        skip_all,
        fields(table = %req.table, commit_id = %req.identity.commit_id)
    )]
    async fn check_append_status(&self, req: &CommitAppend) -> TeoDBResult<teodb_core::traits::catalog::CommitStatus> {
        let started = std::time::Instant::now();
        let result = self.resolve_append_status(req).await;
        if let Some(observer) = &self.observer {
            let outcome = match &result {
                Ok(teodb_core::traits::catalog::CommitStatus::Committed(_)) => {
                    crate::CatalogStatusCheckOutcome::Committed
                }
                Ok(teodb_core::traits::catalog::CommitStatus::NotCommitted) => {
                    crate::CatalogStatusCheckOutcome::NotCommitted
                }
                Ok(teodb_core::traits::catalog::CommitStatus::Unknown { .. }) => {
                    crate::CatalogStatusCheckOutcome::Unknown
                }
                Err(_) => crate::CatalogStatusCheckOutcome::Error,
            };
            observer.on_status_check(outcome, started.elapsed());
        }
        result
    }

    #[instrument(
        name = "catalog.commit_replace",
        skip_all,
        fields(
            table = %req.table,
            snapshot_id = req.base_snapshot_id,
            added_files = req.added_data_files.len(),
            removed_files = req.removed_data_files.len()
        )
    )]
    async fn commit_replace(&self, req: CommitReplace) -> TeoDBResult<Arc<TableMetadata>> {
        let ident = req.table.clone();
        let metadata = self.commit_replace_with_retry(req).await?;
        Self::domain_metadata(&ident, &metadata)
    }

    #[instrument(
        name = "catalog.update_table_properties",
        skip_all,
        fields(
            table = %ident,
            expected_properties = expected.len(),
            updated_properties = updates.len(),
            removed_properties = removals.len()
        )
    )]
    async fn update_table_properties(
        &self,
        ident: &TableIdent,
        expected: HashMap<String, String>,
        updates: HashMap<String, String>,
        removals: Vec<String>,
    ) -> TeoDBResult<Arc<TableMetadata>> {
        let table = self.load_iceberg_table(ident).await?;

        // CAS check: verify expected properties match current values.
        let current_props = table.metadata().properties();
        for (key, expected_val) in &expected {
            let actual = current_props
                .get(key)
                .map(|v| v.as_str())
                .unwrap_or("");
            if actual != expected_val.as_str() {
                return Err(TeoDBError::Conflict {
                    resource: format!("property '{key}'"),
                    expected: expected_val.clone(),
                    actual: actual.to_string(),
                });
            }
        }

        let tx = iceberg::transaction::Transaction::new(&table);
        let mut action = tx.update_table_properties();
        for (k, v) in updates {
            action = action.set(k, v);
        }
        for k in removals {
            action = action.remove(k);
        }
        let tx = iceberg::transaction::ApplyTransactionAction::apply(action, tx).map_err(map_iceberg_error)?;
        let updated = tx
            .commit(&*self.inner)
            .await
            .map_err(map_iceberg_error)?;

        Self::domain_metadata(ident, updated.metadata())
    }
}
