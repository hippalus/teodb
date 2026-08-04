//! `TeoTableProvider` — Exposes an Iceberg-format table to DataFusion.
//!
//! Implements `TableProvider` with partition pruning, statistics pruning, and
//! delete-aware Parquet scans. Queries read **flushed** data only: rows still
//! in the ingest hot buffer become visible after a flush commits them to the
//! catalog (no read-after-ingest overlay). See `docs/CONSISTENCY.md`.

use std::sync::Arc;

use arc_swap::ArcSwap;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;

use teodb_core::file::TableMetadata;
use teodb_core::ident::TableIdent;
use teodb_core::traits::catalog::Catalog;
use teodb_core::traits::storage::StorageFactory;

use crate::conversion::schema_to_arrow;

/// A DataFusion `TableProvider` backed by Iceberg table metadata.
pub struct TeoTableProvider {
    pub(super) ident: TableIdent,
    pub(super) metadata: ArcSwap<TableMetadata>,
    pub(super) arrow_schema: SchemaRef,
    catalog: Arc<dyn Catalog>,
    pub(super) storage_factory: Arc<dyn StorageFactory>,
}

impl TeoTableProvider {
    pub fn try_new(
        ident: TableIdent,
        metadata: Arc<TableMetadata>,
        catalog: Arc<dyn Catalog>,
        storage_factory: Arc<dyn StorageFactory>,
    ) -> teodb_core::error::TeoDBResult<Self> {
        let arrow_schema = schema_to_arrow(metadata.current_schema()?);
        Ok(Self {
            ident,
            metadata: ArcSwap::new(metadata),
            arrow_schema,
            catalog,
            storage_factory,
        })
    }

    /// Replace the cached metadata with a fresh load from the catalog.
    pub async fn refresh(&self) -> teodb_core::error::TeoDBResult<()> {
        // Load table metadata and data files in parallel.
        let (metadata, live_files) = tokio::try_join!(
            self.catalog.load_table(&self.ident),
            self.catalog.load_live_files(&self.ident),
        )?;
        let fresh = (*metadata).clone().with_live_files(live_files)?;
        self.metadata.store(Arc::new(fresh));
        Ok(())
    }

    /// Returns the table identifier.
    pub fn ident(&self) -> &TableIdent {
        &self.ident
    }

    /// Returns the snapshot id this provider currently scans, if any.
    pub fn current_snapshot_id(&self) -> Option<teodb_core::ident::SnapshotId> {
        self.metadata
            .load()
            .current_snapshot
            .as_ref()
            .map(|s| s.snapshot_id)
    }

    /// Returns the storage factory for URL registration.
    pub fn storage_factory(&self) -> &Arc<dyn StorageFactory> {
        &self.storage_factory
    }

    /// Build a `SnapshotScanDescriptor` from the current metadata snapshot.
    ///
    /// Used by the codec to serialize the table state for distributed execution.
    /// Returns `None` if the table has no current snapshot.
    pub fn snapshot_scan_descriptor(
        &self,
    ) -> teodb_core::error::TeoDBResult<Option<teodb_core::SnapshotScanDescriptor>> {
        let metadata = self.metadata.load();
        match metadata.current_snapshot.as_ref() {
            Some(snapshot) => {
                let desc = teodb_core::SnapshotScanDescriptor::from_metadata(&metadata, snapshot)?;
                Ok(Some(desc))
            }
            None => Ok(None),
        }
    }
}

impl std::fmt::Debug for TeoTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TeoTableProvider")
            .field("ident", &self.ident)
            .finish()
    }
}

#[async_trait]
impl TableProvider for TeoTableProvider {
    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.plan_scan(state, projection, filters, limit)
            .await
    }

    fn supports_filters_pushdown(&self, fs: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Always Inexact: pruning happens in `scan` either way, and `Exact`
        // (which makes DataFusion drop the filter above the scan) cannot be
        // guaranteed at file granularity — transformed partition values,
        // null/absent partition values, and metadata refreshes between
        // planning and scan would all leak non-matching rows.
        Ok(vec![TableProviderFilterPushDown::Inexact; fs.len()])
    }
}
