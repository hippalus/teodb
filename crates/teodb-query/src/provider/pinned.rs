//! `PinnedScanTableProvider` — Executor-side table provider that scans from
//! a pre-resolved `SnapshotScanDescriptor`.
//!
//! Unlike `TeoTableProvider` which resolves the current snapshot from the
//! catalog at scan time, this provider takes a frozen descriptor and never
//! touches the catalog. This guarantees snapshot isolation in distributed
//! queries: the planning node resolves and pins the snapshot once, then ships
//! the descriptor to all executors.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_execution::object_store::ObjectStoreUrl;

use teodb_core::file::{DataContent, DataFile};
use teodb_core::scan_descriptor::SnapshotScanDescriptor;

use super::scan_builder::SnapshotScanBuilder;
use crate::conversion::schema_to_arrow;
use crate::pruning::{partition_prune, statistics_prune};

/// A read-only `TableProvider` that builds scans from a pinned
/// `SnapshotScanDescriptor`. Used by executors in distributed queries.
pub struct PinnedScanTableProvider {
    descriptor: SnapshotScanDescriptor,
    arrow_schema: SchemaRef,
}

impl PinnedScanTableProvider {
    /// Create a provider from a pre-resolved scan descriptor.
    pub fn try_new(descriptor: SnapshotScanDescriptor) -> teodb_core::error::TeoDBResult<Self> {
        let arrow_schema = schema_to_arrow(&descriptor.schema);
        Ok(Self {
            descriptor,
            arrow_schema,
        })
    }

    /// Returns the underlying descriptor.
    pub fn descriptor(&self) -> &SnapshotScanDescriptor {
        &self.descriptor
    }

    fn ensure_supported_deletes(&self) -> DFResult<()> {
        if self
            .descriptor
            .delete_files
            .iter()
            .any(|file| file.content == DataContent::EqualityDelete)
        {
            return Err(datafusion::error::DataFusionError::NotImplemented(
                "equality-delete files are not yet supported in TeoDB scans".into(),
            ));
        }
        Ok(())
    }

    async fn load_position_deletes(&self, state: &dyn Session) -> DFResult<Option<super::delete::PositionDeleteSet>> {
        let position_deletes: Vec<DataFile> = self
            .descriptor
            .delete_files
            .iter()
            .filter(|file| file.content == DataContent::PositionDelete)
            .cloned()
            .collect();

        if position_deletes.is_empty() {
            return Ok(None);
        }

        tracing::debug!(
            table = %self.descriptor.table_name,
            snapshot_id = self.descriptor.snapshot_id,
            delete_files = position_deletes.len(),
            "position-delete files present in pinned scan; loading scan filter"
        );

        let store_url = object_store_url_from_location(&self.descriptor.table_location)?;
        let store = state.runtime_env().object_store(&store_url)?;
        super::delete::PositionDeleteSet::load_from_object_store(store.as_ref(), &position_deletes)
            .await
            .map(Some)
            .map_err(crate::error::teodb_to_df)
    }
}

impl std::fmt::Debug for PinnedScanTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinnedScanTableProvider")
            .field("table_name", &self.descriptor.table_name)
            .field("snapshot_id", &self.descriptor.snapshot_id)
            .field("data_files", &self.descriptor.data_files.len())
            .finish()
    }
}

#[async_trait]
impl TableProvider for PinnedScanTableProvider {
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
        let data_files = &self.descriptor.data_files;
        self.ensure_supported_deletes()?;

        // Partition pruning using the pinned partition spec.
        let after_pp = partition_prune(data_files, filters, &self.descriptor.partition_spec, &self.arrow_schema)?;

        // Statistics pruning.
        let after_sp = statistics_prune(&after_pp, filters, &self.arrow_schema, state)?;

        tracing::debug!(
            table = %self.descriptor.table_name,
            snapshot_id = self.descriptor.snapshot_id,
            total_files = data_files.len(),
            after_partition_prune = after_pp.len(),
            after_stats_prune = after_sp.len(),
            delete_files = self.descriptor.delete_files.len(),
            "pinned scan pruning complete"
        );

        if after_sp.is_empty() {
            SnapshotScanBuilder::new(self.descriptor.table_location.clone(), self.arrow_schema.clone())
                .projection(projection)
                .build()
        } else {
            let delete_set = self.load_position_deletes(state).await?;
            SnapshotScanBuilder::new(self.descriptor.table_location.clone(), self.arrow_schema.clone())
                .files(after_sp)
                .delete_set(delete_set.as_ref())
                .projection(projection)
                .limit(limit)
                .build()
        }
    }

    fn supports_filters_pushdown(&self, fs: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Always Inexact — same reasoning as `TeoTableProvider`: `Exact`
        // cannot be guaranteed at file granularity, and pruning in `scan`
        // does not depend on it.
        Ok(vec![TableProviderFilterPushDown::Inexact; fs.len()])
    }
}

fn object_store_url_from_location(loc: &teodb_core::location::ObjectLocation) -> DFResult<ObjectStoreUrl> {
    let url_str = loc.scheme.url_prefix(loc.bucket.as_deref());
    ObjectStoreUrl::parse(&url_str)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use teodb_core::file::{DataContent, FileFormat};
    use teodb_core::location::{ObjectLocation, StorageScheme};
    use teodb_core::scan_descriptor::SnapshotScanDescriptor;
    use teodb_core::schema::*;

    fn test_descriptor() -> SnapshotScanDescriptor {
        SnapshotScanDescriptor {
            table_uuid: uuid::Uuid::nil(),
            namespace: "default".into(),
            table_name: "events".into(),
            table_location: ObjectLocation {
                scheme: StorageScheme::Local,
                bucket: None,
                key: "data/events".into(),
            },
            snapshot_id: 42,
            schema: SchemaDefinition {
                schema_id: 0,
                columns: vec![ColumnMeta {
                    id: 1,
                    name: "event_id".into(),
                    data_type: TeoDataType::Int64,
                    nullable: false,
                    doc: None,
                }],
                identifier_field_ids: vec![1],
            },
            partition_spec: PartitionSpec {
                spec_id: 0,
                fields: vec![],
            },
            sort_order: SortOrder {
                order_id: 0,
                fields: vec![],
            },
            data_files: vec![],
            delete_files: vec![],
        }
    }

    fn test_data_file(path: &str, content: DataContent) -> DataFile {
        DataFile {
            content,
            path: ObjectLocation {
                scheme: StorageScheme::Local,
                bucket: None,
                key: path.into(),
            },
            format: FileFormat::Parquet,
            partition_spec_id: 0,
            sort_order_id: Some(0),
            schema_id: 0,
            partition_values: HashMap::new(),
            record_count: 10,
            file_size_bytes: 1024,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            split_offsets: vec![],
            equality_ids: vec![],
            key_metadata: None,
        }
    }

    #[test]
    fn provider_schema_from_descriptor() {
        let provider = PinnedScanTableProvider::try_new(test_descriptor()).unwrap();
        assert_eq!(provider.schema().fields().len(), 1);
        assert_eq!(provider.schema().field(0).name(), "event_id");
        assert_eq!(provider.table_type(), TableType::Base);
    }

    #[tokio::test]
    async fn scan_empty_descriptor_produces_empty_exec() {
        let provider = PinnedScanTableProvider::try_new(test_descriptor()).unwrap();
        let state = datafusion::execution::SessionStateBuilder::new().build();
        let plan = provider
            .scan(&state, None, &[], None)
            .await
            .unwrap();
        assert_eq!(plan.schema().fields().len(), 1);
    }

    #[test]
    fn equality_delete_files_are_rejected() {
        let mut descriptor = test_descriptor();
        descriptor.delete_files.push(test_data_file(
            "data/events/delete-eq.parquet",
            DataContent::EqualityDelete,
        ));
        let provider = PinnedScanTableProvider::try_new(descriptor).unwrap();

        let error = provider.ensure_supported_deletes().unwrap_err();
        assert!(matches!(error, datafusion::error::DataFusionError::NotImplemented(_)));
    }
}
