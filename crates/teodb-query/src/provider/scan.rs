use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

use teodb_core::file::{DataContent, DataFile, TableMetadata};

use super::TeoTableProvider;
use super::scan_builder::SnapshotScanBuilder;
use crate::pruning::{partition_prune, statistics_prune};

struct SnapshotFiles {
    data: Vec<DataFile>,
    position_deletes: Vec<DataFile>,
}

impl TeoTableProvider {
    pub(super) async fn plan_scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let metadata = self.metadata.load();
        let files = snapshot_files(&metadata)?;
        let pruned = self.prune_files(&metadata, &files.data, filters, state)?;
        let pruned_files = pruned.len();
        let delete_set = self
            .load_position_deletes(&files.position_deletes)
            .await?;
        let snapshot_scan = SnapshotScanBuilder::new(metadata.table_location.clone(), self.arrow_schema.clone())
            .files(pruned)
            .delete_set(delete_set.as_ref())
            .projection(projection)
            .limit(limit)
            .build()?;

        tracing::debug!(
            table = %self.ident.name,
            total_files = files.data.len(),
            after_pruning = pruned_files,
            delete_files = files.position_deletes.len(),
            "scan planning complete"
        );

        Ok(snapshot_scan)
    }

    fn prune_files(
        &self,
        metadata: &TableMetadata,
        files: &[DataFile],
        filters: &[Expr],
        state: &dyn Session,
    ) -> DFResult<Vec<DataFile>> {
        let partitioned = partition_prune(
            files,
            filters,
            metadata
                .current_partition_spec()
                .map_err(crate::error::teodb_to_df)?,
            &self.arrow_schema,
        )?;
        statistics_prune(&partitioned, filters, &self.arrow_schema, state)
    }

    async fn load_position_deletes(&self, files: &[DataFile]) -> DFResult<Option<super::delete::PositionDeleteSet>> {
        if files.is_empty() {
            return Ok(None);
        }

        tracing::debug!(
            table = %self.ident.name,
            delete_files = files.len(),
            "position-delete files present; loading scan filter"
        );
        let (storage, base_path) = self
            .storage_factory
            .resolve(&self.metadata.load().table_location)
            .await
            .map_err(crate::error::teodb_to_df)?;
        super::delete::PositionDeleteSet::load(&*storage, files, &base_path)
            .await
            .map(Some)
            .map_err(crate::error::teodb_to_df)
    }
}

fn snapshot_files(metadata: &TableMetadata) -> DFResult<SnapshotFiles> {
    let Some(snapshot) = metadata.current_snapshot.as_ref() else {
        return Ok(SnapshotFiles {
            data: Vec::new(),
            position_deletes: Vec::new(),
        });
    };
    if snapshot
        .delete_files
        .iter()
        .any(|file| file.content == DataContent::EqualityDelete)
    {
        return Err(datafusion::error::DataFusionError::NotImplemented(
            "equality-delete files are not yet supported in TeoDB scans".into(),
        ));
    }

    Ok(SnapshotFiles {
        data: snapshot
            .data_files
            .iter()
            .filter(|file| file.content == DataContent::Data)
            .cloned()
            .collect(),
        position_deletes: snapshot
            .delete_files
            .iter()
            .filter(|file| file.content == DataContent::PositionDelete)
            .cloned()
            .collect(),
    })
}
