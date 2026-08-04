use std::collections::HashMap;
use std::sync::Arc;

use datafusion::prelude::*;
use futures::TryStreamExt;
use tracing::{debug, info};

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::file::{DataContent, DataFile, TableMetadata};
use teodb_core::location::ObjectLocation;
use teodb_core::scan_descriptor::SnapshotScanDescriptor;
use teodb_query::PinnedScanTableProvider;

use super::{CompactionPlan, Compactor};
use crate::error::from_datafusion;

/// Output of a compaction read/sort/write: the new data files plus the
/// position-delete files made obsolete by the rewrite (every data file they
/// referenced was an input), which the commit removes.
pub(super) struct CompactionWrite {
    pub data_files: Vec<DataFile>,
    pub resolved_deletes: Vec<ObjectLocation>,
}

impl Compactor {
    /// Build the DataFusion session for a compaction run: object store
    /// registered, memory pool bounded (sorts spill to disk beyond it).
    fn compaction_session(&self) -> TeoDBResult<datafusion::execution::context::SessionContext> {
        use datafusion::execution::context::SessionContext;
        use datafusion::execution::memory_pool::FairSpillPool;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;

        let mut builder = RuntimeEnvBuilder::new();
        if let Some(bytes) = self.memory_pool_bytes {
            builder = builder.with_memory_pool(Arc::new(FairSpillPool::new(bytes as usize)));
        }
        if let Some(dir) = &self.spill_dir {
            std::fs::create_dir_all(dir)
                .map_err(|error| TeoDBError::Internal(format!("failed to create compaction spill dir: {error}")))?;
            builder = builder.with_disk_manager_builder(
                datafusion::execution::disk_manager::DiskManagerBuilder::default().with_mode(
                    datafusion::execution::disk_manager::DiskManagerMode::Directories(vec![dir.clone()]),
                ),
            );
        }
        let runtime = builder
            .build_arc()
            .map_err(|error| TeoDBError::Internal(format!("compaction RuntimeEnv build failed: {error}")))?;

        runtime.register_object_store(self.object_store.parsed_url(), self.object_store.store());

        Ok(SessionContext::new_with_config_rt(
            datafusion::prelude::SessionConfig::new(),
            runtime,
        ))
    }

    /// Count valid position-delete entries per target data file.
    #[tracing::instrument(
        name = "compaction.count_position_deletes",
        skip_all,
        fields(delete_files = delete_files.len(), data_files = data_files.len())
    )]
    pub async fn count_position_deletes(
        &self,
        delete_files: &[DataFile],
        data_files: &[DataFile],
    ) -> TeoDBResult<HashMap<String, u64>> {
        let mut counts = HashMap::new();

        for delete_file in delete_files
            .iter()
            .filter(|file| file.content == DataContent::PositionDelete)
        {
            let (storage, path) = self.storage.resolve(&delete_file.path).await?;
            let bytes = storage.get(&path).await?;
            let positions = teodb_storage::parquet::read_position_deletes(bytes)?;

            for (recorded_path, deleted) in &positions {
                if let Some(input) = find_input_for_path(data_files, recorded_path) {
                    let valid_positions = deleted
                        .iter()
                        .filter(|pos| **pos >= 0 && (**pos as u64) < input.record_count)
                        .count() as u64;
                    if valid_positions > 0 {
                        *counts.entry(input.path.to_uri()).or_insert(0) += valid_positions;
                    }
                }
            }
        }

        Ok(counts)
    }

    /// Read input files (applying position deletes), sort by the table's sort
    /// order, and stream output.
    ///
    /// Any input read failure aborts the plan: a skipped input would be
    /// removed by the commit without its rows being rewritten.
    #[tracing::instrument(name = "compaction.rewrite", skip_all)]
    pub(super) async fn read_sort_write(
        &self,
        plan: &CompactionPlan,
        metadata: &TableMetadata,
    ) -> TeoDBResult<CompactionWrite> {
        let sort_order = metadata.current_sort_order()?;
        let schema = metadata.current_schema()?;

        // Resolve the position deletes that apply to the inputs: how many rows
        // they remove (for the conservation check) and which delete files are
        // fully resolved by this rewrite (for removal at commit).
        let DeleteResolution {
            deleted_rows,
            resolved_deletes,
        } = self.resolve_input_deletes(plan).await?;

        debug!(
            table = %plan.table,
            inputs = plan.inputs.len(),
            delete_files = plan.deletes.len(),
            deleted_rows,
            sort_fields = sort_order.fields.len(),
            "compaction: reading and sorting input files"
        );

        let ctx = self.compaction_session()?;
        // Scan via the pinned-snapshot provider so position deletes are applied
        // with the same tested logic as distributed reads — soft-deleted rows
        // must not be resurrected by compaction.
        let descriptor = self.scan_descriptor(plan, metadata)?;
        let provider = PinnedScanTableProvider::try_new(descriptor)?;
        let dataframe = ctx
            .read_table(Arc::new(provider))
            .map_err(|error| TeoDBError::QueryExecution(format!("compaction: failed to open input files: {error}")))?;
        let dataframe = apply_sort(dataframe, sort_order, schema)?;
        let stream = dataframe
            .execute_stream()
            .await
            .map_err(from_datafusion)?;
        let write_spec = self.write_spec(plan, metadata, stream.schema())?;
        let output_location = compacted_file_location(plan, metadata);
        let (storage, _) = self.storage.resolve(&output_location).await?;
        let data_files = teodb_storage::parquet::write_sorted_stream(
            &*storage,
            &output_location,
            stream.map_err(from_datafusion),
            &write_spec,
        )
        .await?;

        verify_row_conservation(plan, &data_files, deleted_rows)?;
        let output_rows = data_files
            .iter()
            .map(|file| file.record_count)
            .sum::<u64>();
        info!(
            table = %plan.table,
            files = data_files.len(),
            rows = output_rows,
            resolved_deletes = resolved_deletes.len(),
            "compaction: output files written"
        );
        Ok(CompactionWrite {
            data_files,
            resolved_deletes,
        })
    }

    /// Build a frozen scan descriptor over the plan's inputs and deletes.
    fn scan_descriptor(&self, plan: &CompactionPlan, metadata: &TableMetadata) -> TeoDBResult<SnapshotScanDescriptor> {
        let partition_spec = metadata
            .partition_specs
            .iter()
            .find(|spec| spec.spec_id == plan.partition_spec_id)
            .ok_or_else(|| {
                TeoDBError::Internal(format!(
                    "partition spec {} not found in table metadata for {}",
                    plan.partition_spec_id, plan.table
                ))
            })?;
        Ok(SnapshotScanDescriptor {
            table_uuid: metadata.table_uuid,
            namespace: plan.table.namespace.clone(),
            table_name: plan.table.name.clone(),
            table_location: metadata.table_location.clone(),
            snapshot_id: plan.base_snapshot_id,
            schema: metadata.current_schema()?.clone(),
            partition_spec: partition_spec.clone(),
            sort_order: metadata.current_sort_order()?.clone(),
            data_files: plan.inputs.clone(),
            delete_files: plan.deletes.clone(),
        })
    }

    /// Read the plan's position-delete files and determine (a) how many input
    /// rows they remove and (b) which delete files reference *only* inputs and
    /// are therefore obsolete once the rewrite drops those rows.
    async fn resolve_input_deletes(&self, plan: &CompactionPlan) -> TeoDBResult<DeleteResolution> {
        let mut deleted_rows = 0u64;
        let mut resolved_deletes = Vec::new();

        for delete_file in plan
            .deletes
            .iter()
            .filter(|f| f.content == DataContent::PositionDelete)
        {
            let (storage, path) = self.storage.resolve(&delete_file.path).await?;
            let bytes = storage.get(&path).await?;
            let positions = teodb_storage::parquet::read_position_deletes(bytes)?;

            let mut references_only_inputs = !positions.is_empty();
            for (recorded_path, deleted) in &positions {
                match find_input_for_path(&plan.inputs, recorded_path) {
                    Some(input) => {
                        // Count only positions inside the file's row range.
                        deleted_rows += deleted
                            .iter()
                            .filter(|pos| **pos >= 0 && (**pos as u64) < input.record_count)
                            .count() as u64;
                    }
                    None => references_only_inputs = false,
                }
            }

            if references_only_inputs {
                resolved_deletes.push(delete_file.path.clone());
            }
        }

        Ok(DeleteResolution {
            deleted_rows,
            resolved_deletes,
        })
    }

    fn write_spec(
        &self,
        plan: &CompactionPlan,
        metadata: &TableMetadata,
        schema: arrow::datatypes::SchemaRef,
    ) -> TeoDBResult<teodb_storage::parquet::WriteSpec> {
        teodb_storage::parquet::WriteSpec::builder(schema)
            .schema_id(metadata.current_schema_id)
            .partition_spec_id(plan.partition_spec_id)
            .partition_values(plan.partition_values.clone())
            .sort_order(metadata.current_sort_order()?.clone())
            .compression(self.compression)
            .row_group_target_bytes(plan.output_target_bytes / 8)
            .build()
    }
}

fn apply_sort(
    dataframe: datafusion::dataframe::DataFrame,
    sort_order: &teodb_core::schema::SortOrder,
    schema: &teodb_core::schema::SchemaDefinition,
) -> TeoDBResult<datafusion::dataframe::DataFrame> {
    let expressions = sort_order
        .fields
        .iter()
        .filter_map(|field| {
            schema
                .columns
                .iter()
                .find(|column| column.id == field.source_id)
                .map(|column| {
                    let ascending = matches!(field.direction, teodb_core::schema::SortDirection::Asc);
                    let nulls_first = matches!(field.null_order, teodb_core::schema::NullOrder::NullsFirst);
                    col(&column.name).sort(ascending, nulls_first)
                })
        })
        .collect::<Vec<_>>();

    if expressions.is_empty() {
        Ok(dataframe)
    } else {
        dataframe
            .sort(expressions)
            .map_err(from_datafusion)
    }
}

fn compacted_file_location(_plan: &CompactionPlan, metadata: &TableMetadata) -> ObjectLocation {
    // Write under the table's `data/` subtree — the single data-file root the
    // orphan sweeper scans — so a failed/conflicted compaction output is
    // reclaimable, same convention as flush output (P1-6).
    ObjectLocation {
        scheme: metadata.table_location.scheme,
        bucket: metadata.table_location.bucket.clone(),
        key: format!("{}/data/{}.parquet", metadata.table_location.key, uuid::Uuid::now_v7()),
    }
}

/// Resolution of the position deletes covering a plan's inputs.
struct DeleteResolution {
    /// Total input rows removed by position deletes.
    deleted_rows: u64,
    /// Delete files whose every referenced data file was an input — obsolete
    /// after the rewrite and removed at commit.
    resolved_deletes: Vec<ObjectLocation>,
}

/// Find the input data file a position-delete entry's `file_path` refers to.
///
/// Delete files may record the table-relative key or a full URI; match either
/// exactly or when a recorded full URI ends with `/{key}` (mirrors the scan
/// path's `PositionDeleteSet::positions_for_file`).
fn find_input_for_path<'a>(inputs: &'a [DataFile], recorded_path: &str) -> Option<&'a DataFile> {
    inputs.iter().find(|input| {
        let key = input.path.key.as_str();
        recorded_path == key
            || recorded_path == input.path.to_uri()
            || recorded_path
                .strip_suffix(key)
                .is_some_and(|prefix| prefix.ends_with('/'))
    })
}

fn verify_row_conservation(plan: &CompactionPlan, data_files: &[DataFile], deleted_rows: u64) -> TeoDBResult<()> {
    let output_rows = data_files
        .iter()
        .map(|file| file.record_count)
        .sum::<u64>();
    let input_rows = plan
        .inputs
        .iter()
        .map(|file| file.record_count)
        .sum::<u64>();
    // Deletes drop rows, so the rewrite conserves inputs minus deleted rows.
    let expected_rows = input_rows.saturating_sub(deleted_rows);
    if output_rows == expected_rows {
        return Ok(());
    }

    Err(TeoDBError::Internal(format!(
        "compaction aborted for {}: output has {output_rows} rows but expected {expected_rows} \
         (inputs claim {input_rows}, {deleted_rows} deleted; unreadable or missing input files); \
         outputs left as orphans",
        plan.table
    )))
}
