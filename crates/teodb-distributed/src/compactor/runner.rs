//! Compaction engine: merges small Parquet files into larger, sorted files.
//!
//! The compactor reads input files at a pinned snapshot, writes sorted output
//! via the Parquet writer, and commits a `replace` operation to the catalog.
//! On conflict (another writer committed since), the output files become
//! orphan candidates for the sweeper.

use std::sync::Arc;

use tracing::{debug, info, warn};

use teodb_core::TeoDBResult;
use teodb_core::error::TeoDBError;
use teodb_core::file::DataFile;

use teodb_core::ident::{FieldId, SnapshotId, TableIdent};
use teodb_core::location::ObjectLocation;
use teodb_core::scalar::TeoScalar;
use teodb_core::traits::catalog::{Catalog, CommitReplace};
use teodb_core::traits::storage::StorageFactory;

use super::execution::CompactionWrite;
use crate::selection::CompactionGroup;

/// A fully specified compaction plan for a single partition group.
#[derive(Debug, Clone)]
pub struct CompactionPlan {
    pub table: TableIdent,
    pub base_snapshot_id: SnapshotId,
    pub partition_spec_id: i32,
    pub partition_values: std::collections::HashMap<FieldId, TeoScalar>,
    pub inputs: Vec<DataFile>,
    /// Position-delete files in the inputs' partition. Applied while reading
    /// so soft-deleted rows are not resurrected in the compacted output.
    pub deletes: Vec<DataFile>,
    pub output_target_bytes: u64,
}

impl CompactionPlan {
    /// Create a plan from a selection group and table metadata.
    pub fn from_group(group: CompactionGroup, table: TableIdent, target_bytes: u64) -> Self {
        Self {
            table,
            base_snapshot_id: group.base_snapshot_id,
            partition_spec_id: group.partition_spec_id,
            partition_values: group.partition_values,
            inputs: group.files,
            deletes: group.delete_files,
            output_target_bytes: target_bytes,
        }
    }
}

/// Outcome of a compaction attempt.
#[derive(Debug)]
pub enum CompactionOutcome {
    /// Files were successfully replaced in the catalog.
    Committed {
        added: Vec<DataFile>,
        removed: Vec<ObjectLocation>,
    },
    /// Another writer committed first; our output files are orphaned.
    ConflictAbandoned { orphan_files: Vec<ObjectLocation> },
    /// No work was needed (snapshot changed or inputs empty).
    NoChange,
}

/// The compactor orchestrates reading, sorting, writing, and committing.
pub struct Compactor {
    pub(super) catalog: Arc<dyn Catalog>,
    pub(super) storage: Arc<dyn StorageFactory>,
    pub(super) compression: teodb_storage::parquet::CompressionCodec,
    pub(super) object_store: teodb_query::ObjectStoreRegistration,
    /// Memory ceiling for the compaction session; the sort spills to
    /// `spill_dir` beyond it. `None` = DataFusion's unbounded default.
    pub(super) memory_pool_bytes: Option<u64>,
    pub(super) spill_dir: Option<std::path::PathBuf>,
}

pub struct CompactorBuilder {
    catalog: Arc<dyn Catalog>,
    storage: Arc<dyn StorageFactory>,
    compression: teodb_storage::parquet::CompressionCodec,
    object_store: teodb_query::ObjectStoreRegistration,
    memory_limit: Option<(u64, std::path::PathBuf)>,
}

impl CompactorBuilder {
    pub fn new(
        catalog: Arc<dyn Catalog>,
        storage: Arc<dyn StorageFactory>,
        object_store: teodb_query::ObjectStoreRegistration,
    ) -> Self {
        Self {
            catalog,
            storage,
            compression: teodb_storage::parquet::CompressionCodec::default(),
            object_store,
            memory_limit: None,
        }
    }

    pub fn compression(mut self, compression: teodb_storage::parquet::CompressionCodec) -> Self {
        self.compression = compression;
        self
    }

    pub fn memory_limit(mut self, pool_bytes: u64, spill_dir: std::path::PathBuf) -> Self {
        self.memory_limit = Some((pool_bytes, spill_dir));
        self
    }

    pub fn build(self) -> TeoDBResult<Compactor> {
        if let Some((pool_bytes, spill_dir)) = &self.memory_limit
            && *pool_bytes > 0
            && spill_dir.as_os_str().is_empty()
        {
            return Err(TeoDBError::Config(
                "compactor memory limit requires a non-empty spill directory".into(),
            ));
        }

        let mut compactor =
            Compactor::new(self.catalog, self.storage, self.object_store).with_compression(self.compression);
        if let Some((pool_bytes, spill_dir)) = self.memory_limit {
            compactor = compactor.with_memory_limit(pool_bytes, spill_dir);
        }
        Ok(compactor)
    }
}

impl Compactor {
    pub fn new(
        catalog: Arc<dyn Catalog>,
        storage: Arc<dyn StorageFactory>,
        object_store: teodb_query::ObjectStoreRegistration,
    ) -> Self {
        Self {
            catalog,
            storage,
            compression: teodb_storage::parquet::CompressionCodec::default(),
            object_store,
            memory_pool_bytes: None,
            spill_dir: None,
        }
    }

    pub fn with_compression(mut self, compression: teodb_storage::parquet::CompressionCodec) -> Self {
        self.compression = compression;
        self
    }

    /// Bound the compaction session's memory: sorts beyond `pool_bytes`
    /// spill to `spill_dir`. 0 disables the bound.
    pub fn with_memory_limit(mut self, pool_bytes: u64, spill_dir: std::path::PathBuf) -> Self {
        self.memory_pool_bytes = (pool_bytes > 0).then_some(pool_bytes);
        self.spill_dir = Some(spill_dir);
        self
    }

    /// Execute a compaction plan.
    ///
    /// Steps:
    /// 1. Verify the snapshot hasn't changed since plan creation.
    /// 2. Read and sort input files (delegated to teodb-storage's Parquet writer).
    /// 3. Write sorted output files.
    /// 4. Commit a `replace` to the catalog.
    #[tracing::instrument(
        name = "compaction.execute",
        skip_all,
        fields(
            table = %plan.table,
            snapshot_id = plan.base_snapshot_id,
            input_files = plan.inputs.len()
        )
    )]
    pub async fn compact(&self, plan: CompactionPlan) -> TeoDBResult<CompactionOutcome> {
        // Step 1: Verify snapshot is still current.
        let (table_metadata, live_files) = tokio::try_join!(
            self.catalog.load_table(&plan.table),
            self.catalog.load_live_files(&plan.table)
        )?;
        let metadata = (*table_metadata)
            .clone()
            .with_live_files(live_files)?;
        if metadata.current_snapshot_id != Some(plan.base_snapshot_id) {
            info!(
                table = %plan.table,
                expected = plan.base_snapshot_id,
                actual = ?metadata.current_snapshot_id,
                "snapshot changed since plan creation, skipping compaction"
            );
            return Ok(CompactionOutcome::NoChange);
        }

        if plan.inputs.is_empty() {
            return Ok(CompactionOutcome::NoChange);
        }

        let _sort_order = metadata.current_sort_order()?.clone();
        let input_count = plan.inputs.len();
        let input_rows: u64 = plan.inputs.iter().map(|f| f.record_count).sum();

        debug!(
            table = %plan.table,
            inputs = input_count,
            rows = input_rows,
            "starting compaction"
        );

        // Step 2-3: Read input files (applying position deletes), sort, and
        // write output. `resolved_deletes` are position-delete files whose
        // every referenced data file was an input — now obsolete.
        let CompactionWrite {
            data_files: added,
            resolved_deletes,
        } = self.read_sort_write(&plan, &metadata).await?;

        // Step 4: Commit replace. Removed = compacted inputs plus the
        // position-delete files fully resolved by the rewrite.
        let mut removed: Vec<ObjectLocation> = plan
            .inputs
            .iter()
            .map(|f| f.path.clone())
            .collect();
        removed.extend(resolved_deletes);
        let removed_uris: Vec<String> = removed.iter().map(|loc| loc.to_uri()).collect();

        match self
            .catalog
            .commit_replace(CommitReplace {
                table: plan.table.clone(),
                base_snapshot_id: plan.base_snapshot_id,
                removed_data_files: removed_uris,
                added_data_files: added.clone(),
                properties: [("teodb.op".into(), "compaction".into())]
                    .into_iter()
                    .collect(),
            })
            .await
        {
            Ok(_) => {
                info!(
                    table = %plan.table,
                    added = added.len(),
                    removed = removed.len(),
                    "compaction committed"
                );
                Ok(CompactionOutcome::Committed { added, removed })
            }
            Err(TeoDBError::Conflict { .. }) => {
                warn!(
                    table = %plan.table,
                    "compaction conflict, output files become orphans"
                );
                Ok(CompactionOutcome::ConflictAbandoned {
                    orphan_files: added.into_iter().map(|f| f.path).collect(),
                })
            }
            Err(e) => Err(e),
        }
    }
}
