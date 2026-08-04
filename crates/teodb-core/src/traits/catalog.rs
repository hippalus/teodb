use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;

use crate::error::TeoDBResult;
use crate::file::{DataFile, TableMetadata};
use crate::ident::{SnapshotId, TableIdent};
use crate::location::ObjectLocation;
use crate::schema::{SchemaDefinition, SortOrder, UnboundPartitionSpec};
use crate::snapshot_retention::SnapshotRetention;
use crate::write_protocol::AppendCommitIdentity;

/// Result of [`Catalog::load_retained_file_paths`]: the file paths protected
/// from orphan sweeping plus the snapshot ids that produced them.
#[derive(Debug, Clone, Default)]
pub struct RetainedFileSet {
    /// File URIs referenced by retained snapshots (manifest-recorded form).
    pub paths: HashSet<String>,
    /// Snapshot ids retained under the policy (current, young, kept, protected).
    pub retained_snapshots: HashSet<SnapshotId>,
    /// Number of snapshots treated as expired by the policy.
    pub expired_snapshots: usize,
}

/// Request to create a new table.
#[derive(Debug, Clone)]
pub struct CreateTableRequest {
    pub ident: TableIdent,
    pub schema: SchemaDefinition,
    pub partition_spec: UnboundPartitionSpec,
    pub sort_order: SortOrder,
    pub location: ObjectLocation,
    pub properties: HashMap<String, String>,
}

/// Options for dropping a table through TeoDB-owned APIs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DropTableOptions {
    /// When true, delete object-store files under the table-owned prefix after
    /// the catalog drop succeeds. Default drop remains metadata-only.
    pub purge: bool,
}

/// Request to commit an append operation to a table.
#[derive(Debug, Clone)]
pub struct CommitAppend {
    pub table: TableIdent,
    pub table_uuid: uuid::Uuid,
    pub identity: AppendCommitIdentity,
    pub base_snapshot_id: Option<SnapshotId>,
    pub added_data_files: Vec<DataFile>,
    /// Non-reserved caller snapshot properties.
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum CommitStatus {
    Committed(Arc<TableMetadata>),
    NotCommitted,
    Unknown { message: String },
}

/// Request to commit a replace operation (compaction) to a table.
#[derive(Debug, Clone)]
pub struct CommitReplace {
    pub table: TableIdent,
    pub base_snapshot_id: SnapshotId,
    pub removed_data_files: Vec<String>,
    pub added_data_files: Vec<DataFile>,
    pub properties: HashMap<String, String>,
}

/// The catalog boundary uses only TeoDB domain types. Concrete catalog and
/// metadata-format integration lives in `teodb-catalog`.
#[async_trait]
pub trait Catalog: Send + Sync + 'static {
    async fn list_namespaces(&self) -> TeoDBResult<Vec<String>>;
    async fn create_namespace(&self, namespace: &str, properties: HashMap<String, String>) -> TeoDBResult<()>;
    async fn drop_namespace(&self, namespace: &str) -> TeoDBResult<()>;
    async fn list_tables(&self, namespace: &str) -> TeoDBResult<Vec<TableIdent>>;

    async fn load_table(&self, ident: &TableIdent) -> TeoDBResult<Arc<TableMetadata>>;
    async fn create_table(&self, req: CreateTableRequest) -> TeoDBResult<Arc<TableMetadata>>;
    async fn drop_table(&self, ident: &TableIdent) -> TeoDBResult<()>;

    /// Load live data and delete files from the current snapshot.
    async fn load_live_files(&self, ident: &TableIdent) -> TeoDBResult<Vec<DataFile>>;

    /// Load every file path referenced by **any** snapshot in the table's
    /// metadata history (data and delete files, as the full URIs recorded in
    /// the manifests).
    ///
    /// Used by the orphan sweeper: a file referenced by any retained snapshot
    /// must never be deleted, or time travel and rollback break. The default
    /// implementation falls back to the current snapshot only; real catalog
    /// implementations must override it with a full history walk.
    async fn load_all_referenced_file_paths(&self, ident: &TableIdent) -> TeoDBResult<HashSet<String>> {
        Ok(self
            .load_live_files(ident)
            .await?
            .iter()
            .map(|file| file.path.to_uri())
            .collect())
    }

    /// Load every file path referenced by snapshots **retained** under the
    /// given retention policy. Snapshots outside the policy are treated as
    /// expired: their exclusive files are *not* returned, making them
    /// eligible for orphan sweeping. `protected` snapshot ids (query pins)
    /// are always retained.
    ///
    /// The default implementation falls back to the full snapshot history
    /// (nothing expires) — safe for implementations that do not walk
    /// history. `expired_snapshots == 0` signals that fallback to callers.
    async fn load_retained_file_paths(
        &self,
        ident: &TableIdent,
        retention: &SnapshotRetention,
        protected: &HashSet<SnapshotId>,
    ) -> TeoDBResult<RetainedFileSet> {
        let _ = (retention, protected);
        Ok(RetainedFileSet {
            paths: self.load_all_referenced_file_paths(ident).await?,
            retained_snapshots: HashSet::new(),
            expired_snapshots: 0,
        })
    }

    /// Commits an append with optimistic concurrency. Implementations must
    /// revalidate the exact writer epoch, generation range, commit ID, table
    /// incarnation, and writer-registry bound on every catalog retry/rebase.
    async fn commit_append(&self, req: CommitAppend) -> TeoDBResult<Arc<TableMetadata>>;

    /// Check whether an exact append commit is visible without performing a
    /// write. This is the only safe resolution primitive after an ambiguous
    /// catalog response.
    async fn check_append_status(&self, req: &CommitAppend) -> TeoDBResult<CommitStatus>;

    /// Commits a replace (compaction) with optimistic concurrency.
    async fn commit_replace(&self, req: CommitReplace) -> TeoDBResult<Arc<TableMetadata>>;

    /// Atomically update table properties via CAS. Loads the current table,
    /// verifies that `expected` properties match, then sets `updates` and
    /// removes `removals`. Returns the updated metadata or `Conflict` if the
    /// expectation check fails (another writer changed the properties).
    async fn update_table_properties(
        &self,
        ident: &TableIdent,
        expected: HashMap<String, String>,
        updates: HashMap<String, String>,
        removals: Vec<String>,
    ) -> TeoDBResult<Arc<TableMetadata>>;
}
