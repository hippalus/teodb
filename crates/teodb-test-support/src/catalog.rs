//! A single configurable [`Catalog`] test double.
//!
//! Every behavior has a safe default (empty lists, `load_table` → `NotFound`,
//! write ops → error), so a test configures only the responses it cares about
//! via [`MockCatalog::builder`].

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::file::TableMetadata;
use teodb_core::ident::{SnapshotId, TableIdent};
use teodb_core::snapshot_retention::{SnapshotRetention, select_expired_snapshots};
use teodb_core::traits::catalog::{
    Catalog, CommitAppend, CommitReplace, CommitStatus, CreateTableRequest, RetainedFileSet,
};

#[derive(Debug, Clone)]
pub enum MockAppendOutcome {
    Success(Arc<TableMetadata>),
    StateUnknown(String),
}

#[derive(Debug, Clone)]
pub enum MockCommitStatus {
    Committed(Arc<TableMetadata>),
    NotCommitted,
    Unknown(String),
}

/// One snapshot's file references, used to configure retention-aware behavior
/// in [`MockCatalog`]. Mirrors what a real catalog records per snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotFiles {
    pub id: SnapshotId,
    pub timestamp_ms: i64,
    pub files: Vec<String>,
}

impl SnapshotFiles {
    /// Convenience constructor.
    pub fn new(id: SnapshotId, timestamp_ms: i64, files: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            id,
            timestamp_ms,
            files: files.into_iter().map(Into::into).collect(),
        }
    }
}

/// A configurable [`Catalog`] test double. Build with [`MockCatalog::builder`].
pub struct MockCatalog {
    namespaces: Vec<String>,
    tables: Vec<TableIdent>,
    metadata_any: Option<Arc<TableMetadata>>,
    metadata_by_name: HashMap<String, Arc<TableMetadata>>,
    referenced: Option<HashSet<String>>,
    snapshots: Vec<SnapshotFiles>,
    current_snapshot: Option<SnapshotId>,
    commit_result: Option<Arc<TableMetadata>>,
    append_outcomes: Mutex<VecDeque<MockAppendOutcome>>,
    status_outcomes: Mutex<VecDeque<MockCommitStatus>>,
    load_table_calls: AtomicUsize,
    commit_append_calls: AtomicUsize,
    drop_table_calls: AtomicUsize,
    created_tables: Mutex<Vec<CreateTableRequest>>,
    append_requests: Mutex<Vec<CommitAppend>>,
}

impl MockCatalog {
    /// Start building a mock catalog.
    pub fn builder() -> MockCatalogBuilder {
        MockCatalogBuilder::default()
    }

    /// An empty catalog: no namespaces or tables; `load_table` → `NotFound`.
    pub fn empty() -> Self {
        Self::builder().build()
    }

    /// Number of `load_table` calls observed so far.
    pub fn load_table_calls(&self) -> usize {
        self.load_table_calls.load(Ordering::SeqCst)
    }

    /// Number of append commits observed so far.
    pub fn commit_append_calls(&self) -> usize {
        self.commit_append_calls.load(Ordering::SeqCst)
    }

    /// Number of `drop_table` calls observed so far.
    pub fn drop_table_calls(&self) -> usize {
        self.drop_table_calls.load(Ordering::SeqCst)
    }

    /// Create-table requests observed by this catalog.
    pub fn created_tables(&self) -> Vec<CreateTableRequest> {
        self.created_tables
            .lock()
            .expect("created_tables mutex poisoned")
            .clone()
    }

    /// Append requests observed by the catalog, in call order.
    pub fn append_requests(&self) -> Vec<CommitAppend> {
        self.append_requests
            .lock()
            .expect("append_requests mutex poisoned")
            .clone()
    }

    fn commit_metadata(&self, op: &str) -> TeoDBResult<Arc<TableMetadata>> {
        self.commit_result
            .clone()
            .ok_or_else(|| TeoDBError::Internal(format!("MockCatalog::{op} called without a configured commit_result")))
    }
}

#[async_trait]
impl Catalog for MockCatalog {
    async fn list_namespaces(&self) -> TeoDBResult<Vec<String>> {
        Ok(self.namespaces.clone())
    }

    async fn create_namespace(&self, _namespace: &str, _properties: HashMap<String, String>) -> TeoDBResult<()> {
        Ok(())
    }

    async fn drop_namespace(&self, _namespace: &str) -> TeoDBResult<()> {
        Ok(())
    }

    async fn list_tables(&self, _namespace: &str) -> TeoDBResult<Vec<TableIdent>> {
        Ok(self.tables.clone())
    }

    async fn load_table(&self, ident: &TableIdent) -> TeoDBResult<Arc<TableMetadata>> {
        self.load_table_calls
            .fetch_add(1, Ordering::SeqCst);
        if let Some(metadata) = self.metadata_by_name.get(&ident.name) {
            return Ok(metadata.clone());
        }
        if let Some(metadata) = &self.metadata_any {
            return Ok(metadata.clone());
        }
        Err(TeoDBError::NotFound {
            resource: ident.to_string(),
        })
    }

    async fn create_table(&self, req: CreateTableRequest) -> TeoDBResult<Arc<TableMetadata>> {
        self.created_tables
            .lock()
            .expect("created_tables mutex poisoned")
            .push(req);
        self.commit_metadata("create_table")
    }

    async fn drop_table(&self, _ident: &TableIdent) -> TeoDBResult<()> {
        self.drop_table_calls
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn load_live_files(&self, _ident: &TableIdent) -> TeoDBResult<Vec<teodb_core::file::DataFile>> {
        Ok(vec![])
    }

    async fn load_all_referenced_file_paths(&self, _ident: &TableIdent) -> TeoDBResult<HashSet<String>> {
        if let Some(referenced) = &self.referenced {
            return Ok(referenced.clone());
        }
        Ok(self
            .snapshots
            .iter()
            .flat_map(|snapshot| snapshot.files.iter().cloned())
            .collect())
    }

    async fn load_retained_file_paths(
        &self,
        ident: &TableIdent,
        retention: &SnapshotRetention,
        protected: &HashSet<SnapshotId>,
    ) -> TeoDBResult<RetainedFileSet> {
        if self.snapshots.is_empty() {
            return Ok(RetainedFileSet {
                paths: self.load_all_referenced_file_paths(ident).await?,
                ..Default::default()
            });
        }

        let history: Vec<(SnapshotId, i64)> = self
            .snapshots
            .iter()
            .map(|s| (s.id, s.timestamp_ms))
            .collect();
        let now_ms = chrono::Utc::now().timestamp_millis();
        let expired = select_expired_snapshots(&history, self.current_snapshot, retention, protected, now_ms);

        let retained_snapshots: HashSet<SnapshotId> = history
            .iter()
            .map(|(id, _)| *id)
            .filter(|id| !expired.contains(id))
            .collect();
        let paths = self
            .snapshots
            .iter()
            .filter(|s| retained_snapshots.contains(&s.id))
            .flat_map(|s| s.files.iter().cloned())
            .collect();

        Ok(RetainedFileSet {
            paths,
            retained_snapshots,
            expired_snapshots: expired.len(),
        })
    }

    async fn commit_append(&self, req: CommitAppend) -> TeoDBResult<Arc<TableMetadata>> {
        self.commit_append_calls
            .fetch_add(1, Ordering::SeqCst);
        self.append_requests
            .lock()
            .expect("append_requests mutex poisoned")
            .push(req.clone());
        if let Some(outcome) = self
            .append_outcomes
            .lock()
            .expect("append_outcomes mutex poisoned")
            .pop_front()
        {
            return match outcome {
                MockAppendOutcome::Success(metadata) => Ok(metadata),
                MockAppendOutcome::StateUnknown(message) => Err(TeoDBError::CommitStateUnknown {
                    table: req.table,
                    commit_id: req.identity.commit_id,
                    message,
                }),
            };
        }
        self.commit_metadata("commit_append")
    }

    async fn check_append_status(&self, _req: &CommitAppend) -> TeoDBResult<CommitStatus> {
        Ok(
            match self
                .status_outcomes
                .lock()
                .expect("status_outcomes mutex poisoned")
                .pop_front()
                .unwrap_or(MockCommitStatus::NotCommitted)
            {
                MockCommitStatus::Committed(metadata) => CommitStatus::Committed(metadata),
                MockCommitStatus::NotCommitted => CommitStatus::NotCommitted,
                MockCommitStatus::Unknown(message) => CommitStatus::Unknown { message },
            },
        )
    }

    async fn commit_replace(&self, _req: CommitReplace) -> TeoDBResult<Arc<TableMetadata>> {
        self.commit_metadata("commit_replace")
    }

    async fn update_table_properties(
        &self,
        _ident: &TableIdent,
        _expected: HashMap<String, String>,
        _updates: HashMap<String, String>,
        _removals: Vec<String>,
    ) -> TeoDBResult<Arc<TableMetadata>> {
        self.commit_metadata("update_table_properties")
    }
}

/// Builder for [`MockCatalog`]. Unset fields keep [`MockCatalog`]'s safe defaults.
#[derive(Default)]
pub struct MockCatalogBuilder {
    namespaces: Vec<String>,
    tables: Vec<TableIdent>,
    metadata_any: Option<Arc<TableMetadata>>,
    metadata_by_name: HashMap<String, Arc<TableMetadata>>,
    referenced: Option<HashSet<String>>,
    snapshots: Vec<SnapshotFiles>,
    current_snapshot: Option<SnapshotId>,
    commit_result: Option<Arc<TableMetadata>>,
    append_outcomes: VecDeque<MockAppendOutcome>,
    status_outcomes: VecDeque<MockCommitStatus>,
}

impl MockCatalogBuilder {
    /// Namespaces returned by `list_namespaces`.
    pub fn namespaces<I, S>(mut self, namespaces: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.namespaces = namespaces.into_iter().map(Into::into).collect();
        self
    }

    /// Tables returned by `list_tables` (for any namespace).
    pub fn tables<I: IntoIterator<Item = TableIdent>>(mut self, tables: I) -> Self {
        self.tables = tables.into_iter().collect();
        self
    }

    /// `load_table` returns this metadata for any ident.
    pub fn serves_any(mut self, metadata: Arc<TableMetadata>) -> Self {
        self.metadata_any = Some(metadata);
        self
    }

    /// `load_table` returns this metadata for an ident whose name matches
    /// `table_name`; other idents fall through to [`serves_any`] or `NotFound`.
    ///
    /// [`serves_any`]: MockCatalogBuilder::serves_any
    pub fn serves(mut self, table_name: impl Into<String>, metadata: Arc<TableMetadata>) -> Self {
        self.metadata_by_name
            .insert(table_name.into(), metadata);
        self
    }

    /// File URIs returned by `load_all_referenced_file_paths`.
    pub fn referenced<I, S>(mut self, uris: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.referenced = Some(uris.into_iter().map(Into::into).collect());
        self
    }

    /// Per-snapshot file references, enabling retention-aware
    /// `load_retained_file_paths` (delegates to `select_expired_snapshots`).
    pub fn snapshots(mut self, snapshots: Vec<SnapshotFiles>, current: Option<SnapshotId>) -> Self {
        self.snapshots = snapshots;
        self.current_snapshot = current;
        self
    }

    /// Metadata returned by the write methods (`create_table`, `commit_append`,
    /// `commit_replace`, `update_table_properties`). Unset → those methods error.
    pub fn commit_result(mut self, metadata: Arc<TableMetadata>) -> Self {
        self.commit_result = Some(metadata);
        self
    }

    /// Script append outcomes in call order.
    pub fn append_outcomes(mut self, outcomes: impl IntoIterator<Item = MockAppendOutcome>) -> Self {
        self.append_outcomes = outcomes.into_iter().collect();
        self
    }

    /// Script exact-status outcomes in call order.
    pub fn status_outcomes(mut self, outcomes: impl IntoIterator<Item = MockCommitStatus>) -> Self {
        self.status_outcomes = outcomes.into_iter().collect();
        self
    }

    /// Finish building.
    pub fn build(self) -> MockCatalog {
        MockCatalog {
            namespaces: self.namespaces,
            tables: self.tables,
            metadata_any: self.metadata_any,
            metadata_by_name: self.metadata_by_name,
            referenced: self.referenced,
            snapshots: self.snapshots,
            current_snapshot: self.current_snapshot,
            commit_result: self.commit_result,
            append_outcomes: Mutex::new(self.append_outcomes),
            status_outcomes: Mutex::new(self.status_outcomes),
            load_table_calls: AtomicUsize::new(0),
            commit_append_calls: AtomicUsize::new(0),
            drop_table_calls: AtomicUsize::new(0),
            created_tables: Mutex::new(Vec::new()),
            append_requests: Mutex::new(Vec::new()),
        }
    }
}
