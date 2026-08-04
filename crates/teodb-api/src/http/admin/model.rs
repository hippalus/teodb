//! Response DTOs for admin endpoints.

use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub server_version: String,
    pub uptime_seconds: u64,
    pub tables_count: usize,
    pub total_rows: u64,
    pub memory_usage_bytes: u64,
    pub components: Vec<ComponentHealth>,
}

#[derive(Debug, Serialize)]
pub struct ComponentHealth {
    pub name: String,
    pub status: &'static str,
    pub message: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TableSummary {
    pub name: String,
    pub namespace: String,
    pub column_count: usize,
    pub row_count: u64,
    pub size_bytes: u64,
    pub partitioned: bool,
    /// Partition fields (e.g. `["region"]` or `["year(timestamp)", "bucket(16, id)"]`).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub partition_fields: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ClusterStatusResponse {
    pub mode: String,
    pub cluster_id: String,
    pub node_id: String,
    pub writer_id: String,
    pub writer_epoch: u64,
    pub recovery_status: &'static str,
    pub uptime_seconds: u64,
    pub pending_tables: usize,
    pub blocked_tables: usize,
    pub wal_segments: Option<usize>,
    pub wal_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal_error: Option<String>,
    /// Registered executors (empty in standalone, or when the scheduler is
    /// unreachable).
    pub workers: Vec<ClusterWorker>,
    /// Active client connections. Not yet tracked — always empty for now.
    pub connections: Vec<ClusterConnection>,
    /// Scheduler endpoint and reachability. `None` in standalone deployments.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduler: Option<SchedulerInfo>,
    /// Jobs the scheduler still owns work for. `None` when there is no scheduler.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_jobs: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct ClusterWorker {
    pub id: String,
    pub host: String,
    pub flight_port: u16,
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_heartbeat: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ClusterConnection {
    pub id: String,
    pub client_address: String,
    pub protocol: String,
    pub connected_at: String,
    pub last_activity: String,
}

#[derive(Debug, Serialize)]
pub struct SchedulerInfo {
    pub address: String,
    pub reachable: bool,
}

#[derive(Debug, Serialize)]
pub struct BlockedFlushResponse {
    pub namespace: String,
    pub table: String,
    pub table_uuid: String,
    pub writer_id: String,
    pub writer_epoch: u64,
    pub commit_id: String,
    pub generation_lo: u64,
    pub generation_hi: u64,
    pub blocked_since_ms: i64,
    pub last_recheck_ms: i64,
    pub status_check_attempts: u32,
    pub last_error_class: String,
}

#[derive(Debug, Serialize)]
pub struct BlockedFlushRecheckResponse {
    pub status: &'static str,
}
