use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterConfig {
    /// Stable deployment-wide identity for writer derivation.
    pub cluster_id: Option<uuid::Uuid>,
    /// Human-readable process identity used consistently in logs and
    /// maintenance coordination.
    pub node_id: Option<String>,
    /// Stable data-writer slot (for example a StatefulSet ordinal).
    pub writer_slot: Option<String>,
    /// Hard bound on writer checkpoint properties in one Iceberg table.
    pub max_writer_checkpoints_per_table: usize,
    /// Start the active Ballista scheduler inside this process.
    pub scheduler_enabled: bool,
    pub scheduler_bind: String,
    pub scheduler_addr: String,
    pub executor_bind: String,
    /// Hostname this data node's executor advertises to the control plane and other
    /// executors. Must be routable cluster-wide (Docker service name, pod DNS
    /// name). Falls back to the system hostname when unset.
    pub executor_advertise_host: Option<String>,
    pub executor_grpc_bind_port: u16,
    pub executor_task_slots: usize,
    pub heartbeat_interval_secs: u64,
    pub heartbeat_miss_threshold: u32,
    /// Minimum live executors the control plane must report before this data node's
    /// `/readyz` passes. Liveness = heartbeat within
    /// `heartbeat_interval_secs * heartbeat_miss_threshold`. 0 falls back to
    /// a plain TCP reachability check of the scheduler.
    pub min_executors: usize,
    /// When the remote scheduler is unreachable at query time, execute the
    /// query on this data node's local DataFusion engine instead of failing it.
    ///
    /// Disabled by default because fallback must preserve the prepared snapshot;
    /// enabling it before that fix can violate snapshot isolation.
    pub local_query_fallback: bool,
    /// Graceful-drain window for the embedded Ballista executor and
    /// scheduler on shutdown: the executor gets this long to finish
    /// in-flight tasks, the scheduler to finish queued/running jobs, before
    /// being aborted. 0 aborts immediately. Keep it below
    /// `shutdown.drain_timeout_secs` — both drains spend from that budget.
    pub drain_timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MaintenanceConfig {
    /// Enable the background maintenance loop (orphan sweeping, cache index
    /// persistence, and any explicitly enabled maintenance sub-task).
    pub enabled: bool,
    /// Enable background compaction.
    ///
    /// Disabled by default until compaction commits use a real Iceberg replace
    /// operation and load the live file set correctly. The current catalog
    /// adapter uses an interim replace implementation while waiting on public
    /// iceberg-rust overwrite support.
    pub compaction_enabled: bool,
    /// Interval between compaction selection runs (seconds).
    pub compaction_interval_secs: u64,
    /// Target output file size for compaction (bytes).
    pub target_file_bytes: u64,
    /// Minimum number of files before a compaction group is eligible.
    pub min_files_per_compaction: usize,
    /// Maximum files included in a single compaction run.
    pub max_files_per_compaction: usize,
    /// Maximum total input bytes in a single compaction run — bounds the
    /// memory and I/O of one run. 0 disables the budget.
    pub max_bytes_per_compaction: u64,
    /// Memory ceiling for the compaction session; sorts spill to the query
    /// spill directory beyond it. 0 = unbounded.
    pub compaction_memory_bytes: u64,
    /// Compression codec for compaction output. Accepts: "zstd", "zstd(3)",
    /// "snappy", "lz4", "gzip", "gzip(6)", "brotli", "brotli(4)", "none".
    /// Default: "zstd(3)".
    pub compression: String,
    /// Interval between orphan sweep runs (seconds).
    pub orphan_sweep_interval_secs: u64,
    /// Minimum age before an orphan file is deleted (seconds).
    pub orphan_retention_secs: u64,
    /// Snapshots older than this are expired during orphan sweeps: files
    /// only they reference (e.g. compacted-away inputs) are reclaimed.
    /// Snapshots remain listed in Iceberg metadata — expiration here governs
    /// file retention, not metadata history. 0 disables expiration and
    /// protects the full snapshot history forever.
    pub snapshot_retention_secs: u64,
    /// Always keep the data files of at least this many of the most recent
    /// snapshots, regardless of age (minimum 1).
    pub snapshot_keep_last: usize,
    /// Compaction lock TTL in seconds. Stale locks older than this are stolen.
    /// Should be at least 2x `compaction_interval_secs`.
    pub lock_ttl_secs: u64,
}
impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            cluster_id: None,
            node_id: None,
            writer_slot: None,
            max_writer_checkpoints_per_table: 32,
            scheduler_enabled: false,
            scheduler_bind: "0.0.0.0:50050".into(),
            scheduler_addr: "localhost:50050".into(),
            executor_bind: "0.0.0.0:50051".into(),
            executor_advertise_host: None,
            executor_grpc_bind_port: 50052,
            executor_task_slots: 0,
            heartbeat_interval_secs: 5,
            heartbeat_miss_threshold: 3,
            min_executors: 1,
            local_query_fallback: false,
            drain_timeout_secs: 20,
        }
    }
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            compaction_enabled: false,
            compaction_interval_secs: 3600,       // 1 hour
            target_file_bytes: 128 * 1024 * 1024, // 128 MiB
            min_files_per_compaction: 8,
            max_files_per_compaction: 64,
            max_bytes_per_compaction: 1024 * 1024 * 1024, // 1 GiB
            compaction_memory_bytes: 512 * 1024 * 1024,   // 512 MiB
            compression: "zstd(3)".into(),
            orphan_sweep_interval_secs: 21600, // 6 hours
            orphan_retention_secs: 86400,      // 24 hours
            snapshot_retention_secs: 0,        // disabled until true Iceberg expiration is implemented
            snapshot_keep_last: 1,
            lock_ttl_secs: 7200, // 2 hours
        }
    }
}
