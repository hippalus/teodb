use serde::{Deserialize, Serialize};
use teodb_ingest::config::CommitStatusCheckConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct QueryConfig {
    pub memory_pool_bytes: u64,
    pub batch_size: usize,
    pub target_partitions: usize,
    pub query_timeout_secs: u64,
    pub slow_query_threshold_ms: u64,
    /// How long resolved table metadata is cached for queries before being
    /// refreshed from the catalog (single-flight). 0 reloads per query.
    pub metadata_refresh_secs: u64,
    /// Maximum number of historical query status records retained per data node.
    pub query_status_max_entries: u64,
    /// How long historical query status records are retained per data node.
    pub query_status_ttl_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestConfig {
    pub buffer_max_bytes: u64,
    pub flush_interval_secs: u64,
    pub max_body_bytes: u64,
    /// Default warehouse URI prefix for new tables (e.g., "s3://warehouse").
    pub default_warehouse_uri: String,
    /// How long an ingest idempotency key is remembered, per data node (default 24h).
    pub idempotency_ttl_secs: u64,
    /// Per-table cap on remembered idempotency keys (default 100k).
    pub idempotency_max_keys_per_table: usize,
    pub commit_status_check: CommitStatusCheckConfig,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            memory_pool_bytes: 4 * 1024 * 1024 * 1024,
            batch_size: 8192,
            target_partitions: 0,
            query_timeout_secs: 300,
            slow_query_threshold_ms: 5000,
            metadata_refresh_secs: 10,
            query_status_max_entries: 100_000,
            query_status_ttl_secs: 60 * 60,
        }
    }
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            buffer_max_bytes: 512 * 1024 * 1024,
            flush_interval_secs: 10,
            max_body_bytes: 64 * 1024 * 1024,
            default_warehouse_uri: "s3://teodb".into(),
            idempotency_ttl_secs: 24 * 60 * 60,
            idempotency_max_keys_per_table: 100_000,
            commit_status_check: CommitStatusCheckConfig::default(),
        }
    }
}
