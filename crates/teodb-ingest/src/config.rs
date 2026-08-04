use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CommitStatusCheckConfig {
    pub num_retries: u32,
    #[serde(rename = "min_wait_ms", with = "duration_millis")]
    pub min_wait: Duration,
    #[serde(rename = "max_wait_ms", with = "duration_millis")]
    pub max_wait: Duration,
    #[serde(rename = "total_timeout_ms", with = "duration_millis")]
    pub total_timeout: Duration,
    #[serde(rename = "blocked_recheck_interval_ms", with = "duration_millis")]
    pub blocked_recheck_interval: Duration,
    pub blocked_recheck_jitter_percent: u8,
    pub max_concurrent_blocked_rechecks: usize,
}

impl Default for CommitStatusCheckConfig {
    fn default() -> Self {
        Self {
            num_retries: 5,
            min_wait: Duration::from_millis(100),
            max_wait: Duration::from_secs(5),
            total_timeout: Duration::from_secs(30),
            blocked_recheck_interval: Duration::from_secs(60),
            blocked_recheck_jitter_percent: 15,
            max_concurrent_blocked_rechecks: 4,
        }
    }
}

mod duration_millis {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let millis = u64::try_from(duration.as_millis()).map_err(serde::ser::Error::custom)?;
        serializer.serialize_u64(millis)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        u64::deserialize(deserializer).map(Duration::from_millis)
    }
}

/// Configuration for the ingestion layer.
#[derive(Debug, Clone)]
pub struct IngestConfig {
    /// Hard cap on pending + in-flight bytes per table buffer.
    pub buffer_max_bytes: u64,
    /// Soft watermark; exceeding emits backpressure signal.
    pub buffer_soft_watermark_bytes: u64,
    /// Flush interval per table.
    pub flush_interval: Duration,
    /// Default warehouse URI prefix for new tables (e.g., "s3://warehouse").
    pub default_warehouse_uri: String,
    /// How long an idempotency key is remembered by this stable writer.
    pub idempotency_ttl: Duration,
    /// Per-table cap on remembered idempotency keys.
    pub idempotency_max_keys_per_table: usize,
    pub commit_status_check: CommitStatusCheckConfig,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            buffer_max_bytes: 512 * 1024 * 1024,            // 512 MiB
            buffer_soft_watermark_bytes: 384 * 1024 * 1024, // 384 MiB
            flush_interval: Duration::from_secs(10),
            default_warehouse_uri: "s3://warehouse".into(),
            idempotency_ttl: Duration::from_secs(24 * 60 * 60),
            idempotency_max_keys_per_table: 100_000,
            commit_status_check: CommitStatusCheckConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let cfg = IngestConfig::default();
        assert_eq!(cfg.buffer_max_bytes, 512 * 1024 * 1024);
        assert_eq!(cfg.flush_interval, Duration::from_secs(10));
    }

    #[test]
    fn commit_status_check_config_serializes_durations_as_milliseconds() {
        let value = serde_json::to_value(CommitStatusCheckConfig::default()).unwrap();

        assert_eq!(value["min_wait_ms"], 100);
        assert_eq!(value["max_wait_ms"], 5_000);
        assert_eq!(value["total_timeout_ms"], 30_000);
        assert_eq!(value["blocked_recheck_interval_ms"], 60_000);
        assert!(value.get("min_wait").is_none());
    }

    #[test]
    fn partial_commit_status_check_config_uses_field_defaults() {
        let config: CommitStatusCheckConfig = serde_json::from_value(serde_json::json!({ "num_retries": 9 })).unwrap();

        assert_eq!(config.num_retries, 9);
        assert_eq!(config.min_wait, Duration::from_millis(100));
        assert_eq!(config.max_wait, Duration::from_secs(5));
        assert_eq!(config.total_timeout, Duration::from_secs(30));
    }

    #[test]
    fn unknown_commit_status_check_fields_are_rejected() {
        let error =
            serde_json::from_value::<CommitStatusCheckConfig>(serde_json::json!({ "min_wait": 100 })).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unknown field `min_wait`")
        );
    }
}
