//! Iceberg catalog protocol metrics.

use prometheus::{Histogram, IntCounter, IntCounterVec, IntGauge, Registry};

use super::registry::{counter, counter_vec, gauge, histogram, register};

pub struct CatalogMetrics {
    pub commit_total: IntCounterVec,
    pub commit_duration_seconds: Histogram,
    pub commit_rebase_total: IntCounter,
    pub status_check_total: IntCounterVec,
    pub status_check_duration_seconds: Histogram,
    pub writer_checkpoint_parse_failure_total: IntCounter,
    pub writer_checkpoint_count: IntGauge,
}

impl CatalogMetrics {
    pub fn new(registry: &Registry) -> Self {
        let commit_total = counter_vec(
            "teodb_catalog_commit_total",
            "Iceberg append commits by bounded protocol outcome",
            &["outcome"],
        );
        let commit_duration_seconds = histogram(
            "teodb_catalog_commit_duration_seconds",
            "End-to-end Iceberg append commit duration",
            vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
        );
        let commit_rebase_total = counter(
            "teodb_catalog_commit_rebase_total",
            "Iceberg transaction retry/rebase attempts after the initial append attempt",
        );
        let status_check_total = counter_vec(
            "teodb_catalog_commit_status_check_total",
            "Exact append status checks by bounded outcome",
            &["outcome"],
        );
        let status_check_duration_seconds = histogram(
            "teodb_catalog_commit_status_check_duration_seconds",
            "Exact append status-check duration",
            vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
        );
        let writer_checkpoint_parse_failure_total = counter(
            "teodb_writer_checkpoint_parse_failure_total",
            "Writer checkpoint metadata validation failures",
        );
        let writer_checkpoint_count = gauge(
            "teodb_writer_checkpoint_count",
            "Writer checkpoints in the most recently observed table metadata",
        );

        register(registry, &commit_total);
        register(registry, &commit_duration_seconds);
        register(registry, &commit_rebase_total);
        register(registry, &status_check_total);
        register(registry, &status_check_duration_seconds);
        register(registry, &writer_checkpoint_parse_failure_total);
        register(registry, &writer_checkpoint_count);

        Self {
            commit_total,
            commit_duration_seconds,
            commit_rebase_total,
            status_check_total,
            status_check_duration_seconds,
            writer_checkpoint_parse_failure_total,
            writer_checkpoint_count,
        }
    }
}
