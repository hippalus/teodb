//! Flush subsystem metrics.

use prometheus::{Histogram, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Registry};

use super::registry::{counter, counter_vec, gauge, gauge_vec, histogram, register};

/// Metrics for the flush pipeline.
pub struct FlushMetrics {
    pub total: IntCounter,
    pub errors_total: IntCounter,
    pub rows_total: IntCounter,
    pub duration_seconds: Histogram,
    pub data_file_write_duration_seconds: Histogram,
    pub inflight: IntCounterVec,
    pub lock_wait_seconds: Histogram,
    pub blocked_tables: IntGauge,
    pub blocked_total: IntCounterVec,
    pub blocked_resolution_total: IntCounterVec,
    pub prepared_flushes: IntGauge,
    pub prepared_oldest_age_seconds: IntGauge,
    pub visibility_lag_seconds: IntGaugeVec,
}

impl FlushMetrics {
    pub fn new(registry: &Registry) -> Self {
        let total = counter("teodb_flush_total", "Total flush operations");
        let errors_total = counter("teodb_flush_errors_total", "Total flush errors");
        let rows_total = counter("teodb_flush_rows_total", "Total rows flushed to Parquet");
        let duration_seconds = histogram(
            "teodb_flush_duration_seconds",
            "Flush operation duration",
            vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0],
        );
        let data_file_write_duration_seconds = histogram(
            "teodb_flush_data_file_write_duration_seconds",
            "Parquet generation and object upload duration within a flush",
            vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
            ],
        );
        let inflight = counter_vec(
            "teodb_flush_inflight",
            "Flush attempts by terminal outcome",
            &["outcome"],
        );
        let lock_wait_seconds = histogram(
            "teodb_flush_lock_wait_seconds",
            "Wait duration for the per-table flush serialization lock",
            vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0],
        );
        let blocked_tables = gauge(
            "teodb_flush_blocked_tables",
            "Tables contained behind unresolved exact commits",
        );
        let blocked_total = counter_vec(
            "teodb_flush_blocked_total",
            "Table-local flush containment transitions by bounded reason",
            &["reason"],
        );
        let blocked_resolution_total = counter_vec(
            "teodb_flush_blocked_resolution_total",
            "Operator/background blocked-flush rechecks by bounded outcome",
            &["outcome"],
        );
        let prepared_flushes = gauge(
            "teodb_prepared_flushes",
            "Durable prepared flush intents currently owned by buffers",
        );
        let prepared_oldest_age_seconds = gauge(
            "teodb_prepared_flush_oldest_age_seconds",
            "Age in seconds of the oldest durable prepared flush intent",
        );
        let visibility_lag_seconds = gauge_vec(
            "teodb_flush_visibility_lag_seconds",
            "Visibility lag of the latest committed buffer range",
            &["namespace", "table"],
        );

        register(registry, &total);
        register(registry, &errors_total);
        register(registry, &rows_total);
        register(registry, &duration_seconds);
        register(registry, &data_file_write_duration_seconds);
        register(registry, &inflight);
        register(registry, &lock_wait_seconds);
        register(registry, &blocked_tables);
        register(registry, &blocked_total);
        register(registry, &blocked_resolution_total);
        register(registry, &prepared_flushes);
        register(registry, &prepared_oldest_age_seconds);
        register(registry, &visibility_lag_seconds);

        Self {
            total,
            errors_total,
            rows_total,
            duration_seconds,
            data_file_write_duration_seconds,
            inflight,
            lock_wait_seconds,
            blocked_tables,
            blocked_total,
            blocked_resolution_total,
            prepared_flushes,
            prepared_oldest_age_seconds,
            visibility_lag_seconds,
        }
    }
}
