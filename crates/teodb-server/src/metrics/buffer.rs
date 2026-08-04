//! Buffer subsystem gauges.

use prometheus::{IntCounter, IntGauge, IntGaugeVec, Registry};

use super::registry::{counter, gauge, gauge_vec, register};

/// Gauges for the in-memory buffer state (set by periodic collector).
pub struct BufferMetrics {
    pub tables: IntGauge,
    pub bytes: IntGauge,
    pub entries: IntGauge,
    pub reserved_bytes: IntGauge,
    pub oldest_pending_age_seconds: IntGaugeVec,
    /// Unflushed rows discarded by DDL buffer eviction (synced by collector).
    pub evicted_rows_total: IntCounter,
}

impl BufferMetrics {
    pub fn new(registry: &Registry) -> Self {
        let tables = gauge("teodb_buffer_tables", "Number of tables with active buffers");
        let bytes = gauge("teodb_buffer_bytes", "Current buffer size in bytes");
        let entries = gauge("teodb_buffer_entries", "Current buffer entry count");
        let reserved_bytes = gauge("teodb_buffer_reserved_bytes", "Bytes reserved before WAL admission");
        let oldest_pending_age_seconds = gauge_vec(
            "teodb_buffer_oldest_pending_age_seconds",
            "Age of the oldest uncommitted buffer entry",
            &["namespace", "table"],
        );
        let evicted_rows_total = counter(
            "teodb_buffer_evicted_rows_total",
            "Unflushed rows discarded by buffer eviction (DDL drop/recreate)",
        );

        register(registry, &tables);
        register(registry, &bytes);
        register(registry, &entries);
        register(registry, &reserved_bytes);
        register(registry, &oldest_pending_age_seconds);
        register(registry, &evicted_rows_total);

        Self {
            tables,
            bytes,
            entries,
            reserved_bytes,
            oldest_pending_age_seconds,
            evicted_rows_total,
        }
    }
}
