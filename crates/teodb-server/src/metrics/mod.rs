//! Prometheus metric definitions for TeoDB.
//!
//! Metrics are organized by subsystem for clarity. Each subsystem module
//! owns its metric creation and registration. The top-level [`Metrics`]
//! struct composes them all and provides the encoding endpoint.

mod buffer;
mod cache;
mod catalog;
mod compaction;
mod encode;
mod flush;
mod ingest;
mod query;
mod registry;
mod security;
mod transport;
mod wal;

use prometheus::Registry;

pub use self::buffer::BufferMetrics;
pub use self::cache::CacheMetrics;
pub use self::catalog::CatalogMetrics;
pub use self::compaction::CompactionMetrics;
pub use self::encode::encode_prometheus;
pub use self::flush::FlushMetrics;
pub use self::ingest::IngestMetrics;
pub use self::query::QueryMetrics;
pub use self::security::SecurityMetrics;
pub use self::transport::TransportMetrics;
pub use self::wal::WalMetrics;

use prometheus::IntGauge;

use self::registry::{gauge, register};

/// All TeoDB server metrics, organized by subsystem.
#[allow(dead_code)]
pub struct Metrics {
    pub registry: Registry,
    pub ingest: IngestMetrics,
    pub flush: FlushMetrics,
    pub catalog: CatalogMetrics,
    pub buffer: BufferMetrics,
    pub query: QueryMetrics,
    pub wal: WalMetrics,
    pub cache: CacheMetrics,
    pub compaction: CompactionMetrics,
    pub security: SecurityMetrics,
    pub transport: TransportMetrics,
    pub uptime_seconds: IntGauge,
}

impl Metrics {
    /// Create and register all metrics with a new registry.
    pub fn new() -> Self {
        let registry = Registry::new();

        let uptime_seconds = gauge("teodb_uptime_seconds", "Server uptime in seconds");
        register(&registry, &uptime_seconds);

        Self {
            ingest: IngestMetrics::new(&registry),
            flush: FlushMetrics::new(&registry),
            catalog: CatalogMetrics::new(&registry),
            buffer: BufferMetrics::new(&registry),
            query: QueryMetrics::new(&registry),
            wal: WalMetrics::new(&registry),
            cache: CacheMetrics::new(&registry),
            compaction: CompactionMetrics::new(&registry),
            security: SecurityMetrics::new(&registry),
            transport: TransportMetrics::new(&registry),
            uptime_seconds,
            registry,
        }
    }

    /// Encode all metrics as Prometheus text format.
    pub fn encode(&self) -> String {
        encode_prometheus(&self.registry)
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_register_and_encode() {
        let m = Metrics::new();
        m.ingest.batches_total.inc();
        m.ingest.rows_total.inc_by(100);
        m.buffer.bytes.set(1024);
        m.catalog
            .commit_total
            .with_label_values(&["committed"])
            .inc();
        m.catalog
            .status_check_total
            .with_label_values(&["not_committed"])
            .inc();
        m.flush
            .inflight
            .with_label_values(&["committed"])
            .inc();
        m.flush
            .blocked_total
            .with_label_values(&["CommitStateUnknown"])
            .inc();
        m.flush
            .blocked_resolution_total
            .with_label_values(&["recommitted"])
            .inc();
        m.wal
            .replay_records_total
            .with_label_values(&["replayed"])
            .inc();
        m.wal
            .recovery_failure_total
            .with_label_values(&["Wal"])
            .inc();

        let text = m.encode();
        assert!(text.contains("teodb_ingest_batches_total"));
        assert!(text.contains("teodb_ingest_rows_total"));
        assert!(text.contains("teodb_buffer_bytes"));
        for name in [
            "teodb_catalog_commit_total",
            "teodb_catalog_commit_rebase_total",
            "teodb_catalog_commit_status_check_total",
            "teodb_catalog_commit_status_check_duration_seconds",
            "teodb_flush_inflight",
            "teodb_flush_blocked_tables",
            "teodb_flush_blocked_total",
            "teodb_flush_blocked_resolution_total",
            "teodb_prepared_flushes",
            "teodb_prepared_flush_oldest_age_seconds",
            "teodb_writer_checkpoint_parse_failure_total",
            "teodb_writer_checkpoint_count",
            "teodb_wal_replay_records_total",
            "teodb_wal_recovery_failure_total",
            "teodb_flush_lock_wait_seconds",
        ] {
            assert!(text.contains(name), "missing metric family {name}");
        }
    }
}
