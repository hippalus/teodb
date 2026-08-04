//! Ingestion subsystem metrics.

use prometheus::{IntCounter, IntCounterVec, Registry};

use super::registry::{counter, counter_vec, register};

/// Metrics for the ingestion pipeline.
pub struct IngestMetrics {
    pub batches_total: IntCounter,
    pub rows_total: IntCounter,
    pub bytes_total: IntCounter,
    pub errors_total: IntCounter,
    pub rejected_writes_total: IntCounterVec,
}

impl IngestMetrics {
    pub fn new(registry: &Registry) -> Self {
        let batches_total = counter("teodb_ingest_batches_total", "Total ingested batches");
        let rows_total = counter("teodb_ingest_rows_total", "Total ingested rows");
        let bytes_total = counter("teodb_ingest_bytes_total", "Total ingested bytes");
        let errors_total = counter("teodb_ingest_errors_total", "Total ingestion errors");
        let rejected_writes_total = counter_vec(
            "teodb_ingest_rejected_writes_total",
            "Rejected writes by bounded reason",
            &["reason"],
        );

        register(registry, &batches_total);
        register(registry, &rows_total);
        register(registry, &bytes_total);
        register(registry, &errors_total);
        register(registry, &rejected_writes_total);

        Self {
            batches_total,
            rows_total,
            bytes_total,
            errors_total,
            rejected_writes_total,
        }
    }
}
