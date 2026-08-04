//! Compaction subsystem metrics.

use prometheus::{Histogram, IntCounter, Registry};

use super::registry::{counter, histogram, register};

/// Metrics for background compaction.
pub struct CompactionMetrics {
    pub total: IntCounter,
    pub errors_total: IntCounter,
    pub duration_seconds: Histogram,
}

impl CompactionMetrics {
    pub fn new(registry: &Registry) -> Self {
        let total = counter("teodb_compaction_total", "Total compaction operations");
        let errors_total = counter("teodb_compaction_errors_total", "Total compaction errors");
        let duration_seconds = histogram(
            "teodb_compaction_duration_seconds",
            "Compaction operation duration",
            vec![1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0],
        );

        register(registry, &total);
        register(registry, &errors_total);
        register(registry, &duration_seconds);

        Self {
            total,
            errors_total,
            duration_seconds,
        }
    }
}
