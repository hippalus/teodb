//! WAL subsystem metrics.

use prometheus::{Histogram, IntCounterVec, Registry};

use super::registry::{counter_vec, histogram, register};

/// Metrics for the write-ahead log.
pub struct WalMetrics {
    pub replay_duration_seconds: Histogram,
    pub replay_records_total: IntCounterVec,
    pub recovery_failure_total: IntCounterVec,
}

impl WalMetrics {
    pub fn new(registry: &Registry) -> Self {
        let replay_duration_seconds = histogram(
            "teodb_wal_replay_duration_seconds",
            "WAL replay duration at startup",
            vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0],
        );
        let replay_records_total = counter_vec(
            "teodb_wal_replay_records_total",
            "WAL records inspected during recovery by bounded outcome",
            &["outcome"],
        );
        let recovery_failure_total = counter_vec(
            "teodb_wal_recovery_failure_total",
            "Fail-closed WAL recovery failures by bounded reason",
            &["reason"],
        );

        register(registry, &replay_duration_seconds);
        register(registry, &replay_records_total);
        register(registry, &recovery_failure_total);

        Self {
            replay_duration_seconds,
            replay_records_total,
            recovery_failure_total,
        }
    }
}
