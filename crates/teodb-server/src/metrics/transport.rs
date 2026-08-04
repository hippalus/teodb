use prometheus::{IntCounterVec, IntGaugeVec, Registry};

use super::registry::{counter_vec, gauge_vec, register};

pub struct TransportMetrics {
    pub active_connections: IntGaugeVec,
    pub result_bytes_total: IntCounterVec,
    pub admission_rejections_total: IntCounterVec,
}

impl TransportMetrics {
    pub fn new(registry: &Registry) -> Self {
        let active_connections = gauge_vec(
            "teodb_transport_active_connections",
            "Active accepted connections by transport",
            &["transport"],
        );
        let result_bytes_total = counter_vec(
            "teodb_transport_result_bytes_total",
            "Application result bytes emitted before transport compression",
            &["transport", "operation"],
        );
        let admission_rejections_total = counter_vec(
            "teodb_transport_admission_rejections_total",
            "API admission rejections by bounded reason",
            &["transport", "reason"],
        );
        register(registry, &active_connections);
        register(registry, &result_bytes_total);
        register(registry, &admission_rejections_total);
        Self {
            active_connections,
            result_bytes_total,
            admission_rejections_total,
        }
    }
}
