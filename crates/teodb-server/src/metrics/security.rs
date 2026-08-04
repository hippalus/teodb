//! Security subsystem metrics.

use prometheus::{IntCounterVec, Registry};

use super::registry::{counter_vec, register};

/// Metrics for authentication and authorization.
#[allow(dead_code)]
pub struct SecurityMetrics {
    /// Authentication outcomes with bounded transport/outcome/reason labels.
    pub auth_total: IntCounterVec,
    /// Authorization decisions: allowed, denied.
    pub authz_total: IntCounterVec,
    /// TLS handshake outcomes.
    pub tls_handshakes: IntCounterVec,
}

impl SecurityMetrics {
    pub fn new(registry: &Registry) -> Self {
        let auth_total = counter_vec(
            "teodb_auth_total",
            "Authentication attempts by bounded outcome and reason",
            &["transport", "outcome", "reason"],
        );
        let authz_total = counter_vec(
            "teodb_authz_total",
            "Authorization decisions by bounded action and resource kind",
            &["transport", "outcome", "action", "resource_kind"],
        );
        let tls_handshakes = counter_vec("teodb_tls_handshakes_total", "TLS handshake outcomes", &["outcome"]);

        register(registry, &auth_total);
        register(registry, &authz_total);
        register(registry, &tls_handshakes);

        Self {
            auth_total,
            authz_total,
            tls_handshakes,
        }
    }
}
