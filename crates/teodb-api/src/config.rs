use std::time::Duration;

use crate::security::TrustedProxyCidr;

/// Router-owned API policy. Listener and socket policy remains in
/// `teodb-server`.
#[derive(Debug, Clone)]
pub struct ApiConfig {
    pub max_body_bytes: u64,
    pub max_result_bytes: u64,
    pub cors_allowed_origins: Vec<String>,
    pub read_requests_per_window: u32,
    pub write_requests_per_window: u32,
    pub public_requests_per_window: u32,
    pub rate_limit_window: Duration,
    pub max_rate_limit_keys: u64,
    pub max_concurrent_operations_per_principal: usize,
    pub trusted_proxy_cidrs: Vec<TrustedProxyCidr>,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            max_body_bytes: 64 * 1024 * 1024,
            max_result_bytes: 64 * 1024 * 1024,
            cors_allowed_origins: Vec::new(),
            read_requests_per_window: 20_000,
            write_requests_per_window: 10_000,
            public_requests_per_window: 40_000,
            rate_limit_window: Duration::from_secs(60),
            max_rate_limit_keys: 8_192,
            max_concurrent_operations_per_principal: 32,
            trusted_proxy_cidrs: Vec::new(),
        }
    }
}
