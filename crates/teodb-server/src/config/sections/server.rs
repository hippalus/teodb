use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub rest_bind: String,
    pub flight_bind: String,
    pub max_http_connections: usize,
    pub max_http_in_flight_requests: usize,
    pub max_flight_connections: usize,
    pub max_flight_in_flight_requests: usize,
    pub max_flight_streams_per_connection: u32,
    pub flight_max_decoding_message_bytes: usize,
    pub flight_max_encoding_message_bytes: usize,
    pub idle_timeout_secs: u64,
    pub max_result_bytes: u64,
    pub max_concurrent_operations_per_principal: usize,
    pub read_requests_per_window: u32,
    pub write_requests_per_window: u32,
    pub public_requests_per_window: u32,
    pub rate_limit_window_secs: u64,
    pub max_rate_limit_keys: u64,
    #[serde(default)]
    pub trusted_proxy_cidrs: Vec<String>,
    /// CORS origin allow-list for the REST API. Empty (default) keeps the
    /// permissive any-origin policy for the embedded SPA and dev proxies.
    #[serde(default)]
    pub cors_allowed_origins: Vec<String>,
}
impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            rest_bind: "0.0.0.0:8080".into(),
            flight_bind: "0.0.0.0:8815".into(),
            max_http_connections: 256,
            max_http_in_flight_requests: 256,
            max_flight_connections: 512,
            max_flight_in_flight_requests: 512,
            max_flight_streams_per_connection: 64,
            flight_max_decoding_message_bytes: 64 * 1024 * 1024,
            flight_max_encoding_message_bytes: 64 * 1024 * 1024,
            idle_timeout_secs: 300,
            max_result_bytes: 64 * 1024 * 1024,
            max_concurrent_operations_per_principal: 32,
            read_requests_per_window: 20_000,
            write_requests_per_window: 10_000,
            public_requests_per_window: 40_000,
            rate_limit_window_secs: 60,
            max_rate_limit_keys: 8_192,
            trusted_proxy_cidrs: Vec::new(),
            cors_allowed_origins: Vec::new(),
        }
    }
}
