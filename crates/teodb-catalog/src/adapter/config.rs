//! Configuration for the Iceberg catalog adapter.

use std::collections::HashMap;
use std::time::Duration;

use crate::retry::RetryConfig;

/// Credentials for Iceberg REST catalog authentication.
#[derive(Debug, Clone)]
pub enum IcebergCredentials {
    None,
    Bearer {
        token: String,
    },
    OAuth2 {
        credential: String,
        scope: Option<String>,
        /// Optional OAuth2 token endpoint. If not set, the catalog server's
        /// `/v1/oauth/tokens` endpoint is used per the Iceberg REST spec.
        oauth_server_uri: Option<String>,
    },
}

/// Configuration for the Iceberg REST catalog adapter.
#[derive(Debug, Clone)]
pub struct IcebergCatalogConfig {
    pub uri: String,
    pub warehouse: String,
    pub credentials: IcebergCredentials,
    pub retry: RetryConfig,
    pub request_timeout: Duration,
    /// Extra S3 storage properties passed through to OpenDAL.
    /// Keys use the Iceberg property names: `s3.endpoint`, `s3.access-key-id`, etc.
    pub s3_props: HashMap<String, String>,
    pub max_writer_checkpoints_per_table: usize,
}

impl Default for IcebergCatalogConfig {
    fn default() -> Self {
        Self {
            uri: "http://localhost:8181".into(),
            warehouse: "teodb".into(),
            credentials: IcebergCredentials::None,
            retry: RetryConfig::default(),
            request_timeout: Duration::from_secs(30),
            s3_props: HashMap::new(),
            max_writer_checkpoints_per_table: 32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let cfg = IcebergCatalogConfig::default();
        assert_eq!(cfg.uri, "http://localhost:8181");
    }
}
