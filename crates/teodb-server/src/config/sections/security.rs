use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Security mode controlling transport encryption and authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum SecurityMode {
    /// No TLS, anonymous access. For local development only.
    #[default]
    Plaintext,
    /// TLS required, allow-list authorization.
    Tls,
    /// TLS + OAuth2/JWT authentication. Recommended for production.
    #[serde(rename = "oauth2")]
    #[value(name = "oauth2")]
    OAuth2,
}

impl std::fmt::Display for SecurityMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plaintext => f.write_str("plaintext"),
            Self::Tls => f.write_str("tls"),
            Self::OAuth2 => f.write_str("oauth2"),
        }
    }
}

impl SecurityMode {
    /// Whether this mode skips production hardening checks.
    pub fn is_insecure(&self) -> bool {
        matches!(self, Self::Plaintext)
    }

    /// Whether anonymous (unauthenticated) access is allowed.
    pub fn allows_anonymous(&self) -> bool {
        matches!(self, Self::Plaintext)
    }

    /// Whether TLS is required.
    pub fn requires_tls(&self) -> bool {
        matches!(self, Self::Tls | Self::OAuth2)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SecurityConfig {
    /// Security mode: plaintext (dev), tls, or oauth2 (production).
    pub mode: SecurityMode,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    /// Path to the allow-list TOML file for authorization rules.
    pub allow_list_path: Option<PathBuf>,
    /// Path to the CA certificate for mTLS client verification.
    /// When set, the server requires valid client certificates signed by this CA.
    pub client_ca_cert: Option<PathBuf>,
    /// JWT issuer for token validation (e.g., <https://auth.example.com>).
    pub jwt_issuer: Option<String>,
    /// JWT audience for token validation (e.g., "teodb").
    pub jwt_audience: Option<String>,
    /// Path to JWT signing key (PEM) for local validation.
    /// For HMAC, the file contains the raw secret bytes.
    pub jwt_signing_key: Option<PathBuf>,
    /// JWT algorithm. Defaults to RS256.
    #[serde(default = "default_jwt_algorithm")]
    pub jwt_algorithm: String,
    /// Bearer token required on admin endpoints and `/metrics`. Unset
    /// (default) leaves them open — the server warns at startup.
    pub admin_token: Option<String>,
}

fn default_jwt_algorithm() -> String {
    "RS256".into()
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            mode: SecurityMode::Plaintext,
            tls_cert: None,
            tls_key: None,
            allow_list_path: None,
            client_ca_cert: None,
            jwt_issuer: None,
            jwt_audience: None,
            jwt_signing_key: None,
            jwt_algorithm: default_jwt_algorithm(),
            admin_token: None,
        }
    }
}
