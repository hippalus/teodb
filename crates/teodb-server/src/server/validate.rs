//! Production-mode configuration validator.

use tracing::{info, warn};

use crate::config::SecurityMode;
use crate::config::TeoDBConfig;

/// Validate security configuration based on the selected `SecurityMode`.
///
/// - `Plaintext`: skips all checks (dev only).
/// - `Tls`: requires TLS cert/key.
/// - `OAuth2`: requires TLS cert/key and authorization rules.
pub fn validate_production_mode(cfg: &TeoDBConfig) -> eyre::Result<()> {
    match cfg.security.mode {
        SecurityMode::Plaintext => {
            warn!("security.mode = plaintext — no TLS, anonymous access. Do not use in production.");
            return Ok(());
        }
        SecurityMode::Tls | SecurityMode::OAuth2 => {}
    }

    let mut issues = Vec::new();

    if cfg.security.tls_cert.is_none() || cfg.security.tls_key.is_none() {
        issues.push("TLS not configured: set security.tls_cert and security.tls_key");
    }

    if !cfg.wal.fsync_on_append {
        issues.push("WAL fsync disabled: set wal.fsync_on_append = true for durability (I1)");
    }

    if cfg.security.mode == SecurityMode::OAuth2 && cfg.security.allow_list_path.is_none() {
        issues.push("oauth2 mode requires authorization rules: set security.allow_list_path");
    }

    if issues.is_empty() {
        info!(mode = %cfg.security.mode, "security validation passed");
        Ok(())
    } else {
        let detail = issues.join("; ");
        Err(eyre::eyre!(
            "security validation failed for mode '{}' ({} issue(s)): {detail}",
            cfg.security.mode,
            issues.len()
        ))
    }
}
