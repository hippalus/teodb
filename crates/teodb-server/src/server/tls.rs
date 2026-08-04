//! TLS configuration loading.
//!
//! Produces a [`TlsBundle`] carrying both the raw PEM bytes (for tonic/Flight)
//! and a parsed `rustls::ServerConfig` (for the server-owned REST listener).
//! Optionally configures mTLS client certificate verification.

use std::sync::Arc;

use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use tracing::info;

use crate::config::SecurityConfig;

/// Everything needed to configure TLS on both REST and Flight transports.
pub struct TlsBundle {
    /// Pre-built rustls config for the server-owned REST TLS listener.
    pub rustls_config: Arc<rustls::ServerConfig>,
    /// Raw PEM certificate bytes for tonic's `Identity::from_pem`.
    pub cert_pem: Vec<u8>,
    /// Raw PEM private key bytes for tonic's `Identity::from_pem`.
    pub key_pem: Vec<u8>,
    /// Raw PEM client CA cert bytes for tonic's `Certificate::from_pem` (mTLS).
    pub client_ca_pem: Option<Vec<u8>>,
}

/// Load TLS configuration from cert/key PEM files, with optional mTLS.
///
/// Returns `None` when neither `tls_cert` nor `tls_key` is set.
/// Returns an error if only one of the two is provided.
pub fn load_tls_bundle(security: &SecurityConfig) -> eyre::Result<Option<TlsBundle>> {
    let (cert_path, key_path) = match (&security.tls_cert, &security.tls_key) {
        (Some(c), Some(k)) => (c, k),
        (None, None) => return Ok(None),
        _ => eyre::bail!("both tls_cert and tls_key must be provided together"),
    };

    // Tonic and HTTP clients can enable multiple rustls crypto backends in the
    // unified feature graph. Select the server's reviewed backend explicitly
    // instead of relying on rustls' single-provider feature inference.
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        eyre::bail!("failed to install the rustls aws-lc crypto provider");
    }

    let cert_pem =
        std::fs::read(cert_path).map_err(|e| eyre::eyre!("failed to read TLS cert {}: {e}", cert_path.display()))?;
    let key_pem =
        std::fs::read(key_path).map_err(|e| eyre::eyre!("failed to read TLS key {}: {e}", key_path.display()))?;

    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| eyre::eyre!("failed to parse TLS cert: {e}"))?;

    if certs.is_empty() {
        eyre::bail!("no certificates found in {}", cert_path.display());
    }

    let key: PrivateKeyDer<'static> =
        PrivateKeyDer::from_pem_slice(&key_pem).map_err(|e| eyre::eyre!("failed to parse TLS key: {e}"))?;

    // Build mTLS client verifier if client_ca_cert is configured.
    let (client_verifier, client_ca_pem) = if let Some(ca_path) = &security.client_ca_cert {
        let ca_pem =
            std::fs::read(ca_path).map_err(|e| eyre::eyre!("failed to read client CA {}: {e}", ca_path.display()))?;

        let ca_certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&ca_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| eyre::eyre!("failed to parse client CA cert: {e}"))?;

        let mut root_store = rustls::RootCertStore::empty();
        for ca_cert in ca_certs {
            root_store.add(ca_cert)?;
        }

        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| eyre::eyre!("failed to build client verifier: {e}"))?;

        info!(ca = %ca_path.display(), "mTLS client verification enabled");
        (Some(verifier), Some(ca_pem))
    } else {
        (None, None)
    };

    let mut rustls_config = if let Some(verifier) = client_verifier {
        rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|e| eyre::eyre!("TLS config error: {e}"))?
    } else {
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| eyre::eyre!("TLS config error: {e}"))?
    };
    rustls_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    info!(cert = %cert_path.display(), key = %key_path.display(), "TLS certificate loaded");

    Ok(Some(TlsBundle {
        rustls_config: Arc::new(rustls_config),
        cert_pem,
        key_pem,
        client_ca_pem,
    }))
}
