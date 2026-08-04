//! Catalog construction from configuration.

use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use super::storage_builder::S3Settings;
use crate::config::CatalogConfig;

/// Build the catalog adapter from config.
pub async fn build_catalog(
    catalog: &CatalogConfig,
    s3: &S3Settings,
    max_writer_checkpoints_per_table: usize,
    observer: Option<Arc<dyn teodb_catalog::CatalogObserver>>,
) -> eyre::Result<Arc<dyn teodb_core::traits::catalog::Catalog>> {
    let credentials = if let Some(ref cred) = catalog.oauth2_credential {
        teodb_catalog::IcebergCredentials::OAuth2 {
            credential: cred.clone(),
            scope: catalog.oauth2_scope.clone(),
            oauth_server_uri: catalog.oauth2_server_uri.clone(),
        }
    } else if let Some(ref token) = catalog.token {
        teodb_catalog::IcebergCredentials::Bearer { token: token.clone() }
    } else {
        teodb_catalog::IcebergCredentials::None
    };

    let config = teodb_catalog::IcebergCatalogConfig {
        uri: catalog.uri.clone(),
        warehouse: catalog
            .warehouse
            .clone()
            .unwrap_or_else(|| "warehouse".into()),
        credentials,
        retry: teodb_catalog::RetryConfig::default(),
        request_timeout: Duration::from_secs(30),
        s3_props: s3.iceberg_properties(),
        max_writer_checkpoints_per_table,
    };

    let mut adapter = teodb_catalog::IcebergCatalogAdapter::open(config)
        .await
        .map_err(|e| eyre::eyre!("failed to initialize catalog at {}: {e}", catalog.uri))?;
    if let Some(observer) = observer {
        adapter = adapter.with_observer(observer);
    }

    let result: Arc<dyn teodb_core::traits::catalog::Catalog> = Arc::new(adapter);

    info!(catalog_type = %catalog.catalog_type, uri = %catalog.uri, "catalog ready");

    Ok(result)
}
