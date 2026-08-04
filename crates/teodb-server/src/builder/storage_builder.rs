//! Server-owned object-store and storage construction.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::config::{StorageConfig, TeoDBConfig};

#[derive(Debug, Clone)]
pub(crate) struct S3Settings {
    endpoint: Option<String>,
    access_key: Option<String>,
    secret_key: Option<String>,
    region: Option<String>,
    allow_http: bool,
}

impl From<&StorageConfig> for S3Settings {
    fn from(config: &StorageConfig) -> Self {
        Self {
            endpoint: config.s3_endpoint.clone(),
            access_key: config.s3_access_key.clone(),
            secret_key: config.s3_secret_key.clone(),
            region: config.s3_region.clone(),
            allow_http: config.s3_allow_http,
        }
    }
}

impl S3Settings {
    pub(crate) fn iceberg_properties(&self) -> HashMap<String, String> {
        let mut properties = HashMap::new();
        if let Some(endpoint) = &self.endpoint {
            properties.insert("s3.endpoint".to_owned(), endpoint.clone());
        }
        if let Some(access_key) = &self.access_key {
            properties.insert("s3.access-key-id".to_owned(), access_key.clone());
        }
        if let Some(secret_key) = &self.secret_key {
            properties.insert("s3.secret-access-key".to_owned(), secret_key.clone());
        }
        if let Some(region) = &self.region {
            properties.insert("s3.region".to_owned(), region.clone());
            properties.insert("client.region".to_owned(), region.clone());
        }
        if self.access_key.is_some() && self.secret_key.is_some() {
            properties.insert("s3.disable-ec2-metadata".to_owned(), "true".to_owned());
            properties.insert("s3.disable-config-load".to_owned(), "true".to_owned());
        }
        if self.allow_http {
            properties.insert("s3.path-style-access".to_owned(), "true".to_owned());
        }
        properties
    }

    fn build_store(&self, bucket: &str) -> eyre::Result<Arc<dyn object_store::ObjectStore>> {
        let mut builder = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket);
        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(access_key) = &self.access_key {
            builder = builder.with_access_key_id(access_key);
        }
        if let Some(secret_key) = &self.secret_key {
            builder = builder.with_secret_access_key(secret_key);
        }
        if let Some(region) = &self.region {
            builder = builder.with_region(region);
        }
        if self.allow_http {
            builder = builder.with_allow_http(true);
        }

        builder
            .build()
            .map(|store| Arc::new(store) as Arc<dyn object_store::ObjectStore>)
            .map_err(|error| eyre::eyre!("failed to build S3 backend for bucket '{bucket}': {error}"))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StorageRuntimeConfig {
    warehouse_uri: String,
    cache_dir: PathBuf,
    cache_max_bytes: u64,
}

impl StorageRuntimeConfig {
    fn from_config(config: &TeoDBConfig) -> Self {
        let warehouse_uri = config
            .catalog
            .warehouse
            .as_deref()
            .filter(|warehouse| warehouse.starts_with("s3://"))
            .unwrap_or(&config.ingest.default_warehouse_uri)
            .to_owned();

        Self {
            warehouse_uri,
            cache_dir: config.storage.cache_dir.clone(),
            cache_max_bytes: config.storage.cache_max_bytes,
        }
    }
}

#[derive(Clone)]
struct StorageBackendRegistration {
    scheme: teodb_core::location::StorageScheme,
    bucket: String,
    storage: Arc<dyn teodb_core::traits::storage::Storage>,
}

pub(crate) struct StorageComponentsBuilder {
    config: StorageRuntimeConfig,
    s3_settings: S3Settings,
}

impl StorageComponentsBuilder {
    pub(crate) fn new(config: StorageRuntimeConfig, s3_settings: S3Settings) -> Self {
        Self { config, s3_settings }
    }

    pub(crate) fn from_config(config: &TeoDBConfig, s3_settings: S3Settings) -> Self {
        Self::new(StorageRuntimeConfig::from_config(config), s3_settings)
    }

    pub(crate) fn build(self) -> eyre::Result<StorageComponents> {
        let (runtime_registration, backend_registration) = self.build_s3_registration()?;

        let backend = (
            backend_registration.scheme,
            backend_registration.bucket,
            backend_registration.storage,
        );
        let (factory, cache_index): (Arc<dyn teodb_core::traits::storage::StorageFactory>, _) =
            if self.config.cache_max_bytes > 0 {
                let factory = teodb_storage::DefaultStorageFactory::with_cache(
                    backend,
                    &self.config.cache_dir,
                    self.config.cache_max_bytes,
                )
                .map_err(|error| eyre::eyre!("failed to initialize SSD cache: {error}"))?;
                let cache_index = factory.cache_index().cloned();
                (Arc::new(factory), cache_index)
            } else {
                (Arc::new(teodb_storage::DefaultStorageFactory::new(backend)), None)
            };

        Ok(StorageComponents {
            factory,
            cache_index,
            object_store_registration: runtime_registration,
        })
    }

    fn build_s3_registration(
        &self,
    ) -> eyre::Result<(teodb_query::ObjectStoreRegistration, StorageBackendRegistration)> {
        let bucket = s3_bucket_from_warehouse(&self.config.warehouse_uri)?;
        let store = self.s3_settings.build_store(&bucket)?;
        let runtime_registration = teodb_query::ObjectStoreRegistration::new(format!("s3://{bucket}"), store.clone())
            .map_err(|error| eyre::eyre!("{error}"))?;
        let storage =
            Arc::new(teodb_storage::ObjectStoreBackend::new(store)) as Arc<dyn teodb_core::traits::storage::Storage>;
        let backend_registration = StorageBackendRegistration {
            scheme: teodb_core::location::StorageScheme::S3,
            bucket,
            storage,
        };

        Ok((runtime_registration, backend_registration))
    }
}

fn s3_bucket_from_warehouse(warehouse_uri: &str) -> eyre::Result<String> {
    let Some(rest) = warehouse_uri.strip_prefix("s3://") else {
        return Err(eyre::eyre!(
            "storage warehouse must be an s3:// URI, got '{warehouse_uri}'"
        ));
    };
    let bucket = rest
        .split('/')
        .next()
        .filter(|bucket| !bucket.is_empty())
        .ok_or_else(|| eyre::eyre!("storage warehouse URI '{warehouse_uri}' is missing a bucket"))?;
    Ok(bucket.to_owned())
}

pub(crate) struct StorageComponents {
    pub(crate) factory: Arc<dyn teodb_core::traits::storage::StorageFactory>,
    pub(crate) cache_index: Option<Arc<teodb_storage::cache::index::CacheIndex>>,
    pub(crate) object_store_registration: teodb_query::ObjectStoreRegistration,
}

impl StorageComponents {
    pub(crate) fn build(config: &TeoDBConfig, settings: &S3Settings) -> eyre::Result<Self> {
        StorageComponentsBuilder::from_config(config, settings.clone()).build()
    }

    pub(crate) fn object_store_registration(&self) -> &teodb_query::ObjectStoreRegistration {
        &self.object_store_registration
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iceberg_properties_come_from_the_same_s3_settings() {
        let settings = S3Settings {
            endpoint: Some("http://rustfs:9000".into()),
            access_key: Some("access".into()),
            secret_key: Some("secret".into()),
            region: Some("eu-west-1".into()),
            allow_http: true,
        };

        let properties = settings.iceberg_properties();

        assert_eq!(properties["s3.endpoint"], "http://rustfs:9000");
        assert_eq!(properties["s3.access-key-id"], "access");
        assert_eq!(properties["s3.secret-access-key"], "secret");
        assert_eq!(properties["s3.region"], "eu-west-1");
        assert_eq!(properties["client.region"], "eu-west-1");
        assert_eq!(properties["s3.path-style-access"], "true");
        assert_eq!(properties["s3.disable-ec2-metadata"], "true");
    }

    #[test]
    fn storage_builder_rejects_non_s3_warehouse_uri() {
        let settings = S3Settings::from(&StorageConfig::default());
        let config = StorageRuntimeConfig {
            warehouse_uri: "file:///tmp/warehouse".to_owned(),
            cache_dir: PathBuf::from("./cache"),
            cache_max_bytes: 0,
        };
        let result = StorageComponentsBuilder::new(config, settings).build();

        assert!(result.is_err());
    }

    #[test]
    fn storage_runtime_config_accepts_s3_catalog_warehouse() {
        let mut config = TeoDBConfig::default();
        config.catalog.warehouse = Some("s3://catalog-bucket/warehouse".to_owned());

        let runtime = StorageRuntimeConfig::from_config(&config);

        assert_eq!(runtime.warehouse_uri, "s3://catalog-bucket/warehouse");
    }

    #[test]
    fn storage_runtime_config_uses_table_default_for_catalog_warehouse_id() {
        let mut config = TeoDBConfig::default();
        config.catalog.warehouse = Some("warehouse".to_owned());
        config.ingest.default_warehouse_uri = "s3://table-bucket".to_owned();

        let runtime = StorageRuntimeConfig::from_config(&config);

        assert_eq!(runtime.warehouse_uri, "s3://table-bucket");
    }
}
