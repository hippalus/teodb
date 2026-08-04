//! `DefaultStorageFactory` — resolves `ObjectLocation` to `(Storage, ObjectPath)`.
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::location::{ObjectLocation, ObjectPath, StorageScheme};
use teodb_core::traits::storage::{Storage, StorageFactory};

use crate::cache::CachedStorage;
use crate::cache::index::{CacheConfig, CacheIndex};

/// A registered storage backend for one warehouse bucket.
#[derive(Clone)]
struct RegisteredBackend {
    scheme: StorageScheme,
    bucket: String,
    storage: Arc<dyn Storage>,
}

/// The default storage factory for one resolved warehouse backend.
pub struct DefaultStorageFactory {
    backend: RegisteredBackend,
    cache_index: Option<Arc<CacheIndex>>,
}

impl DefaultStorageFactory {
    pub fn new(backend: (StorageScheme, String, Arc<dyn Storage>)) -> Self {
        Self {
            backend: RegisteredBackend::from(backend),
            cache_index: None,
        }
    }

    pub fn with_cache(
        backend: (StorageScheme, String, Arc<dyn Storage>),
        cache_dir: &Path,
        max_cache_bytes: u64,
    ) -> TeoDBResult<Self> {
        let cache_config = CacheConfig {
            root_dir: cache_dir.to_path_buf(),
            max_total_bytes: max_cache_bytes,
            ..Default::default()
        };
        let index = CacheIndex::open_with_config(cache_config)?;

        let (scheme, bucket, storage) = backend;
        let uri_prefix = scheme.uri_prefix(Some(&bucket));
        let cached: Arc<dyn Storage> = Arc::new(CachedStorage::new(storage, index.clone(), uri_prefix));

        Ok(Self {
            backend: RegisteredBackend {
                scheme,
                bucket,
                storage: cached,
            },
            cache_index: Some(index),
        })
    }

    pub fn cache_index(&self) -> Option<&Arc<CacheIndex>> {
        self.cache_index.as_ref()
    }

    #[cfg(test)]
    fn empty_for_tests() -> Self {
        Self {
            backend: RegisteredBackend {
                scheme: StorageScheme::S3,
                bucket: "__missing__".to_owned(),
                storage: Arc::new(crate::backends::ObjectStoreBackend::in_memory()),
            },
            cache_index: None,
        }
    }
}

impl From<(StorageScheme, String, Arc<dyn Storage>)> for RegisteredBackend {
    fn from((scheme, bucket, storage): (StorageScheme, String, Arc<dyn Storage>)) -> Self {
        Self {
            scheme,
            bucket,
            storage,
        }
    }
}

#[async_trait]
impl StorageFactory for DefaultStorageFactory {
    async fn resolve(&self, loc: &ObjectLocation) -> TeoDBResult<(Arc<dyn Storage>, ObjectPath)> {
        let bucket = loc.bucket.clone().unwrap_or_default();
        if self.backend.scheme == loc.scheme && self.backend.bucket == bucket {
            return Ok((self.backend.storage.clone(), ObjectPath::new(loc.key.clone())));
        }

        Err(TeoDBError::Config(format!(
            "no storage backend registered for {:?} bucket {:?}",
            loc.scheme, bucket
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use teodb_core::location::StorageScheme;

    #[tokio::test]
    async fn resolve_registered_backend() {
        let backend = Arc::new(crate::backends::ObjectStoreBackend::in_memory());
        let factory = DefaultStorageFactory::new((StorageScheme::S3, "my-bucket".into(), backend));

        let loc = ObjectLocation::parse("s3://my-bucket/data/file.parquet").unwrap();
        let (_, path) = factory.resolve(&loc).await.unwrap();
        assert_eq!(path.as_str(), "data/file.parquet");
    }

    #[tokio::test]
    async fn resolve_unregistered_returns_error() {
        let factory = DefaultStorageFactory::empty_for_tests();
        let loc = ObjectLocation::parse("s3://unknown/key").unwrap();
        let result = factory.resolve(&loc).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn cached_factory_wraps_backends() {
        let backend = Arc::new(crate::backends::ObjectStoreBackend::in_memory());
        let path = ObjectPath::new("test.bin");
        backend
            .put(&path, bytes::Bytes::from("cached-data"))
            .await
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let factory = DefaultStorageFactory::with_cache(
            (
                StorageScheme::S3,
                "cached-bucket".to_string(),
                backend as Arc<dyn Storage>,
            ),
            dir.path(),
            1024 * 1024 * 1024,
        )
        .unwrap();

        let loc = ObjectLocation::parse("s3://cached-bucket/test.bin").unwrap();
        let (storage, obj_path) = factory.resolve(&loc).await.unwrap();

        // First read — cache miss, fetches from inner
        let data = storage.get(&obj_path).await.unwrap();
        assert_eq!(data, bytes::Bytes::from("cached-data"));

        // Second read — served from SSD cache
        let data2 = storage.get(&obj_path).await.unwrap();
        assert_eq!(data2, bytes::Bytes::from("cached-data"));
    }
}
