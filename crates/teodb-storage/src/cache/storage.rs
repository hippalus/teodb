//! Whole-object caching storage adapter.

use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use teodb_core::error::TeoDBResult;
use teodb_core::location::ObjectPath;
use teodb_core::traits::storage::{ObjectMeta, Storage};

use super::flight::SingleFlight;
use super::index::CacheIndex;

/// A `Storage` wrapper that caches whole objects on local NVMe.
pub struct CachedStorage {
    inner: Arc<dyn Storage>,
    index: Arc<CacheIndex>,
    inflight: Arc<SingleFlight>,
    uri_prefix: String,
}

impl CachedStorage {
    pub fn new(inner: Arc<dyn Storage>, index: Arc<CacheIndex>, uri_prefix: impl Into<String>) -> Self {
        Self {
            inner,
            index,
            inflight: SingleFlight::new(),
            uri_prefix: uri_prefix.into(),
        }
    }

    #[inline]
    fn cache_key(&self, path: &ObjectPath) -> String {
        let mut key = String::with_capacity(self.uri_prefix.len() + path.as_str().len());
        key.push_str(&self.uri_prefix);
        key.push_str(path.as_str());
        key
    }
}

#[async_trait]
impl Storage for CachedStorage {
    async fn get(&self, path: &ObjectPath) -> TeoDBResult<Bytes> {
        let uri = self.cache_key(path);

        // Fast path: cache hit.
        if let Some(bytes) = self.index.get_cached(&uri).await? {
            return Ok(bytes);
        }

        // Miss: single-flighted fetch.
        let inner = self.inner.clone();
        let index = self.index.clone();
        let path_owned = path.clone();
        let uri_clone = uri.clone();

        self.inflight
            .run(uri, async move {
                let bytes = inner.get(&path_owned).await?;
                let head = inner.head(&path_owned).await?;

                // Cache the object.
                index
                    .put_cached(&uri_clone, &bytes, head.etag.as_deref())
                    .await?;
                index.maybe_evict().await?;

                Ok(bytes)
            })
            .await
    }

    async fn get_range(&self, path: &ObjectPath, range: Range<u64>) -> TeoDBResult<Bytes> {
        // Validate range bounds to prevent underflow.
        if range.start > range.end {
            return Err(teodb_core::error::TeoDBError::InvalidArgument {
                field: "range".into(),
                message: format!("start ({}) > end ({})", range.start, range.end),
            });
        }

        let uri = self.cache_key(path);

        // If the object is cached, serve the range from local copy.
        if let Some(bytes) = self.index.get_cached(&uri).await? {
            let start = (range.start as usize).min(bytes.len());
            let end = (range.end as usize).min(bytes.len());
            return Ok(bytes.slice(start..end));
        }

        // For small ranges, go direct without caching.
        let range_len = range.end.saturating_sub(range.start);
        if range_len <= 64 * 1024 {
            return self.inner.get_range(path, range).await;
        }

        // Larger range: only promote the whole object into cache when the read
        // covers most of it (full-file reads, typically small files). A small
        // column range from a large Parquet file is served directly so we don't
        // pull the entire object into cache and defeat columnar pruning (P1-14).
        let head = self.inner.head(path).await?;
        let covers_most = head.size == 0 || range_len.saturating_mul(2) >= head.size;
        if covers_most {
            let full = self.get(path).await?;
            let start = (range.start as usize).min(full.len());
            let end = (range.end as usize).min(full.len());
            return Ok(full.slice(start..end));
        }

        self.inner.get_range(path, range).await
    }

    async fn head(&self, path: &ObjectPath) -> TeoDBResult<ObjectMeta> {
        self.inner.head(path).await
    }

    async fn put(&self, path: &ObjectPath, bytes: Bytes) -> TeoDBResult<ObjectMeta> {
        let uri = self.cache_key(path);
        self.index.remove(&uri).await?;
        let meta = self.inner.put(path, bytes.clone()).await?;

        // Cache-fill from the bytes we just wrote.
        self.index
            .put_cached(&uri, &bytes, meta.etag.as_deref())
            .await?;
        Ok(meta)
    }

    async fn delete(&self, path: &ObjectPath) -> TeoDBResult<()> {
        self.index.remove(&self.cache_key(path)).await?;
        self.inner.delete(path).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> TeoDBResult<()> {
        self.index.remove(&self.cache_key(to)).await?;
        self.inner.copy(from, to).await
    }

    async fn list(
        &self,
        prefix: &ObjectPath,
    ) -> TeoDBResult<Pin<Box<dyn futures::Stream<Item = TeoDBResult<ObjectMeta>> + Send>>> {
        self.inner.list(prefix).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::ObjectStoreBackend;

    #[tokio::test]
    async fn cached_get_populates_cache() {
        let inner = Arc::new(ObjectStoreBackend::in_memory());
        let path = ObjectPath::new("test.bin");
        inner
            .put(&path, Bytes::from("hello"))
            .await
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let index = CacheIndex::open(dir.path()).unwrap();
        let cached = CachedStorage::new(inner, index, "mem://bucket/");

        // First get: cache miss, fetches from inner.
        let data = cached.get(&path).await.unwrap();
        assert_eq!(data, Bytes::from("hello"));

        // Verify cached.
        let uri = cached.cache_key(&path);
        let cached_data = cached.index.get_cached(&uri).await.unwrap();
        assert!(cached_data.is_some());
    }

    #[tokio::test]
    async fn small_range_from_large_object_is_not_cached() {
        let inner = Arc::new(ObjectStoreBackend::in_memory());
        let path = ObjectPath::new("big.parquet");
        // 1 MiB object; read a 128 KiB range (well under half).
        inner
            .put(&path, Bytes::from(vec![7u8; 1024 * 1024]))
            .await
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let index = CacheIndex::open(dir.path()).unwrap();
        let cached = CachedStorage::new(inner, index, "mem://bucket/");

        let part = cached
            .get_range(&path, 0..128 * 1024)
            .await
            .unwrap();
        assert_eq!(part.len(), 128 * 1024);

        // The large object must not have been promoted into cache.
        let uri = cached.cache_key(&path);
        assert!(
            cached
                .index
                .get_cached(&uri)
                .await
                .unwrap()
                .is_none(),
            "a small range from a large object must not cache the whole object"
        );
    }

    #[tokio::test]
    async fn full_range_promotes_object_to_cache() {
        let inner = Arc::new(ObjectStoreBackend::in_memory());
        let path = ObjectPath::new("small.parquet");
        let data = Bytes::from(vec![3u8; 256 * 1024]);
        inner.put(&path, data.clone()).await.unwrap();

        let dir = tempfile::tempdir().unwrap();
        let index = CacheIndex::open(dir.path()).unwrap();
        let cached = CachedStorage::new(inner, index, "mem://bucket/");

        // Range covers the whole object → promote to cache.
        let part = cached
            .get_range(&path, 0..256 * 1024)
            .await
            .unwrap();
        assert_eq!(part.len(), 256 * 1024);
        let uri = cached.cache_key(&path);
        assert!(
            cached
                .index
                .get_cached(&uri)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn put_invalidates_and_refills_cache() {
        let inner = Arc::new(ObjectStoreBackend::in_memory());
        let path = ObjectPath::new("update.bin");
        inner.put(&path, Bytes::from("v1")).await.unwrap();

        let dir = tempfile::tempdir().unwrap();
        let index = CacheIndex::open(dir.path()).unwrap();
        let cached = CachedStorage::new(inner, index, "mem://bucket/");

        // Populate cache.
        cached.get(&path).await.unwrap();

        // Put new data.
        cached
            .put(&path, Bytes::from("v2"))
            .await
            .unwrap();

        // Cache should have new data.
        let uri = cached.cache_key(&path);
        let data = cached
            .index
            .get_cached(&uri)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(data, Bytes::from("v2"));
    }
}
