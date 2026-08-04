//! Generic `Storage` adapter wrapping any `object_store::ObjectStore`.
//!
//! All concrete backends (S3, GCS, Azure, Local) use this adapter. Backend-
//! specific configuration (credentials, endpoints) is handled by constructing
//! the underlying `ObjectStore` via its builder.

use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt};
use teodb_core::error::TeoDBResult;
use teodb_core::location::ObjectPath;
use teodb_core::traits::storage::{ObjectMeta, Storage};

use crate::error::from_object_store;

/// A `Storage` implementation backed by any `object_store::ObjectStore`.
///
/// Each instance is scoped to a single bucket / container — the
/// `StorageFactory` selects the right instance based on the
/// `ObjectLocation`'s scheme and bucket.
pub struct ObjectStoreBackend {
    inner: Arc<dyn ObjectStore>,
}

impl ObjectStoreBackend {
    /// Wrap an existing `ObjectStore` instance.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { inner: store }
    }

    #[cfg(test)]
    pub(crate) fn in_memory() -> Self {
        Self {
            inner: Arc::new(object_store::memory::InMemory::new()),
        }
    }

    /// Escape hatch: return the underlying `ObjectStore` for libraries
    /// that require it directly (Parquet async reader, DataFusion).
    pub fn as_object_store(&self) -> Arc<dyn ObjectStore> {
        self.inner.clone()
    }
}

fn to_os_path(path: &ObjectPath) -> OsPath {
    OsPath::from(path.as_str())
}

fn convert_meta(m: &object_store::ObjectMeta, path: &ObjectPath) -> ObjectMeta {
    ObjectMeta {
        path: path.clone(),
        size: m.size,
        etag: m.e_tag.clone(),
        last_modified: m.last_modified,
    }
}

#[async_trait]
impl Storage for ObjectStoreBackend {
    async fn get(&self, path: &ObjectPath) -> TeoDBResult<Bytes> {
        let p = to_os_path(path);
        let result = self
            .inner
            .get(&p)
            .await
            .map_err(from_object_store)?;
        let bytes = result.bytes().await.map_err(from_object_store)?;
        Ok(bytes)
    }

    async fn get_range(&self, path: &ObjectPath, range: Range<u64>) -> TeoDBResult<Bytes> {
        let p = to_os_path(path);
        let bytes = self
            .inner
            .get_range(&p, range)
            .await
            .map_err(from_object_store)?;
        Ok(bytes)
    }

    async fn head(&self, path: &ObjectPath) -> TeoDBResult<ObjectMeta> {
        let p = to_os_path(path);
        let m = self
            .inner
            .head(&p)
            .await
            .map_err(from_object_store)?;
        Ok(convert_meta(&m, path))
    }

    async fn put(&self, path: &ObjectPath, bytes: Bytes) -> TeoDBResult<ObjectMeta> {
        let p = to_os_path(path);
        let payload = object_store::PutPayload::from(bytes);
        self.inner
            .put(&p, payload)
            .await
            .map_err(from_object_store)?;
        // Re-head to get size and etag.
        self.head(path).await
    }

    async fn delete(&self, path: &ObjectPath) -> TeoDBResult<()> {
        let p = to_os_path(path);
        self.inner
            .delete(&p)
            .await
            .map_err(from_object_store)
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> TeoDBResult<()> {
        let f = to_os_path(from);
        let t = to_os_path(to);
        self.inner
            .copy(&f, &t)
            .await
            .map_err(from_object_store)
    }

    async fn list(
        &self,
        prefix: &ObjectPath,
    ) -> TeoDBResult<Pin<Box<dyn futures::Stream<Item = TeoDBResult<ObjectMeta>> + Send>>> {
        let p = to_os_path(prefix);
        let stream = self.inner.list(Some(&p)).map(|r| match r {
            Ok(m) => Ok(ObjectMeta {
                path: ObjectPath::new(m.location.to_string()),
                size: m.size,
                etag: m.e_tag,
                last_modified: m.last_modified,
            }),
            Err(e) => Err(from_object_store(e)),
        });
        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;

    #[tokio::test]
    async fn put_get_roundtrip() {
        let backend = ObjectStoreBackend::in_memory();
        let path = ObjectPath::new("test/file.txt");
        let data = Bytes::from("hello world");

        let meta = backend.put(&path, data.clone()).await.unwrap();
        assert_eq!(meta.size, 11);

        let got = backend.get(&path).await.unwrap();
        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn get_range_works() {
        let backend = ObjectStoreBackend::in_memory();
        let path = ObjectPath::new("range.bin");
        let data = Bytes::from("0123456789");
        backend.put(&path, data).await.unwrap();

        let range = backend.get_range(&path, 2..5).await.unwrap();
        assert_eq!(range.as_ref(), b"234");
    }

    #[tokio::test]
    async fn head_returns_size() {
        let backend = ObjectStoreBackend::in_memory();
        let path = ObjectPath::new("head.bin");
        backend
            .put(&path, Bytes::from("abc"))
            .await
            .unwrap();

        let meta = backend.head(&path).await.unwrap();
        assert_eq!(meta.size, 3);
    }

    #[tokio::test]
    async fn delete_removes_object() {
        let backend = ObjectStoreBackend::in_memory();
        let path = ObjectPath::new("del.bin");
        backend
            .put(&path, Bytes::from("x"))
            .await
            .unwrap();

        backend.delete(&path).await.unwrap();
        let result = backend.head(&path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn copy_duplicates_object() {
        let backend = ObjectStoreBackend::in_memory();
        let src = ObjectPath::new("src.bin");
        let dst = ObjectPath::new("dst.bin");
        backend
            .put(&src, Bytes::from("data"))
            .await
            .unwrap();

        backend.copy(&src, &dst).await.unwrap();
        let got = backend.get(&dst).await.unwrap();
        assert_eq!(got, Bytes::from("data"));
    }

    #[tokio::test]
    async fn list_returns_objects() {
        let backend = ObjectStoreBackend::in_memory();
        backend
            .put(&ObjectPath::new("dir/a.bin"), Bytes::from("a"))
            .await
            .unwrap();
        backend
            .put(&ObjectPath::new("dir/b.bin"), Bytes::from("b"))
            .await
            .unwrap();

        let prefix = ObjectPath::new("dir");
        let stream = backend.list(&prefix).await.unwrap();
        let items: Vec<_> = stream.try_collect().await.unwrap();
        assert_eq!(items.len(), 2);
    }
}
