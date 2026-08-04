use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;

use crate::error::TeoDBResult;
use crate::location::{ObjectLocation, ObjectPath};

/// Metadata about an object as returned by the underlying store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub path: ObjectPath,
    pub size: u64,
    pub etag: Option<String>,
    pub last_modified: chrono::DateTime<chrono::Utc>,
}

/// Byte-level storage abstraction. Each `Storage` instance is scoped to
/// a single bucket / container. The `StorageFactory` resolves an
/// `ObjectLocation` to a `(Storage, ObjectPath)` pair.
///
/// Concrete implementations live in `teodb-storage` and additionally expose
/// `as_object_store() -> Arc<dyn object_store::ObjectStore>` for downstream
/// libraries that require `ObjectStore` directly (Parquet async reader,
/// DataFusion's `ParquetSource`). That escape hatch is NOT part of this
/// trait because `teodb-core` must not depend on `object_store`.
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    async fn get(&self, path: &ObjectPath) -> TeoDBResult<Bytes>;
    async fn get_range(&self, path: &ObjectPath, range: Range<u64>) -> TeoDBResult<Bytes>;
    async fn head(&self, path: &ObjectPath) -> TeoDBResult<ObjectMeta>;
    async fn put(&self, path: &ObjectPath, bytes: Bytes) -> TeoDBResult<ObjectMeta>;
    async fn delete(&self, path: &ObjectPath) -> TeoDBResult<()>;
    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> TeoDBResult<()>;
    async fn list(
        &self,
        prefix: &ObjectPath,
    ) -> TeoDBResult<Pin<Box<dyn Stream<Item = TeoDBResult<ObjectMeta>> + Send>>>;
}

/// Resolves an `ObjectLocation` (scheme + bucket + key) to a per-bucket
/// `Storage` handle and the store-relative `ObjectPath`. This is the
/// *only* place URI parsing and bucket routing happens.
#[async_trait]
pub trait StorageFactory: Send + Sync + 'static {
    async fn resolve(&self, loc: &ObjectLocation) -> TeoDBResult<(Arc<dyn Storage>, ObjectPath)>;
}
