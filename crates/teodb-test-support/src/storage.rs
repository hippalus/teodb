//! Storage-factory test doubles.

use std::sync::Arc;

use async_trait::async_trait;
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::location::{ObjectLocation, ObjectPath};
use teodb_core::traits::storage::{Storage, StorageFactory};
use teodb_storage::ObjectStoreBackend;

/// A [`StorageFactory`] that resolves *every* location to one shared backend,
/// using the location's key as the object path.
pub struct SingleBackendFactory {
    backend: Arc<dyn Storage>,
}

#[async_trait]
impl StorageFactory for SingleBackendFactory {
    async fn resolve(&self, loc: &ObjectLocation) -> TeoDBResult<(Arc<dyn Storage>, ObjectPath)> {
        Ok((self.backend.clone(), ObjectPath::new(loc.key.clone())))
    }
}

/// Build a [`SingleBackendFactory`] over `backend`.
pub fn single_backend_factory(backend: Arc<dyn Storage>) -> Arc<dyn StorageFactory> {
    Arc::new(SingleBackendFactory { backend })
}

/// Build an object-store-backed in-memory storage backend.
pub fn in_memory_backend() -> Arc<ObjectStoreBackend> {
    Arc::new(ObjectStoreBackend::new(Arc::new(object_store::memory::InMemory::new())))
}

/// A [`StorageFactory`] whose `resolve` always fails. For tests that must wire
/// a factory they never actually call (it exists only to satisfy a constructor).
#[derive(Debug, Default, Clone, Copy)]
pub struct StubStorageFactory;

#[async_trait]
impl StorageFactory for StubStorageFactory {
    async fn resolve(&self, _loc: &ObjectLocation) -> TeoDBResult<(Arc<dyn Storage>, ObjectPath)> {
        Err(TeoDBError::Internal(
            "StubStorageFactory::resolve called, but this test did not configure storage".into(),
        ))
    }
}

/// Build a [`StubStorageFactory`].
pub fn stub_storage_factory() -> Arc<dyn StorageFactory> {
    Arc::new(StubStorageFactory)
}
