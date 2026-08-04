//! Shared test doubles and fixtures for crate and integration tests.

mod catalog;
mod fault_storage;
mod fixtures;
#[cfg(feature = "http-app")]
pub mod server;
mod storage;

pub use catalog::{MockAppendOutcome, MockCatalog, MockCatalogBuilder, MockCommitStatus, SnapshotFiles};
pub use fault_storage::{FaultInjectingStorage, StorageFault, StorageFaultKind, StorageOperation};
pub use fixtures::{table_metadata, table_metadata_with_snapshot};
#[cfg(feature = "http-app")]
pub mod node {
    pub use crate::server::{TestNode, TestNodeBuilder};
}
#[cfg(feature = "http-app")]
pub use server::{StubQueryEngine, TestApp, TestAppBuilder, TestNode, TestNodeBuilder};
pub use storage::{
    SingleBackendFactory, StubStorageFactory, in_memory_backend, single_backend_factory, stub_storage_factory,
};
