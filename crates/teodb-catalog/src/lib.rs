//! Iceberg catalog adapter for TeoDB.
//!
//! This crate implements the `Catalog` trait from `teodb-core` by wrapping
//! the `iceberg` crate's REST catalog implementation. All Iceberg-specific
//! types and conversions are confined to this crate — the rest of the system
//! sees only TeoDB domain types.

mod adapter;
mod convert;
mod error;
mod observer;
mod retry;

pub use adapter::{IcebergCatalogAdapter, IcebergCatalogConfig, IcebergCredentials};
pub use convert::{apply_partition_transform_to_scalar, iceberg_partition_path};
pub use observer::{CatalogCommitOutcome, CatalogObserver, CatalogStatusCheckOutcome};
pub use retry::RetryConfig;
