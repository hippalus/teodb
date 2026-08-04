//! Iceberg catalog adapter.

mod append_attempt_guard;
mod catalog;
mod commit;
mod commit_error;
mod commit_metadata;
mod config;
mod idents;
mod manifests;

pub use catalog::IcebergCatalogAdapter;
pub use config::{IcebergCatalogConfig, IcebergCredentials};
