//! DataFusion catalog and table providers.

mod catalog;
mod delete;
mod metadata_cache;
mod pinned;
mod scan;
mod scan_builder;
mod table;
mod table_loader;

pub use catalog::{TeoCatalogProvider, TeoSchemaProvider};
pub use delete::{DeletePositions, PositionDeleteFilterExec};
pub use metadata_cache::MetadataMetricsSnapshot;
pub use pinned::PinnedScanTableProvider;
pub use table::TeoTableProvider;

#[cfg(test)]
use scan_builder::split_by_deletes;

#[cfg(test)]
mod catalog_tests;
#[cfg(test)]
mod table_tests;
