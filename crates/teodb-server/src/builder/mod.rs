//! Production component builders owned by the server composition root.

mod catalog_builder;
mod storage_builder;

pub(crate) use catalog_builder::build_catalog;
pub(crate) use storage_builder::{S3Settings, StorageComponents};
