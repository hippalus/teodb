//! `teodb-storage` — Storage backends, WAL, Parquet writer, and cache for TeoDB.
//!
//! This crate implements the `Storage` and `StorageFactory` traits defined in
//! `teodb-core`, wrapping the `object_store` crate's backends. It also provides
//! the write-ahead log, Parquet writer with sorted output and typed statistics
//! extraction, and an NVMe-backed caching layer.

mod backends;
pub mod cache;
mod convert;
mod error;
mod factory;
pub mod parquet;
pub mod wal;

pub use backends::ObjectStoreBackend;
pub use convert::{
    arrow_to_teo_data_type, arrow_to_teo_scalar, schema_to_arrow, teo_data_type_to_arrow, teo_scalar_to_arrow_scalar,
};
pub use error::{from_arrow, from_object_store, from_parquet};
pub use factory::DefaultStorageFactory;
