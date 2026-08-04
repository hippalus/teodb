//! Distributed execution, partition assignment, and compaction for TeoDB.
//!
//! This crate provides:
//! - **Query engine**: [`BallistaQueryEngine`] — the single query execution path
//!   for both standalone and distributed deployments.
//! - **Partition assignment**: Sort-aware file-to-executor mapping that preserves
//!   global sort order across the executor set.
//! - **Compaction**: Merges small Parquet files into larger, sorted files with
//!   tight statistics, using optimistic concurrency against the Iceberg catalog.
//! - **Selection**: Policy for choosing which files to compact.
//! - **Scheduler/Executor**: Trait-based abstractions for distributed query
//!   execution (Ballista integration point).

pub mod ballista;
pub mod cluster_topology;
mod codec;
pub mod compactor;
pub mod coordination;
mod engine;
mod error;
pub mod orphan;
pub mod scheduler_api;
pub mod selection;
pub mod snapshot_registry;

pub use codec::TeoLogicalExtensionCodec;
pub use engine::{BallistaMode, BallistaQueryEngine, BallistaQueryEngineBuilder, EngineEventObserver};
