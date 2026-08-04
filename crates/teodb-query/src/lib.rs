//! `teodb-query` — DataFusion query integration for TeoDB.
//!
//! This crate bridges the TeoDB domain model with DataFusion's query engine.
//! It provides:
//!
//! - [`QueryEngine`]: Streaming query abstraction for local and distributed execution.
//! - [`TeoTableProvider`]: A `TableProvider` that exposes Iceberg-format tables
//!   to DataFusion with partition and statistics pruning.
//! - [`DataFusionSessionFactory`]: Produces per-request `SessionContext` instances
//!   with the correct runtime environment, UDFs, and catalog bindings.
//! - Pruning functions for partition-level and statistics-level file elimination.
//! - Scalar conversions between `TeoScalar` and `datafusion_common::ScalarValue`.
//!
//! Concrete catalog and storage implementations arrive as `Arc<dyn Trait>`
//! from the caller.

mod conversion;
pub mod ddl;
mod engine;
mod error;
mod provider;
mod pruning;
mod session;
mod udf;

pub use conversion::{
    column_meta_to_arrow_field, field_id_from_arrow_field, scalar_value_to_teo_scalar, schema_to_arrow,
    teo_scalar_to_scalar_value, teo_to_arrow_type,
};
pub use engine::{QueryEngine, QueryHandle, QueryRequest, QueryResultStream};
pub use provider::{
    DeletePositions, MetadataMetricsSnapshot, PinnedScanTableProvider, PositionDeleteFilterExec, TeoCatalogProvider,
    TeoSchemaProvider, TeoTableProvider,
};
pub use session::{
    DataFusionRuntime, DataFusionRuntimeConfig, DataFusionSessionConfig, DataFusionSessionFactory,
    ObjectStoreRegistration,
};
