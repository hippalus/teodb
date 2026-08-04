//! `teodb-core` — Domain types, identifiers, schema, and errors for TeoDB.
//!
//! This crate is the foundation of the TeoDB workspace. It defines every type
//! that crosses crate boundaries and the trait surfaces that downstream crates
//! implement. It has **no** I/O, no storage, and no networking dependencies.

pub mod error;
pub mod file;
pub mod ident;
pub mod lifecycle;
pub mod location;
pub mod problem;
pub mod query_id;
pub mod scalar;
pub mod scan_descriptor;
pub mod schema;
pub mod snapshot_pin;
pub mod snapshot_retention;
pub mod table;
pub mod traits;
pub mod validation;
pub mod write_protocol;

// Re-export the most commonly used types at the crate root for convenience.
pub use error::{ErrorCode, GrpcCode, TeoDBError, TeoDBResult};
pub use file::{DataContent, DataFile, FileFormat, Snapshot, SnapshotOperation, TableMetadata};
pub use ident::{FieldId, Generation, SequenceNumber, SnapshotId, TableIdent, TableUuid};
pub use lifecycle::{RoleLifecycle, RoleState};
pub use location::{LocationError, ObjectLocation, ObjectPath, StorageScheme};
pub use problem::{Link, ProblemDetail};
pub use query_id::QueryId;
pub use scalar::{ColumnBounds, TeoScalar};
pub use scan_descriptor::SnapshotScanDescriptor;
pub use schema::{
    ColumnMeta, NullOrder, PartitionField, PartitionSpec, PartitionTransform, SchemaDefinition, SortDirection,
    SortField, SortOrder, TeoDataType, UnboundPartitionField, UnboundPartitionSpec,
};
pub use snapshot_pin::{ActiveSnapshotRegistry, InMemorySnapshotRegistry, SnapshotPin};
pub use snapshot_retention::SnapshotRetention;
pub use table::{
    CreateTableRequestBuilder, PartitionFieldSpec, PartitionSpecBuilder, PartitionTransformSpec, TableDefinition,
};
pub use traits::query_engine::{QueryInfo, QueryStatus};
pub use write_protocol::{
    AppendCommitIdentity, ClusterId, CommitId, GenerationRange, NodeId, ResolvedIdentity, WalTableKey, WritePosition,
    WriterCheckpoint, WriterEpoch, WriterId, WriterSlot,
};
