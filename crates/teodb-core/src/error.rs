use thiserror::Error;

use crate::ident::TableIdent;
use crate::write_protocol::{CommitId, WriterEpoch, WriterId};

pub type TeoDBResult<T> = std::result::Result<T, TeoDBError>;

pub type ErrorCode = &'static str;

/// Boxed dynamic error used to preserve an underlying cause in the error chain.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Error, Debug)]
pub enum TeoDBError {
    #[error("config: {0}")]
    Config(String),

    #[error("invalid argument: {field}: {message}")]
    InvalidArgument { field: String, message: String },

    #[error("not found: {resource}")]
    NotFound { resource: String },

    #[error("already exists: {resource}")]
    AlreadyExists { resource: String },

    #[error("conflict on {resource}: expected {expected}, found {actual}")]
    Conflict {
        resource: String,
        expected: String,
        actual: String,
    },

    #[error("commit state unknown for {table} (commit {commit_id}): {message}")]
    CommitStateUnknown {
        table: TableIdent,
        commit_id: CommitId,
        message: String,
    },

    #[error("flush blocked for {table} while resolving commit {commit_id}")]
    FlushBlocked { table: TableIdent, commit_id: CommitId },

    #[error(
        "stale writer epoch for {table}: writer {writer_id} requested epoch {request_epoch}, current epoch is {current_epoch}"
    )]
    StaleWriterEpoch {
        table: TableIdent,
        writer_id: WriterId,
        request_epoch: WriterEpoch,
        current_epoch: WriterEpoch,
    },

    #[error("writer checkpoint registry full for {table} (limit {limit})")]
    WriterRegistryFull { table: TableIdent, limit: usize },

    #[error("protocol metadata corruption for {table}: {message}")]
    MetadataCorruption { table: TableIdent, message: String },

    #[error("table incarnation mismatch for {table}: expected {expected}, found {actual}")]
    TableIncarnationMismatch {
        table: TableIdent,
        expected: uuid::Uuid,
        actual: uuid::Uuid,
    },

    #[error("write protocol violation for {table}: {message}")]
    WriteProtocol { table: TableIdent, message: String },

    #[error("unauthorized")]
    Unauthorized,

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("rate limited; retry after {retry_after_ms}ms")]
    RateLimited { retry_after_ms: u64 },

    #[error("backpressure: {0}")]
    Backpressure(String),

    #[error("encoded result exceeds {limit_bytes} byte limit; use Arrow Flight for large results")]
    ResultTooLarge { limit_bytes: u64 },

    #[error("unavailable: {0}")]
    Unavailable(String),

    #[error("object store: {0}")]
    ObjectStore(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("catalog: {0}")]
    Catalog(String),

    #[error("parquet: {0}")]
    Parquet(String),

    #[error("arrow: {0}")]
    Arrow(String),

    /// DataFusion infrastructure/operator failure outside a user query
    /// boundary (for example compaction execution).
    #[error("datafusion: {0}")]
    DataFusion(String),

    /// Failure while planning or executing a user-visible query.
    #[error("query execution: {0}")]
    QueryExecution(String),

    #[error("iceberg: {0}")]
    Iceberg(String),

    #[error("wal: {message}")]
    Wal {
        message: String,
        #[source]
        source: Option<BoxError>,
    },

    #[error("internal: {0}")]
    Internal(String),

    #[error("retryable external error: {0}")]
    ExternalRetryable(String),

    #[error("fatal external error: {0}")]
    ExternalFatal(String),
}

/// Portable gRPC status code enum. Keeps `tonic` out of `teodb-core`.
/// Boundary crates map this to `tonic::Code`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrpcCode {
    Ok,
    Cancelled,
    InvalidArgument,
    NotFound,
    AlreadyExists,
    PermissionDenied,
    Unauthenticated,
    ResourceExhausted,
    FailedPrecondition,
    Aborted,
    Unimplemented,
    Internal,
    Unavailable,
    DataLoss,
}

impl TeoDBError {
    /// WAL error carrying only a message (no preserved source).
    pub fn wal(message: impl Into<String>) -> Self {
        Self::Wal {
            message: message.into(),
            source: None,
        }
    }

    /// WAL error preserving the underlying cause in the error chain.
    pub fn wal_source(message: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::Wal {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    pub fn code(&self) -> ErrorCode {
        match self {
            Self::Config(_) => "Config",
            Self::InvalidArgument { .. } => "InvalidArgument",
            Self::NotFound { .. } => "NotFound",
            Self::AlreadyExists { .. } => "AlreadyExists",
            Self::Conflict { .. } => "Conflict",
            Self::CommitStateUnknown { .. } => "CommitStateUnknown",
            Self::FlushBlocked { .. } => "FlushBlocked",
            Self::StaleWriterEpoch { .. } => "StaleWriterEpoch",
            Self::WriterRegistryFull { .. } => "WriterRegistryFull",
            Self::MetadataCorruption { .. } => "MetadataCorruption",
            Self::TableIncarnationMismatch { .. } => "TableIncarnationMismatch",
            Self::WriteProtocol { .. } => "WriteProtocol",
            Self::Unauthorized => "Unauthorized",
            Self::Forbidden(_) => "Forbidden",
            Self::RateLimited { .. } => "RateLimited",
            Self::Backpressure(_) => "Backpressure",
            Self::ResultTooLarge { .. } => "ResultTooLarge",
            Self::Unavailable(_) => "Unavailable",
            Self::ObjectStore(_) => "ObjectStore",
            Self::Catalog(_) => "Catalog",
            Self::Parquet(_) => "Parquet",
            Self::Arrow(_) => "Arrow",
            Self::DataFusion(_) => "DataFusion",
            Self::QueryExecution(_) => "QueryExecution",
            Self::Iceberg(_) => "Iceberg",
            Self::Wal { .. } => "Wal",
            Self::Internal(_) => "Internal",
            Self::ExternalRetryable(_) => "ExternalRetryable",
            Self::ExternalFatal(_) => "ExternalFatal",
        }
    }

    pub fn http_status(&self) -> u16 {
        match self {
            Self::InvalidArgument { .. } | Self::Config(_) => 400,
            Self::Unauthorized => 401,
            Self::Forbidden(_) => 403,
            Self::NotFound { .. } => 404,
            Self::AlreadyExists { .. }
            | Self::Conflict { .. }
            | Self::StaleWriterEpoch { .. }
            | Self::WriterRegistryFull { .. }
            | Self::TableIncarnationMismatch { .. }
            | Self::WriteProtocol { .. } => 409,
            Self::RateLimited { .. } => 429,
            Self::ResultTooLarge { .. } => 413,
            Self::Backpressure(_)
            | Self::Unavailable(_)
            | Self::ExternalRetryable(_)
            | Self::ObjectStore(_)
            | Self::CommitStateUnknown { .. }
            | Self::FlushBlocked { .. } => 503,
            Self::Parquet(_)
            | Self::Arrow(_)
            | Self::DataFusion(_)
            | Self::QueryExecution(_)
            | Self::Catalog(_)
            | Self::Iceberg(_)
            | Self::Wal { .. }
            | Self::Internal(_)
            | Self::MetadataCorruption { .. }
            | Self::ExternalFatal(_) => 500,
        }
    }

    pub fn grpc_code(&self) -> GrpcCode {
        match self {
            Self::InvalidArgument { .. } | Self::Config(_) => GrpcCode::InvalidArgument,
            Self::Unauthorized => GrpcCode::Unauthenticated,
            Self::Forbidden(_) => GrpcCode::PermissionDenied,
            Self::NotFound { .. } => GrpcCode::NotFound,
            Self::AlreadyExists { .. } => GrpcCode::AlreadyExists,
            Self::Conflict { .. } => GrpcCode::Aborted,
            Self::StaleWriterEpoch { .. }
            | Self::WriterRegistryFull { .. }
            | Self::TableIncarnationMismatch { .. }
            | Self::WriteProtocol { .. } => GrpcCode::FailedPrecondition,
            Self::RateLimited { .. } | Self::Backpressure(_) | Self::ResultTooLarge { .. } => {
                GrpcCode::ResourceExhausted
            }
            Self::Unavailable(_)
            | Self::ExternalRetryable(_)
            | Self::ObjectStore(_)
            | Self::CommitStateUnknown { .. }
            | Self::FlushBlocked { .. } => GrpcCode::Unavailable,
            Self::Parquet(_)
            | Self::Arrow(_)
            | Self::DataFusion(_)
            | Self::QueryExecution(_)
            | Self::Catalog(_)
            | Self::Iceberg(_)
            | Self::Wal { .. }
            | Self::Internal(_)
            | Self::MetadataCorruption { .. }
            | Self::ExternalFatal(_) => GrpcCode::Internal,
        }
    }

    /// Returns `true` if the caller should retry after backoff.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Conflict { .. }
                | Self::CommitStateUnknown { .. }
                | Self::FlushBlocked { .. }
                | Self::RateLimited { .. }
                | Self::Backpressure(_)
                | Self::Unavailable(_)
                | Self::ObjectStore(_)
                | Self::ExternalRetryable(_)
        )
    }

    pub fn retry_after_ms(&self) -> Option<u64> {
        match self {
            Self::RateLimited { retry_after_ms } => Some(*retry_after_ms),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_variants() -> Vec<TeoDBError> {
        vec![
            TeoDBError::Config("bad".into()),
            TeoDBError::InvalidArgument {
                field: "x".into(),
                message: "bad".into(),
            },
            TeoDBError::NotFound { resource: "t".into() },
            TeoDBError::AlreadyExists { resource: "t".into() },
            TeoDBError::Conflict {
                resource: "t".into(),
                expected: "1".into(),
                actual: "2".into(),
            },
            TeoDBError::Unauthorized,
            TeoDBError::Forbidden("denied".into()),
            TeoDBError::RateLimited { retry_after_ms: 100 },
            TeoDBError::Backpressure("full".into()),
            TeoDBError::ResultTooLarge { limit_bytes: 1024 },
            TeoDBError::Unavailable("down".into()),
            TeoDBError::ObjectStore(Box::<std::io::Error>::new(std::io::ErrorKind::Other.into())),
            TeoDBError::Catalog("c".into()),
            TeoDBError::Parquet("p".into()),
            TeoDBError::Arrow("a".into()),
            TeoDBError::DataFusion("d".into()),
            TeoDBError::QueryExecution("q".into()),
            TeoDBError::Iceberg("i".into()),
            TeoDBError::wal("w"),
            TeoDBError::Internal("i".into()),
            TeoDBError::ExternalRetryable("r".into()),
            TeoDBError::ExternalFatal("f".into()),
        ]
    }

    #[test]
    fn http_status_in_valid_set() {
        let valid = [400, 401, 403, 404, 409, 413, 429, 500, 503];
        for e in all_variants() {
            assert!(
                valid.contains(&e.http_status()),
                "bad status for {}: {}",
                e.code(),
                e.http_status()
            );
        }
    }

    #[test]
    fn code_is_non_empty() {
        for e in all_variants() {
            assert!(!e.code().is_empty());
        }
    }

    #[test]
    fn retryable_consistency() {
        for e in all_variants() {
            let retryable = e.is_retryable();
            let status = e.http_status();
            // Retryable errors should map to 429, 503, or 409
            if retryable {
                assert!(
                    status == 429 || status == 503 || status == 409,
                    "{} is retryable but has status {}",
                    e.code(),
                    status
                );
            }
        }
    }

    #[test]
    fn rate_limited_retry_after() {
        let e = TeoDBError::RateLimited { retry_after_ms: 42 };
        assert_eq!(e.retry_after_ms(), Some(42));

        let e2 = TeoDBError::Internal("x".into());
        assert_eq!(e2.retry_after_ms(), None);
    }

    #[test]
    fn writer_registry_full_is_a_non_retryable_capacity_failure() {
        let error = TeoDBError::WriterRegistryFull {
            table: TableIdent::new("analytics", "events"),
            limit: 32,
        };

        assert_eq!(error.http_status(), 409);
        assert_eq!(error.grpc_code(), GrpcCode::FailedPrecondition);
        assert!(!error.is_retryable());
    }

    #[test]
    fn display_includes_context() {
        let e = TeoDBError::Conflict {
            resource: "table sales.orders".into(),
            expected: "17".into(),
            actual: "19".into(),
        };
        let msg = e.to_string();
        assert!(msg.contains("sales.orders"));
        assert!(msg.contains("17"));
        assert!(msg.contains("19"));
    }
}
