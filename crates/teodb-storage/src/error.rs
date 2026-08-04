//! Error conversion helpers for external crate errors into `TeoDBError`.

use teodb_core::error::TeoDBError;

/// Convert an `object_store::Error` to `TeoDBError`.
pub fn from_object_store(e: object_store::Error) -> TeoDBError {
    TeoDBError::ObjectStore(Box::new(e))
}

/// Convert a `parquet::errors::ParquetError` to `TeoDBError`.
pub fn from_parquet(e: parquet::errors::ParquetError) -> TeoDBError {
    TeoDBError::Parquet(e.to_string())
}

/// Convert an `arrow::error::ArrowError` to `TeoDBError`.
pub fn from_arrow(e: arrow::error::ArrowError) -> TeoDBError {
    TeoDBError::Arrow(e.to_string())
}
