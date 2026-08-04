//! Distributed-crate error types.

use teodb_core::error::TeoDBError;

/// Convert a DataFusion error into a TeoDBError.
pub(crate) fn from_datafusion(e: datafusion::error::DataFusionError) -> TeoDBError {
    TeoDBError::DataFusion(e.to_string())
}
