//! Error conversion helpers between DataFusion and TeoDB error types.

use datafusion::error::DataFusionError;
use teodb_core::error::TeoDBError;

/// Convert a `TeoDBError` into a `DataFusionError` for use in DataFusion callbacks.
pub fn teodb_to_df(err: TeoDBError) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wraps_teodb_error_as_external_datafusion_error() {
        let teo = TeoDBError::Internal("test".into());
        let df = teodb_to_df(teo);
        assert!(matches!(df, DataFusionError::External(_)));
    }
}
