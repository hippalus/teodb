//! Statistics-level file pruning using DataFusion's `PruningPredicate`.
//!
//! For each surviving file, the file's typed lower/upper bounds and null
//! counts are fed to DataFusion's pruning infrastructure. A file is dropped
//! only if the predicate proves the result is empty (Invariant I4).

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use arrow::datatypes::SchemaRef;
use datafusion::logical_expr::Expr;
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};
use datafusion_common::ScalarValue;

use teodb_core::file::DataFile;

use crate::conversion::{field_id_from_arrow_field, teo_scalar_to_scalar_value};

/// Prune data files using their column-level statistics.
///
/// Returns files that the predicate cannot prove empty. All files are kept
/// if there are no filters or if the predicate cannot be constructed.
pub fn statistics_prune(
    files: &[DataFile],
    filters: &[Expr],
    schema: &SchemaRef,
    _state: &dyn datafusion::catalog::Session,
) -> datafusion::error::Result<Vec<DataFile>> {
    if files.is_empty() || filters.is_empty() {
        return Ok(files.to_vec());
    }

    // AND-combine all filters into a single expression.
    // Safety: `filters` is checked non-empty above, so `reduce` always returns `Some`.
    let Some(combined) = filters.iter().cloned().reduce(|a, b| a.and(b)) else {
        return Ok(files.to_vec());
    };

    let df_schema = datafusion_common::DFSchema::try_from(schema.as_ref().clone())?;
    let props = datafusion::execution::context::ExecutionProps::new();
    let physical_expr = match datafusion_physical_expr::create_physical_expr(&combined, &df_schema, &props) {
        Ok(expr) => expr,
        Err(e) => {
            // Cannot build a physical expression — keep all files, but make the
            // skipped pruning observable instead of a silent full scan.
            tracing::debug!(error = %e, "statistics pruning skipped: physical expr build failed; keeping all files");
            return Ok(files.to_vec());
        }
    };

    let predicate = match PruningPredicate::try_new(physical_expr, schema.clone()) {
        Ok(p) => p,
        Err(e) => {
            // Cannot build pruning predicate — keep all files (observable).
            tracing::debug!(error = %e, "statistics pruning skipped: predicate build failed; keeping all files");
            return Ok(files.to_vec());
        }
    };

    let stats_provider = FileSetStatistics::new(files, schema);
    let keep_mask = predicate.prune(&stats_provider)?;

    let kept = files
        .iter()
        .zip(keep_mask.iter())
        .filter(|(_, keep)| **keep)
        .map(|(f, _)| f.clone())
        .collect();
    Ok(kept)
}

/// Adapts a slice of `DataFile` into DataFusion's `PruningStatistics` trait.
struct FileSetStatistics<'a> {
    files: &'a [DataFile],
    schema: &'a SchemaRef,
}

impl<'a> FileSetStatistics<'a> {
    fn new(files: &'a [DataFile], schema: &'a SchemaRef) -> Self {
        Self { files, schema }
    }

    /// Resolve a column reference to a stable field ID.
    fn resolve_field_id(&self, column: &datafusion_common::Column) -> Option<i32> {
        self.schema
            .field_with_name(column.name())
            .ok()
            .and_then(field_id_from_arrow_field)
    }
}

impl PruningStatistics for FileSetStatistics<'_> {
    fn min_values(&self, column: &datafusion_common::Column) -> Option<ArrayRef> {
        let field_id = self.resolve_field_id(column)?;
        let field = self.schema.field_with_name(column.name()).ok()?;
        let dt = field.data_type();

        let scalars: Vec<ScalarValue> = self
            .files
            .iter()
            .map(|f| {
                f.lower_bounds
                    .get(&field_id)
                    .and_then(|ts| teo_scalar_to_scalar_value(ts).ok())
                    .unwrap_or_else(|| ScalarValue::try_from(dt).unwrap_or(ScalarValue::Null))
            })
            .collect();

        ScalarValue::iter_to_array(scalars)
            .ok()
            .map(|a| a as ArrayRef)
    }

    fn max_values(&self, column: &datafusion_common::Column) -> Option<ArrayRef> {
        let field_id = self.resolve_field_id(column)?;
        let field = self.schema.field_with_name(column.name()).ok()?;
        let dt = field.data_type();

        let scalars: Vec<ScalarValue> = self
            .files
            .iter()
            .map(|f| {
                f.upper_bounds
                    .get(&field_id)
                    .and_then(|ts| teo_scalar_to_scalar_value(ts).ok())
                    .unwrap_or_else(|| ScalarValue::try_from(dt).unwrap_or(ScalarValue::Null))
            })
            .collect();

        ScalarValue::iter_to_array(scalars)
            .ok()
            .map(|a| a as ArrayRef)
    }

    fn num_containers(&self) -> usize {
        self.files.len()
    }

    fn null_counts(&self, column: &datafusion_common::Column) -> Option<ArrayRef> {
        let field_id = self.resolve_field_id(column)?;

        let counts: Vec<u64> = self
            .files
            .iter()
            .map(|f| {
                f.null_value_counts
                    .get(&field_id)
                    .copied()
                    .unwrap_or(0)
            })
            .collect();

        Some(Arc::new(UInt64Array::from(counts)) as ArrayRef)
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        let counts: Vec<u64> = self
            .files
            .iter()
            .map(|f| f.record_count)
            .collect();
        Some(Arc::new(UInt64Array::from(counts)) as ArrayRef)
    }

    fn contained(
        &self,
        column: &datafusion_common::Column,
        values: &std::collections::HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        // Approximate bloom filter: for each file, check if any of the queried
        // values falls within the file's [min, max] range. This is conservative
        // (Invariant I4): we only prune if ALL values are provably outside the range.
        let field_id = self.resolve_field_id(column)?;

        let results: Vec<bool> = self
            .files
            .iter()
            .map(|f| {
                let lo = f
                    .lower_bounds
                    .get(&field_id)
                    .and_then(|s| teo_scalar_to_scalar_value(s).ok());
                let hi = f
                    .upper_bounds
                    .get(&field_id)
                    .and_then(|s| teo_scalar_to_scalar_value(s).ok());
                match (lo, hi) {
                    (Some(lo), Some(hi)) => {
                        // File might contain a value if any value is in [lo, hi]
                        values.iter().any(|v| v >= &lo && v <= &hi)
                    }
                    _ => true, // No bounds — conservatively keep the file
                }
            })
            .collect();

        Some(BooleanArray::from(results))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use teodb_core::file::{DataContent, FileFormat};
    use teodb_core::location::{ObjectLocation, StorageScheme};
    use teodb_core::scalar::TeoScalar;
    use teodb_core::schema::{ColumnMeta, SchemaDefinition, TeoDataType};

    fn make_schema() -> SchemaRef {
        crate::conversion::schema_to_arrow(&SchemaDefinition {
            schema_id: 0,
            columns: vec![ColumnMeta {
                id: 1,
                name: "x".into(),
                data_type: TeoDataType::Int64,
                nullable: false,
                doc: None,
            }],
            identifier_field_ids: vec![1],
        })
    }

    fn make_file(lo: i64, hi: i64) -> DataFile {
        let mut lower_bounds = HashMap::new();
        lower_bounds.insert(1, TeoScalar::Int64(lo));
        let mut upper_bounds = HashMap::new();
        upper_bounds.insert(1, TeoScalar::Int64(hi));

        DataFile {
            content: DataContent::Data,
            path: ObjectLocation {
                scheme: StorageScheme::Local,
                bucket: None,
                key: format!("data/{lo}_{hi}.parquet"),
            },
            format: FileFormat::Parquet,
            partition_spec_id: 0,
            sort_order_id: None,
            schema_id: 0,
            partition_values: HashMap::new(),
            record_count: 100,
            file_size_bytes: 1024,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds,
            upper_bounds,
            split_offsets: vec![],
            equality_ids: vec![],
            key_metadata: None,
        }
    }

    #[test]
    fn prune_by_statistics() {
        let schema = make_schema();
        let files = vec![
            make_file(0, 50),    // x in [0,50]
            make_file(100, 200), // x in [100,200]
            make_file(300, 400), // x in [300,400]
        ];

        // x > 150 should keep only files [100,200] and [300,400]
        let filter = datafusion::logical_expr::col("x").gt(datafusion::logical_expr::lit(150i64));

        let session = datafusion::execution::SessionStateBuilder::new().build();
        let result = statistics_prune(&files, &[filter], &schema, &session).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].path.key, "data/100_200.parquet");
        assert_eq!(result[1].path.key, "data/300_400.parquet");
    }

    #[test]
    fn no_filters_keeps_all() {
        let schema = make_schema();
        let files = vec![make_file(0, 50), make_file(100, 200)];
        let session = datafusion::execution::SessionStateBuilder::new().build();
        let result = statistics_prune(&files, &[], &schema, &session).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn contained_prunes_by_value_set() {
        let schema = make_schema();
        let files = vec![
            make_file(0, 50),    // x in [0, 50]
            make_file(100, 200), // x in [100, 200]
            make_file(300, 400), // x in [300, 400]
        ];

        let stats = FileSetStatistics::new(&files, &schema);
        let col = datafusion_common::Column::from_name("x");

        // Query for value 150 — only file [100,200] should contain it
        let mut values = std::collections::HashSet::new();
        values.insert(ScalarValue::Int64(Some(150)));
        let result = stats.contained(&col, &values).unwrap();
        assert_eq!(result.len(), 3);
        assert!(!result.value(0)); // [0,50] does not contain 150
        assert!(result.value(1)); // [100,200] contains 150
        assert!(!result.value(2)); // [300,400] does not contain 150
    }
}
