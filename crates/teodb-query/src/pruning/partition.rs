//! Partition-level file pruning.
//!
//! For each data file, constructs a one-row `RecordBatch` from its typed
//! partition values, evaluates the predicate, and drops the file if the
//! result is exactly `false`. Files whose result is `true` or `null` are
//! kept — conservatism is the rule (Invariant I4).

use std::sync::Arc;

use arrow::array::Array as _;
use arrow::array::RecordBatch;
use arrow::datatypes::{Field, Schema};
use datafusion::logical_expr::Expr;
use datafusion_common::ScalarValue;
use datafusion_common::tree_node::TreeNode;

use teodb_core::file::DataFile;
use teodb_core::schema::PartitionSpec;

use crate::conversion::{field_id_from_arrow_field, teo_scalar_to_scalar_value};

/// Prune data files by partition values.
///
/// Returns only files whose partition values are compatible with all `filters`.
/// A file is kept if any filter references a column not in the partition spec,
/// or if the evaluation result is `true` or `null`.
pub fn partition_prune(
    files: &[DataFile],
    filters: &[Expr],
    partition_spec: &PartitionSpec,
    table_schema: &arrow::datatypes::SchemaRef,
) -> datafusion::error::Result<Vec<DataFile>> {
    if partition_spec.fields.is_empty() || filters.is_empty() {
        return Ok(files.to_vec());
    }

    // Only identity-transform fields store the raw column value. Evaluating
    // a column predicate against a transformed value (bucket number, truncated
    // prefix, day ordinal) would prove the wrong thing and prune live files.
    let identity_field_ids: std::collections::HashSet<i32> = partition_spec
        .fields
        .iter()
        .filter(|f| f.transform == teodb_core::schema::PartitionTransform::Identity)
        .map(|f| f.source_id)
        .collect();

    if identity_field_ids.is_empty() {
        return Ok(files.to_vec());
    }

    // Filters arrive as independent conjuncts, so each one whose columns all
    // map to identity partition fields can prune on its own — a file ruled
    // out by any conjunct is ruled out for the query. Filters touching other
    // columns are simply not usable here (statistics pruning sees them).
    let usable_filters: Vec<&Expr> = filters
        .iter()
        .filter(|f| {
            let cols = extract_filter_columns(f);
            !cols.is_empty()
                && cols.iter().all(|name| {
                    table_schema
                        .field_with_name(name)
                        .ok()
                        .and_then(field_id_from_arrow_field)
                        .is_some_and(|id| identity_field_ids.contains(&id))
                })
        })
        .collect();

    if usable_filters.is_empty() {
        return Ok(files.to_vec());
    }

    // Build a partition-only schema for evaluation. Each evaluated column pairs
    // the source column's Arrow field — what the predicate references — with the
    // partition field id the value is actually stored under, which is a distinct
    // id from the source column's.
    let mut partition_fields: Vec<Field> = Vec::new();
    let mut value_field_ids: Vec<teodb_core::FieldId> = Vec::new();
    for spec_field in &partition_spec.fields {
        if spec_field.transform != teodb_core::schema::PartitionTransform::Identity {
            continue;
        }
        if let Some(field) = table_schema
            .fields()
            .iter()
            .find(|f| field_id_from_arrow_field(f) == Some(spec_field.source_id))
        {
            // Nullable regardless of the column's declared nullability: this
            // synthetic batch carries one partition value per file, and a value
            // that is absent or unreadable is represented as null.
            partition_fields.push(field.as_ref().clone().with_nullable(true));
            value_field_ids.push(spec_field.field_id);
        }
    }

    if partition_fields.is_empty() {
        return Ok(files.to_vec());
    }

    let partition_schema = Arc::new(Schema::new(partition_fields));

    // Pre-compile filter expressions once for reuse across all files.
    let props = datafusion::execution::context::ExecutionProps::new();
    let df_schema = datafusion_common::DFSchema::try_from(partition_schema.as_ref().clone())?;
    let compiled_filters: Vec<Arc<dyn datafusion_physical_expr::PhysicalExpr>> = usable_filters
        .iter()
        .filter_map(|f| datafusion_physical_expr::create_physical_expr(f, &df_schema, &props).ok())
        .collect();

    if compiled_filters.is_empty() {
        return Ok(files.to_vec());
    }

    let mut kept = Vec::with_capacity(files.len());
    let mut spec_mismatch = 0usize;
    for file in files {
        // A file written under a different spec may lack values for the
        // current spec's fields — its values cannot be interpreted here, so it
        // is conservatively kept (never pruned by partition).
        if file.partition_spec_id != partition_spec.spec_id {
            spec_mismatch += 1;
            kept.push(file.clone());
        } else if should_keep_file(file, &compiled_filters, &partition_schema, &value_field_ids)? {
            kept.push(file.clone());
        }
    }
    if spec_mismatch > 0 {
        // Observable: partition pruning is degraded for these files (e.g. after
        // a partition-spec evolution) rather than silently scanning everything.
        tracing::debug!(
            spec_mismatch,
            current_spec = partition_spec.spec_id,
            "partition pruning skipped for files written under a different partition spec"
        );
    }
    Ok(kept)
}

/// Evaluate pre-compiled filter expressions against a single file's partition
/// values. `value_field_ids` is positionally aligned with `partition_schema`.
fn should_keep_file(
    file: &DataFile,
    compiled_filters: &[Arc<dyn datafusion_physical_expr::PhysicalExpr>],
    partition_schema: &arrow::datatypes::SchemaRef,
    value_field_ids: &[teodb_core::FieldId],
) -> datafusion::error::Result<bool> {
    // Build a one-row RecordBatch from partition values, keyed by partition
    // field id rather than by the source column's id.
    let mut columns: Vec<arrow::array::ArrayRef> = Vec::with_capacity(value_field_ids.len());
    for (field, value_field_id) in partition_schema
        .fields()
        .iter()
        .zip(value_field_ids)
    {
        let value = file
            .partition_values
            .get(value_field_id)
            .and_then(|value| teo_scalar_to_scalar_value(value).ok())
            .filter(|scalar| scalar.data_type() == *field.data_type());
        // A value that is absent, null, or of an unexpected type proves nothing
        // about the file. Evaluating it as a typed null lets the predicate return
        // null and keep the file (Invariant I4); an untyped `ScalarValue::Null`
        // would instead fail `RecordBatch::try_new` and abort the whole query.
        let scalar = match value {
            Some(scalar) => scalar,
            None => match ScalarValue::try_from(field.data_type()) {
                Ok(typed_null) => typed_null,
                Err(_) => return Ok(true),
            },
        };
        columns.push(scalar.to_array_of_size(1)?);
    }

    let batch = RecordBatch::try_new(partition_schema.clone(), columns)?;

    // Evaluate each pre-compiled filter. If any evaluates to definite `false`,
    // the file can be pruned.
    for physical_expr in compiled_filters {
        let result = physical_expr.evaluate(&batch)?;
        if let datafusion::physical_plan::ColumnarValue::Array(arr) = result
            && let Some(bool_arr) = arr
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
            && !bool_arr.is_empty()
            && bool_arr.is_valid(0)
            && !bool_arr.value(0)
        {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Extract column names referenced by a single filter expression.
pub fn extract_filter_columns(filter: &Expr) -> Vec<String> {
    let mut names = Vec::new();
    collect_columns(filter, &mut names);
    names.sort();
    names.dedup();
    names
}

fn collect_columns(expr: &Expr, names: &mut Vec<String>) {
    expr.apply(|child| {
        if let Expr::Column(col) = child {
            names.push(col.name().to_string());
        }
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    })
    .ok();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use teodb_core::file::{DataContent, FileFormat};
    use teodb_core::location::{ObjectLocation, StorageScheme};
    use teodb_core::scalar::TeoScalar;
    use teodb_core::schema::{PartitionField, PartitionTransform};

    fn make_schema() -> arrow::datatypes::SchemaRef {
        crate::conversion::schema_to_arrow(&teodb_core::schema::SchemaDefinition {
            schema_id: 0,
            columns: vec![
                teodb_core::schema::ColumnMeta {
                    id: 1,
                    name: "region".into(),
                    data_type: teodb_core::schema::TeoDataType::Utf8,
                    nullable: false,
                    doc: None,
                },
                teodb_core::schema::ColumnMeta {
                    id: 2,
                    name: "value".into(),
                    data_type: teodb_core::schema::TeoDataType::Int64,
                    nullable: false,
                    doc: None,
                },
            ],
            identifier_field_ids: vec![1],
        })
    }

    fn make_spec() -> PartitionSpec {
        PartitionSpec {
            spec_id: 0,
            fields: vec![PartitionField {
                source_id: 1,
                field_id: 1000,
                name: "region".into(),
                transform: PartitionTransform::Identity,
            }],
        }
    }

    fn make_file(region: &str) -> DataFile {
        let mut partition_values = HashMap::new();
        // The catalog stores partition values under the partition field's id
        // (1000), which is deliberately distinct from the source column id (1).
        partition_values.insert(1000, TeoScalar::Utf8(region.into()));
        DataFile {
            content: DataContent::Data,
            path: ObjectLocation {
                scheme: StorageScheme::Local,
                bucket: None,
                key: format!("data/{region}.parquet"),
            },
            format: FileFormat::Parquet,
            partition_spec_id: 0,
            sort_order_id: None,
            schema_id: 0,
            partition_values,
            record_count: 100,
            file_size_bytes: 1024,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            split_offsets: vec![],
            equality_ids: vec![],
            key_metadata: None,
        }
    }

    #[test]
    fn prune_no_filters_keeps_all() {
        let schema = make_schema();
        let files = vec![make_file("us"), make_file("eu")];
        let result = partition_prune(&files, &[], &make_spec(), &schema).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn prune_empty_partition_spec_keeps_all() {
        let schema = make_schema();
        let files = vec![make_file("us"), make_file("eu")];
        let empty_spec = PartitionSpec {
            spec_id: 0,
            fields: vec![],
        };
        let filter = datafusion::logical_expr::col("region").eq(datafusion::logical_expr::lit("us"));
        let result = partition_prune(&files, &[filter], &empty_spec, &schema).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn prune_non_partition_column_keeps_all() {
        let schema = make_schema();
        let files = vec![make_file("us"), make_file("eu")];
        // Filter on "value" which is not a partition column.
        let filter = datafusion::logical_expr::col("value").gt(datafusion::logical_expr::lit(10i64));
        let result = partition_prune(&files, &[filter], &make_spec(), &schema).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn prune_partition_filter_drops_other_partitions() {
        let schema = make_schema();
        let files = vec![make_file("us"), make_file("eu")];
        let filter = datafusion::logical_expr::col("region").eq(datafusion::logical_expr::lit("us"));
        let result = partition_prune(&files, &[filter], &make_spec(), &schema).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].path.key, "data/us.parquet");
    }

    #[test]
    fn prune_mixed_filters_still_uses_partition_conjunct() {
        // The non-partition conjunct must not disable pruning on the
        // partition conjunct — each pushed filter prunes independently.
        let schema = make_schema();
        let files = vec![make_file("us"), make_file("eu")];
        let on_partition = datafusion::logical_expr::col("region").eq(datafusion::logical_expr::lit("us"));
        let on_other = datafusion::logical_expr::col("value").gt(datafusion::logical_expr::lit(10i64));
        let result = partition_prune(&files, &[on_partition, on_other], &make_spec(), &schema).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].path.key, "data/us.parquet");
    }

    #[test]
    fn non_identity_transform_is_never_evaluated() {
        // Bucket/truncate/temporal partition values are transformed — a raw
        // column predicate evaluated against them would prune wrongly, so
        // those fields must be ignored entirely.
        let schema = make_schema();
        let spec = PartitionSpec {
            spec_id: 0,
            fields: vec![PartitionField {
                source_id: 1,
                field_id: 1000,
                name: "region_bucket".into(),
                transform: PartitionTransform::Bucket { num_buckets: 16 },
            }],
        };
        let files = vec![make_file("us"), make_file("eu")];
        let filter = datafusion::logical_expr::col("region").eq(datafusion::logical_expr::lit("us"));
        let result = partition_prune(&files, &[filter], &spec, &schema).unwrap();
        assert_eq!(result.len(), 2, "transformed partitions cannot prove anything");
    }

    #[test]
    fn values_stored_under_source_column_id_are_not_read_as_partition_values() {
        // Partition values live under the partition field's id (1000). A file
        // carrying them under the source column id (1) has, as far as this spec
        // is concerned, no usable value — it must be kept, not pruned and not
        // turned into an untyped null that fails batch construction.
        let schema = make_schema();
        let mut mis_keyed = make_file("eu");
        mis_keyed.partition_values = HashMap::from([(1, TeoScalar::Utf8("eu".into()))]);
        let filter = datafusion::logical_expr::col("region").eq(datafusion::logical_expr::lit("us"));

        let result = partition_prune(&[mis_keyed], &[filter], &make_spec(), &schema).unwrap();

        assert_eq!(result.len(), 1, "a value under the wrong id proves nothing");
    }

    #[test]
    fn file_without_partition_values_is_kept_rather_than_erroring() {
        // Reproduces the production failure: a missing value became an untyped
        // `ScalarValue::Null`, so building the one-row batch failed with
        // "expected Utf8 but found Null" and aborted the entire query. Pruning is
        // an optimization and must never turn a valid query into an error.
        let schema = make_schema();
        let mut no_values = make_file("us");
        no_values.partition_values = HashMap::new();
        let filter = datafusion::logical_expr::col("region").eq(datafusion::logical_expr::lit("us"));

        let result = partition_prune(&[no_values], &[filter], &make_spec(), &schema)
            .expect("a file with no partition values must not fail the scan");

        assert_eq!(result.len(), 1, "an unknown partition value cannot prove exclusion");
    }

    #[test]
    fn explicit_null_partition_value_is_kept_rather_than_erroring() {
        // The catalog records an absent Iceberg partition literal as
        // `TeoScalar::Null`, which converts to an untyped `ScalarValue::Null`.
        let schema = make_schema();
        let mut null_valued = make_file("us");
        null_valued.partition_values = HashMap::from([(1000, TeoScalar::Null)]);
        let filter = datafusion::logical_expr::col("region").eq(datafusion::logical_expr::lit("us"));

        let result = partition_prune(&[null_valued], &[filter], &make_spec(), &schema)
            .expect("a null partition value must not fail the scan");

        assert_eq!(result.len(), 1, "null compares as null, which keeps the file");
    }

    #[test]
    fn file_from_other_partition_spec_is_kept() {
        let schema = make_schema();
        let mut old_spec_file = make_file("eu");
        old_spec_file.partition_spec_id = 7; // written under a different spec
        let files = vec![make_file("us"), old_spec_file];
        let filter = datafusion::logical_expr::col("region").eq(datafusion::logical_expr::lit("us"));
        let result = partition_prune(&files, &[filter], &make_spec(), &schema).unwrap();
        assert_eq!(result.len(), 2, "values from another spec are uninterpretable");
    }
}
