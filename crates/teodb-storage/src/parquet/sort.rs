//! Batch sorting by a table `SortOrder` prior to Parquet encoding.

use std::sync::Arc;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::schema::{NullOrder, SortDirection, SortOrder};

/// Sort record batches by the sort order columns. Concatenates all batches,
/// then performs a lexicographic sort using the configured sort fields.
pub(super) fn sort_batches(
    batches: Vec<RecordBatch>,
    sort_order: &SortOrder,
    schema: &SchemaRef,
) -> TeoDBResult<Vec<RecordBatch>> {
    if batches.is_empty() || sort_order.fields.is_empty() {
        return Ok(batches);
    }

    let combined =
        arrow::compute::concat_batches(schema, &batches).map_err(|e| TeoDBError::Parquet(format!("concat: {e}")))?;

    let sort_columns: Vec<arrow::compute::SortColumn> = sort_order
        .fields
        .iter()
        .filter_map(|sf| {
            let col_idx = schema.fields().iter().position(|f| {
                f.metadata()
                    .get("PARQUET:field_id")
                    .and_then(|v| v.parse::<i32>().ok())
                    == Some(sf.source_id)
            })?;
            Some(arrow::compute::SortColumn {
                values: combined.column(col_idx).clone(),
                options: Some(arrow::compute::SortOptions {
                    descending: sf.direction == SortDirection::Desc,
                    nulls_first: sf.null_order == NullOrder::NullsFirst,
                }),
            })
        })
        .collect();

    if sort_columns.is_empty() {
        return Ok(batches);
    }

    let indices = arrow::compute::lexsort_to_indices(&sort_columns, None)
        .map_err(|e| TeoDBError::Parquet(format!("sort: {e}")))?;

    let sorted_columns: Vec<Arc<dyn Array>> = combined
        .columns()
        .iter()
        .map(|col| arrow::compute::take(col, &indices, None))
        .collect::<Result<_, _>>()
        .map_err(|e| TeoDBError::Parquet(format!("take: {e}")))?;

    let sorted = RecordBatch::try_new(schema.clone(), sorted_columns)
        .map_err(|e| TeoDBError::Parquet(format!("rebuild batch: {e}")))?;

    Ok(vec![sorted])
}
