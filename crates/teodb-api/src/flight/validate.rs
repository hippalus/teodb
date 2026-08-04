//! Schema validation for incoming record batches against table metadata.

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, SchemaRef};

use teodb_core::error::{TeoDBError, TeoDBResult};

/// Validate an incoming `RecordBatch` against the expected Arrow schema.
///
/// Checks:
/// - Column count matches
/// - Column names match in order
/// - Column types match (including decimal scale/precision, timestamp unit/timezone)
/// - Non-nullable columns contain no nulls
/// - Field IDs in metadata match when present
pub fn validate_batch(batch: &RecordBatch, expected: &SchemaRef) -> TeoDBResult<()> {
    let actual = batch.schema();

    if actual.fields().len() != expected.fields().len() {
        return Err(TeoDBError::InvalidArgument {
            field: "columns".into(),
            message: format!(
                "expected {} columns, got {}",
                expected.fields().len(),
                actual.fields().len()
            ),
        });
    }

    for (i, (exp, act)) in expected
        .fields()
        .iter()
        .zip(actual.fields().iter())
        .enumerate()
    {
        // Column name match.
        if exp.name() != act.name() {
            return Err(TeoDBError::InvalidArgument {
                field: format!("column[{i}].name"),
                message: format!("expected '{}', got '{}'", exp.name(), act.name()),
            });
        }

        // Type match with special handling for decimal and timestamp.
        validate_data_type(exp.name(), exp.data_type(), act.data_type())?;

        // Non-nullable constraint.
        if !exp.is_nullable() {
            let col = batch.column(i);
            if col.null_count() > 0 {
                return Err(TeoDBError::InvalidArgument {
                    field: format!("column[{}]", exp.name()),
                    message: format!(
                        "non-nullable column '{}' contains {} null value(s)",
                        exp.name(),
                        col.null_count()
                    ),
                });
            }
        }

        // Field ID metadata match when present.
        if let Some(exp_id) = exp.metadata().get("PARQUET:field_id")
            && let Some(act_id) = act.metadata().get("PARQUET:field_id")
            && exp_id != act_id
        {
            return Err(TeoDBError::InvalidArgument {
                field: format!("column[{}].field_id", exp.name()),
                message: format!(
                    "field ID mismatch for '{}': expected {exp_id}, got {act_id}",
                    exp.name()
                ),
            });
        }
    }

    Ok(())
}

/// Validate data type compatibility with detailed checks for decimal and timestamp.
fn validate_data_type(col_name: &str, expected: &DataType, actual: &DataType) -> TeoDBResult<()> {
    match (expected, actual) {
        // Decimal: validate precision and scale.
        (DataType::Decimal128(exp_p, exp_s), DataType::Decimal128(act_p, act_s))
            if exp_p != act_p || exp_s != act_s =>
        {
            return Err(TeoDBError::InvalidArgument {
                field: format!("column[{col_name}].type"),
                message: format!(
                    "decimal mismatch for '{col_name}': expected Decimal128({exp_p},{exp_s}), got Decimal128({act_p},{act_s})"
                ),
            });
        }
        (DataType::Decimal256(exp_p, exp_s), DataType::Decimal256(act_p, act_s))
            if exp_p != act_p || exp_s != act_s =>
        {
            return Err(TeoDBError::InvalidArgument {
                field: format!("column[{col_name}].type"),
                message: format!(
                    "decimal mismatch for '{col_name}': expected Decimal256({exp_p},{exp_s}), got Decimal256({act_p},{act_s})"
                ),
            });
        }
        // Timestamp: validate unit and timezone.
        (DataType::Timestamp(exp_unit, exp_tz), DataType::Timestamp(act_unit, act_tz)) if exp_unit != act_unit => {
            return Err(TeoDBError::InvalidArgument {
                field: format!("column[{col_name}].type"),
                message: format!("timestamp unit mismatch for '{col_name}': expected {exp_unit:?}, got {act_unit:?}"),
            });
        }
        (DataType::Timestamp(_exp_unit, exp_tz), DataType::Timestamp(_act_unit, act_tz)) if exp_tz != act_tz => {
            return Err(TeoDBError::InvalidArgument {
                field: format!("column[{col_name}].type"),
                message: format!("timestamp timezone mismatch for '{col_name}': expected {exp_tz:?}, got {act_tz:?}"),
            });
        }
        // General type comparison.
        (e, a) if e != a => {
            return Err(TeoDBError::InvalidArgument {
                field: format!("column[{col_name}].type"),
                message: format!("expected {expected:?}, got {actual:?}"),
            });
        }
        _ => {}
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Decimal128Array, Int64Array, StringArray, TimestampMicrosecondArray, TimestampNanosecondArray};
    use arrow::datatypes::{Field, Schema, TimeUnit};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn valid_batch_passes() {
        let schema = test_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap();

        assert!(validate_batch(&batch, &schema).is_ok());
    }

    #[test]
    fn wrong_column_count() {
        let schema = test_schema();
        let small_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(small_schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();

        let err = validate_batch(&batch, &schema).unwrap_err();
        assert!(matches!(err, TeoDBError::InvalidArgument { .. }));
    }

    #[test]
    fn wrong_column_name() {
        let schema = test_schema();
        let bad_schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            bad_schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
        )
        .unwrap();

        let err = validate_batch(&batch, &schema).unwrap_err();
        assert!(matches!(err, TeoDBError::InvalidArgument { .. }));
        assert!(err.to_string().contains("user_id"));
    }

    #[test]
    fn wrong_column_type() {
        let schema = test_schema();
        let bad_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            bad_schema,
            vec![
                Arc::new(StringArray::from(vec!["1"])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
        )
        .unwrap();

        let err = validate_batch(&batch, &schema).unwrap_err();
        assert!(matches!(err, TeoDBError::InvalidArgument { .. }));
        assert!(err.to_string().contains("type"));
    }

    #[test]
    fn non_nullable_with_nulls() {
        let schema = test_schema();
        let nullable_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            nullable_schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            ],
        )
        .unwrap();

        let err = validate_batch(&batch, &schema).unwrap_err();
        assert!(matches!(err, TeoDBError::InvalidArgument { .. }));
        assert!(err.to_string().contains("non-nullable"));
    }

    #[test]
    fn decimal_scale_mismatch() {
        let expected = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, 2),
            false,
        )]));
        let actual_schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, 4),
            false,
        )]));
        let batch = RecordBatch::try_new(
            actual_schema,
            vec![Arc::new(
                Decimal128Array::from(vec![12345i128])
                    .with_precision_and_scale(10, 4)
                    .unwrap(),
            )],
        )
        .unwrap();

        let err = validate_batch(&batch, &expected).unwrap_err();
        assert!(err.to_string().contains("decimal mismatch"));
    }

    #[test]
    fn decimal_precision_mismatch() {
        let expected = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, 2),
            false,
        )]));
        let actual_schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(18, 2),
            false,
        )]));
        let batch = RecordBatch::try_new(
            actual_schema,
            vec![Arc::new(
                Decimal128Array::from(vec![12345i128])
                    .with_precision_and_scale(18, 2)
                    .unwrap(),
            )],
        )
        .unwrap();

        let err = validate_batch(&batch, &expected).unwrap_err();
        assert!(err.to_string().contains("decimal mismatch"));
    }

    #[test]
    fn timestamp_unit_mismatch() {
        let expected = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]));
        let actual_schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));
        let batch = RecordBatch::try_new(
            actual_schema,
            vec![Arc::new(TimestampNanosecondArray::from(vec![1_000_000_000i64]))],
        )
        .unwrap();

        let err = validate_batch(&batch, &expected).unwrap_err();
        assert!(
            err.to_string()
                .contains("timestamp unit mismatch")
        );
    }

    #[test]
    fn timestamp_timezone_mismatch() {
        let expected = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )]));
        let actual_schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("US/Eastern".into())),
            false,
        )]));
        let batch = RecordBatch::try_new(
            actual_schema,
            vec![Arc::new(
                TimestampMicrosecondArray::from(vec![1_000_000i64]).with_timezone("US/Eastern"),
            )],
        )
        .unwrap();

        let err = validate_batch(&batch, &expected).unwrap_err();
        assert!(
            err.to_string()
                .contains("timestamp timezone mismatch")
        );
    }

    #[test]
    fn field_id_mismatch() {
        use std::collections::HashMap;

        let mut meta1 = HashMap::new();
        meta1.insert("PARQUET:field_id".to_string(), "1".to_string());
        let mut meta2 = HashMap::new();
        meta2.insert("PARQUET:field_id".to_string(), "2".to_string());

        let expected = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(meta1),
        ]));
        let actual_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(meta2),
        ]));

        let batch = RecordBatch::try_new(actual_schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();

        let err = validate_batch(&batch, &expected).unwrap_err();
        assert!(err.to_string().contains("field ID mismatch"));
    }

    #[test]
    fn valid_decimal_passes() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, 2),
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                Decimal128Array::from(vec![12345i128])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            )],
        )
        .unwrap();

        assert!(validate_batch(&batch, &schema).is_ok());
    }

    #[test]
    fn valid_timestamp_passes() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                TimestampMicrosecondArray::from(vec![1_000_000i64]).with_timezone("UTC"),
            )],
        )
        .unwrap();

        assert!(validate_batch(&batch, &schema).is_ok());
    }
}
