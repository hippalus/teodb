use std::collections::HashMap;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::record_batch::RecordBatch;

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::FieldId;
use teodb_core::scalar::TeoScalar;
use teodb_core::schema::{PartitionSpec, SchemaDefinition, TeoDataType};

pub(super) struct PartitionedBatches {
    pub(super) partition_values: HashMap<FieldId, TeoScalar>,
    pub(super) batches: Vec<RecordBatch>,
}

pub(super) fn partition_batches(
    batches: &[RecordBatch],
    schema: &SchemaDefinition,
    partition_spec: &PartitionSpec,
) -> TeoDBResult<Vec<PartitionedBatches>> {
    let mut groups: HashMap<String, PartitionedBatches> = HashMap::new();

    for batch in batches {
        let mut rows_by_key: HashMap<String, (HashMap<FieldId, TeoScalar>, Vec<u32>)> = HashMap::new();
        for row in 0..batch.num_rows() {
            let (key, values) = partition_key_for_row(batch, row, schema, partition_spec)?;
            rows_by_key
                .entry(key)
                .or_insert_with(|| (values, Vec::new()))
                .1
                .push(row as u32);
        }

        for (key, (values, rows)) in rows_by_key {
            let sliced = take_rows(batch, &rows)?;
            groups
                .entry(key)
                .or_insert_with(|| PartitionedBatches {
                    partition_values: values,
                    batches: Vec::new(),
                })
                .batches
                .push(sliced);
        }
    }

    Ok(groups.into_values().collect())
}

fn partition_key_for_row(
    batch: &RecordBatch,
    row: usize,
    schema: &SchemaDefinition,
    partition_spec: &PartitionSpec,
) -> TeoDBResult<(String, HashMap<FieldId, TeoScalar>)> {
    let mut values = HashMap::new();
    let mut ordered = Vec::with_capacity(partition_spec.fields.len());

    for field in &partition_spec.fields {
        let (column_idx, column) = schema
            .columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.id == field.source_id)
            .ok_or_else(|| TeoDBError::Internal(format!("partition source field {} not found", field.source_id)))?;
        let source = scalar_at(batch.column(column_idx).as_ref(), row, &column.data_type)?;
        let partition_value =
            teodb_catalog::apply_partition_transform_to_scalar(&source, &column.data_type, &field.transform)?;
        values.insert(field.field_id, partition_value.clone());
        ordered.push((field.field_id, partition_value));
    }

    let key = serde_json::to_string(&ordered)
        .map_err(|error| TeoDBError::Internal(format!("failed to serialize partition key: {error}")))?;
    Ok((key, values))
}

fn take_rows(batch: &RecordBatch, rows: &[u32]) -> TeoDBResult<RecordBatch> {
    let indices = UInt32Array::from(rows.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            arrow::compute::take(column.as_ref(), &indices, None)
                .map_err(|error| TeoDBError::Internal(format!("failed to slice partition batch: {error}")))
        })
        .collect::<TeoDBResult<Vec<_>>>()?;
    RecordBatch::try_new(batch.schema(), columns)
        .map_err(|error| TeoDBError::Internal(format!("failed to build partition batch: {error}")))
}

fn scalar_at(array: &dyn Array, row: usize, data_type: &TeoDataType) -> TeoDBResult<TeoScalar> {
    if array.is_null(row) {
        return Ok(TeoScalar::Null);
    }
    Ok(match data_type {
        TeoDataType::Boolean => TeoScalar::Boolean(downcast::<BooleanArray>(array, data_type)?.value(row)),
        TeoDataType::Int8 => TeoScalar::Int8(downcast::<Int8Array>(array, data_type)?.value(row)),
        TeoDataType::Int16 => TeoScalar::Int16(downcast::<Int16Array>(array, data_type)?.value(row)),
        TeoDataType::Int32 => TeoScalar::Int32(downcast::<Int32Array>(array, data_type)?.value(row)),
        TeoDataType::Int64 => TeoScalar::Int64(downcast::<Int64Array>(array, data_type)?.value(row)),
        TeoDataType::UInt8 => TeoScalar::UInt8(downcast::<UInt8Array>(array, data_type)?.value(row)),
        TeoDataType::UInt16 => TeoScalar::UInt16(downcast::<UInt16Array>(array, data_type)?.value(row)),
        TeoDataType::UInt32 => TeoScalar::UInt32(downcast::<UInt32Array>(array, data_type)?.value(row)),
        TeoDataType::UInt64 => TeoScalar::UInt64(downcast::<UInt64Array>(array, data_type)?.value(row)),
        TeoDataType::Float32 => TeoScalar::Float32(downcast::<Float32Array>(array, data_type)?.value(row)),
        TeoDataType::Float64 => TeoScalar::Float64(downcast::<Float64Array>(array, data_type)?.value(row)),
        TeoDataType::Decimal128 { precision, scale } => TeoScalar::Decimal128 {
            value: downcast::<Decimal128Array>(array, data_type)?.value(row),
            precision: *precision,
            scale: *scale,
        },
        TeoDataType::Date32 => TeoScalar::Date32(downcast::<Date32Array>(array, data_type)?.value(row)),
        TeoDataType::TimestampMicros { tz } => TeoScalar::TimestampMicros {
            value: downcast::<TimestampMicrosecondArray>(array, data_type)?.value(row),
            tz: tz.clone(),
        },
        TeoDataType::Time64Micros => {
            TeoScalar::Time64Micros(downcast::<Time64MicrosecondArray>(array, data_type)?.value(row))
        }
        TeoDataType::Utf8 => TeoScalar::Utf8(
            downcast::<StringArray>(array, data_type)?
                .value(row)
                .to_owned(),
        ),
        TeoDataType::Binary => TeoScalar::Binary(
            downcast::<BinaryArray>(array, data_type)?
                .value(row)
                .to_vec(),
        ),
        TeoDataType::FixedSizeBinary(_) => TeoScalar::FixedSizeBinary(
            downcast::<FixedSizeBinaryArray>(array, data_type)?
                .value(row)
                .to_vec(),
        ),
    })
}

fn downcast<'a, T: 'static>(array: &'a dyn Array, data_type: &TeoDataType) -> TeoDBResult<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        TeoDBError::Internal(format!(
            "partition column Arrow array does not match TeoDB type {data_type:?}: actual {:?}",
            array.data_type()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use teodb_core::schema::*;

    fn partitioned_schema() -> SchemaDefinition {
        SchemaDefinition {
            schema_id: 0,
            columns: vec![
                ColumnMeta {
                    id: 1,
                    name: "id".into(),
                    data_type: TeoDataType::Int64,
                    nullable: false,
                    doc: None,
                },
                ColumnMeta {
                    id: 2,
                    name: "region".into(),
                    data_type: TeoDataType::Utf8,
                    nullable: false,
                    doc: None,
                },
            ],
            identifier_field_ids: vec![1],
        }
    }

    #[test]
    fn partition_batches_groups_rows_by_identity_value() {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["eu", "us", "eu"])),
            ],
        )
        .unwrap();
        let spec = PartitionSpec {
            spec_id: 2,
            fields: vec![PartitionField {
                source_id: 2,
                field_id: 1000,
                name: "region".into(),
                transform: PartitionTransform::Identity,
            }],
        };

        let mut groups = partition_batches(&[batch], &partitioned_schema(), &spec).unwrap();
        groups.sort_by_key(|group| match group.partition_values.get(&1000).unwrap() {
            TeoScalar::Utf8(value) => value.clone(),
            other => panic!("unexpected partition value: {other:?}"),
        });

        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups[0].partition_values.get(&1000),
            Some(&TeoScalar::Utf8("eu".into()))
        );
        assert_eq!(
            groups[0]
                .batches
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            2
        );
        assert_eq!(
            groups[1].partition_values.get(&1000),
            Some(&TeoScalar::Utf8("us".into()))
        );
        assert_eq!(
            groups[1]
                .batches
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            1
        );
    }
}
