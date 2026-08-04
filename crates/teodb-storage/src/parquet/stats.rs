//! Parquet footer statistics extraction.
//!
//! After writing a Parquet file, this module reads the footer metadata and
//! populates a `DataFile` with per-column statistics (lower/upper bounds,
//! null counts, value counts, column sizes).

use std::collections::HashMap;

use bytes::Bytes;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::file::statistics::Statistics;
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::file::{DataContent, DataFile, FileFormat};
use teodb_core::ident::FieldId;
use teodb_core::location::ObjectLocation;
use teodb_core::scalar::TeoScalar;

use crate::convert::field_id_from_arrow_field;
use crate::parquet::spec::WriteSpec;

/// Extract a `DataFile` from the raw bytes of a Parquet file.
pub fn extract_data_file_from_bytes(
    data: &Bytes,
    target: &ObjectLocation,
    spec: &WriteSpec,
    file_size: u64,
) -> TeoDBResult<DataFile> {
    let reader =
        SerializedFileReader::new(data.clone()).map_err(|e| TeoDBError::Parquet(format!("open footer: {e}")))?;
    let footer = reader.metadata();

    let mut record_count: u64 = 0;
    let mut column_sizes: HashMap<FieldId, u64> = HashMap::new();
    let mut value_counts: HashMap<FieldId, u64> = HashMap::new();
    let mut null_value_counts: HashMap<FieldId, u64> = HashMap::new();
    let mut lower_bounds: HashMap<FieldId, TeoScalar> = HashMap::new();
    let mut upper_bounds: HashMap<FieldId, TeoScalar> = HashMap::new();
    let mut split_offsets: Vec<i64> = Vec::new();

    // Build field_id mapping from the Arrow schema in the spec.
    let field_ids: Vec<Option<FieldId>> = spec
        .schema
        .fields()
        .iter()
        .map(|f| field_id_from_arrow_field(f))
        .collect();

    for rg_idx in 0..footer.num_row_groups() {
        let rg = footer.row_group(rg_idx);
        record_count += rg.num_rows() as u64;

        // Record the offset of each row group for split planning.
        if let Some(col0) = rg.columns().first() {
            split_offsets.push(col0.data_page_offset());
        }

        for (col_idx, col) in rg.columns().iter().enumerate() {
            let field_id = match field_ids.get(col_idx).copied().flatten() {
                Some(id) => id,
                None => continue,
            };

            let compressed = col.compressed_size() as u64;
            *column_sizes.entry(field_id).or_default() += compressed;

            if let Some(stats) = col.statistics() {
                let num_values = rg.num_rows() as u64;
                *value_counts.entry(field_id).or_default() += num_values;

                if let Some(null_count) = stats.null_count_opt()
                    && null_count > 0
                {
                    *null_value_counts.entry(field_id).or_default() += null_count;
                }

                // Extract typed lower/upper bounds.
                if let Some(scalar) = stats_min_to_scalar(stats) {
                    lower_bounds
                        .entry(field_id)
                        .and_modify(|existing| {
                            if scalar < *existing {
                                *existing = scalar.clone();
                            }
                        })
                        .or_insert(scalar);
                }
                if let Some(scalar) = stats_max_to_scalar(stats) {
                    upper_bounds
                        .entry(field_id)
                        .and_modify(|existing| {
                            if scalar > *existing {
                                *existing = scalar.clone();
                            }
                        })
                        .or_insert(scalar);
                }
            }
        }
    }

    Ok(DataFile {
        content: DataContent::Data,
        path: target.clone(),
        format: FileFormat::Parquet,
        partition_spec_id: spec.partition_spec_id,
        sort_order_id: Some(spec.sort_order.order_id),
        schema_id: spec.schema_id,
        partition_values: spec.partition_values.clone(),
        record_count,
        file_size_bytes: file_size,
        column_sizes,
        value_counts,
        null_value_counts,
        nan_value_counts: HashMap::new(),
        lower_bounds,
        upper_bounds,
        split_offsets,
        equality_ids: vec![],
        key_metadata: None,
    })
}

/// Convert Parquet `Statistics` min value to `TeoScalar`.
fn stats_min_to_scalar(stats: &Statistics) -> Option<TeoScalar> {
    match stats {
        Statistics::Boolean(s) => s.min_opt().map(|v| TeoScalar::Boolean(*v)),
        Statistics::Int32(s) => s.min_opt().map(|v| TeoScalar::Int32(*v)),
        Statistics::Int64(s) => s.min_opt().map(|v| TeoScalar::Int64(*v)),
        Statistics::Float(s) => s.min_opt().map(|v| TeoScalar::Float32(*v)),
        Statistics::Double(s) => s.min_opt().map(|v| TeoScalar::Float64(*v)),
        Statistics::ByteArray(s) => s
            .min_opt()
            .and_then(|v| String::from_utf8(v.data().to_vec()).ok())
            .map(TeoScalar::Utf8),
        Statistics::FixedLenByteArray(s) => s
            .min_opt()
            .map(|v| TeoScalar::Binary(v.data().to_vec())),
        Statistics::Int96(_) => None, // Int96 is deprecated; we don't convert it.
    }
}

/// Convert Parquet `Statistics` max value to `TeoScalar`.
fn stats_max_to_scalar(stats: &Statistics) -> Option<TeoScalar> {
    match stats {
        Statistics::Boolean(s) => s.max_opt().map(|v| TeoScalar::Boolean(*v)),
        Statistics::Int32(s) => s.max_opt().map(|v| TeoScalar::Int32(*v)),
        Statistics::Int64(s) => s.max_opt().map(|v| TeoScalar::Int64(*v)),
        Statistics::Float(s) => s.max_opt().map(|v| TeoScalar::Float32(*v)),
        Statistics::Double(s) => s.max_opt().map(|v| TeoScalar::Float64(*v)),
        Statistics::ByteArray(s) => s
            .max_opt()
            .and_then(|v| String::from_utf8(v.data().to_vec()).ok())
            .map(TeoScalar::Utf8),
        Statistics::FixedLenByteArray(s) => s
            .max_opt()
            .map(|v| TeoScalar::Binary(v.data().to_vec())),
        Statistics::Int96(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    #[test]
    fn extract_stats_from_written_parquet() {
        let mut field_meta = HashMap::new();
        field_meta.insert("PARQUET:field_id".to_owned(), "1".to_owned());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(field_meta),
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![10, 20, 30]))]).unwrap();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let bytes = Bytes::from(buf);
        let target = ObjectLocation {
            scheme: teodb_core::location::StorageScheme::S3,
            bucket: Some("test".into()),
            key: "stats.parquet".into(),
        };
        let spec = crate::parquet::spec::WriteSpec {
            schema,
            ..Default::default()
        };

        let df = extract_data_file_from_bytes(&bytes, &target, &spec, bytes.len() as u64).unwrap();
        assert_eq!(df.record_count, 3);
        assert!(df.column_sizes.contains_key(&1));

        // Lower bound should be 10, upper should be 30.
        if let Some(TeoScalar::Int64(lo)) = df.lower_bounds.get(&1) {
            assert_eq!(*lo, 10);
        }
        if let Some(TeoScalar::Int64(hi)) = df.upper_bounds.get(&1) {
            assert_eq!(*hi, 30);
        }
    }
}
