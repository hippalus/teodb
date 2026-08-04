//! Iceberg position-delete file reader.
//!
//! Position-delete files contain (file_path, pos) pairs indicating which
//! rows in a data file should be excluded from query results.

use std::collections::{HashMap, HashSet};

use arrow::array::{Array, Int64Array, StringArray};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use teodb_core::error::{TeoDBError, TeoDBResult};

/// A set of positions to delete per data file path.
pub type PositionDeleteMap = HashMap<String, HashSet<i64>>;

/// Read a position-delete Parquet file and return a map of file_path → {positions}.
pub fn read_position_deletes(data: Bytes) -> TeoDBResult<PositionDeleteMap> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(data)
        .map_err(|e| TeoDBError::Parquet(format!("delete file reader: {e}")))?
        .build()
        .map_err(|e| TeoDBError::Parquet(format!("delete file build: {e}")))?;

    let mut result: PositionDeleteMap = HashMap::new();

    for batch in reader {
        let batch = batch.map_err(|e| TeoDBError::Parquet(format!("delete batch: {e}")))?;

        let file_paths = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| TeoDBError::Parquet("column 0 not StringArray".into()))?;

        let positions = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| TeoDBError::Parquet("column 1 not Int64Array".into()))?;

        for i in 0..batch.num_rows() {
            if !file_paths.is_null(i) && !positions.is_null(i) {
                let path = file_paths.value(i);
                let pos = positions.value(i);
                result
                    .entry(path.to_string())
                    .or_default()
                    .insert(pos);
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    fn write_position_delete_file(entries: &[(&str, i64)]) -> Bytes {
        let schema = Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ]));

        let file_paths: Vec<&str> = entries.iter().map(|(p, _)| *p).collect();
        let positions: Vec<i64> = entries.iter().map(|(_, pos)| *pos).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(file_paths)),
                Arc::new(Int64Array::from(positions)),
            ],
        )
        .unwrap();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(buf)
    }

    #[test]
    fn read_position_deletes_roundtrip() {
        let data = write_position_delete_file(&[
            ("data/file1.parquet", 0),
            ("data/file1.parquet", 5),
            ("data/file1.parquet", 10),
            ("data/file2.parquet", 3),
        ]);

        let map = read_position_deletes(data).unwrap();

        assert_eq!(map.len(), 2);
        let f1 = map.get("data/file1.parquet").unwrap();
        assert_eq!(f1.len(), 3);
        assert!(f1.contains(&0));
        assert!(f1.contains(&5));
        assert!(f1.contains(&10));

        let f2 = map.get("data/file2.parquet").unwrap();
        assert_eq!(f2.len(), 1);
        assert!(f2.contains(&3));
    }

    #[test]
    fn empty_delete_file() {
        let data = write_position_delete_file(&[]);
        let map = read_position_deletes(data).unwrap();
        assert!(map.is_empty());
    }
}
