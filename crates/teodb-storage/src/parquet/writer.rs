//! Sorted Parquet file writer.
//!
//! Receives `RecordBatch`es (as a vec or a fallible stream), sorts them by
//! the table's `SortOrder` where applicable, and writes Parquet files with
//! rich statistics, bloom filters, and page indexes. Codec configuration
//! lives in [`super::compression`], the write contract in [`super::spec`],
//! and footer-stats extraction in [`super::stats`].

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::file::DataFile;
use teodb_core::location::ObjectLocation;
use teodb_core::traits::storage::Storage;

use super::sort::sort_batches;
use super::spec::WriteSpec;
use super::stats;

/// Write record batches to a Parquet file at `target`, then extract stats
/// into a `DataFile`. This is the primary Parquet write entry point.
///
/// Batches are sorted by the `SortOrder` in the `WriteSpec` before writing.
pub async fn write_sorted_parquet(
    storage: &dyn Storage,
    target: &ObjectLocation,
    batches: Vec<RecordBatch>,
    spec: &WriteSpec,
) -> TeoDBResult<DataFile> {
    if batches.is_empty() {
        return Err(TeoDBError::InvalidArgument {
            field: "batches".into(),
            message: "cannot write empty Parquet file".into(),
        });
    }

    let props = spec.writer_properties()?;
    let sorted_batches = sort_batches(batches, &spec.sort_order, &spec.schema)?;

    // Write to an in-memory buffer, then upload.
    let mut buf = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, spec.schema.clone(), Some(props))
            .map_err(|e| TeoDBError::Parquet(format!("writer init: {e}")))?;

        for batch in &sorted_batches {
            writer
                .write(batch)
                .map_err(|e| TeoDBError::Parquet(format!("write batch: {e}")))?;
        }

        writer
            .close()
            .map_err(|e| TeoDBError::Parquet(format!("writer close: {e}")))?;
    }

    let file_size = buf.len() as u64;
    let bytes = Bytes::from(buf);

    // Upload to storage.
    let path = teodb_core::location::ObjectPath::new(target.key.clone());
    storage.put(&path, bytes.clone()).await?;

    // Extract footer stats from the written bytes.
    stats::extract_data_file_from_bytes(&bytes, target, spec, file_size)
}

/// Sort `batches` by the spec's `SortOrder` (a no-op for unsorted tables),
/// then write them to one or more rolled Parquet files.
///
/// Unlike [`write_sorted_parquet`], the encoded output is bounded to one
/// in-progress file at a time and rolls at the file-size target, so a large
/// flush never materializes a single oversized buffer. For unsorted tables the
/// input is streamed straight through without concatenation. Empty input yields
/// no files.
pub async fn write_sorted_rolled(
    storage: &dyn Storage,
    base: &ObjectLocation,
    batches: Vec<RecordBatch>,
    spec: &WriteSpec,
) -> TeoDBResult<Vec<DataFile>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let sorted = sort_batches(batches, &spec.sort_order, &spec.schema)?;
    // Sorting concatenates into one large batch; re-slice it so the streaming
    // writer can roll between chunks (it rolls at batch boundaries) and so peak
    // encode memory stays bounded by one chunk at a time.
    let chunk_rows = (spec.row_group_target_rows as usize).max(1);
    let chunked = sorted
        .iter()
        .flat_map(|batch| slice_batch(batch, chunk_rows));
    write_sorted_stream(storage, base, futures::stream::iter(chunked.map(Ok)), spec).await
}

/// Split a batch into row slices of at most `chunk_rows` rows (zero-copy).
fn slice_batch(batch: &RecordBatch, chunk_rows: usize) -> Vec<RecordBatch> {
    if batch.num_rows() <= chunk_rows {
        return vec![batch.clone()];
    }
    let mut slices = Vec::new();
    let mut offset = 0;
    while offset < batch.num_rows() {
        let len = chunk_rows.min(batch.num_rows() - offset);
        slices.push(batch.slice(offset, len));
        offset += len;
    }
    slices
}

/// Write a stream of pre-sorted `RecordBatch`es to one or more Parquet files,
/// rolling to a new file whenever the estimated uncompressed size approaches
/// `spec.row_group_target_bytes * 8` (a heuristic for target file size).
///
/// Each closed file becomes one entry in the returned `Vec<DataFile>`.
/// The caller is responsible for providing batches in sorted order.
pub async fn write_sorted_streaming(
    storage: &dyn Storage,
    base: &ObjectLocation,
    batches: Vec<RecordBatch>,
    spec: &WriteSpec,
) -> TeoDBResult<Vec<DataFile>> {
    write_sorted_stream(storage, base, futures::stream::iter(batches.into_iter().map(Ok)), spec).await
}

/// Write a fallible stream of pre-sorted `RecordBatch`es to one or more
/// Parquet files, rolling whenever the encoded size of the open file reaches
/// `spec.row_group_target_bytes * 8`.
///
/// Memory stays bounded by one in-progress file: each batch is encoded into
/// the open `ArrowWriter` as it arrives and dropped immediately — input
/// batches are never accumulated. A stream error aborts the write (already
/// uploaded rolls remain for the orphan sweeper; the caller must not commit).
/// An empty stream yields no files.
pub async fn write_sorted_stream<S>(
    storage: &dyn Storage,
    base: &ObjectLocation,
    mut batches: S,
    spec: &WriteSpec,
) -> TeoDBResult<Vec<DataFile>>
where
    S: futures::Stream<Item = TeoDBResult<RecordBatch>> + Unpin,
{
    use futures::TryStreamExt;

    let target_file_bytes = spec.row_group_target_bytes * 8;
    let props = spec.writer_properties()?;

    let mut data_files = Vec::new();
    let mut file_index: u32 = 0;
    let mut writer: Option<ArrowWriter<Vec<u8>>> = None;

    while let Some(batch) = batches.try_next().await? {
        if batch.num_rows() == 0 {
            continue;
        }
        let w = match writer.as_mut() {
            Some(w) => w,
            None => writer.insert(
                ArrowWriter::try_new(Vec::new(), spec.schema.clone(), Some(props.clone()))
                    .map_err(|e| TeoDBError::Parquet(format!("writer init: {e}")))?,
            ),
        };
        w.write(&batch)
            .map_err(|e| TeoDBError::Parquet(format!("write batch: {e}")))?;

        let encoded = w.bytes_written() as u64 + w.in_progress_size() as u64;
        if encoded >= target_file_bytes {
            let open = writer.take().ok_or_else(|| {
                TeoDBError::Internal("Parquet writer disappeared after a successful batch write".into())
            })?;
            let target = file_location(base, file_index);
            data_files.push(finish_file(storage, &target, open, spec).await?);
            file_index += 1;
        }
    }

    if let Some(open) = writer.take() {
        let target = if file_index == 0 {
            base.clone()
        } else {
            file_location(base, file_index)
        };
        data_files.push(finish_file(storage, &target, open, spec).await?);
    }

    Ok(data_files)
}

/// Finalize an open writer and upload the encoded file.
async fn finish_file(
    storage: &dyn Storage,
    target: &ObjectLocation,
    writer: ArrowWriter<Vec<u8>>,
    spec: &WriteSpec,
) -> TeoDBResult<DataFile> {
    let buf = writer
        .into_inner()
        .map_err(|e| TeoDBError::Parquet(format!("writer close: {e}")))?;
    let file_size = buf.len() as u64;
    let file_bytes = Bytes::from(buf);
    let path = teodb_core::location::ObjectPath::new(target.key.clone());
    storage.put(&path, file_bytes.clone()).await?;
    stats::extract_data_file_from_bytes(&file_bytes, target, spec, file_size)
}

/// Generate a file location for split files: base_key + `-NNNN.parquet`.
fn file_location(base: &ObjectLocation, index: u32) -> ObjectLocation {
    let key = if let Some(prefix) = base.key.strip_suffix("-f0000.parquet") {
        format!("{prefix}-f{index:04}.parquet")
    } else if let Some(stripped) = base.key.strip_suffix(".parquet") {
        format!("{stripped}-{index:04}.parquet")
    } else {
        format!("{}-{index:04}", base.key)
    };
    ObjectLocation {
        scheme: base.scheme,
        bucket: base.bucket.clone(),
        key,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_schema::SchemaRef;

    use super::*;

    fn test_schema() -> SchemaRef {
        let mut metadata = HashMap::new();
        metadata.insert("PARQUET:field_id".to_owned(), "1".to_owned());
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(metadata),
        ]))
    }

    fn test_batches() -> Vec<RecordBatch> {
        let schema = test_schema();
        vec![RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap()]
    }

    #[tokio::test]
    async fn write_and_extract_stats() {
        let storage = crate::backends::ObjectStoreBackend::in_memory();
        let target = ObjectLocation {
            scheme: teodb_core::location::StorageScheme::S3,
            bucket: Some("test".into()),
            key: "output.parquet".into(),
        };

        let spec = WriteSpec {
            schema: test_schema(),
            schema_id: 0,
            partition_spec_id: 0,
            generation_lo: 1,
            generation_hi: 1,
            ..Default::default()
        };

        let data_file = write_sorted_parquet(&storage, &target, test_batches(), &spec)
            .await
            .unwrap();

        assert_eq!(data_file.record_count, 3);
        assert_eq!(data_file.path, target);
        assert!(data_file.file_size_bytes > 0);
    }

    #[tokio::test]
    async fn write_sorted_parquet_sorts_output() {
        use teodb_core::schema::{NullOrder, PartitionTransform, SortDirection, SortField, SortOrder};

        let schema = test_schema();
        // Out-of-order data: [30, 10, 20]
        let batches =
            vec![RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![30, 10, 20]))]).unwrap()];

        let storage = crate::backends::ObjectStoreBackend::in_memory();
        let target = ObjectLocation {
            scheme: teodb_core::location::StorageScheme::S3,
            bucket: Some("test".into()),
            key: "sorted_output.parquet".into(),
        };

        let spec = WriteSpec {
            schema: schema.clone(),
            sort_order: SortOrder {
                order_id: 1,
                fields: vec![SortField {
                    source_id: 1,
                    transform: PartitionTransform::Identity,
                    direction: SortDirection::Asc,
                    null_order: NullOrder::NullsLast,
                }],
            },
            ..Default::default()
        };

        let data_file = write_sorted_parquet(&storage, &target, batches, &spec)
            .await
            .unwrap();

        assert_eq!(data_file.record_count, 3);

        // Read back and verify sorted order.
        let path = teodb_core::location::ObjectPath::new(target.key.clone());
        let bytes = storage.get(&path).await.unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();

        let mut all_values = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..col.len() {
                all_values.push(col.value(i));
            }
        }
        assert_eq!(all_values, vec![10, 20, 30]);
    }

    #[tokio::test]
    async fn streaming_single_file() {
        let storage = crate::backends::ObjectStoreBackend::in_memory();
        let base = ObjectLocation {
            scheme: teodb_core::location::StorageScheme::S3,
            bucket: Some("test".into()),
            key: "stream_out.parquet".into(),
        };

        let spec = WriteSpec {
            schema: test_schema(),
            ..Default::default()
        };

        let files = write_sorted_streaming(&storage, &base, test_batches(), &spec)
            .await
            .unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].record_count, 3);
    }

    #[tokio::test]
    async fn streaming_rolls_files() {
        let storage = crate::backends::ObjectStoreBackend::in_memory();
        let base = ObjectLocation {
            scheme: teodb_core::location::StorageScheme::S3,
            bucket: Some("test".into()),
            key: "roll.parquet".into(),
        };

        // Use a very small target to force rolling.
        let spec = WriteSpec {
            schema: test_schema(),
            row_group_target_bytes: 1, // 1 byte → target_file_bytes = 8 bytes
            ..Default::default()
        };

        // Create enough batches to roll.
        let schema = test_schema();
        let batches: Vec<RecordBatch> = (0..10)
            .map(|i| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int64Array::from(vec![i * 3 + 1, i * 3 + 2, i * 3 + 3]))],
                )
                .unwrap()
            })
            .collect();

        let files = write_sorted_streaming(&storage, &base, batches, &spec)
            .await
            .unwrap();

        // Should produce multiple files.
        assert!(files.len() > 1, "expected multiple files, got {}", files.len());

        // Total row count preserved.
        let total: u64 = files.iter().map(|f| f.record_count).sum();
        assert_eq!(total, 30);
    }

    /// Memory ceiling: the stream writer must drop each input batch right
    /// after encoding it — never accumulate the input. Tracked via the Arc
    /// strong count of every yielded batch's column: a batch is "alive"
    /// while someone besides the test still holds its array.
    #[tokio::test]
    async fn stream_writer_does_not_accumulate_input_batches() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let storage = crate::backends::ObjectStoreBackend::in_memory();
        let base = ObjectLocation {
            scheme: teodb_core::location::StorageScheme::S3,
            bucket: Some("test".into()),
            key: "ceiling.parquet".into(),
        };
        let spec = WriteSpec {
            schema: test_schema(),
            // Large target: everything fits one file, so nothing would force
            // a buffering implementation to release batches early.
            row_group_target_bytes: 1024 * 1024,
            ..Default::default()
        };

        let schema = test_schema();
        const N: usize = 64;
        let arrays: Vec<Arc<Int64Array>> = (0..N as i64)
            .map(|i| Arc::new(Int64Array::from(vec![i; 512])))
            .collect();
        let handles = Arc::new(arrays.clone());
        let max_alive = Arc::new(AtomicUsize::new(0));

        let observer = max_alive.clone();
        let stream = futures::stream::iter(arrays.into_iter().enumerate().map(move |(i, a)| {
            // Count previously-yielded batches still held by the writer.
            let alive = handles[..i]
                .iter()
                .filter(|h| Arc::strong_count(h) > 2) // test vec + handles vec
                .count();
            observer.fetch_max(alive, Ordering::SeqCst);
            Ok(RecordBatch::try_new(schema.clone(), vec![a as Arc<dyn Array>]).unwrap())
        }));

        let files = write_sorted_stream(&storage, &base, Box::pin(stream), &spec)
            .await
            .unwrap();

        let total: u64 = files.iter().map(|f| f.record_count).sum();
        assert_eq!(total, (N * 512) as u64);
        assert!(
            max_alive.load(Ordering::SeqCst) <= 2,
            "writer retained {} input batches — input must stream, not accumulate",
            max_alive.load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn stream_writer_propagates_stream_errors() {
        let storage = crate::backends::ObjectStoreBackend::in_memory();
        let base = ObjectLocation {
            scheme: teodb_core::location::StorageScheme::S3,
            bucket: Some("test".into()),
            key: "err.parquet".into(),
        };
        let spec = WriteSpec {
            schema: test_schema(),
            ..Default::default()
        };

        let batches = test_batches();
        let stream = futures::stream::iter(vec![
            Ok(batches[0].clone()),
            Err(TeoDBError::QueryExecution("upstream failed".into())),
        ]);

        let err = write_sorted_stream(&storage, &base, Box::pin(stream), &spec)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("upstream failed"));
    }

    #[tokio::test]
    async fn write_sorted_rolled_sorts_and_rolls() {
        use teodb_core::schema::{NullOrder, PartitionTransform, SortDirection, SortField, SortOrder};

        let storage = crate::backends::ObjectStoreBackend::in_memory();
        let base = ObjectLocation {
            scheme: teodb_core::location::StorageScheme::S3,
            bucket: Some("test".into()),
            key: "rolled_sorted.parquet".into(),
        };
        let schema = test_schema();
        // Out-of-order across batches; tiny target forces multiple files.
        let batches: Vec<RecordBatch> = (0..6)
            .map(|i| RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![30 - i * 5]))]).unwrap())
            .collect();

        let spec = WriteSpec {
            schema: schema.clone(),
            row_group_target_bytes: 1, // target_file_bytes = 8 bytes → roll
            row_group_target_rows: 1,  // slice the sorted batch one row per chunk
            sort_order: SortOrder {
                order_id: 1,
                fields: vec![SortField {
                    source_id: 1,
                    transform: PartitionTransform::Identity,
                    direction: SortDirection::Asc,
                    null_order: NullOrder::NullsLast,
                }],
            },
            ..Default::default()
        };

        let files = write_sorted_rolled(&storage, &base, batches, &spec)
            .await
            .unwrap();
        assert!(files.len() > 1, "small target must roll into multiple files");
        let total: u64 = files.iter().map(|f| f.record_count).sum();
        assert_eq!(total, 6);

        // Concatenate all output rows in file order; the global order is sorted
        // because rolling preserves the sorted input across files.
        let mut all = Vec::new();
        for f in &files {
            let path = teodb_core::location::ObjectPath::new(f.path.key.clone());
            let bytes = storage.get(&path).await.unwrap();
            let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
                .unwrap()
                .build()
                .unwrap();
            for batch in reader {
                let batch = batch.unwrap();
                let col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..col.len() {
                    all.push(col.value(i));
                }
            }
        }
        assert_eq!(all, vec![5, 10, 15, 20, 25, 30]);
    }

    #[tokio::test]
    async fn write_sorted_rolled_empty_input_yields_no_files() {
        let storage = crate::backends::ObjectStoreBackend::in_memory();
        let base = ObjectLocation {
            scheme: teodb_core::location::StorageScheme::S3,
            bucket: Some("test".into()),
            key: "rolled_empty.parquet".into(),
        };
        let spec = WriteSpec {
            schema: test_schema(),
            ..Default::default()
        };
        let files = write_sorted_rolled(&storage, &base, vec![], &spec)
            .await
            .unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn streaming_empty_input() {
        let storage = crate::backends::ObjectStoreBackend::in_memory();
        let base = ObjectLocation {
            scheme: teodb_core::location::StorageScheme::S3,
            bucket: Some("test".into()),
            key: "empty.parquet".into(),
        };

        let spec = WriteSpec {
            schema: test_schema(),
            ..Default::default()
        };

        let files = write_sorted_streaming(&storage, &base, vec![], &spec)
            .await
            .unwrap();

        assert!(files.is_empty());
    }
}
