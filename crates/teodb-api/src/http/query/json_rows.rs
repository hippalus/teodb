//! Single-pass serialization of query result batches into the response
//! `rows` JSON array (F-31/D2).
//!
//! Batches are written once through arrow-json's `ArrayWriter` with explicit
//! nulls, and the finished bytes are spliced into the response as a
//! [`RawValue`] — no NDJSON intermediate, no per-line `serde_json` re-parse,
//! and no second serialization when axum encodes the response body.

use arrow::json::WriterBuilder;
use arrow::json::writer::JsonArray;
use arrow::record_batch::RecordBatch;
use serde_json::value::RawValue;

use teodb_core::error::{TeoDBError, TeoDBResult};

const MIN_JSON_CONVERSION_INPUT_BYTES: usize = 64 * 1024;

struct BoundedJsonBuffer {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl BoundedJsonBuffer {
    fn new(limit: usize, exceeded: std::sync::Arc<std::sync::atomic::AtomicBool>) -> Self {
        Self {
            bytes: Vec::with_capacity(limit.min(8 * 1024)),
            limit,
            exceeded,
        }
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl std::io::Write for BoundedJsonBuffer {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let Some(projected) = self.bytes.len().checked_add(buffer.len()) else {
            self.exceeded
                .store(true, std::sync::atomic::Ordering::Relaxed);
            return Err(std::io::Error::other("JSON result byte limit exceeded"));
        };
        if projected > self.limit {
            self.exceeded
                .store(true, std::sync::atomic::Ordering::Relaxed);
            return Err(std::io::Error::other("JSON result byte limit exceeded"));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Incrementally serializes record batches into a JSON array of row objects,
/// truncating at a row limit.
pub struct JsonRowsWriter {
    rows: Vec<u8>,
    remaining: usize,
    written: usize,
    max_bytes: usize,
}

impl JsonRowsWriter {
    pub fn new(limit: usize, max_bytes: u64) -> Self {
        Self {
            rows: vec![b'['],
            remaining: limit,
            written: 0,
            max_bytes: usize::try_from(max_bytes).unwrap_or(usize::MAX),
        }
    }

    /// Serialize up to `remaining` rows of `batch`. Returns `true` while the
    /// writer can accept more rows, `false` once the limit is reached.
    pub fn write(&mut self, batch: &RecordBatch) -> TeoDBResult<bool> {
        if self.remaining == 0 {
            return Ok(false);
        }
        let take = batch.num_rows().min(self.remaining);
        let sliced;
        let to_write = if take < batch.num_rows() {
            sliced = batch.slice(0, take);
            &sliced
        } else {
            batch
        };
        let separator = usize::from(self.written > 0 && take > 0);
        let fixed_bytes = self
            .rows
            .len()
            .saturating_add(separator)
            .saturating_add(1);
        let Some(inner_budget) = self.max_bytes.checked_sub(fixed_bytes) else {
            return Err(self.result_too_large());
        };

        // Arrow JSON accumulates a complete row in its own Vec before writing
        // it to the sink. Cap the source batch before conversion so a single
        // pathological value cannot create an unbounded scratch allocation.
        // The floor avoids treating Arrow's small-array bookkeeping as result
        // bytes when callers intentionally configure a tiny response budget.
        let conversion_input_limit = self
            .max_bytes
            .max(MIN_JSON_CONVERSION_INPUT_BYTES);
        if to_write.get_array_memory_size() > conversion_input_limit {
            return Err(self.result_too_large());
        }
        let exceeded = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sink = BoundedJsonBuffer::new(inner_budget.saturating_add(2), exceeded.clone());
        let mut writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, JsonArray>(sink);
        if let Err(error) = writer.write(to_write) {
            return Err(self.json_error("JSON serialization failed", error, &exceeded));
        }
        if let Err(error) = writer.finish() {
            return Err(self.json_error("JSON writer finish failed", error, &exceeded));
        }
        let encoded = writer.into_inner().into_inner();
        let inner = encoded
            .strip_prefix(b"[")
            .and_then(|encoded| encoded.strip_suffix(b"]"))
            .ok_or_else(|| TeoDBError::Internal("JSON writer produced a non-array result".into()))?;
        let projected = self
            .rows
            .len()
            .saturating_add(separator)
            .saturating_add(inner.len())
            .saturating_add(1);
        if projected > self.max_bytes {
            return Err(TeoDBError::ResultTooLarge {
                limit_bytes: self.max_bytes as u64,
            });
        }
        if separator == 1 {
            self.rows.push(b',');
        }
        self.rows.extend_from_slice(inner);
        self.remaining -= take;
        self.written += take;
        Ok(self.remaining > 0)
    }

    fn result_too_large(&self) -> TeoDBError {
        TeoDBError::ResultTooLarge {
            limit_bytes: self.max_bytes as u64,
        }
    }

    fn json_error(
        &self,
        context: &str,
        error: arrow::error::ArrowError,
        exceeded: &std::sync::atomic::AtomicBool,
    ) -> TeoDBError {
        if exceeded.load(std::sync::atomic::Ordering::Relaxed) {
            self.result_too_large()
        } else {
            TeoDBError::Internal(format!("{context}: {error}"))
        }
    }

    /// Finish the JSON array and return it with the number of rows written.
    pub fn finish(mut self) -> TeoDBResult<(Box<RawValue>, usize)> {
        if self.rows.len().saturating_add(1) > self.max_bytes {
            return Err(self.result_too_large());
        }
        self.rows.push(b']');
        let json = String::from_utf8(self.rows)
            .map_err(|e| TeoDBError::Internal(format!("JSON writer produced invalid UTF-8: {e}")))?;
        let raw = RawValue::from_string(json)
            .map_err(|e| TeoDBError::Internal(format!("JSON writer produced invalid JSON: {e}")))?;
        Ok((raw, self.written))
    }
}

/// Serialize already-materialized JSON row objects (DDL results).
pub fn rows_from_maps(rows: &[serde_json::Map<String, serde_json::Value>]) -> TeoDBResult<Box<RawValue>> {
    serde_json::value::to_raw_value(rows).map_err(|e| TeoDBError::Internal(format!("JSON serialization failed: {e}")))
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch(ids: &[i64], names: &[Option<&str>]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    fn parsed(raw: &RawValue) -> serde_json::Value {
        serde_json::from_str(raw.get()).unwrap()
    }

    #[test]
    fn empty_stream_yields_empty_array() {
        let (raw, count) = JsonRowsWriter::new(10, 1024).finish().unwrap();
        assert_eq!(raw.get(), "[]");
        assert_eq!(count, 0);
    }

    #[test]
    fn nulls_are_explicit() {
        let mut w = JsonRowsWriter::new(10, 1024);
        w.write(&batch(&[1, 2], &[Some("a"), None]))
            .unwrap();
        let (raw, count) = w.finish().unwrap();
        assert_eq!(count, 2);
        let rows = parsed(&raw);
        assert_eq!(rows[1]["name"], serde_json::Value::Null);
        assert_eq!(rows[0]["name"], "a");
    }

    #[test]
    fn limit_truncates_mid_batch_and_stops() {
        let mut w = JsonRowsWriter::new(3, 1024);
        assert!(
            w.write(&batch(&[1, 2], &[Some("a"), Some("b")]))
                .unwrap()
        );
        assert!(
            !w.write(&batch(&[3, 4], &[Some("c"), Some("d")]))
                .unwrap()
        );
        assert!(!w.write(&batch(&[5], &[Some("e")])).unwrap());
        let (raw, count) = w.finish().unwrap();
        assert_eq!(count, 3);
        let rows = parsed(&raw);
        assert_eq!(rows.as_array().unwrap().len(), 3);
        assert_eq!(rows[2]["id"], 3);
    }

    #[test]
    fn multiple_batches_concatenate() {
        let mut w = JsonRowsWriter::new(10, 1024);
        w.write(&batch(&[1], &[Some("a")])).unwrap();
        w.write(&batch(&[2], &[Some("b")])).unwrap();
        let (raw, count) = w.finish().unwrap();
        assert_eq!(count, 2);
        assert_eq!(parsed(&raw).as_array().unwrap().len(), 2);
    }

    #[test]
    fn rows_from_maps_round_trips() {
        let mut m = serde_json::Map::new();
        m.insert("status".into(), serde_json::Value::String("ok".into()));
        let raw = rows_from_maps(&[m]).unwrap();
        assert_eq!(parsed(&raw), serde_json::json!([{ "status": "ok" }]));
    }

    #[test]
    fn byte_limit_rejects_large_values_below_row_limit() {
        let large = "x".repeat(4_096);
        let names = [Some(large.as_str())];
        let mut writer = JsonRowsWriter::new(100_000, 128);
        assert!(matches!(
            writer.write(&batch(&[1], &names)),
            Err(TeoDBError::ResultTooLarge { limit_bytes: 128 })
        ));
    }

    #[test]
    fn bounded_json_buffer_never_allocates_past_its_limit() {
        let exceeded = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut buffer = BoundedJsonBuffer::new(32, exceeded.clone());
        assert_eq!(buffer.write(&[0; 32]).unwrap(), 32);
        assert!(buffer.write(&[1]).is_err());
        assert_eq!(buffer.bytes.len(), 32);
        assert!(exceeded.load(std::sync::atomic::Ordering::Relaxed));
    }
}
