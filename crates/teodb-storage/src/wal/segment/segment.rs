//! WAL segment frame format and serialization.
//!
//! Frame layout (all values little-endian):
//! ```text
//! +------------------+------------------+-----------+---------+
//! | length: u32 LE   | crc32: u32 LE    |  payload  | padding |
//! +------------------+------------------+-----------+---------+
//!                                       |<-length-->| up to 7 |
//! ```
//!
//! Payload = JSON-encoded `WalHeader` + `\n` + Arrow IPC stream bytes.
//! Padding aligns the next frame to an 8-byte boundary.

use std::io::Cursor;
use std::path::Path;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::{Generation, TableIdent};
use teodb_core::write_protocol::{WRITE_PROTOCOL_VERSION, WalTableKey};

/// A single durable WAL entry.
#[derive(Debug, Clone)]
pub struct WalRecord {
    pub header: WalHeader,
    pub batch: RecordBatch,
}

impl WalRecord {
    /// Build a drop tombstone for a table. The tombstone voids every WAL
    /// record for that table appended before it, so replay never resurrects
    /// a dropped table (or leaks an old incarnation's rows into a recreated
    /// one). Tombstones carry no batch payload.
    pub fn drop_tombstone(table: TableIdent) -> Self {
        Self {
            header: WalHeader {
                protocol_version: WRITE_PROTOCOL_VERSION,
                table_uuid: None,
                batch_id: uuid::Uuid::now_v7(),
                table,
                schema_id: 0,
                generation: 0,
                created_at_ms: chrono::Utc::now().timestamp_millis(),
                idempotency_key: None,
                row_count: 0,
                byte_count: 0,
                op: WalOp::DropTable,
            },
            batch: RecordBatch::new_empty(std::sync::Arc::new(arrow::datatypes::Schema::empty())),
        }
    }
}

/// What a WAL frame represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalOp {
    /// An ingested batch.
    Append,
    /// The table was dropped: all earlier records for it are void.
    DropTable,
}

/// Header metadata serialized as JSON inside each WAL frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalHeader {
    pub protocol_version: u16,
    pub table_uuid: Option<uuid::Uuid>,
    pub batch_id: uuid::Uuid,
    pub table: TableIdent,
    pub schema_id: i32,
    pub generation: Generation,
    pub created_at_ms: i64,
    pub idempotency_key: Option<String>,
    pub row_count: u64,
    pub byte_count: u64,
    pub op: WalOp,
}

impl WalHeader {
    pub fn table_key(&self) -> TeoDBResult<WalTableKey> {
        self.validate()?;
        let table_uuid = self
            .table_uuid
            .ok_or_else(|| TeoDBError::wal("append WAL record is missing table_uuid"))?;
        Ok(WalTableKey::new(table_uuid, self.table.clone()))
    }

    pub fn validate(&self) -> TeoDBResult<()> {
        if self.protocol_version != WRITE_PROTOCOL_VERSION {
            return Err(TeoDBError::wal(format!(
                "WAL record must use protocol version {WRITE_PROTOCOL_VERSION}, found {}",
                self.protocol_version
            )));
        }
        match (self.op, self.table_uuid) {
            (WalOp::Append, None) => {
                return Err(TeoDBError::wal("append WAL record is missing table_uuid"));
            }
            (WalOp::Append, Some(table_uuid)) if table_uuid.is_nil() => {
                return Err(TeoDBError::wal("append WAL record has a nil table_uuid"));
            }
            (WalOp::DropTable, Some(_)) => {
                return Err(TeoDBError::wal("drop-table WAL record must not carry table_uuid"));
            }
            _ => {}
        }
        Ok(())
    }
}

pub(crate) const FRAME_HEADER_SIZE: usize = 8; // 4 bytes length + 4 bytes crc32
const ALIGNMENT: usize = 8;

/// Upper bound for a plausible frame payload. A length prefix above this is
/// treated as corruption rather than a torn write: real ingest batches are
/// orders of magnitude smaller (REST bodies are capped at tens of MiB), while
/// a corrupted length field is typically a huge random value.
pub(crate) const MAX_PAYLOAD_BYTES: usize = 1 << 30; // 1 GiB

/// Result of decoding one frame from a byte slice.
#[derive(Debug)]
pub enum FrameDecode {
    /// A complete, valid frame and the total bytes consumed (incl. padding).
    Complete(Box<WalRecord>, usize),
    /// Not enough bytes remain for a complete frame. Benign at the tail of a
    /// segment that was being written when the process stopped (torn write —
    /// the record was never fully persisted, so it was never ACKed).
    Incomplete,
    /// Structurally invalid bytes: corrupt length field, CRC mismatch,
    /// undecodable header or batch. A sequential scan cannot locate any
    /// frame past this point, so the rest of the segment is unrecoverable.
    Corrupt(String),
}

/// Encode a `WalRecord` into a complete frame (header + payload + padding).
pub fn encode_frame(record: &WalRecord) -> TeoDBResult<Vec<u8>> {
    record.header.validate()?;
    // Serialize header as JSON.
    let header_json = serde_json::to_vec(&record.header).map_err(|e| TeoDBError::wal(format!("header json: {e}")))?;

    // Combine header JSON + newline + IPC bytes (tombstones carry no batch).
    let mut payload = header_json;
    payload.push(b'\n');
    if record.header.op == WalOp::Append {
        let mut ipc_buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc_buf, &record.batch.schema())
                .map_err(|e| TeoDBError::Arrow(format!("IPC writer init: {e}")))?;
            writer
                .write(&record.batch)
                .map_err(|e| TeoDBError::Arrow(format!("IPC write: {e}")))?;
            writer
                .finish()
                .map_err(|e| TeoDBError::Arrow(format!("IPC finish: {e}")))?;
        }
        payload.extend_from_slice(&ipc_buf);
    }

    let payload_len = payload.len() as u32;
    let crc = crc32fast::hash(&payload);

    // Calculate padding for 8-byte alignment.
    let total_content = FRAME_HEADER_SIZE + payload.len();
    let padding = (ALIGNMENT - (total_content % ALIGNMENT)) % ALIGNMENT;

    let mut frame = Vec::with_capacity(total_content + padding);
    frame.extend_from_slice(&payload_len.to_le_bytes());
    frame.extend_from_slice(&crc.to_le_bytes());
    frame.extend_from_slice(&payload);
    frame.resize(frame.len() + padding, 0);

    Ok(frame)
}

/// Decode a frame from raw bytes. Panic-free by construction: any byte
/// sequence yields `Complete`, `Incomplete`, or `Corrupt`.
pub fn decode_frame(data: &[u8]) -> FrameDecode {
    if data.len() < FRAME_HEADER_SIZE {
        return FrameDecode::Incomplete;
    }

    let payload_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let expected_crc = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);

    if payload_len == 0 {
        // A zero length field is either zero-filled padding/preallocation at
        // the end of a segment (benign) or a torn header write. Distinguish
        // by the remaining bytes: all-zeros is an expected tail; anything
        // else means the length field itself was destroyed.
        return if data.iter().all(|&b| b == 0) {
            FrameDecode::Incomplete
        } else {
            FrameDecode::Corrupt("zero payload length with non-zero trailing bytes".into())
        };
    }

    if payload_len > MAX_PAYLOAD_BYTES {
        return FrameDecode::Corrupt(format!(
            "implausible payload length {payload_len} (corrupt length field)"
        ));
    }

    let total_content = FRAME_HEADER_SIZE + payload_len;
    let padding = (ALIGNMENT - (total_content % ALIGNMENT)) % ALIGNMENT;
    let frame_size = total_content + padding;

    if data.len() < total_content {
        // Partial frame — torn write at the end of a segment.
        return FrameDecode::Incomplete;
    }

    let payload = &data[FRAME_HEADER_SIZE..total_content];

    // Verify CRC.
    let actual_crc = crc32fast::hash(payload);
    if actual_crc != expected_crc {
        return FrameDecode::Corrupt(format!("CRC mismatch: expected {expected_crc:#x}, got {actual_crc:#x}"));
    }

    // Split payload at first newline into header JSON and IPC bytes.
    let Some(newline_pos) = payload.iter().position(|&b| b == b'\n') else {
        return FrameDecode::Corrupt("no newline in WAL payload".into());
    };

    let header: WalHeader = match serde_json::from_slice(&payload[..newline_pos]) {
        Ok(h) => h,
        Err(e) => return FrameDecode::Corrupt(format!("header json parse: {e}")),
    };
    if let Err(error) = header.validate() {
        return FrameDecode::Corrupt(format!("invalid WAL header: {error}"));
    }

    if header.op == WalOp::DropTable {
        let batch = RecordBatch::new_empty(std::sync::Arc::new(arrow::datatypes::Schema::empty()));
        return FrameDecode::Complete(Box::new(WalRecord { header, batch }), frame_size);
    }

    let ipc_bytes = &payload[newline_pos + 1..];
    let cursor = Cursor::new(ipc_bytes);
    let mut reader = match StreamReader::try_new(cursor, None) {
        Ok(r) => r,
        Err(e) => return FrameDecode::Corrupt(format!("IPC read init: {e}")),
    };

    let batch = match reader.next() {
        Some(Ok(b)) => b,
        Some(Err(e)) => return FrameDecode::Corrupt(format!("IPC read batch: {e}")),
        None => return FrameDecode::Corrupt("no batch in IPC stream".into()),
    };

    FrameDecode::Complete(Box::new(WalRecord { header, batch }), frame_size)
}

/// One frame seen while scanning a segment, in append order.
#[derive(Debug, PartialEq, Eq)]
pub enum ScanFrame {
    Append { key: WalTableKey, generation: Generation },
    DropTable { table: TableIdent },
}

/// Outcome of scanning a segment's frames for GC eligibility.
#[derive(Debug)]
pub struct SegmentScan {
    /// Frames in append order (position matters for tombstone reasoning).
    pub frames: Vec<ScanFrame>,
    /// True when the scan hit a corrupt frame. `frames` then covers only the
    /// frames *before* the corruption — frames after it are unknown, so the
    /// segment must never be treated as fully committed (and never GC'd).
    pub corrupt: bool,
}

/// Scan a segment file and return its frames in append order.
/// Used by the GC to determine if a segment is fully committed.
pub async fn scan_segment(path: &Path) -> TeoDBResult<SegmentScan> {
    let data = tokio::fs::read(path)
        .await
        .map_err(|e| TeoDBError::wal(format!("read segment {}: {e}", path.display())))?;

    let mut frames = Vec::new();
    let mut corrupt = false;
    let mut offset = 0;

    while offset < data.len() {
        match decode_frame(&data[offset..]) {
            FrameDecode::Complete(record, consumed) => {
                frames.push(match record.header.op {
                    WalOp::Append => ScanFrame::Append {
                        key: record.header.table_key()?,
                        generation: record.header.generation,
                    },
                    WalOp::DropTable => ScanFrame::DropTable {
                        table: record.header.table,
                    },
                });
                offset += consumed;
            }
            // Torn tail: the partial frame was never fully written, so it
            // was never ACKed — the segment is still safe to evaluate.
            FrameDecode::Incomplete => break,
            FrameDecode::Corrupt(reason) => {
                tracing::warn!(path = %path.display(), offset, reason, "corrupt frame during GC scan");
                corrupt = true;
                break;
            }
        }
    }

    Ok(SegmentScan { frames, corrupt })
}
