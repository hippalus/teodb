use std::sync::Arc;

use super::*;
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};

fn make_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![10, 20, 30]))]).unwrap()
}

fn make_record(generation: Generation) -> WalRecord {
    WalRecord {
        header: WalHeader {
            protocol_version: teodb_core::write_protocol::WRITE_PROTOCOL_VERSION,
            table_uuid: Some(uuid::Uuid::from_u128(1)),
            batch_id: uuid::Uuid::now_v7(),
            table: TableIdent::new("ns", "tbl"),
            schema_id: 0,
            generation,
            created_at_ms: 0,
            idempotency_key: None,
            row_count: 3,
            byte_count: 0,
            op: WalOp::Append,
        },
        batch: make_batch(),
    }
}

fn expect_complete(decode: FrameDecode) -> (WalRecord, usize) {
    match decode {
        FrameDecode::Complete(record, consumed) => (*record, consumed),
        other => panic!("expected Complete, got {other:?}"),
    }
}

#[test]
fn frame_roundtrip() {
    let record = make_record(1);
    let encoded = encode_frame(&record).unwrap();

    // Frame is 8-byte aligned.
    assert_eq!(encoded.len() % 8, 0);

    let (decoded, consumed) = expect_complete(decode_frame(&encoded));
    assert_eq!(consumed, encoded.len());
    assert_eq!(decoded.header.table, record.header.table);
    assert_eq!(decoded.header.generation, 1);
    assert_eq!(decoded.batch.num_rows(), 3);
}

#[test]
fn partial_frame_is_incomplete() {
    let record = make_record(1);
    let encoded = encode_frame(&record).unwrap();
    // Truncate to simulate torn write.
    let result = decode_frame(&encoded[..encoded.len() / 2]);
    assert!(matches!(result, FrameDecode::Incomplete), "got {result:?}");
}

#[test]
fn multiple_frames_decode() {
    let r1 = make_record(1);
    let r2 = make_record(2);
    let mut data = encode_frame(&r1).unwrap();
    data.extend_from_slice(&encode_frame(&r2).unwrap());

    let (d1, c1) = expect_complete(decode_frame(&data));
    assert_eq!(d1.header.generation, 1);

    let (d2, _c2) = expect_complete(decode_frame(&data[c1..]));
    assert_eq!(d2.header.generation, 2);
}

#[test]
fn truncation_at_every_prefix_never_panics() {
    let record = make_record(1);
    let encoded = encode_frame(&record).unwrap();

    // The payload ends before any trailing padding; once it is fully
    // present the frame decodes even if padding bytes are missing.
    let mut full_at = None;
    for i in 0..=encoded.len() {
        match decode_frame(&encoded[..i]) {
            FrameDecode::Incomplete => {
                assert!(full_at.is_none(), "Incomplete after Complete at {i}");
            }
            FrameDecode::Complete(_, consumed) => {
                assert_eq!(consumed, encoded.len());
                full_at.get_or_insert(i);
            }
            FrameDecode::Corrupt(reason) => {
                panic!("truncated valid frame must not be Corrupt at {i}: {reason}")
            }
        }
    }
    assert!(full_at.is_some(), "full frame should decode");
}

#[test]
fn every_single_byte_flip_never_panics() {
    let record = make_record(1);
    let encoded = encode_frame(&record).unwrap();

    for i in 0..encoded.len() {
        let mut mutated = encoded.clone();
        mutated[i] ^= 0xFF;
        // Any outcome is acceptable — the only requirement is no panic.
        let _ = decode_frame(&mutated);
    }
}

#[test]
fn crc_mismatch_is_corrupt() {
    let record = make_record(1);
    let mut encoded = encode_frame(&record).unwrap();
    // Flip a payload byte (past the 8-byte frame header).
    encoded[FRAME_HEADER_SIZE + 2] ^= 0xFF;

    let result = decode_frame(&encoded);
    assert!(
        matches!(result, FrameDecode::Corrupt(ref r) if r.contains("CRC")),
        "got {result:?}"
    );
}

#[test]
fn garbage_length_field_is_corrupt() {
    let record = make_record(1);
    let mut encoded = encode_frame(&record).unwrap();
    encoded[0..4].copy_from_slice(&u32::MAX.to_le_bytes());

    let result = decode_frame(&encoded);
    assert!(
        matches!(result, FrameDecode::Corrupt(ref r) if r.contains("length")),
        "got {result:?}"
    );
}

#[test]
fn all_zero_tail_is_incomplete() {
    // Zero-filled remainder (padding/preallocation) is a benign tail.
    let result = decode_frame(&[0u8; 64]);
    assert!(matches!(result, FrameDecode::Incomplete), "got {result:?}");
}

#[test]
fn zero_length_with_garbage_is_corrupt() {
    let mut data = vec![0u8; 64];
    data[20] = 0xAB; // non-zero past the zeroed header
    let result = decode_frame(&data);
    assert!(matches!(result, FrameDecode::Corrupt(_)), "got {result:?}");
}

#[tokio::test]
async fn scan_reports_corruption_with_prefix_gens() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("0001.wal");

    let mut data = encode_frame(&make_record(1)).unwrap();
    let mut second = encode_frame(&make_record(2)).unwrap();
    second[FRAME_HEADER_SIZE + 2] ^= 0xFF; // corrupt second frame payload
    data.extend_from_slice(&second);
    data.extend_from_slice(&encode_frame(&make_record(3)).unwrap());
    tokio::fs::write(&path, &data).await.unwrap();

    let scan = scan_segment(&path).await.unwrap();
    assert!(scan.corrupt, "scan must flag corruption");
    assert_eq!(
        scan.frames,
        vec![ScanFrame::Append {
            key: teodb_core::write_protocol::WalTableKey::new(uuid::Uuid::from_u128(1), TableIdent::new("ns", "tbl")),
            generation: 1
        }],
        "only frames before the corruption are visible"
    );
}

#[tokio::test]
async fn scan_clean_segment_not_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("0001.wal");

    let mut data = encode_frame(&make_record(1)).unwrap();
    data.extend_from_slice(&encode_frame(&make_record(2)).unwrap());
    tokio::fs::write(&path, &data).await.unwrap();

    let scan = scan_segment(&path).await.unwrap();
    assert!(!scan.corrupt);
    assert_eq!(scan.frames.len(), 2);
}

#[test]
fn tombstone_frame_roundtrip() {
    let record = WalRecord::drop_tombstone(TableIdent::new("ns", "tbl"));
    let encoded = encode_frame(&record).unwrap();

    let (decoded, consumed) = expect_complete(decode_frame(&encoded));
    assert_eq!(consumed, encoded.len());
    assert_eq!(decoded.header.op, WalOp::DropTable);
    assert_eq!(decoded.header.table, TableIdent::new("ns", "tbl"));
    assert_eq!(decoded.batch.num_rows(), 0);
}

#[test]
fn header_without_required_op_field_is_corrupt() {
    let record = make_record(7);
    let mut header_value = serde_json::to_value(&record.header).unwrap();
    header_value.as_object_mut().unwrap().remove("op");
    let header_json = serde_json::to_vec(&header_value).unwrap();

    let encoded = encode_frame(&record).unwrap();
    let old_payload = &encoded[FRAME_HEADER_SIZE..];
    let newline = old_payload
        .iter()
        .position(|&b| b == b'\n')
        .unwrap();
    // Old payload length comes from the original length field, not the
    // padded slice end.
    let old_len = u32::from_le_bytes(encoded[0..4].try_into().unwrap()) as usize;

    let mut payload = header_json;
    payload.push(b'\n');
    payload.extend_from_slice(&old_payload[newline + 1..old_len]);

    let crc = crc32fast::hash(&payload);
    let mut frame = Vec::new();
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&crc.to_le_bytes());
    frame.extend_from_slice(&payload);

    let decoded = decode_frame(&frame);
    assert!(
        matches!(decoded, FrameDecode::Corrupt(ref reason) if reason.contains("missing field `op`")),
        "got {decoded:?}"
    );
}

#[tokio::test]
async fn scan_reports_tombstones_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("0001.wal");

    let mut data = encode_frame(&make_record(1)).unwrap();
    data.extend_from_slice(&encode_frame(&WalRecord::drop_tombstone(TableIdent::new("ns", "tbl"))).unwrap());
    tokio::fs::write(&path, &data).await.unwrap();

    let scan = scan_segment(&path).await.unwrap();
    assert!(!scan.corrupt);
    assert_eq!(
        scan.frames,
        vec![
            ScanFrame::Append {
                key: teodb_core::write_protocol::WalTableKey::new(
                    uuid::Uuid::from_u128(1),
                    TableIdent::new("ns", "tbl")
                ),
                generation: 1
            },
            ScanFrame::DropTable {
                table: TableIdent::new("ns", "tbl")
            },
        ]
    );
}
