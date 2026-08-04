use super::*;
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::path::PathBuf;
use teodb_core::ident::{Generation, TableIdent};

mod basic;
mod checkpoint;
mod gc;
mod replay;
mod tombstone;

fn record_with_generation(generation: Generation) -> WalRecord {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let batch = RecordBatch::try_new(
        std::sync::Arc::new(schema),
        vec![std::sync::Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    WalRecord {
        header: WalHeader {
            protocol_version: teodb_core::write_protocol::WRITE_PROTOCOL_VERSION,
            table_uuid: Some(uuid::Uuid::from_u128(1)),
            batch_id: uuid::Uuid::now_v7(),
            table: TableIdent::new("test", "events"),
            schema_id: 0,
            generation,
            created_at_ms: chrono::Utc::now().timestamp_millis(),
            idempotency_key: None,
            row_count: 3,
            byte_count: 0,
            op: WalOp::Append,
        },
        batch,
    }
}

fn record_for_table(table: &TableIdent, generation: Generation) -> WalRecord {
    let mut record = record_with_generation(generation);
    record.header.table = table.clone();
    record
}

fn table_key(table: &TableIdent) -> teodb_core::write_protocol::WalTableKey {
    teodb_core::write_protocol::WalTableKey::new(uuid::Uuid::from_u128(1), table.clone())
}

fn sample_record() -> WalRecord {
    record_with_generation(1)
}

fn only_segment(dir: &std::path::Path) -> PathBuf {
    let mut segments: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.to_string_lossy().ends_with(".wal"))
        .collect();
    assert_eq!(segments.len(), 1, "expected exactly one segment");
    segments.remove(0)
}

fn corrupt_second_frame(path: &std::path::Path) {
    let mut data = std::fs::read(path).unwrap();
    let first_len = match segment::decode_frame(&data) {
        FrameDecode::Complete(_, consumed) => consumed,
        other => panic!("expected complete first frame, got {other:?}"),
    };
    data[first_len + 10] ^= 0xFF;
    std::fs::write(path, &data).unwrap();
}
