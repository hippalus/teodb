//! WAL segment framing and scanning.

#[path = "segment.rs"]
mod frame;

pub(crate) use frame::{FRAME_HEADER_SIZE, MAX_PAYLOAD_BYTES};
pub use frame::{
    FrameDecode, ScanFrame, SegmentScan, WalHeader, WalOp, WalRecord, decode_frame, encode_frame, scan_segment,
};

#[cfg(test)]
use arrow::record_batch::RecordBatch;
#[cfg(test)]
use teodb_core::ident::{Generation, TableIdent};

#[cfg(test)]
mod tests;
