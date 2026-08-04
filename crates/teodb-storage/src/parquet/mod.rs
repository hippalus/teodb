//! Parquet write/read for TeoDB data files.
//!
//! - [`compression`] — codec configuration (operator strings ↔ parquet codecs)
//! - [`spec`] — the write contract (`WriteSpec`) and its `WriterProperties`
//! - [`writer`] — sorted single-file and rolling streaming write paths
//! - [`stats`] — footer statistics extraction into `DataFile`
//! - [`delete`] — position-delete file reader

pub mod compression;
pub mod delete;
mod sort;
pub mod spec;
pub mod stats;
pub mod writer;

pub use compression::{CompressionCodec, CompressionParseError};
pub use delete::{PositionDeleteMap, read_position_deletes};
pub use spec::{WriteSpec, WriteSpecBuilder};
pub use stats::extract_data_file_from_bytes;
pub use writer::{write_sorted_parquet, write_sorted_rolled, write_sorted_stream, write_sorted_streaming};
