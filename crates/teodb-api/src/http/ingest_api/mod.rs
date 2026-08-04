//! Ingest domain module — JSON row ingestion and buffer flush.

mod handler;
pub(crate) mod model;
mod router;

pub use handler::{flush_table, ingest_json};
pub use router::routes;
