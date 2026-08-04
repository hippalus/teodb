//! Durable ingestion domain: hot buffers, WAL replay, and flush coordination.

pub mod buffer;
pub mod config;
pub mod flush;
pub mod idempotency;
mod json;
pub mod replay;
pub mod service;
