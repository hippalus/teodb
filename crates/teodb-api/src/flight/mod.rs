//! Arrow Flight ingestion and FlightSQL query service.

mod auth;
mod codec;
pub mod error;
mod ingest;
mod prepared;
mod query;
mod server;
pub mod trace;
mod validate;

pub use server::TeoFlightService;
