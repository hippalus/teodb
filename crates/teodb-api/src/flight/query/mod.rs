//! Flight SQL query endpoints.

mod metadata;
mod service;

pub use service::{do_get, get_flight_info, get_schema, poll_flight_info};
