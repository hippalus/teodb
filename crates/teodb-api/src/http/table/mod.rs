//! Table domain module — CRUD operations on Iceberg tables.

mod handler;
pub(crate) mod model;
mod router;

pub use handler::{create_table, drop_table, get_table, list_tables};
pub use router::routes;
