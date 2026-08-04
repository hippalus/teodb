//! Namespace domain module — CRUD operations on Iceberg namespaces.

mod handler;
pub(crate) mod model;
mod router;

pub use handler::{create_namespace, drop_namespace, get_namespace, list_namespaces};
pub use router::routes;
