//! Admin domain module — status, dashboard, and cluster endpoints.

mod handler;
pub(crate) mod model;
mod router;

pub use handler::{all_tables, cluster, status};
pub use router::routes;
