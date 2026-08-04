//! Health probe domain module — liveness and readiness checks.

mod handler;
pub(crate) mod model;
mod router;

pub use handler::{liveness, readiness};
pub use router::routes;
