//! Concrete composition-root builders grouped by owned runtime component.

mod app_state;
mod ingest;
mod query;
mod wal;

pub(super) use app_state::{AppStateDependencies, build_app_state};
pub(super) use ingest::build_ingest_components;
pub(super) use query::build_query_engine;
pub(super) use wal::open_wal;
