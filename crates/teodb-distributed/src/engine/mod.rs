//! Ballista-backed query execution.

mod execution;
mod query_engine;

pub use query_engine::{BallistaMode, BallistaQueryEngine, BallistaQueryEngineBuilder, EngineEventObserver};
use query_engine::{BallistaQueryState, PinReleaser};

#[cfg(test)]
use datafusion::logical_expr::LogicalPlan;
#[cfg(test)]
use futures::StreamExt;
#[cfg(test)]
use std::{sync::Arc, time::Duration};
#[cfg(test)]
use teodb_core::{
    error::TeoDBError, query_id::QueryId, snapshot_pin::ActiveSnapshotRegistry, traits::query_engine::QueryStatus,
};
#[cfg(test)]
use teodb_query::{QueryEngine, QueryRequest};

#[cfg(test)]
use execution::{classify_planning_error, collect_scan_targets, is_scheduler_unreachable};

#[cfg(test)]
mod tests;
