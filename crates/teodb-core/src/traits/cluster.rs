//! Cluster-topology boundary trait.
//!
//! Lets the admin/HTTP layer (`teodb-api`) report distributed cluster
//! topology — registered executors, the scheduler, in-flight jobs — without
//! depending on `teodb-distributed` (which would invert the crate dependency
//! order). The concrete implementation lives in `teodb-distributed` and is
//! injected by the `teodb-server` composition root.

use async_trait::async_trait;

/// A worker (Ballista executor) as reported by the cluster scheduler.
#[derive(Debug, Clone)]
pub struct ClusterWorker {
    pub id: String,
    pub host: String,
    /// The executor's advertised gRPC/Flight port.
    pub port: u16,
    /// Epoch milliseconds of the last heartbeat; `None` if it has registered
    /// but not yet heartbeated.
    pub last_heartbeat_ms: Option<u64>,
    /// True when the last heartbeat is within the configured liveness window.
    pub alive: bool,
}

/// A point-in-time view of distributed cluster topology.
#[derive(Debug, Clone, Default)]
pub struct ClusterTopologySnapshot {
    /// Executors currently known to the scheduler.
    pub workers: Vec<ClusterWorker>,
    /// Jobs the scheduler still owns work for (queued or running).
    pub active_jobs: u64,
    /// The scheduler endpoint this snapshot was taken from.
    pub scheduler_address: String,
    /// True when the scheduler answered the query.
    pub scheduler_reachable: bool,
}

/// Reads distributed cluster topology for the admin surface.
#[async_trait]
pub trait ClusterTopology: Send + Sync + 'static {
    /// Return the current topology. A transient scheduler outage must not be an
    /// error — set `scheduler_reachable = false` and return what is known so
    /// the admin UI can distinguish "no cluster" from "scheduler unreachable".
    async fn snapshot(&self) -> ClusterTopologySnapshot;
}
