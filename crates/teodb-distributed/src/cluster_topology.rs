//! Cluster-topology provider backed by the Ballista scheduler REST API.
//!
//! Implements the [`teodb_core::traits::cluster::ClusterTopology`] boundary
//! trait so the admin endpoint in `teodb-api` can report executors and jobs
//! without depending on this crate. Injected by the `teodb-server` composition
//! root for roles that participate in a Ballista cluster.

use std::time::Duration;

use async_trait::async_trait;
use teodb_core::error::TeoDBResult;
use teodb_core::traits::cluster::{ClusterTopology, ClusterTopologySnapshot, ClusterWorker};

use crate::scheduler_api::SchedulerApiClient;

/// Reports topology by polling the scheduler's `/api/executors` and `/api/jobs`.
pub struct SchedulerTopology {
    client: SchedulerApiClient,
    address: String,
    liveness_window: Duration,
}

impl SchedulerTopology {
    /// `scheduler_endpoint` accepts the same forms as `cluster.scheduler_addr`
    /// (`host:port` or `http://host:port`).
    pub fn new(scheduler_endpoint: &str, liveness_window: Duration, timeout: Duration) -> TeoDBResult<Self> {
        Ok(Self {
            client: SchedulerApiClient::new(scheduler_endpoint, timeout)?,
            address: scheduler_endpoint.to_string(),
            liveness_window,
        })
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

#[async_trait]
impl ClusterTopology for SchedulerTopology {
    async fn snapshot(&self) -> ClusterTopologySnapshot {
        // A scheduler outage is reported as `scheduler_reachable = false`, never
        // an error — the admin UI distinguishes that from standalone (no
        // provider at all).
        let executors = match self.client.list_executors().await {
            Ok(executors) => executors,
            Err(_) => {
                return ClusterTopologySnapshot {
                    scheduler_address: self.address.clone(),
                    scheduler_reachable: false,
                    ..Default::default()
                };
            }
        };

        let now = now_ms();
        let window_ms = self
            .liveness_window
            .as_millis()
            .min(u64::MAX as u128) as u64;
        let workers = executors
            .into_iter()
            .map(|e| ClusterWorker {
                alive: e
                    .last_seen
                    .is_some_and(|ts| now.saturating_sub(ts) <= window_ms),
                id: e.id,
                host: e.host,
                port: e.port,
                last_heartbeat_ms: e.last_seen,
            })
            .collect();

        // Jobs are best-effort: a failure here still yields a useful executor
        // view, so fall back to zero rather than dropping the whole snapshot.
        let active_jobs = self.client.active_job_count().await.unwrap_or(0) as u64;

        ClusterTopologySnapshot {
            workers,
            active_jobs,
            scheduler_address: self.address.clone(),
            scheduler_reachable: true,
        }
    }
}
