use std::net::SocketAddr;
use std::sync::Arc;

use datafusion_proto::logical_plan::LogicalExtensionCodec;
use teodb_core::error::{TeoDBError, TeoDBResult};
use tracing::{info, warn};

use super::{HostPort, SchedulerConfig};

/// Start the Ballista scheduler.
///
/// Binds to the configured address and runs the Ballista scheduler gRPC service.
/// The scheduler accepts executor registrations and distributes query tasks.
pub async fn start_scheduler(
    config: SchedulerConfig,
    _catalog: Arc<dyn teodb_core::traits::catalog::Catalog>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> TeoDBResult<()> {
    let bind_endpoint = HostPort {
        host: config.bind_addr.clone(),
        port: config.bind_port,
    }
    .authority();
    let addr: SocketAddr = bind_endpoint
        .parse()
        .map_err(|e| TeoDBError::Internal(format!("invalid scheduler bind address: {e}")))?;

    let codec: Arc<dyn LogicalExtensionCodec> = Arc::new(crate::codec::TeoLogicalExtensionCodec::new());

    let scheduler_config = Arc::new(ballista_scheduler::config::SchedulerConfig {
        external_host: config.external_host.clone(),
        bind_host: config.bind_addr.clone(),
        bind_port: config.bind_port,
        executor_timeout_seconds: config.executor_timeout_seconds,
        expire_dead_executor_interval_seconds: config.expire_dead_executor_interval_seconds,
        override_logical_codec: Some(codec),
        ..Default::default()
    });

    let cluster = ballista_scheduler::cluster::BallistaCluster::new_from_config(&scheduler_config)
        .await
        .map_err(|e| TeoDBError::Internal(format!("failed to create Ballista cluster: {e}")))?;

    info!(
        bind = %addr,
        "starting Ballista scheduler"
    );

    // Run scheduler in a background task so we can select on shutdown.
    let mut handle = tokio::spawn(async move {
        ballista_scheduler::scheduler_process::start_server(cluster, addr, scheduler_config)
            .await
            .map_err(|e| TeoDBError::Internal(format!("Ballista scheduler error: {e}")))
    });

    tokio::select! {
        result = &mut handle => {
            result.unwrap_or_else(|e| Err(TeoDBError::Internal(format!("scheduler task panicked: {e}"))))
        }
        _ = shutdown.changed() => {
            info!("Ballista scheduler stopping on shutdown signal");
            let endpoint = HostPort {
                host: config.external_host.clone(),
                port: config.bind_port,
            }
            .authority();
            drain_scheduler_jobs(&endpoint, config.drain_timeout, std::time::Duration::from_millis(500)).await;
            handle.abort();
            let _ = handle.await;
            Ok(())
        }
    }
}

/// Hold the scheduler alive until it owns no queued or running jobs, bounded
/// by `drain_timeout`. The scheduler keeps all job state in memory, so
/// aborting it mid-job fails every in-flight distributed query; waiting for
/// active jobs to settle is the only drain Ballista 53 allows (there is no
/// API to fence the scheduler against new submissions).
pub(super) async fn drain_scheduler_jobs(
    endpoint: &str,
    drain_timeout: std::time::Duration,
    poll_interval: std::time::Duration,
) {
    if drain_timeout.is_zero() {
        return;
    }
    let api = match crate::scheduler_api::SchedulerApiClient::new(endpoint, std::time::Duration::from_secs(2)) {
        Ok(api) => api,
        Err(e) => {
            warn!(error = %e, "cannot build scheduler API client for drain; aborting immediately");
            return;
        }
    };

    let deadline = tokio::time::Instant::now() + drain_timeout;
    let mut consecutive_errors = 0u32;
    loop {
        match api.active_job_count().await {
            Ok(0) => {
                info!("scheduler drain complete: no queued or running jobs");
                return;
            }
            Ok(active) => {
                consecutive_errors = 0;
                info!(active_jobs = active, "scheduler draining: waiting for jobs to finish");
            }
            Err(e) => {
                // The API serves from the same process being drained; if it
                // stays unreachable the wait buys nothing.
                consecutive_errors += 1;
                if consecutive_errors >= 3 {
                    warn!(error = %e, "scheduler API unreachable during drain; aborting");
                    return;
                }
            }
        }
        if tokio::time::Instant::now() + poll_interval > deadline {
            warn!(
                timeout_secs = drain_timeout.as_secs(),
                "scheduler drain window elapsed with jobs still active; aborting"
            );
            return;
        }
        tokio::time::sleep(poll_interval).await;
    }
}
