//! Minimal client for the Ballista scheduler REST API.
//!
//! The scheduler serves `GET /api/executors` on the same port as its gRPC
//! service (`rest-api` cargo feature, enabled by default). TeoDB uses it for
//! the executor-quorum readiness check: a node that depends on distributed
//! execution is not ready until the scheduler reports enough live executors.

use std::time::Duration;

use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_tracing::TracingMiddleware;
use serde::Deserialize;

use teodb_core::error::{TeoDBError, TeoDBResult};

/// One executor as reported by `GET /api/executors`. Unknown fields
/// (specification, metrics, os_info) are ignored.
#[derive(Debug, Clone, Deserialize)]
pub struct ExecutorState {
    pub id: String,
    pub host: String,
    pub port: u16,
    /// Epoch timestamp of the executor's last heartbeat in milliseconds
    /// (second precision — the scheduler converts heartbeat seconds).
    /// `None` when the executor registered but has not heartbeated yet.
    pub last_seen: Option<u64>,
}

/// One job as reported by `GET /api/jobs`. Unknown fields (stages,
/// progress, timestamps) are ignored.
#[derive(Debug, Clone, Deserialize)]
pub struct JobState {
    pub job_id: String,
    /// Plain status: `Queued`, `Running`, `Failed`, or `Completed`.
    pub status: String,
}

impl JobState {
    /// True while the scheduler still owns work for this job.
    pub fn is_active(&self) -> bool {
        matches!(self.status.as_str(), "Queued" | "Running")
    }
}

/// HTTP client for the scheduler REST API. Reuses one connection pool —
/// construct once and share (e.g. inside a readiness probe).
pub struct SchedulerApiClient {
    base_url: String,
    client: ClientWithMiddleware,
}

impl SchedulerApiClient {
    /// `scheduler_endpoint` accepts the same forms as `cluster.scheduler_addr`
    /// (`host:port` or `http://host:port`).
    pub fn new(scheduler_endpoint: &str, timeout: Duration) -> TeoDBResult<Self> {
        let base_url = crate::ballista::HostPort::parse(scheduler_endpoint, "cluster.scheduler_addr")?.http_url();
        // One reused, connection-pooling client with request tracing for the
        // cluster's readiness/topology probes. `timeout` bounds the whole
        // request; the connect/read/pool knobs keep a stalled scheduler from
        // hanging probes. No transient-failure retry middleware here on purpose:
        // these are fast-fail probes, and callers (e.g. drain) do their own
        // application-level retry — internal backoff would fight that.
        let transport = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(Duration::from_secs(5)))
            .read_timeout(timeout)
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(4)
            .tcp_keepalive(Duration::from_secs(60))
            .tcp_nodelay(true)
            .build()
            .map_err(|e| TeoDBError::Internal(format!("failed to build scheduler API client: {e}")))?;
        let client = ClientBuilder::new(transport)
            .with(TracingMiddleware::default())
            .build();
        Ok(Self { base_url, client })
    }

    /// Fetch the executors currently known to the scheduler.
    pub async fn list_executors(&self) -> TeoDBResult<Vec<ExecutorState>> {
        let url = format!("{}/api/executors", self.base_url);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| TeoDBError::Unavailable(format!("scheduler API unreachable at {url}: {e}")))?;

        if !response.status().is_success() {
            return Err(TeoDBError::Unavailable(format!(
                "scheduler API at {url} returned {}",
                response.status()
            )));
        }

        response
            .json::<Vec<ExecutorState>>()
            .await
            .map_err(|e| TeoDBError::Internal(format!("invalid scheduler API response from {url}: {e}")))
    }

    /// Fetch the jobs currently known to the scheduler.
    pub async fn list_jobs(&self) -> TeoDBResult<Vec<JobState>> {
        let url = format!("{}/api/jobs", self.base_url);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| TeoDBError::Unavailable(format!("scheduler API unreachable at {url}: {e}")))?;

        if !response.status().is_success() {
            return Err(TeoDBError::Unavailable(format!(
                "scheduler API at {url} returned {}",
                response.status()
            )));
        }

        response
            .json::<Vec<JobState>>()
            .await
            .map_err(|e| TeoDBError::Internal(format!("invalid scheduler API response from {url}: {e}")))
    }

    /// Count jobs the scheduler still owns work for (queued or running).
    pub async fn active_job_count(&self) -> TeoDBResult<usize> {
        Ok(self
            .list_jobs()
            .await?
            .iter()
            .filter(|j| j.is_active())
            .count())
    }

    /// Count executors whose last heartbeat is within `liveness_window`.
    ///
    /// The scheduler expires dead executors on its own timeout, but with a
    /// lag — the heartbeat-age filter keeps the readiness signal honest in
    /// that window.
    pub async fn alive_executor_count(&self, liveness_window: Duration) -> TeoDBResult<usize> {
        let executors = self.list_executors().await?;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis().min(u64::MAX as u128) as u64)
            .unwrap_or(0);
        Ok(count_alive(&executors, now_ms, liveness_window))
    }
}

/// Pure liveness count: executors heartbeated within `window` of `now_ms`.
fn count_alive(executors: &[ExecutorState], now_ms: u64, window: Duration) -> usize {
    let window_ms = window.as_millis().min(u64::MAX as u128) as u64;
    executors
        .iter()
        .filter(|e| {
            e.last_seen
                .is_some_and(|ts| now_ms.saturating_sub(ts) <= window_ms)
        })
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn executor(id: &str, last_seen: Option<u64>) -> ExecutorState {
        ExecutorState {
            id: id.into(),
            host: "exec".into(),
            port: 50051,
            last_seen,
        }
    }

    #[test]
    fn count_alive_filters_by_heartbeat_age() {
        let now = 1_000_000;
        let executors = vec![
            executor("fresh", Some(now - 5_000)),
            executor("stale", Some(now - 60_000)),
            executor("never-heartbeated", None),
        ];
        assert_eq!(count_alive(&executors, now, Duration::from_secs(15)), 1);
    }

    #[test]
    fn count_alive_tolerates_clock_skew_ahead() {
        // A heartbeat timestamped slightly in the future must count as alive.
        let now = 1_000_000;
        let executors = vec![executor("ahead", Some(now + 2_000))];
        assert_eq!(count_alive(&executors, now, Duration::from_secs(15)), 1);
    }

    #[test]
    fn executor_state_deserializes_scheduler_response() {
        // Shape produced by ballista-scheduler's /api/executors handler;
        // extra fields must be ignored.
        let body = r#"[{
            "id": "e1",
            "host": "executor-0",
            "port": 50051,
            "last_seen": 1767000000000,
            "specification": {"task_slots": 4},
            "metrics": [],
            "os_info": null
        }, {
            "id": "e2",
            "host": "executor-1",
            "port": 50051,
            "last_seen": null,
            "specification": {"task_slots": 4},
            "metrics": []
        }]"#;
        let executors: Vec<ExecutorState> = serde_json::from_str(body).unwrap();
        assert_eq!(executors.len(), 2);
        assert_eq!(executors[0].last_seen, Some(1_767_000_000_000));
        assert_eq!(executors[1].last_seen, None);
    }

    #[test]
    fn job_state_deserializes_scheduler_response_and_classifies_activity() {
        // Shape produced by ballista-scheduler's /api/jobs handler; extra
        // fields must be ignored.
        let body = r#"[{
            "job_id": "j1",
            "job_name": "q1",
            "job_status": "Running",
            "status": "Running",
            "num_stages": 3,
            "completed_stages": 1,
            "percent_complete": 33,
            "start_time": 1767000000000,
            "end_time": 0
        }, {
            "job_id": "j2",
            "job_name": "q2",
            "job_status": "Completed. Produced 1 partition containing 5 rows. Elapsed time: 12 ms.",
            "status": "Completed",
            "num_stages": 3,
            "completed_stages": 3,
            "percent_complete": 100,
            "start_time": 1767000000000,
            "end_time": 1767000000012
        }]"#;
        let jobs: Vec<JobState> = serde_json::from_str(body).unwrap();
        assert_eq!(jobs.len(), 2);
        assert!(jobs[0].is_active());
        assert!(!jobs[1].is_active());
        assert!(
            JobState {
                job_id: "j3".into(),
                status: "Queued".into(),
            }
            .is_active()
        );
        assert!(
            !JobState {
                job_id: "j4".into(),
                status: "Failed".into(),
            }
            .is_active()
        );
    }

    /// End-to-end against a raw-TCP HTTP stub: one canned response.
    #[tokio::test]
    async fn client_fetches_and_counts_alive_executors() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let body = format!(
            r#"[{{"id":"e1","host":"x","port":50051,"last_seen":{fresh}}},
                {{"id":"e2","host":"y","port":50051,"last_seen":{stale}}}]"#,
            fresh = now_ms - 1_000,
            stale = now_ms - 120_000,
        );

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await.unwrap();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            socket
                .write_all(response.as_bytes())
                .await
                .unwrap();
        });

        let client = SchedulerApiClient::new(&addr.to_string(), Duration::from_secs(2)).unwrap();
        let alive = client
            .alive_executor_count(Duration::from_secs(15))
            .await
            .unwrap();
        assert_eq!(alive, 1);

        server.await.unwrap();
    }
}
