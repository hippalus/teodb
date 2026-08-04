//! HTTP/REST client for TeoDB API endpoints.

use crate::{ClientError, Result};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_tracing::TracingMiddleware;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use url::Url;

/// Tunable transport settings for the resilient HTTP client.
#[derive(Debug, Clone)]
pub struct HttpClientConfig {
    /// Overall per-request deadline (connect + headers + body). `None` disables.
    pub request_timeout: Option<Duration>,
    /// Max time to establish a TCP connection. `None` disables.
    pub connect_timeout: Option<Duration>,
    /// Per-read socket inactivity timeout (caps a stalled response). `None` disables.
    pub read_timeout: Option<Duration>,
    /// How long an idle pooled connection is kept before it is closed. `None`
    /// keeps connections until the server closes them.
    pub pool_idle_timeout: Option<Duration>,
    /// Max idle connections retained per host in the pool.
    pub pool_max_idle_per_host: usize,
    /// TCP keepalive probe interval. `None` disables keepalive.
    pub tcp_keepalive: Option<Duration>,
    /// Disable Nagle's algorithm (lower latency for small requests).
    pub tcp_nodelay: bool,
    /// Max transient-failure retries with exponential backoff.
    pub max_retries: u32,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            request_timeout: Some(Duration::from_secs(30)),
            connect_timeout: Some(Duration::from_secs(10)),
            read_timeout: Some(Duration::from_secs(30)),
            pool_idle_timeout: Some(Duration::from_secs(90)),
            pool_max_idle_per_host: 8,
            tcp_keepalive: Some(Duration::from_secs(60)),
            tcp_nodelay: true,
            max_retries: 3,
        }
    }
}

/// Build a connection-pooling HTTP client from `config`, layered with request
/// tracing and transient-failure retries (exponential backoff). The returned
/// client is cheap to clone and shares one connection pool — build once and reuse.
pub fn resilient_http_client(config: &HttpClientConfig) -> ClientWithMiddleware {
    let mut transport = reqwest::Client::builder()
        .pool_max_idle_per_host(config.pool_max_idle_per_host)
        .tcp_keepalive(config.tcp_keepalive)
        .tcp_nodelay(config.tcp_nodelay);
    if let Some(timeout) = config.request_timeout {
        transport = transport.timeout(timeout);
    }
    if let Some(timeout) = config.connect_timeout {
        transport = transport.connect_timeout(timeout);
    }
    if let Some(timeout) = config.read_timeout {
        transport = transport.read_timeout(timeout);
    }
    if let Some(timeout) = config.pool_idle_timeout {
        transport = transport.pool_idle_timeout(timeout);
    }
    let transport = transport
        .build()
        .expect("valid reqwest client configuration");

    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(config.max_retries);
    ClientBuilder::new(transport)
        .with(TracingMiddleware::default())
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build()
}

/// HTTP client for TeoDB's REST API.
#[derive(Clone, Debug)]
pub struct HttpClient {
    base_url: Url,
    client: ClientWithMiddleware,
}

impl HttpClient {
    /// Create a new HTTP client from the base URL (e.g. `http://127.0.0.1:8080`)
    /// using default transport settings ([`HttpClientConfig::default`]).
    pub fn new(base_url: &str) -> Result<Self> {
        Self::with_config(base_url, &HttpClientConfig::default())
    }

    /// Create a client with custom transport settings (timeouts, pool, etc.).
    pub fn with_config(base_url: &str, config: &HttpClientConfig) -> Result<Self> {
        Ok(Self {
            base_url: Url::parse(base_url)?,
            client: resilient_http_client(config),
        })
    }

    /// Create a client reusing an existing resilient HTTP client (shares the
    /// connection pool across multiple `HttpClient`s).
    pub fn with_client(base_url: &str, client: ClientWithMiddleware) -> Result<Self> {
        Ok(Self {
            base_url: Url::parse(base_url)?,
            client,
        })
    }

    /// Execute a SQL query and return JSON result.
    pub async fn query(&self, sql: &str, limit: Option<usize>) -> Result<QueryResponse> {
        self.post_json("/api/v1/query", &QueryRequest { sql, limit })
            .await
    }

    /// Execute a SQL explain plan.
    pub async fn explain(&self, sql: &str) -> Result<QueryResponse> {
        self.post_json("/api/v1/query/explain", &ExplainRequest { sql })
            .await
    }

    /// Create a namespace.
    pub async fn create_namespace(&self, namespace: &str) -> Result<serde_json::Value> {
        self.post_json("/api/v1/namespaces", &serde_json::json!({"namespace": namespace}))
            .await
    }

    /// List namespaces.
    pub async fn list_namespaces(&self) -> Result<serde_json::Value> {
        self.get_json("/api/v1/namespaces").await
    }

    /// Create a table via DDL SQL through the query endpoint.
    pub async fn create_table_sql(&self, ddl: &str) -> Result<QueryResponse> {
        self.query(ddl, None).await
    }

    /// List tables in a namespace.
    pub async fn list_tables(&self, namespace: &str) -> Result<serde_json::Value> {
        self.get_json(&format!("/api/v1/namespaces/{namespace}/tables"))
            .await
    }

    /// Ingest JSON rows into a table.
    pub async fn ingest(&self, namespace: &str, table: &str, rows: Vec<serde_json::Value>) -> Result<IngestResponse> {
        self.post_json(
            &format!("/api/v1/tables/{namespace}/{table}/ingest"),
            &IngestRequest { rows },
        )
        .await
    }

    /// Flush a table's buffer to Parquet.
    pub async fn flush(&self, namespace: &str, table: &str) -> Result<serde_json::Value> {
        let url = self.endpoint(&format!("/api/v1/tables/{namespace}/{table}/flush"))?;
        let response = self.client.post(url).send().await?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            return Err(ClientError::Server { status, body });
        }
        Ok(response.json().await?)
    }

    /// Check liveness.
    pub async fn is_live(&self) -> bool {
        self.client
            .get(
                self.endpoint("/live")
                    .unwrap_or_else(|_| self.base_url.clone()),
            )
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
    }

    /// Check readiness.
    pub async fn is_ready(&self) -> bool {
        self.client
            .get(
                self.endpoint("/ready")
                    .unwrap_or_else(|_| self.base_url.clone()),
            )
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
    }

    fn endpoint(&self, path: &str) -> Result<Url> {
        self.base_url
            .join(path)
            .map_err(ClientError::from)
    }

    async fn get_json<R: DeserializeOwned>(&self, path: &str) -> Result<R> {
        let url = self.endpoint(path)?;
        let response = self.client.get(url).send().await?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            return Err(ClientError::Server { status, body });
        }
        Ok(response.json().await?)
    }

    async fn post_json<T: Serialize, R: DeserializeOwned>(&self, path: &str, payload: &T) -> Result<R> {
        let url = self.endpoint(path)?;
        let response = self.client.post(url).json(payload).send().await?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            return Err(ClientError::Server { status, body });
        }
        Ok(response.json().await?)
    }
}

#[derive(Debug, Serialize)]
struct QueryRequest<'a> {
    sql: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct ExplainRequest<'a> {
    sql: &'a str,
}

#[derive(Debug, Serialize)]
struct IngestRequest {
    rows: Vec<serde_json::Value>,
}

/// Column metadata in a query response.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
}

/// Response from the query endpoint (HATEOAS-flat envelope).
#[derive(Debug, Clone, Deserialize)]
pub struct QueryResponse {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<serde_json::Map<String, serde_json::Value>>,
    pub row_count: usize,
    pub elapsed_ms: u64,
}

/// Response from the ingest endpoint (HATEOAS-flat envelope).
#[derive(Debug, Clone, Deserialize)]
pub struct IngestResponse {
    pub accepted_rows: u64,
    pub batch_id: String,
    pub generation: u64,
}
