# Configuration

Navigation: [README](../README.md) | [Architecture](ARCHITECTURE.md) |
Related: [Deployment](DEPLOYMENT.md), [Security](../SECURITY.md), [CLI](CLI.md)

TeoDB uses one layered configuration model:

```text
CLI flags > environment variables (TEODB__*) > TOML file > compiled defaults
```

Each layer overrides only the keys it sets.

## TOML Files

Use `--config <path>` or `TEODB_CONFIG=<path>` to load a TOML file. Deployment examples live
under [deploy/docker/config](../deploy/docker/config/):

- `standalone.toml`
- `data-node.toml`
- `control-plane.toml`

Minimal shape:

```toml
role = "standalone"
data_dir = "/var/lib/teodb/data"

[server]
rest_bind = "0.0.0.0:8080"
flight_bind = "0.0.0.0:8815"

[catalog]
type = "rest"
uri = "http://iceberg-rest:8181"
warehouse = "s3://teodb"

[storage]
cache_dir = "/var/lib/teodb/cache"
spill_dir = "/var/lib/teodb/spill"
s3_endpoint = "http://s3.local:9000"
s3_allow_http = true
```

## Environment Variables

Any TOML key can be set with a `TEODB__` variable. Use double underscores between sections and fields:

| TOML key                  | Environment variable              |
|---------------------------|-----------------------------------|
| `server.rest_bind`        | `TEODB__SERVER__REST_BIND`        |
| `storage.cache_max_bytes` | `TEODB__STORAGE__CACHE_MAX_BYTES` |
| `security.admin_token`    | `TEODB__SECURITY__ADMIN_TOKEN`    |
| `cluster.scheduler_addr`  | `TEODB__CLUSTER__SCHEDULER_ADDR`  |
| `ingest.commit_status_check.num_retries` | `TEODB__INGEST__COMMIT_STATUS_CHECK__NUM_RETRIES` |

Example:

```bash
TEODB__SERVER__REST_BIND=0.0.0.0:9090 teodb --config deploy/docker/config/standalone.toml
```

## S3 Credential Fallback

When the corresponding `[storage]` keys are unset, TeoDB reads standard AWS-style variables:

| Storage key     | Environment fallback    |
|-----------------|-------------------------|
| `s3_access_key` | `AWS_ACCESS_KEY_ID`     |
| `s3_secret_key` | `AWS_SECRET_ACCESS_KEY` |
| `s3_region`     | `AWS_REGION`            |
| `s3_endpoint`   | `AWS_ENDPOINT_URL`      |

Compose and Helm use this path so credentials do not need to be rendered into TOML files.

## CLI Overrides

CLI flags are intentionally limited to bootstrap and per-process values:

| Flag                               | Purpose                                                   |
|------------------------------------|-----------------------------------------------------------|
| `--config <path>`                  | Select TOML file.                                         |
| `--role <ROLE>`                    | Set `standalone`, `data-node`, or `control-plane`.        |
| `--security-mode <MODE>`           | Set `plaintext`, `tls`, or `oauth2`.                      |
| `--rest-bind <addr>`               | Override REST bind address.                               |
| `--flight-bind <addr>`             | Override Flight SQL bind address.                         |
| `--executor-advertise-host <host>` | Set the routable host advertised by a data-node executor. |
| `--log-level <level>`              | Override tracing level.                                   |
| `--log-format <FORMAT>`            | Set `json`, `pretty`, or `compact`.                       |

Run `teodb --help` for the generated CLI surface.

## Key Sections

### Process

| Key        | Default      | Notes                                           |
|------------|--------------|-------------------------------------------------|
| `role`     | `standalone` | `standalone`, `data-node`, or `control-plane`.  |
| `data_dir` | `./data`     | Base directory for local durable/process state. |

Relative `cache_dir` and `spill_dir` values are resolved under `data_dir`.

### Server

| Key                                               | Default        | Notes                                                                 |
|---------------------------------------------------|----------------|-----------------------------------------------------------------------|
| `server.rest_bind`                                | `0.0.0.0:8080` | REST, health, admin UI, and metrics listener.                          |
| `server.flight_bind`                              | `0.0.0.0:8815` | Arrow Flight SQL listener.                                            |
| `server.max_http_connections`                     | `256`          | Accepted REST TCP/TLS connections. New excess sockets are closed.     |
| `server.max_http_in_flight_requests`              | `256`          | Node-wide REST request ceiling; saturation returns HTTP 503.           |
| `server.max_flight_connections`                   | `512`          | Accepted Flight TCP/TLS connections. New excess sockets are closed.   |
| `server.max_flight_in_flight_requests`            | `512`          | Node-wide Flight RPC ceiling; saturation is `ResourceExhausted`.       |
| `server.max_flight_streams_per_connection`        | `64`           | HTTP/2/Flight streams admitted on one connection.                     |
| `server.flight_max_decoding_message_bytes`        | `67108864`     | Maximum decoded inbound Flight message size (64 MiB).                 |
| `server.flight_max_encoding_message_bytes`        | `67108864`     | Maximum encoded outbound Flight message size (64 MiB).                |
| `server.idle_timeout_secs`                        | `300`          | Activity-based read/write idle deadline for both public listeners.    |
| `server.max_result_bytes`                         | `67108864`     | Encoded REST query-result ceiling; larger results return HTTP 413.    |
| `server.max_concurrent_operations_per_principal`  | `32`           | Concurrent REST/Flight operations for one authenticated principal.    |
| `server.read_requests_per_window`                 | `20000`        | Per-node, per-principal read budget.                                  |
| `server.write_requests_per_window`                | `10000`        | Per-node, per-principal write budget.                                 |
| `server.public_requests_per_window`               | `40000`        | Per-node, per-address health/metrics budget.                          |
| `server.rate_limit_window_secs`                   | `60`           | Fixed rate-limit window.                                              |
| `server.max_rate_limit_keys`                      | `8192`         | Bound on tracked rate/principal keys; overflow uses fixed stripes.    |
| `server.trusted_proxy_cidrs`                      | empty          | CIDRs allowed to supply `Forwarded`/`X-Forwarded-For`.                |
| `server.cors_allowed_origins`                     | empty          | Empty means permissive CORS for the embedded UI and development.      |

Counts, byte limits, and time limits must be above zero. A connection permit
lasts for the socket life. A request permit lasts until its response or Flight
stream ends.

Rate limits apply per node. More nodes add more total capacity.

TeoDB ignores forwarded client headers by default. It trusts them only when the
direct peer is in `trusted_proxy_cidrs`. A bad header falls back to the direct
peer address.

### Catalog

| Key                    | Default                 | Notes                                   |
|------------------------|-------------------------|-----------------------------------------|
| `catalog.type`         | `rest`                  | Current implementation is Iceberg REST. |
| `catalog.uri`          | `http://localhost:8181` | Iceberg REST catalog endpoint.          |
| `catalog.warehouse`    | unset                   | Usually `s3://<bucket>` in deployments. |
| `catalog.oauth2_credential` | unset              | OAuth2 client credential.               |
| `catalog.oauth2_scope` | unset                   | OAuth2 scope.                            |
| `catalog.oauth2_server_uri` | unset              | OAuth2 token server.                     |
| `catalog.token`        | unset                   | Static catalog token.                    |

### Storage

| Key                              | Default        | Notes                             |
|----------------------------------|----------------|-----------------------------------|
| `storage.cache_dir`              | `./data/cache` | Local object cache.               |
| `storage.spill_dir`              | `./data/spill` | DataFusion/Ballista spill.        |
| `storage.cache_max_bytes`        | `10737418240`  | 10 GiB.                           |
| `storage.cache_max_per_object_bytes` | `536870912` | 512 MiB.                       |
| `storage.s3_allow_http`          | `false`        | Must be true for local HTTP object-store endpoints. |

### WAL

| Key                     | Default      | Notes                                                          |
|-------------------------|--------------|----------------------------------------------------------------|
| `wal.max_segment_bytes` | `268435456`  | 256 MiB.                                                       |
| `wal.fsync_on_append`   | `true`       | Keep true for durable deployments.                             |
| `wal.soft_watermark_bytes` | `4294967296` | 4 GiB backpressure watermark.                               |
| `wal.hard_cap_bytes`       | `8589934592` | 8 GiB write-rejection ceiling.                              |
| `wal.recovery_mode`     | `fail`       | `fail` stops on corruption; `salvage` moves the bad segment aside. |
| `wal.max_prepared_files` | `10000`      | Maximum file descriptors accepted in one prepared intent.      |
| `wal.max_prepared_bytes` | `16777216`   | Maximum serialized prepared-intent sidecar size.                |

### Query

| Key                             | Default      | Notes                            |
|---------------------------------|--------------|----------------------------------|
| `query.memory_pool_bytes`       | `4294967296` | DataFusion runtime memory pool.  |
| `query.batch_size`              | `8192`       | Record batch target.             |
| `query.target_partitions`       | `0`          | `0` means available parallelism. |
| `query.query_timeout_secs`      | `300`        | End-to-end query deadline.       |
| `query.slow_query_threshold_ms` | `5000`       | Logging/metrics threshold.       |
| `query.metadata_refresh_secs`   | `10`         | Table metadata cache refresh.    |
| `query.query_status_max_entries` | `100000`    | Saved query status record limit. |
| `query.query_status_ttl_secs`    | `3600`       | Saved query status lifetime.     |

### Ingest

| Key                                        | Default      | Notes                                        |
|--------------------------------------------|--------------|----------------------------------------------|
| `ingest.buffer_max_bytes`                  | `536870912`  | Hot buffer capacity.                         |
| `ingest.flush_interval_secs`               | `10`         | Periodic flush interval.                     |
| `ingest.max_body_bytes`                    | `67108864`   | API-owned REST request body limit, including chunked bodies. |
| `ingest.default_warehouse_uri`             | `s3://teodb` | Table location base for auto-created tables. |
| `ingest.idempotency_ttl_secs`              | `86400`      | Per-writer idempotency retention.            |
| `ingest.idempotency_max_keys_per_table`    | `100000`     | Per-table cap.                               |
| `ingest.commit_status_check.num_retries`                         | `5`     | Exact ambiguous-commit retry budget.                    |
| `ingest.commit_status_check.min_wait_ms`                          | `100`   | Initial delay between exact status checks.              |
| `ingest.commit_status_check.max_wait_ms`                          | `5000`  | Maximum delay between exact status checks.              |
| `ingest.commit_status_check.total_timeout_ms`                     | `30000` | Maximum exact status-check window.                      |
| `ingest.commit_status_check.blocked_recheck_interval_ms`         | `60000` | Background recheck cadence for blocked flushes.         |
| `ingest.commit_status_check.blocked_recheck_jitter_percent`      | `15`    | Jitter applied to the background recheck cadence.       |
| `ingest.commit_status_check.max_concurrent_blocked_rechecks`     | `4`     | Process-wide concurrent blocked-recheck limit.          |

### Cluster

| Key                                         | Default           | Notes                                                                 |
|---------------------------------------------|-------------------|-----------------------------------------------------------------------|
| `cluster.cluster_id`                        | generated standalone / required data-node | Stable UUID shared by all writers.                 |
| `cluster.node_id`                           | generated standalone / required data-node | Human-readable operational identity.               |
| `cluster.writer_slot`                       | generated standalone / required data-node | Stable, deployment-unique writer slot.             |
| `cluster.max_writer_checkpoints_per_table`  | `32`              | Hard bound on catalog checkpoint properties.                          |
| `cluster.scheduler_enabled`                 | `false`           | Data node can optionally run a scheduler too.                         |
| `cluster.scheduler_bind`                    | `0.0.0.0:50050`   | Scheduler bind address.                                               |
| `cluster.scheduler_addr`                    | `localhost:50050` | Scheduler address used by data nodes.                                 |
| `cluster.executor_bind`                     | `0.0.0.0:50051`   | Executor bind address.                                                |
| `cluster.executor_advertise_host`           | unset             | Executor host shared with the cluster.                                |
| `cluster.executor_grpc_bind_port`           | `50052`           | Executor gRPC port.                                                   |
| `cluster.executor_task_slots`               | `0`               | `0` means available parallelism.                                      |
| `cluster.heartbeat_interval_secs`           | `5`               | Executor heartbeat period.                                            |
| `cluster.heartbeat_miss_threshold`          | `3`               | Missed heartbeats before an executor is stale.                         |
| `cluster.local_query_fallback`              | `false`           | Remote data nodes can fall back to local execution only when enabled. |
| `cluster.min_executors`                     | `1`               | Readiness executor quorum.                                            |
| `cluster.drain_timeout_secs`                | `20`              | Ballista shutdown drain limit.                                        |

### Maintenance

| Key                                      | Default  | Notes                                          |
|------------------------------------------|----------|------------------------------------------------|
| `maintenance.enabled`                    | `true`   | Enables maintenance loop.                      |
| `maintenance.compaction_enabled`         | `false`  | Disabled by default.                           |
| `maintenance.compaction_interval_secs`   | `3600`   | Periodic compaction cadence.                   |
| `maintenance.target_file_bytes`          | `134217728` | Target compacted file size.                  |
| `maintenance.min_files_per_compaction`   | `8`      | Smallest compaction group.                     |
| `maintenance.max_files_per_compaction`   | `64`     | Largest compaction group.                      |
| `maintenance.max_bytes_per_compaction`   | `1073741824` | Input byte limit for one run.                |
| `maintenance.compaction_memory_bytes`    | `536870912` | Memory limit for one run.                    |
| `maintenance.compression`                | `zstd(3)` | Output compression.                           |
| `maintenance.orphan_sweep_interval_secs` | `21600`  | Six hours.                                     |
| `maintenance.orphan_retention_secs`      | `86400`  | Minimum age before deleting orphan data files. |
| `maintenance.snapshot_retention_secs`    | `0`      | Disabled until safe Iceberg metadata expiration is implemented. |
| `maintenance.snapshot_keep_last`         | `1`      | Always keep at least this many snapshots when enabled.          |
| `maintenance.lock_ttl_secs`              | `7200`   | Compaction advisory lock lifetime.             |

## Security Configuration

| Key                                                | Notes                                                  |
|----------------------------------------------------|--------------------------------------------------------|
| `security.mode`                                    | `plaintext`, `tls`, or `oauth2`.                       |
| `security.admin_token`                             | Guards admin endpoints and `/metrics` when set.        |
| `security.allow_list_path`                         | TOML allow-list file for authorization rules.          |
| `security.client_ca_cert`                          | CA file for client certificate checks.                 |
| `security.jwt_issuer`                              | Required JWT issuer.                                   |
| `security.jwt_audience`                            | Required JWT audience.                                 |
| `security.jwt_signing_key`                         | Static key path for JWT validation.                    |
| `security.jwt_algorithm`                           | HMAC, RSA, or EC algorithm supported by the validator. |
| `security.tls_cert` / `security.tls_key`           | Required for TLS/OAuth2 production mode.               |

TLS and OAuth2 need a certificate and key. OAuth2 also needs an allow-list.
Production config must keep WAL fsync on.

### Observability

| Key | Default | Notes |
|-----|---------|-------|
| `observability.log_format` | `pretty` | `pretty`, `json`, or `compact`. |
| `observability.log_level` | `info` | `trace`, `debug`, `info`, `warn`, or `error`. |
| `observability.otlp_endpoint` | unset | OTLP gRPC trace endpoint. |
| `observability.service_name` | `teodb` | Service name in traces. |

### Runtime And Shutdown

| Key | Default | Notes |
|-----|---------|-------|
| `runtime.worker_threads` | `0` | `0` lets Tokio choose. |
| `runtime.max_blocking_threads` | `512` | Blocking worker limit. |
| `runtime.thread_stack_size` | `8388608` | Worker stack size in bytes. |
| `shutdown.drain_timeout_secs` | `60` | Total shutdown drain limit. |
| `shutdown.flush_on_shutdown` | `true` | Flush local buffers before exit. |

## Deployment Layering

| Deployment       | TOML                            | Environment                                     | CLI                                   |
|------------------|---------------------------------|-------------------------------------------------|---------------------------------------|
| Docker Compose   | Mounted config files            | `.env`, `AWS_*`, `TEODB__SECURITY__ADMIN_TOKEN` | `--config`, data-node advertised host |
| Helm             | Rendered ConfigMap              | Kubernetes Secret, Downward API, `AWS_*`        | `--config`, pod DNS advertised host   |
| Local source run | Checked-in config or local file | Shell env                                       | Developer overrides                   |

## Validation

Config validation rejects bad zero values, bad addresses, unsafe production
security, and invalid compaction settings.
