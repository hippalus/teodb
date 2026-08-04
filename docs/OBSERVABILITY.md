# Observability

Navigation: [README](../README.md) | [Configuration](CONFIGURATION.md) |
[Debugging](DEBUGGING.md) | [API](API.md)

## Metrics Endpoints

Prometheus scrapes:

```text
GET /metrics
```

The admin UI page is:

```text
GET /ui/metrics
```

These paths have different jobs. `/metrics` returns Prometheus text. The UI
page reads that text and draws charts.

When `security.admin_token` is set, both admin APIs and `/metrics` need its
bearer token.

The UI shows at most 200 metric cards. It charts at most six selected series.
It loads the full raw scrape only when the user opens it.

## API And Transport Metrics

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `teodb_transport_active_connections` | gauge | `transport` | Open REST or Flight sockets. |
| `teodb_transport_result_bytes_total` | counter | `transport`, `operation` | Result bytes before transport compression. |
| `teodb_transport_admission_rejections_total` | counter | `transport`, `reason` | Work rejected by a limit. |
| `teodb_auth_total` | counter | `transport`, `outcome`, `reason` | Authentication results. |
| `teodb_authz_total` | counter | `transport`, `outcome`, `action`, `resource_kind` | Authorization results. |

`transport` is `rest` or `flight`. Admission reasons include connection,
global, caller, rate, body, and result limits.

Labels do not include SQL, request IDs, tokens, user names, table names, or raw
error text.

## Write Metrics

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `teodb_buffer_bytes` | gauge | none | Bytes held by table buffers. |
| `teodb_buffer_reserved_bytes` | gauge | none | Bytes reserved before WAL write. |
| `teodb_buffer_oldest_pending_age_seconds` | gauge | `namespace`, `table` | Age of the oldest durable row not yet committed. |
| `teodb_flush_visibility_lag_seconds` | gauge | `namespace`, `table` | Time from ingest to catalog visibility. |
| `teodb_ingest_rejected_writes_total` | counter | `reason` | Rejected writes by reason. |

Write rejection reasons include `buffer_capacity`, `wal_capacity`,
`flush_blocked`, and `writer_registry`.

The table gauges exist only for active buffers. TeoDB removes them when it
removes the buffer.

## Reading The Metrics

- A rising pending age means flush is not keeping up.
- Reserved bytes that stay high can mean WAL work is stuck.
- Connection or global rejections mean the node is full.
- Caller or rate rejections point to one client.
- Large REST result counts may be a reason to use Flight SQL.

## Error Logs

HTTP access logs include:

- Method and path.
- Request ID.
- Status and time.

An internal server error also includes:

- `error_code`
- `error_message`
- `error_sources`
- `error_origin_file`
- `error_origin_line`
- `error_origin_column`
- `trace_id` when tracing is active
- `error_backtrace` for non-retryable HTTP 500 errors

The normal tracing file and line show where the log was written. The
`error_origin_*` fields show where the domain error entered the HTTP boundary.
The backtrace shows the Rust call path.

Overload and temporary dependency errors do not include a backtrace. This keeps
an incident from creating more load.

Internal details stay in server logs. Client errors keep the public RFC 9457
shape. Use the request ID or trace ID to match a client error to its log.

## Trace Export

Set `observability.otlp_endpoint` to send traces by OTLP gRPC. Leave it empty to
turn trace export off. Set `observability.service_name` when several TeoDB
services send data to the same tracing system.
