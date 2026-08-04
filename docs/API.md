# API

Navigation: [README](../README.md) | [Architecture](ARCHITECTURE.md) |
Related: [Network Protocols](NETWORK_PROTOCOL.md), [CLI](CLI.md), [OpenAPI](openapi.yaml)

TeoDB exposes a REST API and an Arrow Flight SQL API. The REST API is documented in [openapi.yaml](openapi.yaml); this file
explains how the API maps to the system.

## REST Overview

Health:

| Method | Path     | Purpose                                      |
|--------|----------|----------------------------------------------|
| `GET`  | `/live`  | Process liveness.                            |
| `GET`  | `/ready` | Readiness, dependency, and lifecycle checks. |

Query:

| Method | Path                    | Purpose                               |
|--------|-------------------------|---------------------------------------|
| `POST` | `/api/v1/query`         | Execute SQL and return JSON rows.     |
| `POST` | `/api/v1/query/explain` | Return a query plan/explain response. |

Ingest:

| Method | Path                                        | Purpose                                          |
|--------|---------------------------------------------|--------------------------------------------------|
| `POST` | `/api/v1/tables/{namespace}/{table}/ingest` | Ingest JSON rows.                                |
| `POST` | `/api/v1/tables/{namespace}/{table}/flush`  | Force flush for one table on the receiving node. |

Namespaces and tables:

| Method   | Path                                            | Purpose              |
|----------|-------------------------------------------------|----------------------|
| `GET`    | `/api/v1/namespaces`                            | List namespaces.     |
| `POST`   | `/api/v1/namespaces`                            | Create namespace.    |
| `DELETE` | `/api/v1/namespaces/{namespace}`                | Drop namespace.      |
| `GET`    | `/api/v1/namespaces/{namespace}/tables`         | List tables.         |
| `POST`   | `/api/v1/namespaces/{namespace}/tables`         | Create table.        |
| `GET`    | `/api/v1/namespaces/{namespace}/tables/{table}` | Load table metadata. |
| `DELETE` | `/api/v1/namespaces/{namespace}/tables/{table}` | Drop table.          |

Admin and metrics:

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/v1/admin/status` | Process and subsystem status. |
| `GET` | `/api/v1/admin/tables` | Table summaries. |
| `GET` | `/api/v1/admin/cluster` | Node, scheduler, job, and executor status. |
| `GET` | `/api/v1/admin/flush-blocked` | Tables with an unknown flush result. |
| `POST` | `/api/v1/admin/flush-blocked/{namespace}/{table}/recheck` | Check one blocked flush again. |
| `GET` | `/metrics` | Prometheus scrape data. |

Admin endpoints and metrics are protected by `security.admin_token` when configured.
The admin metrics page is `/ui/metrics`. It is not a scrape endpoint.

## Query Example

```bash
curl -fsS -X POST http://localhost:8080/api/v1/query \
  -H 'content-type: application/json' \
  -d '{"sql":"SELECT COUNT(*) AS n FROM default.events"}'
```

## Ingest Example

```bash
curl -fsS -X POST http://localhost:8080/api/v1/tables/default/events/ingest \
  -H 'content-type: application/json' \
  -d '{"rows":[{"id":1,"kind":"open"},{"id":2,"kind":"close"}],"idempotency_key":"batch-1"}'
```

Then flush:

```bash
curl -fsS -X POST http://localhost:8080/api/v1/tables/default/events/flush
```

## Error Model

REST errors use RFC 9457 problem details:

```json
{
  "type": "https://teodb.io/problems/conflict",
  "title": "Conflict",
  "status": 409,
  "detail": "conflict on table: expected 1, found 2",
  "errorCode": "Conflict",
  "retryable": true
}
```

The `instance` field is filled with the request path when available. Retryable errors include `retryable: true`; rate limit errors
can include `retryAfterMs`.

### Size and admission limits

REST request bodies are capped by `ingest.max_body_bytes`. Both an oversized `Content-Length` and a chunked body that crosses the
limit return HTTP 413 as an RFC 9457 problem response.

JSON query results are independently capped by `server.max_result_bytes`. TeoDB checks encoded bytes while batches are appended;
if the result crosses the ceiling, it drops the stream, best-effort cancels the query, and returns HTTP 413 with guidance to use
Arrow Flight. Raising the incoming body limit does not raise the outgoing result limit. Use Flight for large or streaming result
sets.

Node-wide request/RPC limits, per-principal concurrency, and per-node rate budgets can reject work before a handler runs. REST
uses HTTP 503 for global request saturation and HTTP 429 with `Retry-After` for rate/principal admission. Flight uses
`ResourceExhausted` and includes retry metadata for rate limits.

## Arrow Flight SQL

Flight SQL supports:

- Handshake.
- Direct statement query.
- Prepared statements.
- Statement update for DDL/DML paths.
- Arrow batch ingest through `do_put`.
- Metadata commands for catalogs, schemas, tables, table types, SQL info, and primary keys.

Unsupported commands return gRPC unimplemented errors rather than silent no-ops.

Flight decoding, encoding, concurrent RPCs, streams per connection, accepted connections, and idle IO are bounded by the
`server.*` settings documented in [Configuration](CONFIGURATION.md).

## Frontend Contract

The embedded admin frontend uses the REST API contract in [openapi.yaml](openapi.yaml). Frontend API code lives in:

- `frontend/src/api/types.ts`
- `frontend/src/api/admin.ts`
- `frontend/src/api/client.ts`

When a REST endpoint or DTO changes, update the backend handler, OpenAPI spec, frontend wrapper/types, and tests in the same
change. Generated TypeScript types are not wired into the frontend today; add generation only if OpenAPI drift becomes common
enough to justify the dependency and review cost.

Useful validation commands:

```bash
TEODB_SKIP_UI_BUILD=1 cargo test -p teodb-server --test rest_api
cd frontend
npm run test
npm run build
```

## Consistency Reminder

Ingest acknowledgement is not query visibility. Query visibility begins after flush commits data files to the Iceberg catalog.
See [Consistency](CONSISTENCY.md).
