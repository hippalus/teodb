# Network Protocols

Navigation: [README](../README.md) | [Architecture](ARCHITECTURE.md) |
Related: [API](API.md), [CLI](CLI.md), [Distributed Mode](DISTRIBUTED.md)

TeoDB exposes REST and Arrow Flight SQL to clients. Distributed mode also uses Ballista gRPC internally.

## Public Protocols

| Protocol         | Default bind   | Purpose                                                                         |
|------------------|----------------|---------------------------------------------------------------------------------|
| HTTP REST        | `0.0.0.0:8080` | SQL query, ingest, DDL/table APIs, admin APIs, health, metrics, embedded UI.    |
| Arrow Flight SQL | `0.0.0.0:8815` | Arrow-native query, metadata commands, prepared statements, Arrow batch ingest. |

## REST

REST endpoints live under `/api/v1` except:

- `/live`
- `/ready`
- `/metrics`
- Embedded UI fallback routes.

Errors use RFC 9457 problem details with `application/problem+json`. Responses include request IDs and trace context where
middleware can propagate them.

Accepted connections, node-wide in-flight requests, per-principal operations, incoming request bytes, encoded JSON result bytes,
idle IO, and per-node request rates are bounded independently. HTTP 413 result-limit responses direct large-result clients to
Flight.

## Flight SQL

Flight SQL is served over tonic gRPC. TeoDB supports:

- Handshake.
- Direct statement query.
- Prepared statement create/execute/close.
- DDL/DML statement update path.
- Arrow batch ingest through `do_put`.
- Metadata commands for catalogs, schemas, tables, table types, SQL info, and primary keys.

`do_exchange` is not implemented.

Flight applies accepted-connection, node-wide RPC, per-principal operation, per-connection stream, message-size, and idle-IO
limits. Permits live until response streams finish rather than only until a handler returns its stream object.

## Internal Ballista Traffic

Distributed mode uses:

| Port          | Default | Purpose                           |
|---------------|---------|-----------------------------------|
| Scheduler     | `50050` | Control-plane Ballista scheduler. |
| Executor bind | `50051` | Data-node executor bind.          |
| Executor gRPC | `50052` | Data-node executor communication. |

These ports should be reachable only inside the TeoDB cluster network.

## TLS And Authentication

Security mode controls transport and authentication expectations:

- `plaintext`: development-friendly, anonymous access allowed.
- `tls`: TLS with optional allow-list authorization.
- `oauth2`: TLS plus JWT validation and authorization.

Admin endpoints and metrics are additionally guarded by `security.admin_token` when set.

Forwarded client-address headers are ignored unless the immediate socket peer is in `server.trusted_proxy_cidrs`. Admission
limits are per node, not a cluster-wide quota.

## Load Balancing

In distributed mode, load balancers can route REST and Flight traffic to any data node. For ingest clients using idempotency keys
before flush, stable routing is recommended because idempotency is node-local.
