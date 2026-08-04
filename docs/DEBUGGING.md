# Debugging

Navigation: [README](../README.md) | [Architecture](ARCHITECTURE.md) |
Related: [Testing](TESTING.md), [Configuration](CONFIGURATION.md), [API](API.md)

This document lists practical debugging paths for common TeoDB failures.

## Startup Failure

Check:

1. Config path and syntax.
2. `TEODB__*` environment overrides.
3. Iceberg REST catalog reachability.
4. Object-store endpoint and credentials.
5. Writable data, cache, and spill directories.
6. WAL lease ownership.
7. Production security validation.

Useful commands:

```bash
curl -fsS http://localhost:8181/v1/config | jq .
curl -fsS http://localhost:19000/health
```

## Not Ready

Call:

```bash
curl -fsS http://localhost:8080/ready | jq .
```

Readiness can fail because the process is draining, catalog checks fail, scheduler is unreachable, executor quorum is not met, or
local directories are not writable.

## Ingest Accepted But Query Returns No Rows

This is usually expected: query visibility begins after flush.

```bash
curl -fsS -X POST http://localhost:8080/api/v1/tables/default/events/flush
```

Then query again.

## WAL Replay Problems

Look for:

- WAL corruption messages.
- Recovery mode.
- Missing table tombstone handling.
- Catalog committed generation metadata.
- WAL directory lease conflicts.

Do not delete WAL files to “fix” replay unless you intentionally accept loss of unflushed rows.

## Query Failures

Check:

- SQL validity.
- Catalog table resolution.
- Unsupported equality deletes.
- Object-store read errors.
- Spill directory capacity.
- Query timeout.
- Ballista scheduler/executor reachability in distributed mode.

Use `POST /api/v1/query/explain` to inspect plans.

## Distributed Failures

Check:

- `cluster.scheduler_addr` from data nodes.
- Control-plane scheduler bind address.
- Data-node `--executor-advertise-host`.
- Internal ports between pods/containers.
- Executor quorum in `/ready`.
- HAProxy or service routing.

## Metrics And Logs

Use `/metrics` for counters and histograms. If `security.admin_token` is set, include the bearer token.

Set log level through config, environment, or CLI:

```bash
teodb --config deploy/docker/config/standalone.toml --log-level debug
```
