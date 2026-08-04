# Iceberg Multi-Writer Operations

TeoDB uses one write protocol. Every data node must start with the current
writer identity, WAL, checkpoint, and prepared-intent formats.

## Required Identity

Each data node requires:

- one deployment-wide non-nil `cluster_id`;
- one unique and stable `node_id`;
- one unique and stable `writer_slot`;
- one dedicated WAL volume.

The writer ID is deterministically derived from `(cluster_id, writer_slot)`.
Never run two processes with the same writer slot or attach one WAL volume to
multiple processes.

The WAL identity file is authoritative for the local writer. Startup fails if
configured identity disagrees with durable identity, if durable WAL state has
no identity, or if the identity/checkpoint files are malformed.

## Normal Commit Flow

1. Ingest is appended to the WAL before acknowledgement.
2. A flush reserves an exact writer-local generation range.
3. Unpartitioned data files are written under
   `<table>/data/<writer_id>/<commit_id>-...parquet`; partitioned files use
   `<table>/data/<iceberg-url-encoded-partition>/<writer_id>/<commit_id>-...parquet`.
4. The exact commit ID and file set are fsynced as a prepared intent.
5. One Iceberg transaction publishes the files and advances only that writer’s
   checkpoint.
6. Exact catalog proof advances the local WAL checkpoint and removes the
   prepared intent.

Retries reuse the same commit ID and file set. A new identity is never created
for an unresolved commit.

## Ambiguous Commit State

If the catalog response does not prove success or failure, TeoDB performs
bounded exact-status checks. Persistent uncertainty blocks only the affected
table.

Inspect blocked tables:

```text
GET /api/v1/admin/flush-blocked
```

Trigger another exact check:

```text
POST /api/v1/admin/flush-blocked/{namespace}/{table}/recheck
```

There is no force-success or force-discard endpoint. Resolve catalog
availability or metadata corruption, then recheck.

## Metrics

The multi-writer protocol exposes bounded-label Prometheus families for append
outcomes/rebases, exact status checks, checkpoint validation/count, flush
lock/write/block/resolution outcomes, prepared intents, and WAL replay/failure
outcomes. The relevant names begin with `teodb_catalog_commit_`,
`teodb_writer_checkpoint_`, `teodb_flush_`, `teodb_prepared_flush`, and
`teodb_wal_replay_`/`teodb_wal_recovery_`.

Table names, table UUIDs, writer IDs, and commit IDs are deliberately absent
from metric labels. Use the admin response and structured logs for exact
identity details.

## Operational Invariants

- `cluster_id`, `node_id`, and `writer_slot` stay stable for a WAL volume.
- Writer slots are unique among live writers.
- WAL and prepared-intent storage are durable.
- `cluster.max_writer_checkpoints_per_table` bounds the writer registry.
- Object cleanup respects the orphan retention window.
- Missing, forbidden, timed-out, or corrupt position-delete files fail scans.

Back up the catalog, table metadata, and every writer’s WAL volume together
when a consistent disaster-recovery point is required.
