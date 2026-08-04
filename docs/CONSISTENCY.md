# Consistency

Navigation: [README](../README.md) | [Architecture](ARCHITECTURE.md) |
Related: [Transaction Model](TRANSACTION_MODEL.md), [Write Path](WRITE_PATH.md), [Read Path](READ_PATH.md)

TeoDB’s consistency model is based on WAL durability for ingest and Iceberg snapshot isolation for query visibility.

## The Short Version

- A successful ingest means the rows were written to the accepting node’s WAL and admitted to that node’s hot buffer.
- The ingest receipt’s exact local position is `(table_uuid, writer_id, generation)`;
  a generation alone is not globally unique.
- Queries read flushed data only.
- Rows become query-visible after a flush writes Parquet files and commits an Iceberg snapshot.
- Each query pins one snapshot for its lifetime.
- Running queries are protected from cleanup by snapshot pins.
- There is no cross-node WAL replication or row-level MVCC today.

## Ingest Visibility

REST and Flight ingest return after:

1. Input validation.
2. Table resolution or auto-create.
3. Buffer capacity reservation.
4. WAL append.
5. Buffer insertion.

That response does not mean the data is query-visible. It means the accepting data node can replay the rows after a crash,
assuming its local WAL storage is intact.

To make rows query-visible deterministically:

```bash
curl -fsS -X POST http://localhost:8080/api/v1/tables/default/events/flush
```

or wait for the periodic flush loop.

## Query Snapshot Isolation

At planning time, TeoDB resolves the current Iceberg snapshot for each scanned table. The query engine pins the snapshot while the
query is alive. Later flushes or maintenance jobs do not change what that running query reads.

Snapshot pins are also consulted by orphan sweeping and retention so files needed by active queries are not deleted.

This is snapshot-based file visibility, not row-level MVCC. TeoDB does not maintain per-row transaction ids, undo chains,
tuple-visibility checks, multi-version hot-buffer reads, or long-running SQL transactions.

## Write Conflicts

Each flush gets one immutable `commit_id`, a writer epoch, a writer-local
generation range, and a stable set of Parquet files. The Iceberg transaction
atomically appends those files and advances only
`teodb.writer.v1.<writer_id>`. Concurrent writers therefore update distinct
checkpoint properties. Iceberg may rebase an append onto newer metadata, but
TeoDB revalidates table UUID, exact commit identity, writer epoch, generation
monotonicity, and the bounded writer registry on every retry attempt.

An ambiguous catalog response is never converted into a blind retry with new
files or a new commit ID. TeoDB searches retained snapshot history and the
writer checkpoint for the exact identity. If the outcome remains unknown
after the bounded status-check budget, that table enters `FlushBlocked`: its
prepared intent, WAL, files, and in-flight rows remain owned and new writes to
that table are rejected. Other tables and queries continue. Operators can
inspect and recheck this state through the authenticated admin endpoints; no
force-success or discard operation exists.

## Distributed Consistency

Distributed mode does not change the visibility model. Data nodes still own local WAL and hot buffers. The Iceberg catalog remains
the shared committed state.

Consequences:

- A query routed to a different data node can see committed snapshots, but not another node’s unflushed buffer.
- Idempotency keys are local to the stable writer that accepted the request.
- Receipts include `writer_id`; clients should retry against that writer while
  relying on the local idempotency window.

## What TeoDB Does Not Claim

TeoDB does not currently provide:

- Immediate read-after-ingest.
- Serializable multi-statement transactions.
- Row-level MVCC.
- Cross-node WAL replication.
- Automatic failover for unflushed node-local data.
- Global idempotency across data nodes.
- A cluster-wide meaning for a bare generation number.

Those are roadmap-level design topics, not hidden capabilities.
