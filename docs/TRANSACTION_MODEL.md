# Transaction Model

Navigation: [README](../README.md) | [Architecture](ARCHITECTURE.md) |
Related: [Consistency](CONSISTENCY.md), [Catalog](CATALOG.md), [Locking](LOCKING.md)

TeoDB does not implement general SQL transactions. Its transaction model is a set of narrower atomicity boundaries around ingest,
flush, and catalog commits.

## Supported Atomicity Boundaries

| Operation      | Atomic boundary                                                                  |
|----------------|----------------------------------------------------------------------------------|
| Ingest request | WAL append plus buffer admission on one data node.                               |
| Flush          | Iceberg catalog commit of produced data files.                                   |
| Query          | One pinned snapshot per table scan for the query lifetime.                       |
| DDL            | Catalog operation, plus local cleanup effects where implemented.                 |
| Compaction     | Intended as a catalog replace commit, currently interim and disabled by default. |

## Ingest Is Not A SQL Transaction

An ingest request is accepted when rows are durable in the accepting node’s WAL. The request does not create a new query-visible
snapshot by itself.

If the process crashes after WAL append and before flush, startup replay rebuilds the unflushed buffer from WAL records.

If the local WAL disk is lost before flush, TeoDB has no replicated copy of those unflushed rows.

## Flush Is The Visibility Commit

Flush is the operation that crosses from local durability to global query visibility:

1. Drain pending buffer generations.
2. Write Parquet files.
3. Commit an Iceberg append.
4. Mark buffer generations committed.
5. Mark WAL generations committed and run GC.

The catalog commit is the point where other query planners can see the data.

## DDL

DDL is routed before normal DataFusion query execution. Supported DDL operations operate through the catalog and service layer.
DDL does not create a multi-statement transaction with following writes or reads.

Drop table handling includes WAL tombstone behavior so replay does not resurrect records for a dropped table.

## Isolation

Queries use snapshot isolation at the Iceberg snapshot level. They do not see unflushed writes. They also do not change view
mid-query when another flush commits a newer snapshot.

## Conflict And Retry

Optimistic catalog commits can conflict. Conflicts are surfaced as conflict errors and retried only where the caller can safely
rebuild state.

Retryable external failures are marked separately from fatal failures. Clients should not blindly retry non-idempotent operations
unless they understand the operation boundary.

## Not Supported

TeoDB does not support:

- `BEGIN` / `COMMIT` / `ROLLBACK`.
- Serializable transactions across multiple statements.
- Atomic transactions across multiple tables.
- Cross-node distributed transactions.
- Row-level write/write conflict tracking.
