# Catalog

Navigation: [README](../README.md) | [Architecture](ARCHITECTURE.md) |
Related: [Consistency](CONSISTENCY.md), [Transaction Model](TRANSACTION_MODEL.md), [Object Storage](OBJECT_STORAGE.md)

The catalog is TeoDB’s authority for committed table state. The current implementation is an Apache Iceberg REST catalog adapter
behind the `teodb-core` catalog trait.

## Responsibilities

The catalog trait covers:

- Namespace create, list, and drop.
- Table create, list, load, and drop.
- Loading current table metadata.
- Listing live data files.
- Listing all referenced data paths for retention decisions.
- Commit append.
- Commit replace.
- Compare-and-swap table-property updates.

It does not own local WAL, hot buffers, object cache files, or query execution state.

## Commit Append

Flush uses commit append:

1. The flusher writes one or more Parquet files to object storage.
2. It durably records a prepared intent containing the table UUID, writer
   identity/epoch, commit ID, generation range, and exact file set.
3. One Iceberg transaction appends the files and updates
   `teodb.writer.v1.<writer_id>` with a versioned checkpoint.
4. The snapshot carries the exact commit tuple for history lookup.
5. Only after catalog success, or authoritative proof of that exact success,
   are the buffer/WAL cutoff advanced and the prepared sidecar removed.

Generation numbers are writer-local. Recovery reads only its own writer
checkpoint; another writer’s higher generation cannot suppress local WAL.
Every checkpoint entry is nevertheless parsed and validated so malformed
foreign metadata fails closed.

## Conflict Handling

Catalog commits are optimistic. Append rebases are safe across independent
writers because the exact immutable append is re-applied to current metadata.
An `AppendAttemptGuard` validates the protocol before the first apply and
after every Iceberg reload/retry. A stale epoch, overlapping generation with a
different commit ID, table-incarnation mismatch, registry overflow, or
malformed checkpoint is a non-retryable protocol error.

External systems can accept a commit before the client sees success. Status
resolution therefore searches for the exact `commit_id` plus writer, epoch,
generation range, table UUID, and protocol version—not merely a high
generation. Persistent uncertainty becomes a contained blocked flush.

## Commit Replace And Compaction

The catalog trait includes `commit_replace` because compaction needs to replace smaller files with larger files.

The current pinned Iceberg Rust API surface used by TeoDB does not expose the native overwrite/replace operation needed for a
final compaction commit. TeoDB therefore uses an interim model:

- New compacted files are appended.
- Superseded file paths are recorded in snapshot properties.
- TeoDB read and maintenance paths reconcile those markers when loading live files.

This is why compaction is disabled by default. The compaction machinery exists, but the commit path should be replaced with a
native Iceberg operation before it is treated as production behavior.

## Manifest Inspection

The catalog adapter reads Iceberg manifests for:

- Current live data files.
- Files referenced by retained snapshots.
- Files referenced by expired snapshots.
- Position delete files.
- Data-file reachability for orphan sweeping.

Retention protects files referenced by active or retained snapshots and by active query pins.

## Table Properties

TeoDB uses table properties for small coordination and metadata records, including:

- Versioned per-writer checkpoints (`teodb.writer.v1.<writer_id>`), bounded by
  `cluster.max_writer_checkpoints_per_table`.
- Snapshot pin registry state in distributed mode.
- Compaction advisory lock owner and timestamp.
- Interim replaced-file markers for compaction.

Properties are not a replacement for a consensus system. They are useful for catalog-scoped compare-and-swap operations and
metadata that belongs with the table.

## Catalog And Object Store Boundary

The catalog records what files are live. The object store stores the bytes. TeoDB writes files before committing metadata and
treats uncommitted files as orphan candidates after the retention window.

This ordering is intentional: object store writes cannot make data query-visible without a catalog snapshot.
