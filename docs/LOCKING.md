# Locking

Navigation: [README](../README.md) | [Architecture](ARCHITECTURE.md) |
Related: [Catalog](CATALOG.md), [Consistency](CONSISTENCY.md), [Compaction](COMPACTION.md)

TeoDB uses a small number of locks and compare-and-swap mechanisms. It does not implement SQL row locks or table locks.

## Local WAL Lease

Each WAL directory has an advisory lease file. A process must acquire the lease before writing to that WAL. This prevents two
TeoDB processes from appending to the same local WAL directory.

The lease is local to the filesystem semantics of the data directory. It is not a distributed lock.

## Catalog Optimistic Concurrency

Iceberg catalog commits use optimistic concurrency around snapshot state. If another writer commits first, TeoDB sees a conflict
and reloads table metadata.

This is the primary correctness mechanism for committed table state.

## Table Property CAS

The catalog trait exposes compare-and-swap updates for table properties. TeoDB uses this for small coordination records such as
snapshot pins and compaction advisory locks.

Property CAS is useful, but it should not be mistaken for a general distributed lock service.

## Compaction Advisory Lock

Compaction uses catalog properties for an advisory owner/timestamp lock. Correctness is still fenced by the catalog commit. If two
compactors race, a loser can leave newly written files that are not committed; those files become orphan candidates after the
retention window.

## In-Memory Synchronization

Hot buffers, idempotency state, cache metadata, and query status maps use in-process synchronization. Those locks protect memory
invariants inside one process only.

## Not Implemented

TeoDB does not implement:

- Row locks.
- Predicate locks.
- Table-level SQL locks.
- Distributed lock leasing for data ownership.
- Consensus-backed leader election for storage shards.
