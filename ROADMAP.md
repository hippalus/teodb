# Roadmap

Navigation: [README](README.md) | [Architecture](docs/ARCHITECTURE.md) |
[Design Rules](docs/DESIGN.md)

This roadmap shows direction. It is not a release promise.

## Current Focus

The next work should make one node safer and easier to run:

1. Remove Iceberg types from the public `teodb-core` catalog boundary.
2. Keep one clear query-engine construction path.
3. Add a process-wide ingest memory limit. The current limit is per table.
4. Wake the flush loop when memory pressure is high. Do not wait only for the
   timer.
5. Supervise important background tasks and report task failure.
6. Finish missing end-to-end tests for metrics, Flight size limits, and the
   real Docker stack.
7. Set useful latency and visibility targets from repeatable tests.

## Storage Work

- Reduce extra WAL checkpoint writes at high table counts.
- Measure memory, disk, and object-store pressure under long runs.
- Improve delete-file and Iceberg reader tests.
- Keep orphan cleanup safe for active snapshots and in-flight writes.

## Query And Cluster Work

- Test scheduler restart, executor loss, query cancel, and pin renewal.
- Keep cluster status useful when the scheduler cannot be reached.
- Measure local fallback and distributed query behavior under load.
- Test process drain and task shutdown.

## Work Delayed On Purpose

The following work is not part of the current hardening phase:

- WAL replication or quorum writes.
- A cluster-wide idempotency ledger.
- Full fencing for two live writers with the same epoch.
- A highly available control plane.
- Rolling upgrades and old format support.

These features need a full ownership and failure design. They are not small
refactors.

## Work Waiting For Upstream Support

TeoDB will use public Iceberg tools when they support the needed operation.
Until then:

- Snapshot metadata expiration stays off.
- Compaction stays off by default because there is no native replace action in
  the current Iceberg Rust API.
- TeoDB will not add a custom Iceberg metadata format only to enable these
  features.

This rule also applies to new metadata pin or retention ideas. Use an existing
tool when it is safe. Otherwise, wait.
