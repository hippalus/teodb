# Changelog

This file lists changes that affect users and contributors.

## Unreleased

### Added

- Added the `teodb-api` crate for REST, Flight SQL, auth, and API services.
- Added strict multi-writer IDs, checkpoints, prepared intents, and exact
  commit status checks.
- Added admin APIs for blocked flushes and cluster scheduler status.
- Added transport, auth, catalog, buffer, flush, and WAL metrics.
- Added detailed server error logs with source chains, source locations, and
  backtraces for internal failures.
- Added real RustFS and Iceberg REST tests for the multi-writer release gate.

### Changed

- Replaced MinIO development files with RustFS files.
- Moved REST and Flight SQL code out of `teodb-ingest` and into `teodb-api`.
- Made `/metrics` the Prometheus scrape endpoint and `/ui/metrics` the admin UI
  page.
- Added scheduler, executor, and active job data to the Cluster page.
- Limited protocol timing checks to the fixed runner used by the reviewed
  baseline.
- Added retry around BuildKit container startup in CI.
- Set the active development rule: the latest design wins, with no backward
  compatibility promise.

### Fixed

- Fixed Metrics page polling and navigation after Chart.js received reactive
  Vue arrays.
- Fixed the raw metrics view.
- Fixed request logs so internal errors show the real error and origin.

### Current Limits

- Query visibility starts after flush.
- WAL and idempotency state are not copied across nodes.
- Compaction and snapshot metadata expiration are off by default.
- The control plane has one active in-memory scheduler.

## 0.1.0

First development version.
