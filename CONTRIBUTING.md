# Contributing

Navigation: [README](README.md) | [Architecture](docs/ARCHITECTURE.md) | Related: [Code Style](CODE_STYLE.md), [Testing](docs/TESTING.md), [Security](SECURITY.md)

TeoDB is a database internals project. Contributions are most useful when they improve correctness, observability, maintainability, or the clarity of the architecture.

## Before You Start

Read the subsystem document for the area you plan to touch:

- Storage changes: [Storage Engine](docs/STORAGE_ENGINE.md), [Write Path](docs/WRITE_PATH.md), [Object Storage](docs/OBJECT_STORAGE.md), [Cache](docs/CACHE.md).
- Query changes: [Query Engine](docs/QUERY_ENGINE.md), [Query Planner](docs/QUERY_PLANNER.md), [Query Execution](docs/QUERY_EXECUTION.md).
- Catalog and correctness changes: [Catalog](docs/CATALOG.md), [Consistency](docs/CONSISTENCY.md), [Transaction Model](docs/TRANSACTION_MODEL.md).
- Distributed changes: [Distributed Mode](docs/DISTRIBUTED.md), [Network Protocols](docs/NETWORK_PROTOCOL.md).
- API changes: [API](docs/API.md), [CLI](docs/CLI.md), [docs/openapi.yaml](docs/openapi.yaml).

If a change alters behavior, update the relevant docs in the same pull request.

## Development Setup

Install the Rust toolchain from `rust-toolchain.toml` and the system dependencies needed by Arrow Flight and protobuf builds.

```bash
cargo build --workspace --locked
```

For backend-only work, skip the embedded UI build:

```bash
TEODB_SKIP_UI_BUILD=1 cargo build --workspace --locked
```

Start a local S3-compatible object store and Iceberg REST catalog before running tests that need external storage.

## Local Checks

Run the narrowest meaningful checks first, then broaden before opening a pull request.

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
```

Frontend checks:

```bash
cd frontend
npm ci
npm run typecheck
npm run test
npm run build
```

The CI workflow also validates the Dockerfile, runs security audits, and executes a standalone Compose smoke test on main/manual runs.

## Pull Request Expectations

A good pull request explains:

- What changed.
- Why the change belongs in this project.
- What invariants it preserves or changes.
- How it was tested.
- Any impact on durability, query visibility, catalog state, object storage layout, or distributed execution.

Avoid mixing unrelated refactors with behavior changes. Database code is easier to review when the mechanical movement and the semantic change are separate.

TeoDB is in active development. The latest design wins. Do not add old API,
config, WAL, or metadata paths only for backward compatibility.

## Correctness Invariants

Treat these as design contracts unless a pull request explicitly changes and documents them:

- Ingest acknowledgement means WAL durability, not query visibility.
- Queries read flushed Iceberg snapshots only.
- Snapshot pins protect data files from orphan sweeping while queries are running.
- Catalog commits are optimistic and must tolerate conflicts.
- WAL replay must be idempotent.
- Data nodes own their local WAL and hot buffers.
- Compaction must not drop rows or make files invisible to pinned queries.
- Admin and metrics endpoints must remain guarded when `security.admin_token` is set.

## Licensing

TeoDB is dual licensed under `MIT OR Apache-2.0`. By contributing, you agree that your contribution is provided under the same dual license. New Rust source files should use:

```rust
// SPDX-License-Identifier: MIT OR Apache-2.0
```

Other new source files should use the equivalent comment form where that is idiomatic.
