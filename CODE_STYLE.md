# Code Style

Navigation: [README](README.md) | [Architecture](docs/ARCHITECTURE.md) | Related: [Contributing](CONTRIBUTING.md), [Testing](docs/TESTING.md)

TeoDB code should read like database infrastructure code: explicit ownership, narrow boundaries, clear errors, and tests around invariants.

## Rust

The workspace uses Rust 2024, the toolchain pinned in `rust-toolchain.toml`, `rustfmt`, and strict clippy settings. The root manifest forbids unsafe code.

Use existing local abstractions before introducing new ones. In this codebase that usually means:

- Domain types and traits live in `teodb-core`.
- IO implementations live in storage, catalog, query, ingest, distributed, or server boundary crates.
- `teodb-server` composes systems but should not become a business-logic crate.
- Errors crossing public crate boundaries should use `TeoDBError` or a crate-specific error that maps cleanly to it.
- Background work should observe shutdown and drain deadlines.

## Documentation Style

Keep documentation direct and easy to read.

- Use short sentences.
- State what the code does now.
- Keep one fact in one place when possible.
- Use examples that run against the current API.
- Do not add generic background text or sales language.
- Do not claim a planned feature is ready.

Documentation should describe this implementation, not databases in general.
Prefer concrete statements:

- Good: "Queries read the current Iceberg snapshot through `TeoTableProvider` and do not inspect the hot buffer."
- Avoid: "Modern databases often use snapshots for consistency."

When a feature is missing, say so directly and link to the design or roadmap section.

## Comments

Use comments where they preserve reasoning that the code cannot show by itself: ordering guarantees, crash-safety assumptions, catalog conflict behavior, or lifecycle constraints. Avoid comments that restate a function name or obvious Rust syntax.

## Errors

Errors should be useful at the boundary where they are observed:

- HTTP errors should map to RFC 9457 problem details.
- Flight errors should map to appropriate gRPC status codes.
- Retryable external failures should be distinguishable from fatal failures.
- Ambiguous commit states must be documented and resolved by reading authoritative state.

## Tests

Add tests around behavior, not around the shape of implementation unless the shape is the contract. Storage, WAL, replay, catalog commits, pruning, and distributed fallback all need tests for edge cases and failure paths.

## SPDX

New source files should include the project license expression in the file’s native comment syntax:

```rust
// SPDX-License-Identifier: MIT OR Apache-2.0
```

Do not add license headers mechanically to generated files or third-party files.
