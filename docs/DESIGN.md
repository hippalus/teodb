# Design Rules

Navigation: [README](../README.md) | [Architecture](ARCHITECTURE.md) |
[Consistency](CONSISTENCY.md) | [Roadmap](../ROADMAP.md)

This file lists the main choices in TeoDB. It describes the current code. It
does not describe a general database design.

## Ingest And Query Visibility

A successful ingest means the rows are in the local WAL. The rows become
visible after flush commits an Iceberg snapshot.

Queries do not join the hot buffer with table files. This keeps one query view
stable and lets any executor read the same committed files.

## Iceberg Owns Committed Table State

The Iceberg REST catalog is the source of committed table metadata. Object
storage holds the file bytes. Local WAL and buffer state are not catalog state.

Writers use Iceberg optimistic commits. A retry must keep the same commit ID
and file set. TeoDB does not treat a network error as proof that a commit
failed.

## Each Writer Owns One WAL

Each data writer has a stable writer slot and a private WAL directory. Two
live processes must not share a writer slot or WAL directory.

The WAL is not copied to another node. Loss of the WAL disk can lose rows that
were not flushed.

## Data Nodes Have The Same Public Role

Each data node can serve REST and Flight SQL, accept ingest, plan queries, and
run a Ballista executor. The control plane runs the active scheduler.

This makes public routing simple. It does not provide data replication.

## DataFusion And Ballista Work Together

DataFusion builds and runs query plans. Ballista sends plan work to executors.
Standalone mode starts an embedded scheduler and executor. Data-node mode uses
the configured control plane.

Arrow, DataFusion, and Ballista versions move as one set because plans and data
cross process boundaries.

## REST And Flight Share Services

REST is useful for JSON and simple tools. Flight SQL is useful for Arrow data
and larger result streams.

Both protocols call the same query, ingest, catalog, auth, and admission
services. Protocol code should only translate requests, replies, and errors.

## Use Upstream Features First

TeoDB should not invent an Iceberg metadata action when the Rust library does
not support it safely.

- Compaction stays off until a native replace action is available.
- Snapshot metadata expiration stays off until safe expiration is available.
- New pin or retention work should use public Iceberg tools where possible.

A custom format or coordination service needs a separate design decision. It
must not be added as a small workaround.

## Active Development Uses The Latest Design

TeoDB is pre-1.0 and has no backward compatibility promise. The latest config,
API, WAL, and metadata design is the only supported design.

Do not add old code paths, format readers, or migration layers unless the
project first changes this rule. See [Versioning](VERSIONING.md).

## Keep Limits Clear

TeoDB does not hide missing safety behind a feature flag or a success reply.
Current limits include:

- No WAL quorum or replication.
- No cluster-wide idempotency ledger.
- No full writer fencing across equal epochs.
- No highly available control plane.
- No rolling upgrade promise.

These items stay delayed until there is a clear design and a real need.
