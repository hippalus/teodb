# Object Storage

Navigation: [README](../README.md) | [Architecture](ARCHITECTURE.md) |
Related: [Storage Engine](STORAGE_ENGINE.md), [Cache](CACHE.md), [Catalog](CATALOG.md)

Object storage holds TeoDB’s durable columnar data files. The current deployment examples assume S3-compatible storage.

## Role In The System

Object storage is where flushed Parquet files live. It is not the authority for table membership. The Iceberg catalog decides
which object paths are part of a table snapshot.

This means:

- Uploading a file does not make it query-visible.
- Deleting a catalog-unreferenced file does not change committed table state.
- Query planning should not list object storage to discover table files.

## Path Layout

Flush writes data files under the table location, typically in a `data/` subtree. File names include unique identifiers and
generation ranges so operators can inspect where a file came from.

Metadata files are managed by Iceberg. TeoDB’s orphan sweeper is intentionally scoped to table data paths and avoids deleting
metadata paths.

## S3 Configuration

TeoDB supports:

- Endpoint override for S3-compatible services.
- Region.
- Static access key and secret key.
- Standard AWS environment variable fallback.
- HTTP transport for local development endpoints when `s3_allow_http = true`.

For local Compose stacks, the defaults are development credentials. Production deployments should inject credentials through
environment variables or Kubernetes Secrets.

## Registration With Query Engines

The server registers object store access with DataFusion and Ballista runtime environments. This keeps query workers, compaction,
and flush on the same storage abstraction.

## Failure Model

Object-store writes can succeed before catalog commit. If catalog commit later fails, the uploaded files are not visible to
queries and can be cleaned up as orphans after the configured retention window.

Object-store reads can fail during query execution. Those failures surface as query or external storage errors, depending on where
they occur.

## Local Development

For source development, run an S3-compatible object store and Iceberg REST catalog locally, then point TeoDB at those endpoints through configuration or environment variables.
