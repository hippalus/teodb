# FAQ

Navigation: [README](../README.md) | [Architecture](ARCHITECTURE.md) |
Related: [Consistency](CONSISTENCY.md), [Roadmap](../ROADMAP.md)

## Is TeoDB production-ready?

No. It has real storage and query parts, but it is still early software. Read
the current limits before you run it.

## Why do I need to flush before querying?

Ingest acknowledgement means WAL durability. Query visibility is tied to Iceberg snapshot commits. Flush writes Parquet files and
commits the snapshot that queries can read.

## Does TeoDB implement MVCC?

Not row-level MVCC. It uses Iceberg snapshot isolation for query reads.

## Does distributed mode replicate data?

No. Distributed mode uses Ballista for query execution across data nodes. It does not replicate WAL or hot buffers.

## Is there a primary key?

No. TeoDB does not enforce primary keys, unique constraints, or secondary indexes today.

## What happens if a data node crashes?

If its local WAL remains intact, startup replay rebuilds unflushed buffers. If local WAL storage is lost before flush, TeoDB has
no replicated copy of those rows.

## Why is compaction disabled by default?

The mechanics exist, but the current Iceberg Rust API path used by TeoDB does not expose the final native replace/overwrite commit
operation needed for production compaction. The current marker model is intentionally not enabled by default.

## Which API should I use?

Use REST for simple automation and JSON workflows. Use Flight SQL for Arrow-native clients and larger result sets.

## Can I use S3-compatible object storage?

Yes. TeoDB is configured through endpoint, region, and credential settings. Set `s3_allow_http = true` only for trusted local HTTP endpoints.
