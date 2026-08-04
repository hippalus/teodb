# Performance

Navigation: [README](../README.md) | [Architecture](ARCHITECTURE.md) |
Related: [Benchmarks](BENCHMARKS.md), [Cache](CACHE.md), [Compaction](COMPACTION.md), [Query Execution](QUERY_EXECUTION.md)

TeoDB performance depends on four paths: ingest, flush, query planning, and query execution.

## Ingest

Important knobs:

- `ingest.buffer_max_bytes`
- `ingest.flush_interval_secs`
- `ingest.max_body_bytes`
- WAL segment size and fsync behavior

The WAL writer uses group commit to amortize fsync. Buffer reservation happens before WAL append to keep post-WAL insertion from
failing.

## Flush

Flush performance depends on:

- Batch sizes in hot buffers.
- Parquet row group configuration.
- Sort order.
- Object-store write throughput.
- Catalog commit latency.

Small frequent flushes reduce visibility latency but create more files. Larger flushes improve file layout but increase
ingest-to-query delay.

## Query Planning

Planning cost depends on:

- Catalog latency.
- Metadata cache freshness.
- Manifest size.
- Number of files in the current snapshot.
- Pruning effectiveness.

Compaction and partition design directly affect planning and execution cost.

## Query Execution

Execution is shaped by:

- DataFusion memory pool.
- Spill directory throughput.
- Target partitions.
- Ballista scheduler/executor placement.
- Object-store read throughput.
- Object cache hit rate.
- Result serialization format.

Flight SQL is usually more efficient than REST for large result sets because it streams Arrow batches instead of JSON rows.

## Object Cache

The whole-object cache improves repeated reads of immutable files. It is most effective when the working set fits under
`storage.cache_max_bytes` and files are below `storage.cache_max_per_object_bytes`.

## Compaction

Compaction should eventually reduce small-file pressure and improve scan efficiency. It is disabled by default today because the
commit path still uses an interim replacement marker model.

## Measuring Changes

Use the perf suite for end-to-end measurements and ordinary Rust benchmarks for local microbenchmarks. Avoid drawing conclusions
from a single warm-cache run.
