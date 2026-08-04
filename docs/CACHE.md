# Cache

Navigation: [README](../README.md) | [Architecture](ARCHITECTURE.md) |
Related: [Object Storage](OBJECT_STORAGE.md), [Read Path](READ_PATH.md), [Performance](PERFORMANCE.md)

TeoDB’s local object cache is a whole-object read-through cache for immutable object-store files.

## Purpose

Analytical queries often reread Parquet footers, metadata, and hot data files. A local cache reduces repeated object-store reads
without changing correctness.

The cache is never authoritative. Removing cache files should affect performance only.

## Behavior

- `get` checks the local index first.
- Misses use single-flight loading to avoid duplicate concurrent downloads.
- Cached files are content-addressed and checksum-verified.
- Range reads are served from cache when the whole object is present.
- Small ranges can bypass cache fill.
- Large ranges can promote an object when they cover enough of it.
- Writes, deletes, and copies invalidate affected entries.
- The cache index is persisted periodically and on shutdown by maintenance.

## Limits

Configuration controls:

- Total cache size.
- Maximum object size to cache.
- Cache directory.

The default total size is 10 GiB and the default max object size is 512 MiB.

## Tradeoffs

Whole-object caching is deliberately simple. It works well for repeated reads of moderate-size files and avoids the complexity of
a block cache. It can waste space for sparse reads of very large objects.

A block cache should be considered only if benchmarks show that whole-object caching is the limiting factor.
