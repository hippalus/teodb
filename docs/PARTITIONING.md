# Partitioning

Navigation: [README](../README.md) | [Architecture](ARCHITECTURE.md) |
Related: [Query Planner](QUERY_PLANNER.md), [Write Path](WRITE_PATH.md), [Indexes](INDEXES.md)

TeoDB uses Apache Iceberg partition specs for table file layout and pruning. It does not implement automatic database sharding.

## Table Partitioning

When a table has a partition spec, flush groups rows into partitioned output files and records partition values in Iceberg data
file metadata.

During query planning, TeoDB uses partition values to skip files when it can prove a predicate cannot match.

## Pruning Support

Current pruning support is conservative:

- Identity partition transforms can be evaluated for pruning.
- Unsupported or non-identity transforms are kept rather than incorrectly pruned.
- Files with mismatched or unclear spec information are kept.

Keeping a file is slower than pruning it, but it preserves correctness.

## Sort Order

Sort order is separate from partitioning. When table metadata includes a sort order, the Parquet writer can sort flushed batches
before writing. This improves file statistics and scan locality for compatible predicates.

## Distributed Execution Partitions

Do not confuse table partitioning with distributed execution partitions. DataFusion and Ballista split work into execution
partitions based on runtime planning and configuration. Iceberg partitioning controls data layout and file pruning.

## Not Implemented

TeoDB does not currently provide:

- Automatic shard creation.
- Tablet ownership.
- Rebalancing.
- Partition-level write routing.
- Partition-specific replication.
