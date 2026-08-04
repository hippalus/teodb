# Indexes And Pruning

Navigation: [README](../README.md) | [Architecture](ARCHITECTURE.md) |
Related: [Query Planner](QUERY_PLANNER.md), [Partitioning](PARTITIONING.md), [Read Path](READ_PATH.md)

TeoDB does not implement secondary indexes.

Query performance currently comes from columnar layout, partition pruning, file statistics, DataFusion planning, Ballista
execution, and object-cache locality.

## What Exists

- Iceberg partition metadata.
- Parquet and Iceberg file statistics.
- DataFusion pruning predicates.
- Sort-order-aware Parquet writes where table metadata provides a sort order.
- Local whole-object cache.

## What Does Not Exist

- B-tree indexes.
- Hash indexes.
- Inverted indexes.
- Zone maps maintained independently from Parquet/Iceberg statistics.
- Primary-key enforcement.
- Unique constraints.

## Why This Is Acceptable For Now

TeoDB is focused on OLAP scans over immutable columnar files. For that workload, file skipping and columnar execution are the
first correctness and performance layers to get right.

Secondary indexes introduce maintenance, consistency, recovery, and query-planner obligations. They should be added only when the
storage and transaction model for them is explicit.

## Practical Guidance

To improve scan performance today:

- Choose partition columns that match common filters.
- Use sort order for columns frequently used in range predicates.
- Keep compaction healthy once the production replace path is complete.
- Size the object cache for repeated working sets.
- Use Flight SQL for large result transfer.
