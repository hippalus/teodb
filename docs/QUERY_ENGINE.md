# Query Engine

Navigation: [README](../README.md) | [Architecture](ARCHITECTURE.md) |
Related: [Query Planner](QUERY_PLANNER.md), [Query Execution](QUERY_EXECUTION.md), [Distributed Mode](DISTRIBUTED.md)

TeoDB’s query engine is a thin database-specific layer around DataFusion and Ballista.

## Responsibilities

- Build DataFusion sessions with TeoDB catalog providers.
- Register object stores and runtime configuration.
- Classify and route DDL before normal query execution.
- Prepare SQL queries.
- Pin scanned snapshots.
- Execute queries locally or through Ballista depending on role/configuration.
- Track query status and cancellation.
- Stream results to REST and Flight boundaries.

## DataFusion Session Factory

`DataFusionSessionFactory` owns shared runtime configuration:

- Memory pool size.
- Spill directory.
- Batch size.
- Target partitions.
- Object store registrations.
- TeoDB catalog provider.
- Project UDFs such as `url_path_hash`.
- Metadata refresh behavior.

Sessions are created per principal so security and session state can be carried without making the runtime global mutable state.

## Table Providers

TeoDB table providers bridge Iceberg metadata into DataFusion:

- `TeoTableProvider` plans scans against current catalog metadata.
- Pinned scan providers execute a frozen snapshot descriptor.

Providers are responsible for loading manifest information, selecting data files, applying pruning, loading position deletes, and
producing scan plans.

## DDL Routing

DDL statements are classified before falling through to DataFusion. Supported DDL operations use TeoDB catalog services. Other SQL
is planned by DataFusion.

## Standalone Mode

Standalone mode starts embedded Ballista scheduler and executor components and uses them through the same query-engine surface.
This keeps standalone closer to distributed execution than a completely separate local path.

## Data-Node Mode

Data-node mode builds a remote Ballista query engine pointed at the configured scheduler. Local query fallback is available only
when `cluster.local_query_fallback` is enabled and only before partial remote results have been returned.

## Query Status

The query engine retains bounded query status entries with a configurable TTL. This supports admin visibility without unbounded
memory growth.

## Limits

TeoDB does not implement a separate SQL optimizer. DataFusion owns SQL parsing, logical planning, optimization, and physical plan
construction. TeoDB contributes providers, pruning data, delete filtering, DDL routing, and distributed execution integration.
