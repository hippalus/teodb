# Query Execution

Navigation: [README](../README.md) | [Architecture](ARCHITECTURE.md) |
Related: [Query Engine](QUERY_ENGINE.md), [Network Protocols](NETWORK_PROTOCOL.md), [Performance](PERFORMANCE.md)

Query execution runs prepared DataFusion plans either through the embedded/remote Ballista engine or through local fallback when
explicitly enabled.

## Execution Modes

| Mode                          | Behavior                                                                                                 |
|-------------------------------|----------------------------------------------------------------------------------------------------------|
| Standalone                    | Embedded scheduler and executor in one process.                                                          |
| Data node                     | Submit to configured remote scheduler.                                                                   |
| Data node with local fallback | Fall back to local execution only when the scheduler is unreachable before any result batch is returned. |

Fallback after partial results would create ambiguous client behavior, so TeoDB does not do it.

## REST Execution

REST `/api/v1/query`:

- Authorizes query action.
- Rejects empty SQL.
- Applies configured result limit.
- Uses one query deadline across planning, execution, and stream polling.
- Cancels the query on timeout.
- Serializes result batches into JSON rows.

`/api/v1/query/explain` returns the plan view used for debugging.

## Flight SQL Execution

Flight SQL:

- Streams Arrow batches as `FlightData`.
- Supports direct statement query.
- Supports prepared statement creation, execution, and close.
- Implements metadata commands for catalogs, schemas, tables, table types, SQL info, and primary keys.
- Does not implement `do_exchange`.

Flight is the preferred API for larger result sets because it preserves Arrow columnar batches.

## Cancellation And Timeouts

The query engine exposes cancellation and status tracking. REST applies an end-to-end timeout and attempts best-effort
cancellation when the deadline is exceeded.

## Memory And Spill

Execution uses DataFusion/Ballista memory settings from configuration. Spill goes to the configured local spill directory.
Operators should monitor spill and memory settings together; under-sized spill storage can turn a memory-pressure event into query
failure.

## Position Delete Filtering

Position deletes are applied by wrapping scans with a filter that tracks row positions in a file. This is conservative and correct
for supported delete files, but it is not a replacement for equality delete support.
