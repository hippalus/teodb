# Benchmarks

Navigation: [README](../README.md) | [Architecture](ARCHITECTURE.md) |
Related: [Performance](PERFORMANCE.md), [Testing](TESTING.md), [CLI](CLI.md)

Benchmarking lives in `crates/teodb-perf-suite`. It is an external workload runner rather than an in-process microbenchmark
harness.

## Commands

```bash
cargo run -p teodb-perf-suite -- prepare-data --dataset crates/teodb-perf-suite/datasets/tpch.toml
cargo run -p teodb-perf-suite -- load --dataset crates/teodb-perf-suite/datasets/tpch.toml
cargo run -p teodb-perf-suite -- run-suite --bench <scenario.toml>
cargo run -p teodb-perf-suite -- run-flight-bench --bench <flight-bench.toml>
cargo run -p teodb-perf-suite -- report --input perf-results/suite-results.json
```

Common options:

- `--http`, default `http://127.0.0.1:8080`.
- `--flight`, default `http://127.0.0.1:8815`.
- `--user`, default `admin`.
- `--password`, default `password`.
- `--work-dir`, default `artifacts/perf-suite`.

## Dataset Manifests

Dataset manifests are stored in `crates/teodb-perf-suite/datasets`. They cover synthetic JSON, nested JSON, TPC-H,
Flight-specific, and external Parquet-style workloads.

## CI Benchmarks

The CI workflow can run `cargo bench --workspace --locked` when invoked with `run_bench: true`. The full external perf suite is
better suited for dedicated benchmark machines because it depends on server deployment, object storage, catalog behavior, and
network paths.

## Benchmark Hygiene

When reporting performance:

- Record commit SHA and configuration.
- Record deployment mode.
- Record object store and catalog placement.
- Record cache state: cold, warm, or mixed.
- Separate ingest, flush, query, and end-to-end timings.
- Include failure counts and query errors, not only successful latency.
