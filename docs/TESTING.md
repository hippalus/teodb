# Testing

Navigation: [README](../README.md) | [Debugging](DEBUGGING.md) |
[Benchmarks](BENCHMARKS.md) | [Contributing](../CONTRIBUTING.md)

## Rust Checks

Run the full local set:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
```

Skip the admin UI build for backend-only work:

```bash
TEODB_SKIP_UI_BUILD=1 cargo test --workspace --all-targets --locked
```

Run one crate or test name first when the change is small:

```bash
TEODB_SKIP_UI_BUILD=1 cargo test -p teodb-query --locked
TEODB_SKIP_UI_BUILD=1 cargo test -p teodb-query --locked pruning
```

## Frontend Checks

```bash
cd frontend
npm ci
npm run typecheck
npm run test
npm run build
```

## Real Object Storage Tests

Some tests need Docker. They start pinned RustFS, Postgres, and Iceberg REST
containers with Testcontainers.

Example:

```bash
TEODB_SKIP_UI_BUILD=1 cargo test \
  -p teodb-server \
  --test object_storage_tier \
  --locked \
  drop_purge_reclaims_only_the_dropped_table_prefix \
  -- --ignored
```

Tests can also use an existing stack. Set these variables:

- `TEODB_TEST_CATALOG_URI`
- `TEODB_TEST_WAREHOUSE`
- `AWS_ENDPOINT_URL`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`

## Multi-writer Release Gate

Start the pinned CI stack:

```bash
docker compose \
  -f deploy/docker/docker-compose.rustfs.yaml \
  -f deploy/docker/docker-compose.rustfs.ci.yaml \
  up -d
```

Run the gate:

```bash
bash scripts/ci/multi-writer-release-gate.sh
```

Stop the stack:

```bash
docker compose \
  -f deploy/docker/docker-compose.rustfs.yaml \
  -f deploy/docker/docker-compose.rustfs.ci.yaml \
  down -v --remove-orphans
```

## Protocol Budget

The baseline file is:

```text
crates/teodb-catalog/benches/baselines/multi_writer_protocol.json
```

It records the source commit, runner, Rust version, test shape, and raw runs.
The structural limits run on every CI runner.

Time ratios run as a release gate only on the same fixed runner as the
baseline. Other runners print the ratios but do not compare them.

Run the strict check on the matching runner:

```bash
TEODB_PROTOCOL_TIMING_MODE=required \
  bash scripts/ci/check-multi-writer-protocol-budget.sh
```

Do not replace the baseline from a feature branch. A new baseline needs several
runs from the fixed runner and a clear performance reason.

## CI Jobs

CI checks:

- GitHub Actions files.
- Rust format, clippy, docs, and tests.
- Frontend types, tests, and build.
- Dockerfile and container build.
- Standalone Compose smoke flow.
- Helm render and lint.
- Real RustFS and Iceberg REST multi-writer flow.
- Multi-writer protocol limits.
- Optional full benchmarks.
- Rust and npm dependency security.

## Risk Areas

| Area | Tests to add |
|------|--------------|
| WAL | Corruption, replay, rotation, checkpoints, and tombstones. |
| Flush | Conflicts, unknown commit results, object errors, and WAL state. |
| Query | Snapshot pins, pruning, deletes, timeout, and cancel. |
| Cluster | Scheduler loss, executor loss, fallback, and drain. |
| Security | Tokens, allow-list rules, JWT, limits, and public errors. |
| Deploy | Config order, readiness, secrets, and ports. |
