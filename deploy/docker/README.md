# Docker Compose

These files run TeoDB with RustFS and an Iceberg REST catalog.

## Needs

- Docker Engine 24 or newer.
- Docker Compose V2.
- At least 8 GB of Docker memory for image builds.

## Standalone

Use standalone mode for local work:

```bash
docker compose -f deploy/docker/docker-compose.standalone.yaml up -d --build
```

| Service | Address |
|---------|---------|
| REST and admin UI | <http://localhost:8080> |
| Flight SQL | `localhost:8815` |
| RustFS console | <http://localhost:9001/rustfs/console/> |
| Iceberg REST | <http://localhost:8181> |

Check readiness:

```bash
curl -fsS http://localhost:8080/ready
```

## Local Services Only

Use this file when TeoDB runs from source:

```bash
docker compose -f deploy/docker/docker-compose.rustfs.yaml up -d
cargo run -p teodb-server -- --config config/dev.toml
```

| Service | Address |
|---------|---------|
| RustFS S3 API | <http://localhost:19000> |
| RustFS console | <http://localhost:19001/rustfs/console/> |
| Iceberg REST | <http://localhost:8181> |

The `bucket-init` service creates the `teodb` bucket. `config/dev.toml` already
uses these addresses and local test keys.

## Single-host Cluster

The production-like file starts:

- Three equal data nodes.
- One active control plane.
- HAProxy for REST and Flight SQL.
- RustFS, Postgres, and Iceberg REST.
- Private volumes for each WAL, cache, and spill path.

Create the local environment file first:

```bash
cd deploy/docker
cp .env.example .env
docker compose -f docker-compose.production.yaml up -d --build
```

| Service | Address |
|---------|---------|
| REST through HAProxy | <http://localhost:8080> |
| Flight SQL through HAProxy | `localhost:8815` |
| HAProxy status | <http://localhost:8404/stats> |
| RustFS console | <http://localhost:9001/rustfs/console/> |

Each data node serves reads and writes. Each node also runs one Ballista
executor. HAProxy uses round-robin routing.

Keep `TEODB_CLUSTER_ID` stable for the cluster. Each data node must have a
unique writer slot and WAL volume. Do not mount one WAL volume in two live
containers.

Load sample data through HAProxy:

```bash
TEODB_HTTP=http://127.0.0.1:8080 \
TEODB_FLIGHT=http://127.0.0.1:8815 \
./scripts/load-test-data.sh --only tpch,nested,partition,smoke,flight
```

## Secrets

The production-like stack reads secrets from `deploy/docker/.env`. The file is
ignored by Git.

Main keys:

- `TEODB_S3_ACCESS_KEY`
- `TEODB_S3_SECRET_KEY`
- `TEODB_S3_REGION`
- `TEODB_PG_DB`
- `TEODB_PG_USER`
- `TEODB_PG_PASSWORD`
- `TEODB_CLUSTER_ID`
- `TEODB_ADMIN_TOKEN`

An empty admin token leaves admin APIs and `/metrics` open. The server writes a
warning at startup.

See [Configuration](../../docs/CONFIGURATION.md) for config order and all
server keys.

## Build Only The Image

```bash
docker build -f deploy/docker/Dockerfile -t teodb:latest .
```

Set build jobs when memory is limited:

```bash
docker build -f deploy/docker/Dockerfile \
  --build-arg CARGO_BUILD_JOBS=4 \
  --build-arg CARGO_PROFILE_RELEASE_LTO=thin \
  -t teodb:latest .
```

## Stop And Remove Data

This command removes the standalone volumes:

```bash
docker compose -f deploy/docker/docker-compose.standalone.yaml down -v
```
