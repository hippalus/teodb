# Deployment

Navigation: [README](../README.md) | [Architecture](ARCHITECTURE.md) |
Related: [Configuration](CONFIGURATION.md), [Distributed Mode](DISTRIBUTED.md), [Security](../SECURITY.md)

TeoDB includes Docker Compose and Helm deployment artifacts. They are useful for development, evaluation, and production-style
exercises, but they are not a substitute for environment-specific review.

## Docker Compose

### Standalone

```bash
docker compose -f deploy/docker/docker-compose.standalone.yaml up --build
```

This starts:

- TeoDB standalone.
- S3-compatible object storage.
- Postgres.
- Iceberg REST catalog.

Public endpoints:

- REST: `http://localhost:8080`
- Flight SQL: `localhost:8815`
- Iceberg REST: `http://localhost:8181`

### Production-Like Cluster

```bash
cd deploy/docker
cp .env.example .env
docker compose -f docker-compose.production.yaml up --build
```

This starts one control plane, three data nodes, HAProxy, object storage, Postgres, and Iceberg REST. It is useful for understanding the distributed topology on one host.

It is not a complete production environment. Review secrets, TLS, network isolation, storage durability, backup, and resource
sizing before exposing it.

### Infrastructure Only

When running TeoDB from source, start just the object store (RustFS) and Iceberg REST catalog:

```bash
docker compose -f deploy/docker/docker-compose.rustfs.yaml up -d
cargo run -p teodb-server -- --config config/dev.toml
```

This exposes S3 on `localhost:19000`, the RustFS console on `localhost:19001/rustfs/console/`, and the Iceberg REST catalog on
`localhost:8181` — the endpoints `config/dev.toml` already points at.

## Helm

The Helm chart deploys TeoDB but expects an existing object store and Iceberg REST catalog.

Standalone:

```bash
helm install teodb deploy/helm/teodb -f deploy/helm/teodb/values-standalone.yaml
```

Cluster:

```bash
helm install teodb deploy/helm/teodb -f deploy/helm/teodb/values-production.yaml
```

The chart renders TOML config into a ConfigMap and injects S3/admin credentials through a Secret.

## Storage Requirements

Data-node and standalone processes need:

- Durable local storage for WAL and data directory.
- Local cache storage if cache is enabled.
- Local spill storage sized for query and compaction pressure.
- Access to object storage for table data files.
- Access to Iceberg REST catalog.

The control plane needs much less durable local state in the current design.

## Network Requirements

Expose to clients:

- REST.
- Flight SQL.

Restrict internally:

- Ballista scheduler.
- Ballista executor bind and executor gRPC.
- Object store credentials and endpoint.
- Iceberg REST catalog.

## Security Checklist

- Set `security.admin_token`.
- Do not use dev object-store credentials.
- Keep credentials in environment variables or Kubernetes Secrets.
- Use TLS or a trusted TLS-terminating proxy.
- Restrict internal gRPC ports.
- Review CORS origins for browser exposure.
- Keep WAL fsync enabled.

## Readiness And Shutdown

Use `/ready` for load balancer readiness. During shutdown TeoDB marks itself draining, stops accepting new work, drains transports
and background workers, and releases local WAL ownership.
