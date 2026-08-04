# TeoDB Helm Chart

This chart runs TeoDB on Kubernetes. It does not install object storage or an
Iceberg REST catalog.

## Modes

| Mode | Workloads |
|------|-----------|
| `cluster` | One active control-plane Deployment and a data-node StatefulSet. |
| `standalone` | One StatefulSet with all services in one process. |

A data node serves REST and Flight SQL. It owns one WAL volume and runs one
Ballista executor. The control plane runs the scheduler.

## Install

Standalone:

```bash
helm install teodb deploy/helm/teodb \
  -f deploy/helm/teodb/values-standalone.yaml
```

Cluster:

```bash
helm install teodb deploy/helm/teodb \
  -f deploy/helm/teodb/values-production.yaml
```

Check the service:

```bash
helm test teodb
kubectl port-forward svc/teodb 8080:8080
curl -fsS http://localhost:8080/ready
```

## External Services

Set these values for your Iceberg and S3 services:

- `catalog.uri`
- `catalog.warehouse`
- `storage.endpoint`
- `storage.region`
- `storage.allowHttp`

Use HTTP only on a trusted local network.

## Config Order

TeoDB reads config in this order:

```text
CLI > environment > TOML > defaults
```

The chart uses:

| Source | Content |
|--------|---------|
| ConfigMap | Main TOML config. |
| Secret and Downward API | S3 keys, admin token, cluster ID, node ID, and writer slot. |
| Process args | Config path and executor host. |

Extra TOML can go in `dataNode.extraConfigToml`,
`controlPlane.extraConfigToml`, or `standalone.extraConfigToml`.

## Secrets

The chart can create a Secret:

```yaml
secret:
  create: true
  s3AccessKey: "..."
  s3SecretKey: "..."
  adminToken: "..."
```

Pass secret values outside Git. Do not commit them in a values file.

You can also use `secret.existingSecret`. Its keys must match `secret.keys.*`.
It also needs `cluster-id` unless `cluster.id` is set.

When `cluster.id` is empty and the chart owns the Secret, the chart creates and
keeps one cluster ID.

An empty admin token leaves admin APIs and `/metrics` open. The server writes a
warning at startup.

## Local Storage

| Volume | Default backing | Path | Work |
|--------|-----------------|------|------|
| `data` | PVC | `/var/lib/teodb/data` | WAL and durable local state. |
| `cache` | PVC or `emptyDir` | `/var/lib/teodb/cache` | Object cache. |
| `spill` | `emptyDir` | `/var/lib/teodb/spill` | Query and compaction spill. |

Each data-node pod needs its own WAL PVC. Do not clone a live WAL PVC or run two
pods with the same StatefulSet number.

See [Multi-writer Operations](../../../docs/MULTI_WRITER_OPERATIONS.md).

## Ports

| Port | Name | Use |
|------|------|-----|
| `8080` | REST | Public REST, UI, health, and metrics. |
| `8815` | Flight | Public Flight SQL. |
| `50050` | control-plane | Internal scheduler traffic. |
| `50051` | exec-bind | Internal executor bind. |
| `50052` | exec-grpc | Internal executor gRPC. |

Set `networkPolicy.enabled: true` to limit the internal ports to pods in the
same TeoDB release.

## Check The Chart

```bash
helm lint deploy/helm/teodb
helm template teodb deploy/helm/teodb >/dev/null
helm template teodb deploy/helm/teodb --set mode=standalone >/dev/null
```

CI also checks `values.yaml`, `values-standalone.yaml`, and
`values-production.yaml`.

## Main Values

| Key | Default |
|-----|---------|
| `mode` | `cluster` |
| `image.repository` | `ghcr.io/hippalus/teodb` |
| `dataNode.replicas` | `3` |
| `cluster.maxWriterCheckpointsPerTable` | `32` |
| `maintenance.snapshotRetentionSecs` | `0` |
| `controlPlane.replicas` | `1` |
| `dataNode.persistence.wal.size` | `10Gi` |
| `networkPolicy.enabled` | `false` |
| `serviceMonitor.enabled` | `false` |
| `ingress.enabled` | `false` |

See [`values.yaml`](values.yaml) for all values.
