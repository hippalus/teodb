#!/usr/bin/env bash
#
# k8s-smoke.sh — end-to-end Kubernetes smoke test for the TeoDB Helm chart.
#
# Flow:
#   1. create a kind cluster
#   2. build + load the teodb image (skippable)
#   3. deploy RustFS + Postgres + Iceberg REST (object store + catalog)
#   4. helm install teodb (standalone mode)
#   5. ingest batch A -> flush -> query        (baseline)
#   6. ingest batch B (NO flush)               (lives only in WAL + buffer)
#   7. kill the teodb pod                       (crash)
#   8. wait for reschedule -> WAL replay -> flush -> query
#      assert all of A+B survived               (WAL-replay check)
#   9. teardown
#
# The kill-and-replay step is the point: batch B was never flushed before the
# pod died, so seeing it after the restart proves WAL replay off the reattached
# PVC.
#
# Usage:
#   scripts/k8s-smoke.sh
#
# Environment:
#   CLUSTER_NAME   kind cluster name        (default: teodb-smoke)
#   NAMESPACE      k8s namespace            (default: teodb-smoke)
#   RELEASE        helm release name        (default: teodb)
#   IMAGE          teodb image ref          (default: teodb:smoke)
#   SKIP_BUILD=1   reuse an existing IMAGE in the docker daemon
#   SKIP_LOAD=1    skip `kind load` (image already reachable, e.g. a registry)
#   KEEP=1         keep the cluster running on exit (for debugging)
#   ROWS_A / ROWS_B  row counts for the two batches (defaults: 50 / 30)

set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-teodb-smoke}"
NAMESPACE="${NAMESPACE:-teodb-smoke}"
RELEASE="${RELEASE:-teodb}"
IMAGE="${IMAGE:-teodb:smoke}"
ROWS_A="${ROWS_A:-50}"
ROWS_B="${ROWS_B:-30}"
PF_PORT="${PF_PORT:-18080}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART="${REPO_ROOT}/deploy/helm/teodb"

S3_KEY="teodbadmin"
S3_SECRET="teodbadmin123"
ADMIN_TOKEN="smoke-admin-token"
TABLE="default/smoke"

# ── pretty logging ───────────────────────────────────────────────────
c_blue="\033[1;34m"; c_green="\033[1;32m"; c_red="\033[1;31m"; c_yellow="\033[1;33m"; c_off="\033[0m"
log()  { printf "${c_blue}==>${c_off} %s\n" "$*"; }
ok()   { printf "${c_green}  ✔${c_off} %s\n" "$*"; }
warn() { printf "${c_yellow}  ! ${c_off}%s\n" "$*"; }
die()  { printf "${c_red}  ✗ %s${c_off}\n" "$*" >&2; exit 1; }

PF_PID=""
cleanup() {
  local rc=$?
  [[ -n "$PF_PID" ]] && kill "$PF_PID" >/dev/null 2>&1 || true
  if [[ "${KEEP:-0}" == "1" ]]; then
    warn "KEEP=1 — leaving cluster '${CLUSTER_NAME}' up. Delete with: kind delete cluster --name ${CLUSTER_NAME}"
  else
    log "Tearing down kind cluster '${CLUSTER_NAME}'"
    kind delete cluster --name "${CLUSTER_NAME}" >/dev/null 2>&1 || true
  fi
  exit $rc
}
trap cleanup EXIT

kc() { kubectl --context "kind-${CLUSTER_NAME}" -n "${NAMESPACE}" "$@"; }

# ── preflight ────────────────────────────────────────────────────────
log "Preflight"
for bin in kind kubectl helm docker jq; do
  command -v "$bin" >/dev/null 2>&1 || die "missing required tool: $bin"
done
ok "tooling present"
helm lint "${CHART}" >/dev/null || die "helm lint failed"
ok "helm lint clean"

# ── cluster ──────────────────────────────────────────────────────────
if kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
  warn "cluster '${CLUSTER_NAME}' already exists — reusing"
else
  log "Creating kind cluster '${CLUSTER_NAME}'"
  kind create cluster --name "${CLUSTER_NAME}" --wait 120s
fi
kubectl --context "kind-${CLUSTER_NAME}" create namespace "${NAMESPACE}" \
  --dry-run=client -o yaml | kubectl --context "kind-${CLUSTER_NAME}" apply -f - >/dev/null
ok "namespace ${NAMESPACE} ready"

# ── image ────────────────────────────────────────────────────────────
if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  log "Building image ${IMAGE} (this is the slow part)"
  docker build -f "${REPO_ROOT}/deploy/docker/Dockerfile" -t "${IMAGE}" "${REPO_ROOT}"
  ok "image built"
else
  warn "SKIP_BUILD=1 — using existing ${IMAGE}"
fi
if [[ "${SKIP_LOAD:-0}" != "1" ]]; then
  log "Loading ${IMAGE} into kind"
  kind load docker-image "${IMAGE}" --name "${CLUSTER_NAME}"
  ok "image loaded"
fi

# ── dependency stack: RustFS + Postgres + Iceberg REST ────────────────
log "Deploying object store + catalog"
kc apply -f - >/dev/null <<YAML
apiVersion: apps/v1
kind: Deployment
metadata: { name: rustfs, labels: { app: rustfs } }
spec:
  replicas: 1
  selector: { matchLabels: { app: rustfs } }
  template:
    metadata: { labels: { app: rustfs } }
    spec:
      containers:
        - name: rustfs
          image: rustfs/rustfs:latest
          env:
            # One data dir: the multi-dir layout enables erasure coding, which
            # requires a distinct physical disk per dir.
            - { name: RUSTFS_VOLUMES, value: "/data/rustfs0" }
            - { name: RUSTFS_ADDRESS, value: "0.0.0.0:9000" }
            - { name: RUSTFS_ACCESS_KEY, value: "${S3_KEY}" }
            - { name: RUSTFS_SECRET_KEY, value: "${S3_SECRET}" }
          ports: [{ containerPort: 9000 }]
          readinessProbe:
            httpGet: { path: /health, port: 9000 }
            periodSeconds: 3
---
apiVersion: v1
kind: Service
metadata: { name: rustfs }
spec:
  selector: { app: rustfs }
  ports: [{ name: s3, port: 9000, targetPort: 9000 }]
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: postgres, labels: { app: postgres } }
spec:
  replicas: 1
  selector: { matchLabels: { app: postgres } }
  template:
    metadata: { labels: { app: postgres } }
    spec:
      containers:
        - name: postgres
          image: postgres:18.4-alpine
          env:
            - { name: POSTGRES_DB, value: iceberg_catalog }
            - { name: POSTGRES_USER, value: iceberg }
            - { name: POSTGRES_PASSWORD, value: iceberg }
            - { name: PGDATA, value: /var/lib/postgresql/data/pgdata }
          ports: [{ containerPort: 5432 }]
          readinessProbe:
            exec: { command: ["pg_isready", "-U", "iceberg", "-d", "iceberg_catalog"] }
            periodSeconds: 3
---
apiVersion: v1
kind: Service
metadata: { name: postgres }
spec:
  selector: { app: postgres }
  ports: [{ port: 5432, targetPort: 5432 }]
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: iceberg-rest, labels: { app: iceberg-rest } }
spec:
  replicas: 1
  selector: { matchLabels: { app: iceberg-rest } }
  template:
    metadata: { labels: { app: iceberg-rest } }
    spec:
      containers:
        - name: iceberg-rest
          image: tabulario/iceberg-rest:latest
          env:
            - { name: CATALOG_WAREHOUSE, value: "s3://teodb/" }
            - { name: CATALOG_IO__IMPL, value: "org.apache.iceberg.aws.s3.S3FileIO" }
            - { name: CATALOG_S3_ENDPOINT, value: "http://rustfs:9000" }
            - { name: CATALOG_S3_PATH__STYLE__ACCESS, value: "true" }
            - { name: CATALOG_URI, value: "jdbc:postgresql://postgres:5432/iceberg_catalog" }
            - { name: CATALOG_JDBC_USER, value: "iceberg" }
            - { name: CATALOG_JDBC_PASSWORD, value: "iceberg" }
            - { name: AWS_ACCESS_KEY_ID, value: "${S3_KEY}" }
            - { name: AWS_SECRET_ACCESS_KEY, value: "${S3_SECRET}" }
            - { name: AWS_REGION, value: "us-east-1" }
          ports: [{ containerPort: 8181 }]
          readinessProbe:
            tcpSocket: { port: 8181 }
            initialDelaySeconds: 5
            periodSeconds: 3
---
apiVersion: v1
kind: Service
metadata: { name: iceberg-rest }
spec:
  selector: { app: iceberg-rest }
  ports: [{ port: 8181, targetPort: 8181 }]
YAML

log "Waiting for RustFS + Postgres"
kc rollout status deploy/rustfs --timeout=120s
kc rollout status deploy/postgres --timeout=120s

log "Creating bucket 'teodb'"
kc run bucket-init --image=amazon/aws-cli:latest --restart=Never --rm -i --quiet \
  --env="AWS_ACCESS_KEY_ID=${S3_KEY}" --env="AWS_SECRET_ACCESS_KEY=${S3_SECRET}" --env="AWS_REGION=us-east-1" \
  --command -- sh -c \
  "aws --endpoint-url http://rustfs:9000 s3 mb s3://teodb 2>/dev/null; aws --endpoint-url http://rustfs:9000 s3 ls s3://teodb >/dev/null && echo bucket-ready" \
  | grep -q bucket-ready && ok "bucket ready"

log "Waiting for Iceberg REST"
kc rollout status deploy/iceberg-rest --timeout=120s

# ── install teodb ────────────────────────────────────────────────────
log "helm install ${RELEASE} (standalone)"
helm --kube-context "kind-${CLUSTER_NAME}" upgrade --install "${RELEASE}" "${CHART}" \
  -n "${NAMESPACE}" \
  --set mode=standalone \
  --set image.repository="${IMAGE%:*}" \
  --set image.tag="${IMAGE##*:}" \
  --set image.pullPolicy=Never \
  --set catalog.uri=http://iceberg-rest:8181 \
  --set catalog.warehouse=s3://teodb \
  --set storage.endpoint=http://rustfs:9000 \
  --set storage.allowHttp=true \
  --set storage.region=us-east-1 \
  --set secret.s3AccessKey="${S3_KEY}" \
  --set secret.s3SecretKey="${S3_SECRET}" \
  --set secret.adminToken="${ADMIN_TOKEN}" \
  --set standalone.ingest.flushIntervalSecs=3600 \
  --set standalone.persistence.wal.size=2Gi \
  --set standalone.persistence.cache.size=2Gi \
  --wait --timeout 180s
ok "release installed"

STS="${RELEASE}"
POD="${STS}-0"
log "Waiting for ${POD} to be Ready"
kc rollout status "statefulset/${STS}" --timeout=180s
ok "teodb ready"

# ── port-forward ─────────────────────────────────────────────────────
start_pf() {
  [[ -n "$PF_PID" ]] && kill "$PF_PID" >/dev/null 2>&1 || true
  kc port-forward "svc/${RELEASE}" "${PF_PORT}:8080" >/dev/null 2>&1 &
  PF_PID=$!
  for _ in $(seq 1 30); do
    curl -fsS "http://127.0.0.1:${PF_PORT}/ready" >/dev/null 2>&1 && return 0
    sleep 1
  done
  die "port-forward to ${RELEASE} never became ready"
}
BASE="http://127.0.0.1:${PF_PORT}"

ingest() { # $1=start $2=count
  local rows="" i
  for ((i="$1"; i<"$1"+"$2"; i++)); do
    rows="${rows}{\"id\":${i},\"v\":\"r${i}\"},"
  done
  rows="[${rows%,}]"
  curl -fsS -X POST "${BASE}/api/v1/tables/${TABLE}/ingest" \
    -H 'content-type: application/json' \
    -d "{\"rows\":${rows}}" | jq -e '.accepted_rows' >/dev/null
}
flush() { curl -fsS -X POST "${BASE}/api/v1/tables/${TABLE}/flush" >/dev/null; }
count() {
  curl -fsS -X POST "${BASE}/api/v1/query" \
    -H 'content-type: application/json' \
    -d "{\"sql\":\"SELECT COUNT(*) AS n FROM ${TABLE/\//.}\"}" \
    | jq -r '.rows[0].n'
}

start_pf

# ── phase 1: baseline ingest -> flush -> query ───────────────────────
log "Batch A: ingest ${ROWS_A} rows -> flush -> query"
ingest 1 "${ROWS_A}"
flush
got="$(count)"
[[ "$got" == "$ROWS_A" ]] || die "baseline count mismatch: expected ${ROWS_A}, got ${got}"
ok "baseline query returned ${got} rows"

# ── phase 2: ingest WITHOUT flush, then crash the pod ────────────────
log "Batch B: ingest ${ROWS_B} more rows (NO flush) — lives only in WAL + buffer"
ingest $((ROWS_A + 1)) "${ROWS_B}"

log "Killing pod ${POD} to force a crash"
kc delete pod "${POD}" --grace-period=0 --force >/dev/null 2>&1 || kc delete pod "${POD}"
ok "pod deleted"

log "Waiting for StatefulSet to reschedule + replay WAL"
# Give the StatefulSet controller a beat to recreate the pod before polling.
sleep 3
kc rollout status "statefulset/${STS}" --timeout=180s
ok "pod back, WAL replayed on startup"

start_pf

# ── phase 3: flush replayed buffer -> query -> assert A+B survived ────
EXPECT=$((ROWS_A + ROWS_B))
log "Flush + query — expecting all ${EXPECT} rows (A+B) after replay"
flush
got="$(count)"
[[ "$got" == "$EXPECT" ]] || die "WAL-replay check FAILED: expected ${EXPECT}, got ${got}"
ok "WAL-replay check passed: ${got} rows survived the crash"

# ── helm test ────────────────────────────────────────────────────────
log "Running helm test"
helm --kube-context "kind-${CLUSTER_NAME}" test "${RELEASE}" -n "${NAMESPACE}" --timeout 120s
ok "helm test passed"

printf "\n${c_green}✔ k8s smoke test passed${c_off} (baseline=${ROWS_A}, replayed total=${EXPECT})\n"
