{{/*
Expand the name of the chart.
*/}}
{{- define "teodb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fully qualified app name.
*/}}
{{- define "teodb.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "teodb.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "teodb.labels" -}}
helm.sh/chart: {{ include "teodb.chart" . }}
{{ include "teodb.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: teodb
{{- end }}

{{/*
Selector labels (immutable subset).
*/}}
{{- define "teodb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "teodb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Per-role selector labels: pass a dict {root, role}.
*/}}
{{- define "teodb.componentSelectorLabels" -}}
{{ include "teodb.selectorLabels" .root }}
app.kubernetes.io/component: {{ .role }}
{{- end }}

{{- define "teodb.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "teodb.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Name of the Secret holding S3 creds + admin token.
*/}}
{{- define "teodb.secretName" -}}
{{- if .Values.secret.existingSecret }}
{{- .Values.secret.existingSecret }}
{{- else }}
{{- printf "%s-secrets" (include "teodb.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Resolve the cluster UUID stored in the chart-managed Secret. A configured value
wins. Otherwise reuse the durable Secret value, or generate one when the
release Secret is first created. Data-node pods only ever consume the Secret
value; they never render a fresh UUID into pod configuration.
*/}}
{{- define "teodb.clusterId" -}}
{{- if .Values.cluster.id -}}
{{- if not (regexMatch "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$" .Values.cluster.id) -}}
{{- fail "cluster.id must be a canonical UUID" -}}
{{- end -}}
{{- if eq (lower .Values.cluster.id) "00000000-0000-0000-0000-000000000000" -}}
{{- fail "cluster.id must not be the nil UUID" -}}
{{- end -}}
{{- .Values.cluster.id -}}
{{- else -}}
{{- $secretName := include "teodb.secretName" . -}}
{{- $existing := lookup "v1" "Secret" .Release.Namespace $secretName -}}
{{- $key := .Values.secret.keys.clusterId -}}
{{- if and $existing (hasKey $existing.data $key) -}}
{{- index $existing.data $key | b64dec -}}
{{- else -}}
{{- uuidv4 -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
Resolve the container image reference. Pass a dict {root, image} where image is
the per-role image override map (may be empty).
*/}}
{{- define "teodb.image" -}}
{{- $img := .image | default dict -}}
{{- $repo := $img.repository | default .root.Values.image.repository -}}
{{- $tag := $img.tag | default .root.Values.image.tag | default .root.Chart.AppVersion -}}
{{- printf "%s:%s" $repo $tag -}}
{{- end }}

{{- define "teodb.controlPlaneHost" -}}
{{- printf "%s-control-plane" (include "teodb.fullname" .) }}
{{- end }}

{{- define "teodb.dataNodeHeadlessHost" -}}
{{- printf "%s-data-node-headless" (include "teodb.fullname" .) }}
{{- end }}

{{/*
Mount paths (kept off the image's default /app/data so PVCs are explicit).
*/}}
{{- define "teodb.dataDir" -}}/var/lib/teodb/data{{- end }}
{{- define "teodb.cacheDir" -}}/var/lib/teodb/cache{{- end }}
{{- define "teodb.spillDir" -}}/var/lib/teodb/spill{{- end }}
{{- define "teodb.configDir" -}}/etc/teodb{{- end }}

{{/*
================= Config TOML renderers (mirror deploy/docker/config) =========
Each renders a complete TOML document from values. S3 credentials are NOT
rendered here — they arrive via AWS_* env from the Secret.
*/}}

{{- define "teodb.commonStorageToml" -}}
{{- $s := .Values.storage -}}
[storage]
cache_dir = "{{ include "teodb.cacheDir" . }}"
spill_dir = "{{ include "teodb.spillDir" . }}"
cache_max_bytes = {{ if $s.cache.enabled }}{{ $s.cache.maxBytes | int64 }}{{ else }}0{{ end }}
cache_max_per_object_bytes = {{ $s.cache.maxPerObjectBytes | int64 }}
{{- if $s.endpoint }}
s3_endpoint = "{{ $s.endpoint }}"
{{- end }}
{{- if $s.region }}
s3_region = "{{ $s.region }}"
{{- end }}
s3_allow_http = {{ $s.allowHttp }}
{{- end }}

{{- define "teodb.commonCatalogToml" -}}
[catalog]
type = "{{ .Values.catalog.type }}"
uri = "{{ .Values.catalog.uri }}"
{{- if .Values.catalog.warehouse }}
warehouse = "{{ .Values.catalog.warehouse }}"
{{- end }}
{{- end }}

{{- define "teodb.commonTailToml" -}}
[security]
mode = "{{ .Values.security.mode }}"

[observability]
log_level = "{{ .Values.observability.logLevel }}"
log_format = "{{ .Values.observability.logFormat }}"

[shutdown]
drain_timeout_secs = {{ .Values.shutdown.drainTimeoutSecs }}
{{- end }}

{{/* Data-node role config. */}}
{{- define "teodb.dataNodeConfig" -}}
# Rendered by the teodb Helm chart — do not edit in-cluster.
role = "data-node"
data_dir = "{{ include "teodb.dataDir" . }}"

[server]
rest_bind = "0.0.0.0:{{ .Values.service.restPort }}"
flight_bind = "0.0.0.0:{{ .Values.service.flightPort }}"

{{ include "teodb.commonCatalogToml" . }}

[wal]
fsync_on_append = {{ .Values.dataNode.wal.fsyncOnAppend }}
max_prepared_files = {{ .Values.dataNode.wal.maxPreparedFiles }}
max_prepared_bytes = {{ .Values.dataNode.wal.maxPreparedBytes | int64 }}

[cluster]
scheduler_enabled = false
scheduler_addr = "{{ include "teodb.controlPlaneHost" . }}:{{ .Values.controlPlane.grpcPort }}"
max_writer_checkpoints_per_table = {{ .Values.cluster.maxWriterCheckpointsPerTable }}
executor_bind = "0.0.0.0:{{ .Values.internalPorts.executorBind }}"
executor_grpc_bind_port = {{ .Values.internalPorts.executorGrpc }}
executor_task_slots = {{ .Values.dataNode.taskSlots }}
heartbeat_interval_secs = {{ .Values.cluster.heartbeatIntervalSecs }}
heartbeat_miss_threshold = {{ .Values.cluster.heartbeatMissThreshold }}
min_executors = {{ .Values.cluster.minExecutors }}

{{ include "teodb.commonStorageToml" . }}

[query]
memory_pool_bytes = {{ .Values.dataNode.query.memoryPoolBytes | int64 }}
batch_size = {{ .Values.dataNode.query.batchSize | int64 }}

[ingest]
buffer_max_bytes = {{ .Values.dataNode.ingest.bufferMaxBytes | int64 }}
flush_interval_secs = {{ .Values.dataNode.ingest.flushIntervalSecs | int64 }}

[maintenance]
# Snapshot expiration remains opt-in until the metadata-expiration protocol is
# implemented. Zero is the fail-safe production default.
snapshot_retention_secs = {{ .Values.maintenance.snapshotRetentionSecs | int64 }}

{{ include "teodb.commonTailToml" . }}
{{- with .Values.dataNode.extraConfigToml }}

# --- extraConfigToml ---
{{ . }}
{{- end }}
{{- end }}

{{/* Control-plane role config. */}}
{{- define "teodb.controlPlaneConfig" -}}
# Rendered by the teodb Helm chart — do not edit in-cluster.
role = "control-plane"
data_dir = "{{ include "teodb.dataDir" . }}"

{{ include "teodb.commonCatalogToml" . }}

[cluster]
scheduler_bind = "0.0.0.0:{{ .Values.controlPlane.grpcPort }}"
scheduler_addr = "{{ include "teodb.controlPlaneHost" . }}:{{ .Values.controlPlane.grpcPort }}"
heartbeat_interval_secs = {{ .Values.cluster.heartbeatIntervalSecs }}
heartbeat_miss_threshold = {{ .Values.cluster.heartbeatMissThreshold }}

{{ include "teodb.commonStorageToml" . }}

{{ include "teodb.commonTailToml" . }}
{{- with .Values.controlPlane.extraConfigToml }}

# --- extraConfigToml ---
{{ . }}
{{- end }}
{{- end }}

{{/* Standalone role config. */}}
{{- define "teodb.standaloneConfig" -}}
# Rendered by the teodb Helm chart — do not edit in-cluster.
role = "standalone"
data_dir = "{{ include "teodb.dataDir" . }}"

[server]
rest_bind = "0.0.0.0:{{ .Values.service.restPort }}"
flight_bind = "0.0.0.0:{{ .Values.service.flightPort }}"

{{ include "teodb.commonCatalogToml" . }}

[wal]
fsync_on_append = {{ .Values.standalone.wal.fsyncOnAppend }}
max_prepared_files = {{ .Values.standalone.wal.maxPreparedFiles }}
max_prepared_bytes = {{ .Values.standalone.wal.maxPreparedBytes | int64 }}

{{ include "teodb.commonStorageToml" . }}

[query]
memory_pool_bytes = {{ .Values.standalone.query.memoryPoolBytes | int64 }}
batch_size = {{ .Values.standalone.query.batchSize | int64 }}

[ingest]
buffer_max_bytes = {{ .Values.standalone.ingest.bufferMaxBytes | int64 }}
flush_interval_secs = {{ .Values.standalone.ingest.flushIntervalSecs | int64 }}

{{ include "teodb.commonTailToml" . }}
{{- with .Values.standalone.extraConfigToml }}

# --- extraConfigToml ---
{{ . }}
{{- end }}
{{- end }}

{{/*
Shared env block: S3 creds + admin token from the Secret, plus region/endpoint.
Pass the root context.
*/}}
{{- define "teodb.commonEnv" -}}
- name: POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: POD_NAMESPACE
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
- name: AWS_REGION
  value: {{ .Values.storage.region | quote }}
{{- if .Values.storage.endpoint }}
- name: AWS_ENDPOINT_URL
  value: {{ .Values.storage.endpoint | quote }}
{{- end }}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ include "teodb.secretName" . }}
      key: {{ .Values.secret.keys.s3AccessKey }}
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "teodb.secretName" . }}
      key: {{ .Values.secret.keys.s3SecretKey }}
- name: TEODB__SECURITY__ADMIN_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ include "teodb.secretName" . }}
      key: {{ .Values.secret.keys.adminToken }}
      optional: true
{{- with .Values.extraEnv }}
{{ toYaml . }}
{{- end }}
{{- end }}
