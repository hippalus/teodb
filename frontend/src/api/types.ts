// API response types matching TeoDB backend DTOs

export interface ReadinessResponse {
  status: string;
  checks: ReadinessCheck[];
}

export interface ReadinessCheck {
  name: string;
  status: 'pass' | 'fail';
  detail?: string;
}

export interface StatusResponse {
  server_version: string;
  uptime_seconds: number;
  tables_count: number;
  total_rows: number;
  memory_usage_bytes: number;
  components: ComponentHealth[];
}

export interface ComponentHealth {
  name: string;
  status: string;
  message?: string;
}

export interface TableSummary {
  name: string;
  namespace: string;
  column_count: number;
  row_count: number;
  size_bytes: number;
  partitioned: boolean;
}

export interface TableDetail {
  name: string;
  namespace: string;
  current_schema_id: number;
  current_snapshot_id?: number;
  columns: ColumnSchema[];
  properties: Record<string, string>;
  _links?: Record<string, HateoasLink>;
}

export interface HateoasLink {
  href: string;
  method?: string;
  title?: string;
}

export interface ColumnSchema {
  field_id: number;
  name: string;
  data_type: string;
  nullable: boolean;
  comment?: string;
}

export interface SqlQueryRequest {
  sql: string;
  limit?: number;
}

export interface SqlQueryResponse {
  columns: ColumnInfo[];
  rows: Record<string, unknown>[];
  row_count: number;
  elapsed_ms: number;
}

export interface ColumnInfo {
  name: string;
  data_type: string;
}

export interface SqlExplainResponse {
  plan: string;
  elapsed_ms: number;
}

export interface ClusterStatusResponse {
  mode: string;
  cluster_id: string;
  node_id: string;
  writer_id: string;
  writer_epoch: number;
  recovery_status: string;
  uptime_seconds: number;
  pending_tables: number;
  blocked_tables: number;
  wal_segments: number | null;
  wal_bytes: number | null;
  wal_error?: string;
  workers: ClusterWorker[];
  connections: ClusterConnection[];
  scheduler?: SchedulerInfo;
  active_jobs?: number;
}

export interface SchedulerInfo {
  address: string;
  reachable: boolean;
}

export interface ClusterWorker {
  id: string;
  host: string;
  flight_port: number;
  status: 'active' | 'draining' | 'offline' | string;
  last_heartbeat?: string;
}

export interface ClusterConnection {
  id: string;
  client_address: string;
  protocol: string;
  connected_at: string;
  last_activity: string;
}

export interface IngestRequest {
  rows: Record<string, unknown>[];
  idempotency_key?: string;
}

export interface IngestResponse {
  accepted_rows: number;
  batch_id: string;
  generation: number;
}

export interface CreateTableRequest {
  name: string;
  columns: CreateColumnDef[];
  properties?: Record<string, string>;
}

export interface CreateColumnDef {
  name: string;
  data_type: string;
  nullable?: boolean;
}

export interface ProblemDetail {
  type: string;
  title: string;
  status: number;
  detail: string;
  instance?: string;
}

export interface QueryHistoryEntry {
  id: string;
  sql: string;
  executed_at: string;
  elapsed_ms?: number;
  row_count?: number;
  error?: string;
}
