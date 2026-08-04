import type { AxiosRequestConfig } from 'axios';
import client, { rawClient } from './client';
import type {
  ReadinessResponse,
  StatusResponse,
  TableSummary,
  TableDetail,
  SqlQueryRequest,
  SqlQueryResponse,
  SqlExplainResponse,
  ClusterStatusResponse,
  IngestRequest,
  IngestResponse,
  CreateTableRequest,
} from './types';

type RequestOptions = Pick<AxiosRequestConfig, 'signal'>;

// ── Health & Status ────────────────────────────────────────────

export async function fetchLiveness(options?: RequestOptions) {
  const { data } = await rawClient.get('/live', options);
  return data;
}

export async function fetchReadiness(options?: RequestOptions): Promise<ReadinessResponse> {
  const { data } = await rawClient.get<ReadinessResponse>('/ready', options);
  return data;
}

export async function fetchStatus(options?: RequestOptions): Promise<StatusResponse> {
  const { data } = await client.get<StatusResponse>('/admin/status', options);
  return data;
}

export async function fetchClusterStatus(options?: RequestOptions): Promise<ClusterStatusResponse> {
  const { data } = await client.get<ClusterStatusResponse>('/admin/cluster', options);
  return data;
}

// ── Tables ─────────────────────────────────────────────────────

export async function fetchTables(options?: RequestOptions): Promise<TableSummary[]> {
  const { data } = await client.get<TableSummary[]>('/admin/tables', options);
  return data;
}

export async function fetchTable(namespace: string, name: string, options?: RequestOptions): Promise<TableDetail> {
  const { data } = await client.get<TableDetail>(
    `/namespaces/${encodeURIComponent(namespace)}/tables/${encodeURIComponent(name)}`,
    options,
  );
  return data;
}

export async function createTable(
  namespace: string,
  request: CreateTableRequest,
  options?: RequestOptions,
): Promise<void> {
  await client.post(`/namespaces/${encodeURIComponent(namespace)}/tables`, request, options);
}

export async function dropTable(namespace: string, name: string, options?: RequestOptions): Promise<void> {
  await client.delete(
    `/namespaces/${encodeURIComponent(namespace)}/tables/${encodeURIComponent(name)}`,
    options,
  );
}

export async function ingestData(
  namespace: string,
  tableName: string,
  request: IngestRequest,
  options?: RequestOptions,
): Promise<IngestResponse> {
  const { data } = await client.post<IngestResponse>(
    `/tables/${encodeURIComponent(namespace)}/${encodeURIComponent(tableName)}/ingest`,
    request,
    options,
  );
  return data;
}

export async function flushTable(namespace: string, tableName: string, options?: RequestOptions): Promise<void> {
  await client.post(
    `/tables/${encodeURIComponent(namespace)}/${encodeURIComponent(tableName)}/flush`,
    undefined,
    options,
  );
}

export async function readTable(
  namespace: string,
  tableName: string,
  params?: { limit?: number },
  options?: RequestOptions,
): Promise<SqlQueryResponse> {
  const sql = `SELECT * FROM "${namespace}"."${tableName}" LIMIT ${params?.limit ?? 100}`;
  return executeQuery({ sql }, options);
}

// ── SQL ────────────────────────────────────────────────────────

export async function executeQuery(
  request: SqlQueryRequest,
  options?: RequestOptions,
): Promise<SqlQueryResponse> {
  const { data } = await client.post<SqlQueryResponse>('/query', request, options);
  return data;
}

export async function explainQuery(
  request: SqlQueryRequest,
  options?: RequestOptions,
): Promise<SqlExplainResponse> {
  const { data } = await client.post<SqlExplainResponse>('/query/explain', request, options);
  return data;
}

// ── Metrics ────────────────────────────────────────────────────

export async function fetchMetrics(options?: RequestOptions): Promise<string> {
  const { data } = await rawClient.get<string>('/metrics', options);
  return data;
}
