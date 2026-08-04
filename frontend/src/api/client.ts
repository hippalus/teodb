import axios, { type AxiosInstance, type InternalAxiosRequestConfig } from 'axios';
import type { ProblemDetail } from './types';

/**
 * TeoDB server origin, resolved in precedence order:
 * 1. `window.__TEODB_SERVER_URL__` — runtime injection (e.g. a config script
 *    served next to the SPA), so one build can target any server.
 * 2. `VITE_TEODB_SERVER_URL` — build-time environment.
 * 3. `''` — same origin (UI embedded in the TeoDB binary).
 */
export function serverOrigin(): string {
  return (
    window.__TEODB_SERVER_URL__ ??
    import.meta.env.VITE_TEODB_SERVER_URL ??
    ''
  ).replace(/\/+$/, '');
}

/** Attach the optional bearer token to every request. Exported for testing. */
export function bearerInterceptor(config: InternalAxiosRequestConfig): InternalAxiosRequestConfig {
  // sessionStorage (per-tab, cleared on close) — see useAuth.ts (FE-2).
  const token = sessionStorage.getItem('teodb_token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
}

/** Reject with an enriched error when the body is an RFC 9457 problem detail.
 *  Exported for testing. */
export function problemDetailInterceptor(error: unknown): Promise<never> {
  if (axios.isAxiosError(error) && error.response) {
    const data = error.response.data;
    if (data && typeof data === 'object' && 'type' in data && 'title' in data) {
      const problem = data as ProblemDetail;
      const enriched = new Error(problem.detail || problem.title);
      enriched.name = 'ApiError';
      (enriched as Error & { problem: ProblemDetail }).problem = problem;
      return Promise.reject(enriched);
    }
  }
  return Promise.reject(error);
}

function withInterceptors(instance: AxiosInstance): AxiosInstance {
  instance.interceptors.request.use(bearerInterceptor);
  instance.interceptors.response.use((response) => response, problemDetailInterceptor);
  return instance;
}

/** Client for `/api/v1` endpoints. */
const client = withInterceptors(
  axios.create({
    baseURL: `${serverOrigin()}/api/v1`,
    timeout: 30_000,
    headers: {
      'Content-Type': 'application/json',
    },
  })
);

export default client;

/** Client for root-level paths (`/live`, `/ready`, `/metrics`). These can
 * also require auth (`/metrics` is admin-guarded), so it shares interceptors. */
export const rawClient = withInterceptors(
  axios.create({
    baseURL: serverOrigin(),
    timeout: 15_000,
  })
);
