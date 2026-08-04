import type { ProblemDetail } from '@/api/types';

/** An Error enriched by the api client's problem-detail interceptor. */
type ApiError = Error & { problem?: ProblemDetail };

/**
 * Render any thrown value into a user-facing message. When the api client has
 * attached an RFC 9457 problem detail, prefer its `detail`/`title` and prefix
 * the HTTP status so the toast carries the same context the server sent.
 */
export function apiErrorMessage(err: unknown, fallback = 'Request failed'): string {
  if (err instanceof Error) {
    const problem = (err as ApiError).problem;
    if (problem) {
      const text = problem.detail || problem.title || err.message;
      return problem.status ? `${problem.status} · ${text}` : text;
    }
    return err.message || fallback;
  }
  return fallback;
}

export function isAbortError(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  const code = (err as Error & { code?: string }).code;
  return err.name === 'AbortError' || err.name === 'CanceledError' || code === 'ERR_CANCELED';
}
