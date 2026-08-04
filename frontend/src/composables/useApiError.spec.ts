import { describe, expect, it } from 'vitest';
import { apiErrorMessage } from './useApiError';
import type { ProblemDetail } from '@/api/types';

function withProblem(problem: Partial<ProblemDetail>): Error {
  const err = new Error('raw') as Error & { problem?: ProblemDetail };
  err.problem = {
    type: 'about:blank',
    title: 'Bad Request',
    status: 400,
    detail: '',
    ...problem,
  };
  return err;
}

describe('apiErrorMessage', () => {
  it('prefers problem.detail and prefixes the status', () => {
    const msg = apiErrorMessage(withProblem({ status: 422, detail: 'bad sql' }));
    expect(msg).toBe('422 · bad sql');
  });

  it('falls back to problem.title when detail is empty', () => {
    const msg = apiErrorMessage(withProblem({ status: 404, title: 'Not Found', detail: '' }));
    expect(msg).toBe('404 · Not Found');
  });

  it('uses a plain Error message when no problem is attached', () => {
    expect(apiErrorMessage(new Error('network down'))).toBe('network down');
  });

  it('uses the fallback for non-Error throwables', () => {
    expect(apiErrorMessage('weird', 'Query failed')).toBe('Query failed');
    expect(apiErrorMessage(undefined)).toBe('Request failed');
  });
});
