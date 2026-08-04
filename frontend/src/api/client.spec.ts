import { AxiosError, AxiosHeaders, type InternalAxiosRequestConfig } from 'axios';
import { beforeEach, describe, expect, it } from 'vitest';
import { bearerInterceptor, problemDetailInterceptor } from './client';
import type { ProblemDetail } from './types';

function makeConfig(): InternalAxiosRequestConfig {
  return { headers: new AxiosHeaders() } as InternalAxiosRequestConfig;
}

describe('bearerInterceptor', () => {
  beforeEach(() => sessionStorage.clear());

  it('attaches Authorization when a token is stored', () => {
    sessionStorage.setItem('teodb_token', 'abc');
    const config = bearerInterceptor(makeConfig());
    expect(config.headers.Authorization).toBe('Bearer abc');
  });

  it('leaves Authorization unset when no token is stored', () => {
    const config = bearerInterceptor(makeConfig());
    expect(config.headers.Authorization).toBeUndefined();
  });
});

describe('problemDetailInterceptor', () => {
  it('enriches the error with the parsed problem detail', async () => {
    const problem: ProblemDetail = {
      type: 'about:blank',
      title: 'Unprocessable Entity',
      status: 422,
      detail: 'invalid SQL',
      instance: '/api/v1/query',
    };
    const axiosErr = new AxiosError('Request failed', 'ERR_BAD_REQUEST');
    axiosErr.response = { data: problem, status: 422 } as AxiosError['response'];

    await expect(problemDetailInterceptor(axiosErr)).rejects.toMatchObject({
      name: 'ApiError',
      message: 'invalid SQL',
      problem: { status: 422, detail: 'invalid SQL' },
    });
  });

  it('passes through errors whose body is not a problem detail', async () => {
    const axiosErr = new AxiosError('boom');
    axiosErr.response = { data: { foo: 'bar' }, status: 500 } as AxiosError['response'];
    await expect(problemDetailInterceptor(axiosErr)).rejects.toBe(axiosErr);
  });

  it('passes through non-axios errors untouched', async () => {
    const plain = new Error('offline');
    await expect(problemDetailInterceptor(plain)).rejects.toBe(plain);
  });
});
