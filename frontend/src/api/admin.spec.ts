import { beforeEach, describe, expect, it, vi } from 'vitest';

const rawGet = vi.hoisted(() => vi.fn());

vi.mock('./client', () => ({
  default: {},
  rawClient: { get: rawGet },
}));

import { fetchMetrics } from './admin';

describe('fetchMetrics', () => {
  beforeEach(() => rawGet.mockReset());

  it('uses the single canonical Prometheus endpoint', async () => {
    rawGet.mockResolvedValue({ data: '# HELP teodb_uptime_seconds Server uptime' });

    await expect(fetchMetrics()).resolves.toContain('teodb_uptime_seconds');
    expect(rawGet).toHaveBeenCalledWith('/metrics', undefined);
  });
});
