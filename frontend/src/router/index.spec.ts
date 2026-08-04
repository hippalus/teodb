import { describe, expect, it } from 'vitest';
import router from './index';

describe('admin UI routes', () => {
  it('keeps the Metrics UI separate from the Prometheus scrape endpoint', () => {
    const routes = router.getRoutes();
    const metricsRoute = routes.find((route) => route.name === 'metrics');

    expect(metricsRoute?.path).toBe('/ui/metrics');
    expect(routes.some((route) => route.path === '/metrics')).toBe(false);
  });
});
