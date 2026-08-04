import { flushPromises, mount } from '@vue/test-utils';
import { createMemoryHistory, createRouter } from 'vue-router';
import { describe, expect, it } from 'vitest';
import AppMenu from './AppMenu.vue';

describe('AppMenu navigation', () => {
  it('can leave the Metrics UI for another admin page', async () => {
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [
        {
          path: '/:pathMatch(.*)*',
          component: { template: '<div />' },
        },
      ],
    });
    await router.push('/ui/metrics');
    await router.isReady();

    const wrapper = mount(AppMenu, {
      global: {
        plugins: [router],
      },
    });
    const clusterItem = wrapper.findAll('a').find((item) => item.text() === 'Cluster');

    expect(clusterItem).toBeDefined();
    await clusterItem!.trigger('click');
    await flushPromises();

    expect(router.currentRoute.value.path).toBe('/cluster');
  });
});
