import { defineComponent } from 'vue';
import { flushPromises, mount } from '@vue/test-utils';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const fetchClusterStatus = vi.hoisted(() => vi.fn());

vi.mock('@/api/admin', () => ({ fetchClusterStatus }));
vi.mock('@/composables/useErrorLog', () => ({ reportError: vi.fn() }));
vi.mock('primevue/usetoast', () => ({ useToast: () => ({ add: vi.fn() }) }));

import ClusterView from './ClusterView.vue';

const CardStub = defineComponent({
  template: '<section><slot name="content"/></section>',
});

describe('ClusterView scheduler status', () => {
  beforeEach(() => {
    fetchClusterStatus.mockReset();
    fetchClusterStatus.mockResolvedValue({
      mode: 'data-node',
      cluster_id: '019fbf7d-0000-7000-8000-000000000001',
      node_id: 'data-node-1',
      writer_id: 'writer-1',
      writer_epoch: 3,
      recovery_status: 'complete',
      uptime_seconds: 42,
      pending_tables: 0,
      blocked_tables: 0,
      wal_segments: 2,
      wal_bytes: 4096,
      workers: [],
      connections: [],
      scheduler: {
        address: 'teodb-control-plane:50050',
        reachable: true,
      },
      active_jobs: 7,
    });
  });

  it('renders the scheduler endpoint, reachability, and active job count', async () => {
    const wrapper = mount(ClusterView, {
      global: {
        stubs: {
          Card: CardStub,
          Button: true,
          DataTable: true,
          Column: true,
          Tag: true,
        },
      },
    });

    await flushPromises();

    expect(wrapper.text()).toContain('Scheduler');
    expect(wrapper.text()).toContain('Reachable');
    expect(wrapper.text()).toContain('teodb-control-plane:50050 · 7 active jobs');
    wrapper.unmount();
  });
});
