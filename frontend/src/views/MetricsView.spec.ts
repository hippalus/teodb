import { defineComponent, nextTick } from 'vue';
import { flushPromises, mount } from '@vue/test-utils';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const chartSpies = vi.hoisted(() => ({
  create: vi.fn(),
  destroy: vi.fn(),
  update: vi.fn(),
}));
const fetchMetrics = vi.hoisted(() => vi.fn());
const reportError = vi.hoisted(() => vi.fn());

vi.mock('@/api/admin', () => ({ fetchMetrics }));
vi.mock('@/composables/useErrorLog', () => ({ reportError }));
vi.mock('primevue/usetoast', () => ({ useToast: () => ({ add: vi.fn() }) }));
vi.mock('chart.js', () => {
  class ChartMock {
    static register = vi.fn();
    data: { datasets: Array<{ data: unknown[] }> };

    constructor(_target: HTMLCanvasElement, config: { data: { datasets: Array<{ data: unknown[] }> } }) {
      this.data = config.data;
      this.observeDataArrays();
      chartSpies.create();
    }

    update(mode?: string) {
      this.observeDataArrays();
      chartSpies.update(mode);
    }

    destroy() {
      chartSpies.destroy();
    }

    private observeDataArrays() {
      for (const dataset of this.data.datasets) {
        const values = dataset.data as unknown[] & { __chartObserved__?: boolean };
        if (values.__chartObserved__) continue;

        const basePush = values.push;
        Object.defineProperty(values, 'push', {
          configurable: true,
          value: function (this: unknown[], ...items: unknown[]) {
            return basePush.apply(this, items);
          },
        });
        Object.defineProperty(values, '__chartObserved__', { value: true });
      }
    }
  }

  return {
    Chart: ChartMock,
    LineElement: {},
    PointElement: {},
    LinearScale: {},
    CategoryScale: {},
    LineController: {},
    Tooltip: {},
    Filler: {},
  };
});

import MetricsView from './MetricsView.vue';

const CardStub = defineComponent({
  template: '<section><slot name="title"/><slot name="content"/></section>',
});
const ButtonStub = defineComponent({
  props: {
    label: { type: String, default: '' },
  },
  emits: ['click'],
  template: '<button type="button" @click="$emit(\'click\')">{{ label }}</button>',
});

describe('MetricsView chart lifecycle', () => {
  beforeEach(() => {
    chartSpies.create.mockClear();
    chartSpies.destroy.mockClear();
    chartSpies.update.mockClear();
    fetchMetrics.mockReset();
    reportError.mockReset();
    fetchMetrics.mockResolvedValue([
      '# HELP teodb_transport_result_bytes_total Application result bytes',
      '# TYPE teodb_transport_result_bytes_total counter',
      'teodb_transport_result_bytes_total{operation="query",transport="rest"} 3',
    ].join('\n'));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('updates one chart instance and destroys it when route ownership ends', async () => {
    const wrapper = mount(MetricsView, {
      global: {
        stubs: { Card: CardStub, Button: ButtonStub },
      },
    });

    await flushPromises();
    await nextTick();
    expect(chartSpies.create).toHaveBeenCalledOnce();
    expect(wrapper.text()).toContain('teodb_transport_result_bytes_total{operation="query",transport="rest"}');
    expect(wrapper.find('pre').exists()).toBe(false);

    const metricCard = wrapper.get('.card');
    await metricCard.trigger('click');
    await nextTick();

    expect(chartSpies.create).toHaveBeenCalledOnce();
    expect(chartSpies.update).toHaveBeenCalledWith('none');

    wrapper.unmount();
    expect(chartSpies.destroy).toHaveBeenCalledOnce();
  });

  it('bounds high-cardinality series rendering', async () => {
    fetchMetrics.mockResolvedValue([
      '# HELP teodb_table_bytes Buffered bytes by table',
      '# TYPE teodb_table_bytes gauge',
      ...Array.from({ length: 201 }, (_, index) => `teodb_table_bytes{table="table-${index}"} ${index}`),
    ].join('\n'));

    const wrapper = mount(MetricsView, {
      global: {
        stubs: { Card: CardStub, Button: ButtonStub },
      },
    });

    await flushPromises();
    await nextTick();

    expect(wrapper.findAll('.card')).toHaveLength(200);
    expect(wrapper.text()).toContain('1 additional series');
    expect(wrapper.find('pre').exists()).toBe(false);
    wrapper.unmount();
  });

  it('keeps polling state isolated from Chart.js and reveals the raw scrape', async () => {
    vi.useFakeTimers();
    const wrapper = mount(MetricsView, {
      global: {
        stubs: { Card: CardStub, Button: ButtonStub },
      },
    });

    await flushPromises();
    await nextTick();
    expect(fetchMetrics).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(5_000);
    await flushPromises();
    await nextTick();

    expect(fetchMetrics).toHaveBeenCalledTimes(2);
    expect(reportError).not.toHaveBeenCalled();

    const rawButton = wrapper.findAll('button').find((button) => button.text() === 'Show raw scrape');
    expect(rawButton).toBeDefined();
    await rawButton!.trigger('click');
    await nextTick();

    expect(wrapper.get('pre').text()).toContain('teodb_transport_result_bytes_total');
    wrapper.unmount();
  });
});
