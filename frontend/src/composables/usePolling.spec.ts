import { defineComponent, h } from 'vue';
import { mount } from '@vue/test-utils';
import { describe, expect, it, vi } from 'vitest';
import { usePolling } from './usePolling';

describe('usePolling', () => {
  it('runs a manual refresh with a real AbortSignal while polling is paused', async () => {
    const callback = vi.fn(async (signal: AbortSignal) => {
      expect(signal).toBeInstanceOf(AbortSignal);
      expect(signal.aborted).toBe(false);
    });
    let controls!: ReturnType<typeof usePolling>;
    const Harness = defineComponent({
      setup() {
        controls = usePolling(callback);
        return () => h('div');
      },
    });
    const wrapper = mount(Harness);

    await controls.refresh();

    expect(callback).toHaveBeenCalledOnce();
    expect(controls.isPolling.value).toBe(false);
    wrapper.unmount();
  });

  it('aborts an in-flight callback when its owner unmounts', async () => {
    let observedSignal!: AbortSignal;
    const callback = vi.fn(
      (signal: AbortSignal) => new Promise<void>((resolve) => {
        observedSignal = signal;
        signal.addEventListener('abort', () => resolve(), { once: true });
      }),
    );
    let controls!: ReturnType<typeof usePolling>;
    const Harness = defineComponent({
      setup() {
        controls = usePolling(callback);
        return () => h('div');
      },
    });
    const wrapper = mount(Harness);

    controls.start();
    await vi.waitFor(() => expect(callback).toHaveBeenCalledOnce());
    wrapper.unmount();

    expect(observedSignal.aborted).toBe(true);
  });
});
