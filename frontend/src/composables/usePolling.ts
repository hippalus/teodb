import { ref, onUnmounted } from 'vue';
import { isAbortError } from './useApiError';
import { reportError } from './useErrorLog';

export function usePolling(callback: (signal: AbortSignal) => Promise<void>, intervalMs: number = 10_000) {
  const isPolling = ref(false);
  let timer: ReturnType<typeof setInterval> | null = null;
  let controller: AbortController | null = null;
  let inFlight = false;

  async function runOnce() {
    if (inFlight) return;
    controller = new AbortController();
    inFlight = true;
    try {
      await callback(controller.signal);
    } catch (error) {
      if (!isAbortError(error)) {
        reportError('usePolling.callback', error);
      }
    } finally {
      controller = null;
      inFlight = false;
    }
  }

  async function poll() {
    if (!isPolling.value) return;
    await runOnce();
  }

  function start() {
    if (isPolling.value) return;
    isPolling.value = true;
    void poll();
    timer = setInterval(() => {
      void poll();
    }, intervalMs);
  }

  function stop() {
    isPolling.value = false;
    controller?.abort();
    controller = null;
    if (timer !== null) {
      clearInterval(timer);
      timer = null;
    }
  }

  function setInterval_(ms: number) {
    if (isPolling.value) {
      stop();
      intervalMs = ms;
      start();
    } else {
      intervalMs = ms;
    }
  }

  async function refresh() {
    await runOnce();
  }

  onUnmounted(() => stop());

  return { isPolling, start, stop, refresh, setInterval: setInterval_ };
}
