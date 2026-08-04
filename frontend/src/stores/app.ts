import { defineStore } from 'pinia';
import { ref } from 'vue';
import type { StatusResponse, TableSummary } from '@/api/types';

export const useAppStore = defineStore('app', () => {
  const serverStatus = ref<StatusResponse | null>(null);
  const tables = ref<TableSummary[]>([]);
  const isConnected = ref(false);
  const darkMode = ref(false);

  /// Apply the persisted (or system-preferred) color scheme on app start.
  function initDarkMode() {
    const stored = localStorage.getItem('teo-dark-mode');
    const prefersDark =
      stored === null && window.matchMedia?.('(prefers-color-scheme: dark)').matches;
    darkMode.value = stored === 'true' || !!prefersDark;
    document.documentElement.classList.toggle('app-dark', darkMode.value);
  }

  function setStatus(status: StatusResponse) {
    serverStatus.value = status;
    isConnected.value = true;
  }

  function setTables(t: TableSummary[]) {
    tables.value = t;
  }

  function setDisconnected() {
    isConnected.value = false;
  }

  function toggleDarkMode() {
    darkMode.value = !darkMode.value;
    document.documentElement.classList.toggle('app-dark', darkMode.value);
    localStorage.setItem('teo-dark-mode', String(darkMode.value));
  }

  return {
    serverStatus,
    tables,
    isConnected,
    darkMode,
    setStatus,
    setTables,
    setDisconnected,
    toggleDarkMode,
    initDarkMode,
  };
});
