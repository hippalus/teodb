import { ref } from 'vue';
import type { QueryHistoryEntry } from '@/api/types';
import { reportError } from '@/composables/useErrorLog';

const STORAGE_KEY = 'teodb_query_history';
const MAX_ENTRIES = 100;

function loadHistory(): QueryHistoryEntry[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? JSON.parse(raw) as QueryHistoryEntry[] : [];
  } catch (error) {
    reportError('useQueryHistory.loadHistory', error);
    return [];
  }
}

function saveHistory(entries: QueryHistoryEntry[]) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(entries));
}

export function useQueryHistory() {
  const history = ref<QueryHistoryEntry[]>(loadHistory());

  function addEntry(entry: Omit<QueryHistoryEntry, 'id' | 'executed_at'>) {
    const newEntry: QueryHistoryEntry = {
      id: crypto.randomUUID(),
      executed_at: new Date().toISOString(),
      ...entry,
    };
    history.value = [newEntry, ...history.value].slice(0, MAX_ENTRIES);
    saveHistory(history.value);
  }

  function removeEntry(id: string) {
    history.value = history.value.filter((e) => e.id !== id);
    saveHistory(history.value);
  }

  function clearHistory() {
    history.value = [];
    saveHistory(history.value);
  }

  function reload() {
    history.value = loadHistory();
  }

  return { history, addEntry, removeEntry, clearHistory, reload };
}
