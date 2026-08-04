import { beforeEach, describe, expect, it } from 'vitest';
import { useQueryHistory } from './useQueryHistory';

const STORAGE_KEY = 'teodb_query_history';

describe('useQueryHistory', () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it('adds an entry, stamping id + executed_at, and persists it', () => {
    const { history, addEntry } = useQueryHistory();
    addEntry({ sql: 'SELECT 1', elapsed_ms: 5, row_count: 1 });

    expect(history.value).toHaveLength(1);
    const entry = history.value[0];
    expect(entry.sql).toBe('SELECT 1');
    expect(entry.id).toBeTruthy();
    expect(entry.executed_at).toBeTruthy();

    const persisted = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? '[]');
    expect(persisted).toHaveLength(1);
    expect(persisted[0].sql).toBe('SELECT 1');
  });

  it('prepends newest first', () => {
    const { history, addEntry } = useQueryHistory();
    addEntry({ sql: 'first' });
    addEntry({ sql: 'second' });
    expect(history.value.map((e) => e.sql)).toEqual(['second', 'first']);
  });

  it('caps history at 100 entries', () => {
    const { history, addEntry } = useQueryHistory();
    for (let i = 0; i < 130; i++) addEntry({ sql: `q${i}` });
    expect(history.value).toHaveLength(100);
    // Newest kept, oldest dropped.
    expect(history.value[0].sql).toBe('q129');
    expect(history.value.some((e) => e.sql === 'q0')).toBe(false);
  });

  it('removes a single entry by id', () => {
    const { history, addEntry, removeEntry } = useQueryHistory();
    addEntry({ sql: 'keep' });
    addEntry({ sql: 'drop' });
    const dropId = history.value.find((e) => e.sql === 'drop')!.id;
    removeEntry(dropId);
    expect(history.value.map((e) => e.sql)).toEqual(['keep']);
  });

  it('clears all history', () => {
    const { history, addEntry, clearHistory } = useQueryHistory();
    addEntry({ sql: 'a' });
    clearHistory();
    expect(history.value).toHaveLength(0);
    expect(JSON.parse(localStorage.getItem(STORAGE_KEY) ?? '[]')).toHaveLength(0);
  });

  it('survives corrupt localStorage without throwing', () => {
    localStorage.setItem(STORAGE_KEY, '{not json');
    const { history } = useQueryHistory();
    expect(history.value).toEqual([]);
  });
});
