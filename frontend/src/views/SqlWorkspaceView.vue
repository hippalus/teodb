<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, ref, watch } from 'vue';
import { reportError } from '@/composables/useErrorLog';
import { useToast } from 'primevue/usetoast';
import { useRoute, useRouter } from 'vue-router';
import { executeQuery, explainQuery, fetchTables } from '@/api/admin';
import { useQueryHistory } from '@/composables/useQueryHistory';
import { apiErrorMessage, isAbortError } from '@/composables/useApiError';
import { useAppStore } from '@/stores/app';
import type { SqlQueryResponse, SqlExplainResponse } from '@/api/types';
import SqlEditor from '@/components/SqlEditor.vue';
import KeyboardShortcuts from '@/components/KeyboardShortcuts.vue';
import DataTable from 'primevue/datatable';
import Column from 'primevue/column';
import Button from 'primevue/button';
import Tabs from 'primevue/tabs';
import TabList from 'primevue/tablist';
import Tab from 'primevue/tab';
import TabPanels from 'primevue/tabpanels';
import TabPanel from 'primevue/tabpanel';
import Splitter from 'primevue/splitter';
import SplitterPanel from 'primevue/splitterpanel';
import Drawer from 'primevue/drawer';

const toast = useToast();
const route = useRoute();
const router = useRouter();
const appStore = useAppStore();
const { history, addEntry, removeEntry, clearHistory } = useQueryHistory();

const sqlText = ref('SELECT 1');
const sqlEditorRef = ref<InstanceType<typeof SqlEditor> | null>(null);
const executing = ref(false);
const explaining = ref(false);
const result = ref<SqlQueryResponse | null>(null);
const explainResult = ref<SqlExplainResponse | null>(null);
const activeTab = ref(0);
const historySidebarVisible = ref(false);
const shortcutsVisible = ref(false);
let executeController: AbortController | null = null;
let explainController: AbortController | null = null;
let tablesController: AbortController | null = null;

const tableNames = computed(() => appStore.tables.map((t) => t.name));

async function handleExecute() {
  if (!sqlText.value.trim()) return;
  executeController?.abort();
  executeController = new AbortController();
  const signal = executeController.signal;
  executing.value = true;
  explainResult.value = null;
  activeTab.value = 0;
  try {
    const queryResult = await executeQuery({ sql: sqlText.value, limit: 1000 }, { signal });
    if (signal.aborted) return;
    result.value = queryResult;
    addEntry({
      sql: sqlText.value,
      elapsed_ms: result.value.elapsed_ms,
      row_count: result.value.row_count,
    });
  } catch (err) {
    if (signal.aborted || isAbortError(err)) return;
    reportError('SqlWorkspaceView.executeQuery', err);
    const message = apiErrorMessage(err, 'Query failed');
    toast.add({ severity: 'error', summary: 'Query Error', detail: message, life: 5000 });
    addEntry({ sql: sqlText.value, error: message });
    result.value = null;
  } finally {
    if (!signal.aborted) {
      executing.value = false;
    }
  }
}

async function handleExplain() {
  if (!sqlText.value.trim()) return;
  explainController?.abort();
  explainController = new AbortController();
  const signal = explainController.signal;
  explaining.value = true;
  result.value = null;
  activeTab.value = 1;
  try {
    const nextExplain = await explainQuery({ sql: sqlText.value }, { signal });
    if (signal.aborted) return;
    explainResult.value = nextExplain;
  } catch (err) {
    if (signal.aborted || isAbortError(err)) return;
    reportError('SqlWorkspaceView.explainQuery', err);
    const message = apiErrorMessage(err, 'Explain failed');
    toast.add({ severity: 'error', summary: 'Error', detail: message, life: 5000 });
    explainResult.value = null;
  } finally {
    if (!signal.aborted) {
      explaining.value = false;
    }
  }
}

async function loadFromHistory(sql: string) {
  sqlText.value = sql;
  result.value = null;
  explainResult.value = null;
  activeTab.value = 0;
  historySidebarVisible.value = false;
  await nextTick();
  sqlEditorRef.value?.focus();
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleString();
}

function routeHistoryId(): string | null {
  const historyQuery = route.query.history;
  if (Array.isArray(historyQuery)) {
    return historyQuery[0] ?? null;
  }
  return historyQuery ?? null;
}

function clearRouteHistoryId() {
  const query = { ...route.query };
  delete query.history;
  void router.replace({ name: 'sql-workspace', query });
}

async function loadRouteHistoryQuery() {
  const queryId = routeHistoryId();
  if (!queryId) return;

  const entry = history.value.find((item) => item.id === queryId);
  if (entry) {
    await loadFromHistory(entry.sql);
  }
  clearRouteHistoryId();
}

onMounted(async () => {
  await loadRouteHistoryQuery();

  tablesController = new AbortController();
  try {
    const tables = await fetchTables({ signal: tablesController.signal });
    if (tablesController.signal.aborted) return;
    appStore.setTables(tables);
  } catch (error) {
    if (tablesController.signal.aborted || isAbortError(error)) return;
    reportError('SqlWorkspaceView.loadTables', error);
    // Tables may not be available
  }
});

onUnmounted(() => {
  executeController?.abort();
  explainController?.abort();
  tablesController?.abort();
});

watch(
  () => route.query.history,
  () => {
    void loadRouteHistoryQuery();
  }
);
</script>

<template>
  <div class="flex flex-col" style="height: calc(100vh - 140px)">
    <div class="flex items-center justify-between mb-3">
      <h1 class="text-2xl font-bold text-surface-800">SQL Workspace</h1>
      <div class="flex gap-2">
        <Button
          icon="pi pi-question-circle"
          text
          rounded
          severity="secondary"
          v-tooltip="'Keyboard Shortcuts'"
          @click="shortcutsVisible = true"
        />
        <Button
          icon="pi pi-history"
          text
          rounded
          severity="secondary"
          v-tooltip="'Query History'"
          @click="historySidebarVisible = true"
        />
      </div>
    </div>

    <Splitter class="flex-1" layout="vertical" :gutterSize="6">
      <!-- Editor Panel -->
      <SplitterPanel :size="40" :minSize="20">
        <div class="flex flex-col h-full">
          <SqlEditor
            ref="sqlEditorRef"
            v-model="sqlText"
            :tableNames="tableNames"
            height="100%"
            placeholder="Enter SQL query... (Ctrl+Enter to execute)"
            @execute="handleExecute"
          />
          <div class="flex items-center gap-2 py-2">
            <Button
              label="Execute"
              icon="pi pi-play"
              size="small"
              :loading="executing"
              @click="handleExecute"
            />
            <Button
              label="Explain"
              icon="pi pi-sitemap"
              size="small"
              severity="secondary"
              :loading="explaining"
              @click="handleExplain"
            />
            <span class="text-xs text-surface-400 ml-auto">Ctrl+Enter to execute</span>
          </div>
        </div>
      </SplitterPanel>

      <!-- Results Panel -->
      <SplitterPanel :size="60" :minSize="20">
        <Tabs v-model:value="activeTab" class="h-full flex flex-col">
          <TabList>
            <Tab :value="0">Results</Tab>
            <Tab :value="1">Explain</Tab>
          </TabList>
          <TabPanels class="flex-1 overflow-auto">
            <TabPanel :value="0">
            <div v-if="executing" class="flex justify-center py-8">
              <i class="pi pi-spin pi-spinner text-2xl text-surface-400"></i>
            </div>
            <div v-else-if="result">
              <div class="flex items-center gap-3 mb-3">
                <span class="text-sm text-surface-500">
                  {{ result.row_count }} rows · {{ result.elapsed_ms }}ms
                </span>
              </div>
              <DataTable
                :value="result.rows"
                stripedRows
                scrollable
                scrollHeight="flex"
                paginator
                :rows="50"
                :rowsPerPageOptions="[25, 50, 100]"
                size="small"
              >
                <Column
                  v-for="col in result.columns"
                  :key="col.name"
                  :field="col.name"
                  :header="col.name"
                  sortable
                >
                  <template #body="{ data }">
                    <span class="text-sm font-mono">{{ data[col.name] ?? 'NULL' }}</span>
                  </template>
                </Column>
              </DataTable>
            </div>
            <div v-else class="flex flex-col items-center justify-center py-12 text-surface-400">
              <i class="pi pi-play text-4xl mb-3"></i>
              <p>Execute a query to see results</p>
            </div>
            </TabPanel>

            <TabPanel :value="1">
            <div v-if="explaining" class="flex justify-center py-8">
              <i class="pi pi-spin pi-spinner text-2xl text-surface-400"></i>
            </div>
            <div v-else-if="explainResult">
              <div class="text-sm text-surface-500 mb-3">
                Plan generated in {{ explainResult.elapsed_ms }}ms
              </div>
              <pre class="bg-surface-900 text-green-400 p-4 rounded-lg overflow-auto text-sm font-mono leading-relaxed">{{ explainResult.plan }}</pre>
            </div>
            <div v-else class="flex flex-col items-center justify-center py-12 text-surface-400">
              <i class="pi pi-sitemap text-4xl mb-3"></i>
              <p>Click "Explain" to view the query plan</p>
            </div>
            </TabPanel>
          </TabPanels>
        </Tabs>
      </SplitterPanel>
    </Splitter>

    <!-- History Drawer -->
    <Drawer v-model:visible="historySidebarVisible" position="right" :style="{ width: '400px' }">
      <template #header>
        <div class="flex items-center justify-between w-full">
          <span class="font-bold text-lg">Query History</span>
          <Button
            label="Clear"
            icon="pi pi-trash"
            text
            size="small"
            severity="danger"
            @click="clearHistory"
            :disabled="history.length === 0"
          />
        </div>
      </template>

      <div v-if="history.length === 0" class="text-center py-8 text-surface-400">
        <i class="pi pi-history text-3xl mb-2"></i>
        <p>No query history</p>
      </div>
      <div v-else class="flex flex-col gap-2">
        <div
          v-for="entry in history"
          :key="entry.id"
          class="p-3 bg-surface-50 rounded-lg hover:bg-surface-100 transition-colors cursor-pointer group"
          @click="loadFromHistory(entry.sql)"
        >
          <code class="text-xs text-surface-700 block truncate">{{ entry.sql }}</code>
          <div class="flex items-center justify-between mt-2">
            <div class="flex items-center gap-2">
              <span class="text-xs text-surface-400">{{ formatDate(entry.executed_at) }}</span>
              <span v-if="entry.elapsed_ms" class="text-xs text-surface-400">{{ entry.elapsed_ms }}ms</span>
              <span v-if="entry.error" class="text-xs text-red-500">
                <i class="pi pi-exclamation-circle"></i>
              </span>
            </div>
            <Button
              icon="pi pi-times"
              text
              rounded
              size="small"
              severity="danger"
              class="opacity-0 group-hover:opacity-100"
              @click.stop="removeEntry(entry.id)"
            />
          </div>
        </div>
      </div>
    </Drawer>

    <KeyboardShortcuts v-model:visible="shortcutsVisible" />
  </div>
</template>
