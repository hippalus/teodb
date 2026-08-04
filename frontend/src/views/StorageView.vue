<script setup lang="ts">
import { ref, onMounted } from 'vue';
import { reportError } from '@/composables/useErrorLog';
import { useToast } from 'primevue/usetoast';
import { fetchMetrics } from '@/api/admin';
import Card from 'primevue/card';
import DataTable from 'primevue/datatable';
import Column from 'primevue/column';
import Button from 'primevue/button';

const toast = useToast();
const loading = ref(true);

interface WalMetrics {
  wal_entries: number;
  wal_size_bytes: number;
  wal_flushes: number;
  wal_errors: number;
}

const walMetrics = ref<WalMetrics>({
  wal_entries: 0,
  wal_size_bytes: 0,
  wal_flushes: 0,
  wal_errors: 0,
});

interface StorageMetric {
  name: string;
  value: number;
}

const storageMetrics = ref<StorageMetric[]>([]);

function parseWalMetrics(text: string) {
  const lines = text.split('\n');
  storageMetrics.value = [];
  for (const line of lines) {
    if (line.startsWith('#')) continue;
    const match = line.match(/^([a-zA-Z_]+)\s+([\d.eE+-]+)/);
    if (!match) continue;
    const [, name, val] = match;
    const numVal = parseFloat(val);
    if (name.includes('wal_entries')) walMetrics.value.wal_entries = numVal;
    if (name.includes('wal_size') || name.includes('wal_bytes')) walMetrics.value.wal_size_bytes = numVal;
    if (name.includes('wal_flush')) walMetrics.value.wal_flushes = numVal;
    if (name.includes('wal_error')) walMetrics.value.wal_errors = numVal;

    if (name.includes('storage') || name.includes('parquet') || name.includes('object_store')) {
      storageMetrics.value.push({ name, value: numVal });
    }
  }
}

function formatSize(bytes: number): string {
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

async function loadData() {
  loading.value = true;
  try {
    const metricsText = await fetchMetrics();
    parseWalMetrics(metricsText);
  } catch (error) {
    reportError('StorageView.loadMetrics', error);
    toast.add({ severity: 'warn', summary: 'Warning', detail: 'Metrics data unavailable', life: 3000 });
  } finally {
    loading.value = false;
  }
}

onMounted(loadData);
</script>

<template>
  <div>
    <div class="flex items-center justify-between mb-6">
      <h1 class="text-2xl font-bold text-surface-800">Storage</h1>
      <Button icon="pi pi-refresh" size="small" severity="secondary" text @click="loadData" />
    </div>

    <div v-if="loading" class="flex justify-center py-12">
      <i class="pi pi-spin pi-spinner text-3xl text-surface-400"></i>
    </div>

    <div v-else>
      <!-- WAL Stats -->
      <div class="grid grid-cols-1 md:grid-cols-4 gap-5 mb-6">
        <Card class="!shadow-sm">
          <template #content>
            <div class="stat-card">
              <div class="stat-icon bg-blue-100 text-blue-600">
                <i class="pi pi-file-edit"></i>
              </div>
              <div>
                <div class="text-sm text-surface-500 font-medium">WAL Entries</div>
                <div class="text-2xl font-bold text-surface-800">{{ walMetrics.wal_entries.toLocaleString() }}</div>
              </div>
            </div>
          </template>
        </Card>

        <Card class="!shadow-sm">
          <template #content>
            <div class="stat-card">
              <div class="stat-icon bg-purple-100 text-purple-600">
                <i class="pi pi-database"></i>
              </div>
              <div>
                <div class="text-sm text-surface-500 font-medium">WAL Size</div>
                <div class="text-2xl font-bold text-surface-800">{{ formatSize(walMetrics.wal_size_bytes) }}</div>
              </div>
            </div>
          </template>
        </Card>

        <Card class="!shadow-sm">
          <template #content>
            <div class="stat-card">
              <div class="stat-icon bg-green-100 text-green-600">
                <i class="pi pi-check-circle"></i>
              </div>
              <div>
                <div class="text-sm text-surface-500 font-medium">WAL Flushes</div>
                <div class="text-2xl font-bold text-surface-800">{{ walMetrics.wal_flushes.toLocaleString() }}</div>
              </div>
            </div>
          </template>
        </Card>

        <Card class="!shadow-sm">
          <template #content>
            <div class="stat-card">
              <div class="stat-icon" :class="walMetrics.wal_errors > 0 ? 'bg-red-100 text-red-600' : 'bg-green-100 text-green-600'">
                <i class="pi" :class="walMetrics.wal_errors > 0 ? 'pi-exclamation-triangle' : 'pi-check'"></i>
              </div>
              <div>
                <div class="text-sm text-surface-500 font-medium">WAL Errors</div>
                <div class="text-2xl font-bold text-surface-800">{{ walMetrics.wal_errors }}</div>
              </div>
            </div>
          </template>
        </Card>
      </div>

      <!-- Storage Metrics -->
      <Card v-if="storageMetrics.length > 0" class="!shadow-sm">
        <template #title>
          <div class="flex items-center gap-2">
            <i class="pi pi-chart-bar text-primary"></i>
            <span>Storage Metrics</span>
          </div>
        </template>
        <template #content>
          <DataTable :value="storageMetrics" stripedRows>
            <Column field="name" header="Metric" />
            <Column field="value" header="Value" style="width: 200px">
              <template #body="{ data }">
                <span class="font-mono text-sm">{{ data.value.toLocaleString() }}</span>
              </template>
            </Column>
          </DataTable>
        </template>
      </Card>
    </div>
  </div>
</template>
