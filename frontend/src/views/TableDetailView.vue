<script setup lang="ts">
import { ref, onMounted } from 'vue';
import { reportError } from '@/composables/useErrorLog';
import { useRouter } from 'vue-router';
import { useToast } from 'primevue/usetoast';
import { useConfirm } from 'primevue/useconfirm';
import { fetchTable, readTable, flushTable, dropTable, ingestData } from '@/api/admin';
import type { TableDetail, SqlQueryResponse } from '@/api/types';
import { apiErrorMessage } from '@/composables/useApiError';
import Tabs from 'primevue/tabs';
import TabList from 'primevue/tablist';
import Tab from 'primevue/tab';
import TabPanels from 'primevue/tabpanels';
import TabPanel from 'primevue/tabpanel';
import DataTable from 'primevue/datatable';
import Column from 'primevue/column';
import Button from 'primevue/button';
import Tag from 'primevue/tag';
import Dialog from 'primevue/dialog';
import Textarea from 'primevue/textarea';
import Card from 'primevue/card';

const props = defineProps<{
  namespace: string;
  name: string;
}>();

const router = useRouter();
const toast = useToast();
const confirm = useConfirm();

const table = ref<TableDetail | null>(null);
const sampleData = ref<SqlQueryResponse | null>(null);
const loading = ref(true);
const dataLoading = ref(false);
const ingestDialogVisible = ref(false);
const ingestJson = ref('[\n  { "id": 1, "name": "example" }\n]');
const ingesting = ref(false);

function parseIngestRows(): Record<string, unknown>[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(ingestJson.value);
  } catch (error) {
    throw new Error(`Invalid JSON: ${error instanceof Error ? error.message : 'parse failed'}`);
  }

  if (!Array.isArray(parsed)) {
    throw new Error('Ingest payload must be a JSON array of row objects.');
  }

  for (const [index, row] of parsed.entries()) {
    if (row === null || typeof row !== 'object' || Array.isArray(row)) {
      throw new Error(`Row ${index + 1} must be a JSON object.`);
    }
  }

  return parsed as Record<string, unknown>[];
}

async function loadTable() {
  loading.value = true;
  try {
    table.value = await fetchTable(props.namespace, props.name);
  } catch (error) {
    reportError('TableDetailView.action', error);
    toast.add({ severity: 'error', summary: 'Error', detail: `Failed to load table "${props.namespace}.${props.name}"`, life: 3000 });
  } finally {
    loading.value = false;
  }
}

async function loadSampleData() {
  dataLoading.value = true;
  try {
    sampleData.value = await readTable(props.namespace, props.name, { limit: 100 });
  } catch (error) {
    reportError('TableDetailView.action', error);
    toast.add({ severity: 'error', summary: 'Error', detail: 'Failed to load sample data', life: 3000 });
  } finally {
    dataLoading.value = false;
  }
}

function handleFlush() {
  confirm.require({
    message: `Flush table "${props.namespace}.${props.name}" to Parquet?`,
    header: 'Confirm Flush',
    icon: 'pi pi-exclamation-triangle',
    accept: async () => {
      try {
        await flushTable(props.namespace, props.name);
        toast.add({ severity: 'success', summary: 'Flushed', detail: 'Table flushed to Parquet', life: 3000 });
        await loadSampleData();
      } catch (error) {
        reportError('TableDetailView.flushTable', error);
        toast.add({ severity: 'error', summary: 'Error', detail: 'Flush failed', life: 3000 });
      }
    },
  });
}

function handleDrop() {
  confirm.require({
    message: `Drop table "${props.namespace}.${props.name}"? This cannot be undone.`,
    header: 'Confirm Drop',
    icon: 'pi pi-exclamation-triangle',
    acceptClass: 'p-button-danger',
    accept: async () => {
      try {
        await dropTable(props.namespace, props.name);
        toast.add({ severity: 'success', summary: 'Dropped', detail: 'Table dropped', life: 3000 });
        router.push('/tables');
      } catch (error) {
        reportError('TableDetailView.dropTable', error);
        toast.add({ severity: 'error', summary: 'Error', detail: 'Drop failed', life: 3000 });
      }
    },
  });
}

async function handleIngest() {
  let rows: Record<string, unknown>[];
  try {
    rows = parseIngestRows();
  } catch (error) {
    reportError('TableDetailView.parseIngestJson', error);
    toast.add({ severity: 'error', summary: 'Invalid JSON', detail: apiErrorMessage(error, 'Invalid JSON'), life: 5000 });
    return;
  }

  ingesting.value = true;
  try {
    const result = await ingestData(props.namespace, props.name, { rows });
    try {
      await flushTable(props.namespace, props.name);
    } catch (error) {
      reportError('TableDetailView.flushAfterIngest', error);
      toast.add({
        severity: 'warn',
        summary: 'Rows accepted',
        detail: `${result.accepted_rows} rows are durable but not query-visible until a flush succeeds.`,
        life: 7000,
      });
      ingestDialogVisible.value = false;
      return;
    }
    toast.add({
      severity: 'success',
      summary: 'Ingested & flushed',
      detail: `${result.accepted_rows} rows accepted and flushed (batch: ${result.batch_id})`,
      life: 3000,
    });
    ingestDialogVisible.value = false;
    await loadTable();
    await loadSampleData();
  } catch (err) {
    reportError('TableDetailView.ingestRows', err);
    const message = apiErrorMessage(err, 'Ingest failed');
    toast.add({ severity: 'error', summary: 'Error', detail: message, life: 5000 });
  } finally {
    ingesting.value = false;
  }
}

onMounted(() => {
  loadTable();
});
</script>

<template>
  <div>
    <div class="flex items-center justify-between mb-6">
      <div class="flex items-center gap-3">
        <Button icon="pi pi-arrow-left" text rounded severity="secondary" @click="router.push('/tables')" />
        <h1 class="text-2xl font-bold text-surface-800">{{ namespace }}.{{ name }}</h1>
      </div>
      <div class="flex gap-2">
        <Button label="Ingest" icon="pi pi-upload" severity="info" size="small" @click="ingestDialogVisible = true" />
        <Button label="Flush" icon="pi pi-download" severity="warning" size="small" @click="handleFlush" />
        <Button label="Drop" icon="pi pi-trash" severity="danger" size="small" @click="handleDrop" />
      </div>
    </div>

    <div v-if="loading" class="flex justify-center py-12">
      <i class="pi pi-spin pi-spinner text-3xl text-surface-400"></i>
    </div>

    <div v-else-if="table">
      <!-- Summary Cards -->
      <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
        <Card class="!shadow-sm">
          <template #content>
            <div class="text-sm text-surface-500">Columns</div>
            <div class="text-xl font-bold">{{ table.columns.length }}</div>
          </template>
        </Card>
        <Card class="!shadow-sm">
          <template #content>
            <div class="text-sm text-surface-500">Schema ID</div>
            <div class="text-xl font-bold">{{ table.current_schema_id }}</div>
          </template>
        </Card>
        <Card class="!shadow-sm">
          <template #content>
            <div class="text-sm text-surface-500">Snapshot ID</div>
            <div class="text-xl font-bold">{{ table.current_snapshot_id ?? '—' }}</div>
          </template>
        </Card>
      </div>

      <Tabs value="0">
        <TabList>
          <Tab value="0">Schema</Tab>
          <Tab value="1">Data</Tab>
          <Tab value="2">Properties</Tab>
        </TabList>
        <TabPanels>
          <!-- Schema Tab -->
          <TabPanel value="0">
          <DataTable :value="table.columns" stripedRows dataKey="field_id">
            <Column field="field_id" header="Field ID" style="width: 100px" />
            <Column field="name" header="Name" />
            <Column field="data_type" header="Type">
              <template #body="{ data }">
                <Tag :value="data.data_type" severity="info" />
              </template>
            </Column>
            <Column field="nullable" header="Nullable" style="width: 100px">
              <template #body="{ data }">
                <Tag :value="data.nullable ? 'Yes' : 'No'" :severity="data.nullable ? 'warning' : 'success'" />
              </template>
            </Column>
            <Column field="comment" header="Comment" />
          </DataTable>
          </TabPanel>

          <!-- Data Tab -->
          <TabPanel value="1">
          <div class="mb-4">
            <Button
              label="Load Sample Data"
              icon="pi pi-refresh"
              size="small"
              :loading="dataLoading"
              @click="loadSampleData"
            />
            <span v-if="sampleData" class="ml-3 text-sm text-surface-500">
              {{ sampleData.row_count }} rows · {{ sampleData.elapsed_ms }}ms
            </span>
          </div>
          <DataTable
            v-if="sampleData && sampleData.columns.length > 0"
            :value="sampleData.rows"
            :loading="dataLoading"
            stripedRows
            scrollable
            scrollHeight="500px"
            paginator
            :rows="25"
          >
            <Column
              v-for="col in sampleData.columns"
              :key="col.name"
              :field="col.name"
              :header="col.name"
            >
              <template #body="{ data }">
                <span class="text-sm">{{ data[col.name] ?? '—' }}</span>
              </template>
            </Column>
          </DataTable>
          <div v-else-if="!dataLoading" class="text-center py-8 text-surface-400">
            Click "Load Sample Data" to preview rows
          </div>
          </TabPanel>

          <!-- Properties Tab -->
          <TabPanel value="2">
          <DataTable :value="Object.entries(table.properties).map(([k, v]) => ({ key: k, value: v }))" stripedRows>
            <Column field="key" header="Key" />
            <Column field="value" header="Value" />
          </DataTable>
          </TabPanel>
        </TabPanels>
      </Tabs>
    </div>

    <!-- Ingest Dialog -->
    <Dialog v-model:visible="ingestDialogVisible" header="Ingest JSON Data" :modal="true" :style="{ width: '600px' }">
      <div class="mb-4">
        <label class="block text-sm font-medium text-surface-600 mb-2">
          JSON Array of Rows
        </label>
        <Textarea v-model="ingestJson" rows="12" class="w-full font-mono text-sm" />
      </div>
      <template #footer>
        <Button label="Cancel" severity="secondary" text @click="ingestDialogVisible = false" />
        <Button label="Ingest" icon="pi pi-upload" :loading="ingesting" @click="handleIngest" />
      </template>
    </Dialog>
  </div>
</template>
