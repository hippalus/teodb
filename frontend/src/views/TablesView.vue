<script setup lang="ts">
import { ref, computed, onMounted } from 'vue';
import { reportError } from '@/composables/useErrorLog';
import { useRouter } from 'vue-router';
import { useToast } from 'primevue/usetoast';
import { useConfirm } from 'primevue/useconfirm';
import { fetchTables, flushTable, dropTable } from '@/api/admin';
import { executeQuery } from '@/api/admin';
import { apiErrorMessage } from '@/composables/useApiError';
import { useAppStore } from '@/stores/app';
import type { TableSummary } from '@/api/types';
import DataTable from 'primevue/datatable';
import Column from 'primevue/column';
import Button from 'primevue/button';
import InputText from 'primevue/inputtext';
import Dialog from 'primevue/dialog';
import Tag from 'primevue/tag';
import SqlEditor from '@/components/SqlEditor.vue';

const router = useRouter();
const toast = useToast();
const confirm = useConfirm();
const appStore = useAppStore();

const tables = ref<TableSummary[]>([]);
const loading = ref(true);
const searchQuery = ref('');
const createDialogVisible = ref(false);
const createSql = ref('CREATE TABLE my_table (\n  id INT NOT NULL,\n  name VARCHAR NOT NULL,\n  value DOUBLE\n)');
const creating = ref(false);

const filteredTables = computed(() => {
  if (!searchQuery.value) return tables.value;
  const q = searchQuery.value.toLowerCase();
  return tables.value.filter((t) => t.name.toLowerCase().includes(q));
});

async function loadTables() {
  loading.value = true;
  try {
    tables.value = await fetchTables();
    appStore.setTables(tables.value);
  } catch (err) {
    reportError('TablesView.loadTables', err);
    toast.add({ severity: 'error', summary: 'Error', detail: 'Failed to load tables', life: 3000 });
  } finally {
    loading.value = false;
  }
}

async function handleCreate() {
  creating.value = true;
  try {
    await executeQuery({ sql: createSql.value });
    toast.add({ severity: 'success', summary: 'Success', detail: 'Table created', life: 3000 });
    createDialogVisible.value = false;
    await loadTables();
  } catch (err) {
    reportError('TablesView.createTable', err);
    const message = apiErrorMessage(err, 'Failed to create table');
    toast.add({ severity: 'error', summary: 'Error', detail: message, life: 5000 });
  } finally {
    creating.value = false;
  }
}

function handleFlush(table: TableSummary) {
  confirm.require({
    message: `Flush table "${table.namespace}.${table.name}" to Parquet?`,
    header: 'Confirm Flush',
    icon: 'pi pi-exclamation-triangle',
    acceptClass: 'p-button-warning',
    accept: async () => {
      try {
        await flushTable(table.namespace, table.name);
        toast.add({ severity: 'success', summary: 'Flushed', detail: `Table "${table.name}" flushed`, life: 3000 });
      } catch (error) {
        reportError('TablesView.flushTable', error);
        toast.add({ severity: 'error', summary: 'Error', detail: 'Flush failed', life: 3000 });
      }
    },
  });
}

function handleDrop(table: TableSummary) {
  confirm.require({
    message: `Are you sure you want to drop table "${table.namespace}.${table.name}"? This cannot be undone.`,
    header: 'Confirm Drop',
    icon: 'pi pi-exclamation-triangle',
    acceptClass: 'p-button-danger',
    accept: async () => {
      try {
        await dropTable(table.namespace, table.name);
        toast.add({ severity: 'success', summary: 'Dropped', detail: `Table "${table.name}" dropped`, life: 3000 });
        await loadTables();
      } catch (error) {
        reportError('TablesView.dropTable', error);
        toast.add({ severity: 'error', summary: 'Error', detail: 'Drop failed', life: 3000 });
      }
    },
  });
}

function formatSize(bytes: number): string {
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

onMounted(loadTables);
</script>

<template>
  <div>
    <div class="flex items-center justify-between mb-6">
      <h1 class="text-2xl font-bold text-surface-800">Tables</h1>
      <Button label="Create Table" icon="pi pi-plus" @click="createDialogVisible = true" />
    </div>

    <div class="card">
      <div class="flex items-center gap-3 mb-4">
        <span class="p-input-icon-left flex-1">
          <i class="pi pi-search" />
          <InputText v-model="searchQuery" placeholder="Search tables..." class="w-full" />
        </span>
        <Button icon="pi pi-refresh" severity="secondary" text @click="loadTables" />
      </div>

      <DataTable
        :value="filteredTables"
        :loading="loading"
        stripedRows
        paginator
        :rows="20"
        :rowsPerPageOptions="[10, 20, 50]"
        dataKey="name"
        sortField="name"
        :sortOrder="1"
        emptyMessage="No tables found"
      >
        <Column field="name" header="Name" sortable>
          <template #body="{ data }">
            <router-link
              :to="{ name: 'table-detail', params: { namespace: data.namespace, name: data.name } }"
              class="text-primary font-medium hover:underline"
            >
              {{ data.namespace }}.{{ data.name }}
            </router-link>
          </template>
        </Column>
        <Column field="namespace" header="Namespace" sortable style="width: 140px" />
        <Column field="column_count" header="Columns" sortable style="width: 100px" />
        <Column field="row_count" header="Rows" sortable style="width: 120px">
          <template #body="{ data }">
            {{ data.row_count.toLocaleString() }}
          </template>
        </Column>
        <Column field="size_bytes" header="Size" sortable style="width: 100px">
          <template #body="{ data }">
            {{ formatSize(data.size_bytes) }}
          </template>
        </Column>
        <Column field="partitioned" header="Partitioned" style="width: 120px">
          <template #body="{ data }">
            <Tag :value="data.partitioned ? 'Yes' : 'No'" :severity="data.partitioned ? 'success' : 'secondary'" />
          </template>
        </Column>
        <Column header="Actions" style="width: 180px">
          <template #body="{ data }">
            <div class="flex gap-1">
              <Button
                icon="pi pi-eye"
                text
                rounded
                size="small"
                v-tooltip="'View'"
                @click="router.push({ name: 'table-detail', params: { namespace: data.namespace, name: data.name } })"
              />
              <Button
                icon="pi pi-download"
                text
                rounded
                size="small"
                severity="warning"
                v-tooltip="'Flush'"
                @click="handleFlush(data)"
              />
              <Button
                icon="pi pi-trash"
                text
                rounded
                size="small"
                severity="danger"
                v-tooltip="'Drop'"
                @click="handleDrop(data)"
              />
            </div>
          </template>
        </Column>
      </DataTable>
    </div>

    <!-- Create Table Dialog -->
    <Dialog
      v-model:visible="createDialogVisible"
      header="Create Table"
      :modal="true"
      :style="{ width: '650px' }"
    >
      <div class="mb-4">
        <label class="block text-sm font-medium text-surface-600 mb-2">SQL Statement</label>
        <SqlEditor v-model="createSql" height="200px" placeholder="CREATE TABLE ..." />
      </div>
      <template #footer>
        <Button label="Cancel" severity="secondary" text @click="createDialogVisible = false" />
        <Button label="Create" icon="pi pi-check" :loading="creating" @click="handleCreate" />
      </template>
    </Dialog>
  </div>
</template>
