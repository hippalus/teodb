<script setup lang="ts">
import { ref, onMounted } from 'vue';
import { reportError } from '@/composables/useErrorLog';
import { useToast } from 'primevue/usetoast';
import { fetchClusterStatus } from '@/api/admin';
import type { ClusterStatusResponse } from '@/api/types';
import { usePolling } from '@/composables/usePolling';
import DataTable from 'primevue/datatable';
import Column from 'primevue/column';
import Card from 'primevue/card';
import Tag from 'primevue/tag';
import Button from 'primevue/button';

const toast = useToast();
const cluster = ref<ClusterStatusResponse | null>(null);
const loading = ref(true);

async function loadCluster(signal: AbortSignal) {
  try {
    const nextCluster = await fetchClusterStatus({ signal });
    if (signal.aborted) return;
    cluster.value = nextCluster;
    loading.value = false;
  } catch (error) {
    if (signal.aborted) return;
    reportError('ClusterView.loadCluster', error);
    if (loading.value) {
      toast.add({ severity: 'warn', summary: 'Warning', detail: 'Failed to load cluster status', life: 3000 });
      loading.value = false;
    }
  }
}

const { start, stop, refresh, isPolling } = usePolling(loadCluster, 10_000);

function workerStatusSeverity(status: string): string {
  switch (status) {
    case 'active': return 'success';
    case 'draining': return 'warning';
    case 'offline': return 'danger';
    default: return 'secondary';
  }
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleString();
}

onMounted(() => {
  start();
});
</script>

<template>
  <div>
    <div class="flex items-center justify-between mb-6">
      <h1 class="text-2xl font-bold text-surface-800">Cluster</h1>
      <div class="flex gap-2">
        <Button
          :icon="isPolling ? 'pi pi-pause' : 'pi pi-play'"
          :label="isPolling ? 'Pause' : 'Resume'"
          size="small"
          severity="secondary"
          @click="isPolling ? stop() : start()"
        />
        <Button icon="pi pi-refresh" size="small" severity="secondary" text @click="refresh" />
      </div>
    </div>

    <div v-if="loading" class="flex justify-center py-12">
      <i class="pi pi-spin pi-spinner text-3xl text-surface-400"></i>
    </div>

    <div v-else-if="cluster">
      <!-- Cluster Info Cards -->
      <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-5 mb-6">
        <Card class="!shadow-sm">
          <template #content>
            <div class="stat-card">
              <div class="stat-icon bg-indigo-100 text-indigo-600">
                <i class="pi pi-server"></i>
              </div>
              <div>
                <div class="text-sm text-surface-500 font-medium">Mode</div>
                <div class="text-xl font-bold text-surface-800 capitalize">{{ cluster.mode }}</div>
              </div>
            </div>
          </template>
        </Card>

        <Card class="!shadow-sm">
          <template #content>
            <div class="stat-card">
              <div
                class="stat-icon"
                :class="cluster.scheduler
                  ? (cluster.scheduler.reachable ? 'bg-green-100 text-green-600' : 'bg-red-100 text-red-600')
                  : 'bg-surface-100 text-surface-500'"
              >
                <i
                  class="pi"
                  :class="cluster.scheduler
                    ? (cluster.scheduler.reachable ? 'pi-check-circle' : 'pi-exclamation-triangle')
                    : 'pi-minus-circle'"
                ></i>
              </div>
              <div class="min-w-0">
                <div class="text-sm text-surface-500 font-medium">Scheduler</div>
                <div class="text-xl font-bold text-surface-800">
                  {{ cluster.scheduler ? (cluster.scheduler.reachable ? 'Reachable' : 'Unavailable') : 'Not configured' }}
                </div>
                <div v-if="cluster.scheduler" class="text-xs text-surface-500 truncate" :title="cluster.scheduler.address">
                  {{ cluster.scheduler.address }} · {{ cluster.active_jobs ?? 0 }} active jobs
                </div>
                <div v-else class="text-xs text-surface-500">No Ballista scheduler for this role</div>
              </div>
            </div>
          </template>
        </Card>

        <Card class="!shadow-sm">
          <template #content>
            <div class="stat-card">
              <div class="stat-icon bg-blue-100 text-blue-600">
                <i class="pi pi-users"></i>
              </div>
              <div>
                <div class="text-sm text-surface-500 font-medium">Workers</div>
                <div class="text-xl font-bold text-surface-800">{{ cluster.workers.length }}</div>
              </div>
            </div>
          </template>
        </Card>

        <Card class="!shadow-sm">
          <template #content>
            <div class="stat-card">
              <div class="stat-icon bg-green-100 text-green-600">
                <i class="pi pi-link"></i>
              </div>
              <div>
                <div class="text-sm text-surface-500 font-medium">Connections</div>
                <div class="text-xl font-bold text-surface-800">{{ cluster.connections.length }}</div>
              </div>
            </div>
          </template>
        </Card>
      </div>

      <!-- Workers Table -->
      <div class="card mb-6">
        <h2 class="text-lg font-semibold text-surface-700 mb-4">
          <i class="pi pi-users mr-2"></i>Workers
        </h2>
        <DataTable
          v-if="cluster.workers.length > 0"
          :value="cluster.workers"
          stripedRows
          dataKey="id"
        >
          <Column field="id" header="ID" />
          <Column field="host" header="Host" />
          <Column field="flight_port" header="Flight Port" style="width: 120px" />
          <Column field="status" header="Status" style="width: 120px">
            <template #body="{ data }">
              <Tag :value="data.status" :severity="workerStatusSeverity(data.status)" />
            </template>
          </Column>
          <Column field="last_heartbeat" header="Last Heartbeat">
            <template #body="{ data }">
              {{ data.last_heartbeat ? formatDate(data.last_heartbeat) : '—' }}
            </template>
          </Column>
        </DataTable>
        <div v-else class="text-center py-6 text-surface-400">
          <p>No workers registered (standalone mode)</p>
        </div>
      </div>

      <!-- Connections Table -->
      <div class="card">
        <h2 class="text-lg font-semibold text-surface-700 mb-4">
          <i class="pi pi-link mr-2"></i>Active Connections
        </h2>
        <DataTable
          v-if="cluster.connections.length > 0"
          :value="cluster.connections"
          stripedRows
          dataKey="id"
        >
          <Column field="id" header="ID" />
          <Column field="client_address" header="Client" />
          <Column field="protocol" header="Protocol">
            <template #body="{ data }">
              <Tag :value="data.protocol" severity="info" />
            </template>
          </Column>
          <Column field="connected_at" header="Connected At">
            <template #body="{ data }">
              {{ formatDate(data.connected_at) }}
            </template>
          </Column>
          <Column field="last_activity" header="Last Activity">
            <template #body="{ data }">
              {{ formatDate(data.last_activity) }}
            </template>
          </Column>
        </DataTable>
        <div v-else class="text-center py-6 text-surface-400">
          <p>No active connections</p>
        </div>
      </div>
    </div>

    <div v-else class="card text-center py-12">
      <i class="pi pi-exclamation-triangle text-4xl text-surface-400 mb-3"></i>
      <p class="text-surface-500">Unable to load cluster information</p>
      <Button label="Retry" icon="pi pi-refresh" class="mt-4" @click="refresh" />
    </div>
  </div>
</template>
