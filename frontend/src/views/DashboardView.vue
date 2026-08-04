<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue';
import { reportError } from '@/composables/useErrorLog';
import { useRouter } from 'vue-router';
import { useAppStore } from '@/stores/app';
import { fetchStatus } from '@/api/admin';
import { useQueryHistory } from '@/composables/useQueryHistory';
import Card from 'primevue/card';
import Button from 'primevue/button';
import type { StatusResponse } from '@/api/types';
import { Chart, ArcElement, Tooltip as ChartTooltip, Legend, DoughnutController } from 'chart.js';

Chart.register(ArcElement, ChartTooltip, Legend, DoughnutController);

const router = useRouter();
const appStore = useAppStore();
const { history } = useQueryHistory();
const status = ref<StatusResponse | null>(null);
const loading = ref(true);

const recentQueries = computed(() => history.value.slice(0, 5));

const tablesCount = computed(() => status.value?.tables_count ?? appStore.tables.length);
const totalRows = computed(() => {
  if (status.value?.total_rows !== undefined) return status.value.total_rows;
  return appStore.tables.reduce((sum, t) => sum + t.row_count, 0);
});

const memoryUsage = computed(() => {
  const bytes = status.value?.memory_usage_bytes ?? 0;
  if (bytes === 0) return '—';
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
});

const healthStatus = computed(() => {
  if (!appStore.isConnected) return 'Disconnected';
  return status.value?.components?.every((c) => c.status === 'healthy') ? 'Healthy' : 'Degraded';
});

const healthColor = computed(() => {
  if (!appStore.isConnected) return 'bg-red-500';
  return healthStatus.value === 'Healthy' ? 'bg-green-500' : 'bg-yellow-500';
});

const chartRef = ref<HTMLCanvasElement>();
let chartInstance: Chart | null = null;
let statusController: AbortController | null = null;

function renderChart() {
  if (!chartRef.value || !status.value?.components) return;
  if (chartInstance) {
    chartInstance.destroy();
    chartInstance = null;
  }

  const components = status.value.components;
  const healthy = components.filter((c) => c.status === 'healthy').length;
  const degraded = components.filter((c) => c.status === 'degraded').length;
  const unhealthy = components.filter((c) => c.status === 'unhealthy').length;

  chartInstance = new Chart(chartRef.value, {
    type: 'doughnut',
    data: {
      labels: ['Healthy', 'Degraded', 'Unhealthy'],
      datasets: [
        {
          data: [healthy, degraded, unhealthy],
          backgroundColor: ['#22c55e', '#eab308', '#ef4444'],
          borderWidth: 0,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      plugins: {
        legend: {
          position: 'bottom',
          labels: { padding: 16, usePointStyle: true },
        },
      },
      cutout: '65%',
    },
  });
}

onMounted(async () => {
  const controller = new AbortController();
  statusController = controller;
  try {
    const nextStatus = await fetchStatus({ signal: controller.signal });
    if (controller.signal.aborted) return;
    status.value = nextStatus;
    appStore.setStatus(status.value);
    renderChart();
  } catch (error) {
    if (controller.signal.aborted) return;
    reportError('DashboardView.loadDashboard', error);
    appStore.setDisconnected();
  } finally {
    if (!controller.signal.aborted) {
      loading.value = false;
    }
  }
});

// Destroy the chart on unmount so navigating away doesn't leak the instance
// (FE-7).
onUnmounted(() => {
  statusController?.abort();
  statusController = null;
  if (chartInstance) {
    chartInstance.destroy();
    chartInstance = null;
  }
});

function formatDate(iso: string): string {
  return new Date(iso).toLocaleString();
}

function openRecentQuery(queryId: string) {
  void router.push({
    name: 'sql-workspace',
    query: { history: queryId },
  });
}
</script>

<template>
  <div>
    <div class="flex items-center justify-between mb-6">
      <h1 class="text-2xl font-bold text-surface-800">Dashboard</h1>
      <div class="flex gap-2">
        <Button label="Create Table" icon="pi pi-plus" size="small" @click="router.push('/tables')" />
        <Button
          label="SQL Editor"
          icon="pi pi-code"
          size="small"
          severity="secondary"
          @click="router.push('/sql')"
        />
      </div>
    </div>

    <!-- Stat Cards -->
    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-5 mb-6">
      <Card class="!shadow-sm">
        <template #content>
          <div class="stat-card">
            <div class="stat-icon bg-blue-100 text-blue-600">
              <i class="pi pi-table"></i>
            </div>
            <div>
              <div class="text-sm text-surface-500 font-medium">Total Tables</div>
              <div class="text-2xl font-bold text-surface-800">{{ tablesCount }}</div>
            </div>
          </div>
        </template>
      </Card>

      <Card class="!shadow-sm">
        <template #content>
          <div class="stat-card">
            <div class="stat-icon bg-green-100 text-green-600">
              <i class="pi pi-list"></i>
            </div>
            <div>
              <div class="text-sm text-surface-500 font-medium">Total Rows</div>
              <div class="text-2xl font-bold text-surface-800">{{ totalRows.toLocaleString() }}</div>
            </div>
          </div>
        </template>
      </Card>

      <Card class="!shadow-sm">
        <template #content>
          <div class="stat-card">
            <div class="stat-icon bg-purple-100 text-purple-600">
              <i class="pi pi-microchip"></i>
            </div>
            <div>
              <div class="text-sm text-surface-500 font-medium">Memory Usage</div>
              <div class="text-2xl font-bold text-surface-800">{{ memoryUsage }}</div>
            </div>
          </div>
        </template>
      </Card>

      <Card class="!shadow-sm">
        <template #content>
          <div class="stat-card">
            <div class="stat-icon" :class="[healthColor.replace('bg-', 'bg-').replace('500', '100'), healthColor.replace('bg-', 'text-')]">
              <i class="pi pi-heart"></i>
            </div>
            <div>
              <div class="text-sm text-surface-500 font-medium">Server Status</div>
              <div class="text-2xl font-bold text-surface-800">{{ healthStatus }}</div>
            </div>
          </div>
        </template>
      </Card>
    </div>

    <div class="grid grid-cols-1 lg:grid-cols-2 gap-5">
      <!-- Health Chart -->
      <Card class="!shadow-sm">
        <template #title>
          <div class="flex items-center gap-2">
            <i class="pi pi-chart-pie text-primary"></i>
            <span>Component Health</span>
          </div>
        </template>
        <template #content>
          <div v-if="loading" class="flex justify-center py-8">
            <i class="pi pi-spin pi-spinner text-2xl text-surface-400"></i>
          </div>
          <div v-else-if="status?.components?.length" class="flex justify-center">
            <canvas ref="chartRef" style="max-height: 250px; max-width: 250px"></canvas>
          </div>
          <div v-else class="text-center py-8 text-surface-400">
            <i class="pi pi-info-circle text-2xl mb-2"></i>
            <p>No component data available</p>
          </div>
        </template>
      </Card>

      <!-- Recent Queries -->
      <Card class="!shadow-sm">
        <template #title>
          <div class="flex items-center gap-2">
            <i class="pi pi-history text-primary"></i>
            <span>Recent Queries</span>
          </div>
        </template>
        <template #content>
          <div v-if="recentQueries.length === 0" class="text-center py-8 text-surface-400">
            <i class="pi pi-code text-2xl mb-2"></i>
            <p>No recent queries</p>
          </div>
          <div v-else class="flex flex-col gap-3">
            <div
              v-for="q in recentQueries"
              :key="q.id"
              class="p-3 bg-surface-50 rounded-lg cursor-pointer hover:bg-surface-100 transition-colors"
              @click="openRecentQuery(q.id)"
            >
              <code class="text-sm text-surface-700 block truncate">{{ q.sql }}</code>
              <div class="flex items-center gap-3 mt-1">
                <span class="text-xs text-surface-400">{{ formatDate(q.executed_at) }}</span>
                <span v-if="q.elapsed_ms" class="text-xs text-surface-400">
                  {{ q.elapsed_ms }}ms
                </span>
                <span v-if="q.row_count !== undefined" class="text-xs text-surface-400">
                  {{ q.row_count }} rows
                </span>
                <span v-if="q.error" class="text-xs text-red-500">
                  <i class="pi pi-exclamation-circle"></i> Error
                </span>
              </div>
            </div>
          </div>
        </template>
      </Card>
    </div>
  </div>
</template>
