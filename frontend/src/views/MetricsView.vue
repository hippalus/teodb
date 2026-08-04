<script setup lang="ts">
import { ref, onMounted, onBeforeUnmount, watch } from 'vue';
import { reportError } from '@/composables/useErrorLog';
import { useToast } from 'primevue/usetoast';
import { fetchMetrics } from '@/api/admin';
import { usePolling } from '@/composables/usePolling';
import Card from 'primevue/card';
import Button from 'primevue/button';
import {
  Chart,
  LineElement,
  PointElement,
  LinearScale,
  CategoryScale,
  LineController,
  Tooltip as ChartTooltip,
  Filler,
} from 'chart.js';

Chart.register(LineElement, PointElement, LinearScale, CategoryScale, LineController, ChartTooltip, Filler);

const toast = useToast();
const rawMetrics = ref('');
const loading = ref(true);

interface ParsedMetric {
  series: string;
  name: string;
  help: string;
  type: string;
  value: number;
}

const parsedMetrics = ref<ParsedMetric[]>([]);
const omittedSeries = ref(0);
const rawVisible = ref(false);
const metricHistory = new Map<string, { timestamps: string[]; values: number[] }>();

const chartRef = ref<HTMLCanvasElement>();
let chartInstance: Chart<'line'> | null = null;
const selectedMetrics = ref<string[]>([]);
const chartRevision = ref(0);
const MAX_HISTORY = 30;
const MAX_RENDERED_SERIES = 200;
const MAX_SELECTED_METRICS = 6;

const HELP_LINE = /^# HELP ([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\s+(.*))?$/;
const TYPE_LINE = /^# TYPE ([a-zA-Z_:][a-zA-Z0-9_:]*)\s+(\S+)$/;
const SAMPLE_LINE = /^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{.*\})?\s+(\S+)(?:\s+\d+)?\s*$/;

function parsePrometheusValue(raw: string): number {
  if (raw === '+Inf') return Number.POSITIVE_INFINITY;
  if (raw === '-Inf') return Number.NEGATIVE_INFINITY;
  if (raw === 'NaN') return Number.NaN;
  return Number(raw);
}

function parsePrometheusMetrics(text: string): { metrics: ParsedMetric[]; totalSeries: number } {
  const metrics: ParsedMetric[] = [];
  const metadata = new Map<string, { help: string; type: string }>();
  let totalSeries = 0;

  for (const line of text.split('\n')) {
    const help = HELP_LINE.exec(line);
    if (help) {
      const existing = metadata.get(help[1]);
      metadata.set(help[1], { help: help[2] ?? '', type: existing?.type ?? 'unknown' });
      continue;
    }

    const type = TYPE_LINE.exec(line);
    if (type) {
      const existing = metadata.get(type[1]);
      metadata.set(type[1], { help: existing?.help ?? '', type: type[2] });
      continue;
    }

    if (!line || line.startsWith('#')) continue;
    const sample = SAMPLE_LINE.exec(line);
    if (!sample) continue;

    totalSeries += 1;
    if (metrics.length >= MAX_RENDERED_SERIES) continue;

    const name = sample[1];
    const labels = sample[2] ?? '';
    const details = metadata.get(name);
    metrics.push({
      series: `${name}${labels}`,
      name,
      help: details?.help ?? '',
      type: details?.type ?? 'unknown',
      value: parsePrometheusValue(sample[3]),
    });
  }
  return { metrics, totalSeries };
}

async function loadMetrics(signal: AbortSignal) {
  try {
    const nextMetrics = await fetchMetrics({ signal });
    if (signal.aborted) return;

    rawMetrics.value = nextMetrics;
    const parsed = parsePrometheusMetrics(rawMetrics.value);
    parsedMetrics.value = parsed.metrics;
    omittedSeries.value = Math.max(0, parsed.totalSeries - parsed.metrics.length);
    loading.value = false;

    const activeNames = new Set(parsedMetrics.value.map((m) => m.series));
    const retainedSelection = selectedMetrics.value.filter((name) => activeNames.has(name));
    if (retainedSelection.length !== selectedMetrics.value.length) {
      selectedMetrics.value = retainedSelection;
    }
    for (const name of metricHistory.keys()) {
      if (!activeNames.has(name)) {
        metricHistory.delete(name);
      }
    }

    const now = new Date().toLocaleTimeString();
    for (const m of parsedMetrics.value) {
      if (!metricHistory.has(m.series)) {
        metricHistory.set(m.series, { timestamps: [], values: [] });
      }
      const hist = metricHistory.get(m.series)!;
      hist.timestamps.push(now);
      hist.values.push(m.value);
      if (hist.timestamps.length > MAX_HISTORY) {
        hist.timestamps.shift();
        hist.values.shift();
      }
    }

    // Auto-select first 3 metrics if none selected
    if (selectedMetrics.value.length === 0 && parsedMetrics.value.length > 0) {
      selectedMetrics.value = parsedMetrics.value.slice(0, Math.min(3, parsedMetrics.value.length)).map((m) => m.series);
    }
    chartRevision.value += 1;
  } catch (error) {
    if (signal.aborted) return;
    reportError('MetricsView.loadMetrics', error);
    if (loading.value) {
      toast.add({ severity: 'warn', summary: 'Warning', detail: 'Failed to load metrics', life: 3000 });
      loading.value = false;
    }
  }
}

const colors = ['#6366f1', '#22c55e', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4'];

function renderChart() {
  if (!chartRef.value) return;

  const datasets = selectedMetrics.value.map((name, i) => {
    const hist = metricHistory.get(name);
    return {
      label: name,
      data: [...(hist?.values ?? [])],
      borderColor: colors[i % colors.length],
      backgroundColor: colors[i % colors.length] + '20',
      fill: true,
      tension: 0.4,
      pointRadius: 2,
    };
  });

  const labels = selectedMetrics.value.length > 0
    ? [...(metricHistory.get(selectedMetrics.value[0])?.timestamps ?? [])]
    : [];

  if (chartInstance) {
    chartInstance.data.labels = labels;
    chartInstance.data.datasets = datasets;
    chartInstance.update('none');
    return;
  }

  chartInstance = new Chart(chartRef.value, {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      scales: {
        y: { beginAtZero: true },
      },
      plugins: {
        tooltip: { enabled: true },
      },
    },
  });
}

function toggleMetric(name: string) {
  const idx = selectedMetrics.value.indexOf(name);
  if (idx >= 0) {
    selectedMetrics.value.splice(idx, 1);
  } else {
    if (selectedMetrics.value.length >= MAX_SELECTED_METRICS) {
      toast.add({
        severity: 'info',
        summary: 'Chart limit reached',
        detail: `Select at most ${MAX_SELECTED_METRICS} metric series at once`,
        life: 2500,
      });
      return;
    }
    selectedMetrics.value.push(name);
  }
}

function destroyChart() {
  chartInstance?.destroy();
  chartInstance = null;
}

const { start, stop, refresh, isPolling } = usePolling(loadMetrics, 5_000);

watch([selectedMetrics, chartRevision], () => renderChart(), { deep: true, flush: 'post' });

onMounted(() => {
  start();
});

onBeforeUnmount(() => {
  stop();
  destroyChart();
});
</script>

<template>
  <div>
    <div class="flex items-center justify-between mb-6">
      <h1 class="text-2xl font-bold text-surface-800">Metrics</h1>
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

    <div v-else>
      <!-- Chart -->
      <Card class="!shadow-sm mb-6">
        <template #title>
          <div class="flex items-center justify-between gap-2">
            <div class="flex items-center gap-2">
              <i class="pi pi-chart-line text-primary"></i>
              <span>Metrics Over Time</span>
            </div>
            <span class="text-xs font-normal text-surface-500">
              {{ selectedMetrics.length }}/{{ MAX_SELECTED_METRICS }} selected
            </span>
          </div>
        </template>
        <template #content>
          <div style="height: 300px">
            <canvas ref="chartRef"></canvas>
          </div>
        </template>
      </Card>

      <!-- Metrics Grid -->
      <div v-if="omittedSeries > 0" class="mb-4 rounded-lg bg-amber-50 px-4 py-3 text-sm text-amber-800">
        Showing the first {{ MAX_RENDERED_SERIES }} metric series; {{ omittedSeries.toLocaleString() }} additional series are
        omitted to keep this page responsive. Use the raw scrape or a Prometheus-compatible dashboard for full-cardinality analysis.
      </div>
      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-6">
        <div
          v-for="metric in parsedMetrics"
          :key="metric.series"
          class="card cursor-pointer transition-all duration-200 hover:shadow-md"
          :class="{ 'ring-2 ring-primary-400': selectedMetrics.includes(metric.series) }"
          @click="toggleMetric(metric.series)"
        >
          <div class="flex items-center justify-between mb-1">
            <span class="text-sm font-medium text-surface-700 truncate" :title="metric.series">{{ metric.series }}</span>
            <span class="text-xs px-2 py-0.5 bg-surface-100 rounded text-surface-500">{{ metric.type }}</span>
          </div>
          <div class="text-2xl font-bold text-surface-800">
            {{ typeof metric.value === 'number' ? metric.value.toLocaleString() : metric.value }}
          </div>
          <div v-if="metric.help" class="text-xs text-surface-400 mt-1 truncate">
            {{ metric.help }}
          </div>
        </div>
      </div>

      <!-- Raw Metrics -->
      <Card class="!shadow-sm">
        <template #title>
          <div class="flex items-center justify-between gap-2">
            <div class="flex items-center gap-2">
              <i class="pi pi-file-edit text-primary"></i>
              <span>Raw Prometheus Output</span>
            </div>
            <Button
              :label="rawVisible ? 'Hide raw scrape' : 'Show raw scrape'"
              size="small"
              severity="secondary"
              text
              @click="rawVisible = !rawVisible"
            />
          </div>
        </template>
        <template #content>
          <pre
            v-if="rawVisible"
            class="bg-surface-900 text-green-400 p-4 rounded-lg overflow-auto text-xs font-mono leading-relaxed max-h-96"
          >{{ rawMetrics || 'No metrics available' }}</pre>
          <p v-else class="text-sm text-surface-500">Raw scrape rendering is paused to keep high-cardinality pages responsive.</p>
        </template>
      </Card>
    </div>
  </div>
</template>
