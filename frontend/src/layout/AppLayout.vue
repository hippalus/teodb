<script setup lang="ts">
import { onMounted } from 'vue';
import { reportError } from '@/composables/useErrorLog';
import AppTopbar from './AppTopbar.vue';
import AppSidebar from './AppSidebar.vue';
import AppFooter from './AppFooter.vue';
import { useLayout } from './composables/layout';
import { useAppStore } from '@/stores/app';
import { fetchStatus, fetchTables } from '@/api/admin';
import { usePolling } from '@/composables/usePolling';

const { sidebarVisible, staticMenuDesktopInactive, hideSidebar } = useLayout();
const appStore = useAppStore();

async function refreshStatus(signal: AbortSignal) {
  try {
    const [status, tables] = await Promise.all([fetchStatus({ signal }), fetchTables({ signal })]);
    if (signal.aborted) return;
    appStore.setStatus(status);
    appStore.setTables(tables);
  } catch (error) {
    if (signal.aborted) return;
    reportError('AppLayout.refreshStatus', error);
    appStore.setDisconnected();
  }
}

const { start } = usePolling(refreshStatus, 15_000);

onMounted(() => {
  start();
});
</script>

<template>
  <div
    class="layout-wrapper"
    :class="{
      'layout-static-inactive': staticMenuDesktopInactive,
    }"
  >
    <AppSidebar :class="{ active: sidebarVisible }" />
    <button
      v-if="sidebarVisible"
      class="layout-mask"
      type="button"
      aria-label="Close navigation menu"
      @click="hideSidebar"
    ></button>
    <div class="layout-main-container">
      <AppTopbar />
      <div class="layout-main">
        <RouterView />
      </div>
      <AppFooter />
    </div>
  </div>
</template>
