<script setup lang="ts">
import { ref } from 'vue';
import AppMenuItem from './AppMenuItem.vue';

interface MenuItem {
  label: string;
  icon: string;
  to?: string;
  items?: MenuItem[];
  separator?: boolean;
}

const menuItems = ref<MenuItem[]>([
  {
    label: 'Dashboard',
    icon: 'pi pi-home',
    to: '/',
  },
  {
    label: 'Tables',
    icon: 'pi pi-table',
    to: '/tables',
  },
  {
    label: 'SQL Workspace',
    icon: 'pi pi-code',
    to: '/sql',
  },
  {
    label: 'Cluster',
    icon: 'pi pi-server',
    to: '/cluster',
  },
  {
    label: 'Metrics',
    icon: 'pi pi-chart-bar',
    to: '/ui/metrics',
  },
  {
    label: 'Storage',
    icon: 'pi pi-box',
    to: '/storage',
  },
]);
</script>

<template>
  <nav class="px-3 py-2">
    <ul class="list-none p-0 m-0">
      <AppMenuItem v-for="item in menuItems" :key="item.label" :item="item" />
    </ul>
  </nav>
</template>
