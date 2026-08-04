<script setup lang="ts">
import AppMenu from './AppMenu.vue';
import { useLayout } from './composables/layout';

const { onSidebarClick } = useLayout();
</script>

<template>
  <div class="layout-sidebar bg-surface-900 text-white" @click="onSidebarClick">
    <div class="flex items-center justify-center py-6 px-4">
      <div class="flex items-center gap-2">
        <i class="pi pi-database text-primary-400 text-2xl"></i>
        <span class="text-xl font-bold text-white">TeoDB</span>
      </div>
    </div>
    <AppMenu />
  </div>
</template>
