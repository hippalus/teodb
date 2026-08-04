<script setup lang="ts">
import { useLayout } from './composables/layout';
import { useAppStore } from '@/stores/app';
import Button from 'primevue/button';
import AuthTokenButton from '@/components/AuthTokenButton.vue';

const { onMenuToggle } = useLayout();
const appStore = useAppStore();
</script>

<template>
  <div class="layout-topbar flex items-center justify-between px-5 py-3 bg-surface-0 border-b border-surface-200">
    <div class="flex items-center gap-3">
      <Button
        icon="pi pi-bars"
        text
        rounded
        severity="secondary"
        @click="onMenuToggle"
        aria-label="Toggle menu"
      />
      <div class="flex items-center gap-2">
        <i class="pi pi-database text-primary text-xl"></i>
        <span class="text-xl font-bold text-surface-800">TeoDB</span>
        <span class="text-sm text-surface-500">Admin</span>
      </div>
    </div>

    <div class="flex items-center gap-4">
      <div class="flex items-center gap-2">
        <span
          class="inline-block w-2.5 h-2.5 rounded-full"
          :class="appStore.isConnected ? 'bg-green-500' : 'bg-red-500'"
        ></span>
        <span class="text-sm text-surface-600">
          {{ appStore.isConnected ? 'Connected' : 'Disconnected' }}
        </span>
      </div>

      <AuthTokenButton />

      <Button
        :icon="appStore.darkMode ? 'pi pi-sun' : 'pi pi-moon'"
        text
        rounded
        severity="secondary"
        @click="appStore.toggleDarkMode"
        aria-label="Toggle dark mode"
      />
    </div>
  </div>
</template>
