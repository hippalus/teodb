<script setup lang="ts">
import { RouterView } from 'vue-router';
import Toast from 'primevue/toast';
import ConfirmDialog from 'primevue/confirmdialog';
import { useAppStore } from '@/stores/app';

// Apply the saved/preferred color scheme before first paint of the views.
useAppStore().initDarkMode();
</script>

<template>
  <Toast position="top-right" />
  <ConfirmDialog />
  <RouterView />
</template>
