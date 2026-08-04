<script setup lang="ts">
import Dialog from 'primevue/dialog';

defineProps<{
  visible: boolean;
}>();

const emit = defineEmits<{
  'update:visible': [value: boolean];
}>();

interface Shortcut {
  keys: string;
  description: string;
}

const shortcuts: Shortcut[] = [
  { keys: 'Ctrl/Cmd + Enter', description: 'Execute SQL query' },
  { keys: 'Ctrl/Cmd + Shift + E', description: 'Explain query plan' },
  { keys: 'Ctrl/Cmd + S', description: 'Save query to history' },
  { keys: 'Ctrl/Cmd + L', description: 'Clear editor' },
  { keys: 'Tab', description: 'Accept autocomplete suggestion' },
  { keys: 'Escape', description: 'Close dialogs' },
];
</script>

<template>
  <Dialog
    :visible="visible"
    header="Keyboard Shortcuts"
    :modal="true"
    :style="{ width: '450px' }"
    @update:visible="emit('update:visible', $event)"
  >
    <div class="flex flex-col gap-3">
      <div
        v-for="shortcut in shortcuts"
        :key="shortcut.keys"
        class="flex items-center justify-between py-2 border-b border-surface-200 last:border-0"
      >
        <span class="text-surface-600">{{ shortcut.description }}</span>
        <kbd class="px-2 py-1 bg-surface-100 rounded text-sm font-mono text-surface-700 border border-surface-300">
          {{ shortcut.keys }}
        </kbd>
      </div>
    </div>
  </Dialog>
</template>
