<script setup lang="ts">
import { useRouter, useRoute } from 'vue-router';
import { computed } from 'vue';
import { useLayout } from './composables/layout';

interface MenuItem {
  label: string;
  icon: string;
  to?: string;
  items?: MenuItem[];
}

const props = defineProps<{
  item: MenuItem;
}>();

const router = useRouter();
const route = useRoute();
const { hideSidebar } = useLayout();

const isActive = computed(() => {
  if (!props.item.to) return false;
  if (props.item.to === '/') return route.path === '/';
  return route.path.startsWith(props.item.to);
});

function navigate() {
  if (props.item.to) {
    router.push(props.item.to);
    if (window.innerWidth <= 991) {
      hideSidebar();
    }
  }
}
</script>

<template>
  <li class="mb-1">
    <a
      class="flex items-center gap-3 px-4 py-3 rounded-lg cursor-pointer transition-colors duration-200"
      :class="[
        isActive
          ? 'bg-primary-500/20 text-primary-300'
          : 'text-surface-300 hover:bg-surface-800 hover:text-white',
      ]"
      @click="navigate"
    >
      <i :class="item.icon" class="text-lg"></i>
      <span class="font-medium text-sm">{{ item.label }}</span>
    </a>
    <ul v-if="item.items" class="list-none pl-4 m-0">
      <AppMenuItem v-for="child in item.items" :key="child.label" :item="child" />
    </ul>
  </li>
</template>
