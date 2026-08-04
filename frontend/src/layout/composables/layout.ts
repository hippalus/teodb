import { computed, ref } from 'vue';

const sidebarVisible = ref(false);
const staticMenuDesktopInactive = ref(false);
const menuClick = ref(false);
const desktopBreakpoint = 991;

function isDesktop() {
  return typeof window !== 'undefined' && window.innerWidth > desktopBreakpoint;
}

export function useLayout() {
  const isSidebarActive = computed(() => sidebarVisible.value);

  function onMenuToggle() {
    if (isDesktop()) {
      staticMenuDesktopInactive.value = !staticMenuDesktopInactive.value;
    } else {
      sidebarVisible.value = !sidebarVisible.value;
    }
  }

  function onSidebarClick() {
    menuClick.value = true;
  }

  function hideSidebar() {
    sidebarVisible.value = false;
  }

  return {
    sidebarVisible,
    staticMenuDesktopInactive,
    isSidebarActive,
    onMenuToggle,
    onSidebarClick,
    hideSidebar,
  };
}
