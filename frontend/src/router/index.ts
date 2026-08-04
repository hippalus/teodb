import { createRouter, createWebHistory } from 'vue-router';

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      component: () => import('@/layout/AppLayout.vue'),
      children: [
        {
          path: '',
          name: 'dashboard',
          component: () => import('@/views/DashboardView.vue'),
        },
        {
          path: 'tables',
          name: 'tables',
          component: () => import('@/views/TablesView.vue'),
        },
        {
          path: 'tables/:namespace/:name',
          name: 'table-detail',
          component: () => import('@/views/TableDetailView.vue'),
          props: true,
        },
        {
          path: 'sql',
          name: 'sql-workspace',
          component: () => import('@/views/SqlWorkspaceView.vue'),
        },
        {
          path: 'cluster',
          name: 'cluster',
          component: () => import('@/views/ClusterView.vue'),
        },
        {
          path: 'ui/metrics',
          name: 'metrics',
          component: () => import('@/views/MetricsView.vue'),
        },
        {
          path: 'storage',
          name: 'storage',
          component: () => import('@/views/StorageView.vue'),
        },
      ],
    },
  ],
});

export default router;
