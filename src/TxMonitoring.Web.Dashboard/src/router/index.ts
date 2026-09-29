import { createRouter, createWebHistory, type RouteRecordRaw } from 'vue-router'
import DashboardView from '../views/DashboardView.vue'
import AlertsView from '../views/AlertsView.vue'
import NotFoundView from '../views/NotFoundView.vue'

export const routes: RouteRecordRaw[] = [
  { path: '/', name: 'dashboard', component: DashboardView },
  { path: '/alerts', name: 'alerts', component: AlertsView },
  { path: '/:pathMatch(.*)*', name: 'not-found', component: NotFoundView },
]

export const router = createRouter({
  history: createWebHistory(),
  routes,
})
