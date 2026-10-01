<script setup lang="ts">
import { useMonitoringStore } from './stores/monitoring'
import { useRiskFeed } from './composables/useRiskFeed'
import './dashboard.css'

const store = useMonitoringStore()

useRiskFeed(store.pushEvent, store.pushAlert)
</script>

<template>
  <div class="dashboard">
    <h1>Transaction Risk Monitor</h1>
    <p class="banner">
      Synthetic data for monitoring demonstration. This dashboard observes a generated feed; it
      moves no money and approves nothing.
    </p>
    <nav class="tabs">
      <RouterLink to="/">
        Live
      </RouterLink>
      <RouterLink to="/alerts">
        Review queue
      </RouterLink>
    </nav>
    <RouterView />
  </div>
</template>

<style scoped>
.tabs {
  display: flex;
  gap: 12px;
  margin: 16px 0;
}
.tabs a {
  text-decoration: none;
  padding: 6px 12px;
  border-radius: 6px;
}
.tabs a.router-link-exact-active {
  font-weight: 600;
  text-decoration: underline;
}
</style>
