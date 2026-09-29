<script setup lang="ts">
import { storeToRefs } from 'pinia'
import { useMonitoringStore } from '../stores/monitoring'
import RiskAlertsList from '../components/RiskAlertsList.vue'

const store = useMonitoringStore()
const { alerts } = storeToRefs(store)
</script>

<template>
  <section>
    <h2>Review queue ({{ alerts.length }})</h2>
    <RiskAlertsList :alerts="alerts" />
  </section>
</template>
