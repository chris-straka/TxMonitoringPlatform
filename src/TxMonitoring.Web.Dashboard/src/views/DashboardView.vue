<script setup lang="ts">
import { storeToRefs } from 'pinia'
import { useMonitoringStore } from '../stores/monitoring'
import TransactionChart from '../components/TransactionChart.vue'
import RiskAlertsList from '../components/RiskAlertsList.vue'

const store = useMonitoringStore()
const { events, alerts } = storeToRefs(store)
</script>

<template>
  <div class="grid">
    <section>
      <TransactionChart :data="events" />
    </section>
    <aside>
      <RiskAlertsList :alerts="alerts" />
    </aside>
  </div>
</template>
