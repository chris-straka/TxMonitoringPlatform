<script setup lang="ts">
import type { RiskAlert } from '../types/transactions'
import RiskAlertRow from './RiskAlertRow.vue'

defineProps<{
  alerts: RiskAlert[]
}>()
</script>

<template>
  <table class="alerts">
    <caption>
      Transactions Recommended for Review
    </caption>
    <thead>
      <tr>
        <th>Detected</th>
        <th>Account</th>
        <th>Amount</th>
        <th>Score</th>
        <th>Why</th>
      </tr>
    </thead>
    <tbody>
      <RiskAlertRow
        v-for="alert in alerts"
        :key="alert.messageId"
        :alert="alert"
      />
    </tbody>
  </table>
</template>
