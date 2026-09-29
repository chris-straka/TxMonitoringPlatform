<script setup lang="ts">
import { computed } from 'vue'
import Highcharts from 'highcharts'
import { Chart } from 'highcharts-vue'
import type { TransactionEvent } from '../types/transactions'

const props = defineProps<{
  data: TransactionEvent[]
}>()

// One series per account. A single series over a mixed feed draws a line that
// jumps between accounts, which looks like activity no account produced.
const chartOptions = computed<Highcharts.Options>(() => {
  const byAccount = new Map<string, [number, number][]>()
  for (const event of props.data) {
    const points = byAccount.get(event.accountId) ?? []
    points.push([Date.parse(event.occurredAt), event.amountMinor])
    byAccount.set(event.accountId, points)
  }
  return {
    title: { text: 'Live Transaction Amounts (minor units)' },
    // Event time, so transactions drained after an outage land where they happened
    // rather than bunching up at the moment the buffer flushed.
    xAxis: { type: 'datetime' },
    yAxis: { title: { text: 'Amount (minor units)' } },
    series: [...byAccount].map(
      ([accountId, points]): Highcharts.SeriesLineOptions => ({
        type: 'line',
        name: accountId,
        data: points,
      }),
    ),
  }
})
</script>

<template>
  <Chart :options="chartOptions" />
</template>
