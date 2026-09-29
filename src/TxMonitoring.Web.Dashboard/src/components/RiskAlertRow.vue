<script setup lang="ts">
import { useDateFormat } from '@vueuse/core'
import type { RiskAlert } from '../types/transactions'
import { formatAmount, reasonPhrase } from '../types/transactions'

const props = defineProps<{
  alert: RiskAlert
}>()

const detectedAt = useDateFormat(() => props.alert.detectedAt, 'HH:mm:ss')
const why = `${props.alert.reasons.map((reason) => reasonPhrase(reason.code)).join('; ')}${props.alert.velocityStatus !== 'Available' ? ` (velocity ${props.alert.velocityStatus})` : ''}`
</script>

<template>
  <tr>
    <td>{{ detectedAt }}</td>
    <td>{{ alert.accountId }}</td>
    <td>{{ formatAmount(alert.amountMinor, alert.currency) }}</td>
    <td>{{ alert.riskScore }}</td>
    <td>{{ why }}</td>
  </tr>
</template>
