import { ref } from 'vue'
import { defineStore } from 'pinia'
import type { RiskAlert, TransactionEvent } from '../types/transactions'

export const MAX_EVENTS = 200
export const MAX_ALERTS = 50

/// Live feed state. Kafka and the outbox are both at-least-once, so both push
/// paths share the same bounded identity window: an ambiguous ACK can replay a
/// message without drawing the same point or alert twice in the live UI.
export const useMonitoringStore = defineStore('monitoring', () => {
  const events = ref<TransactionEvent[]>([])
  const alerts = ref<RiskAlert[]>([])

  const seenEventIds = new Set<string>()
  const eventIdOrder: string[] = []
  const seenAlertIds = new Set<string>()
  const alertIdOrder: string[] = []

  // Shared across every account, so this is ~50 points each at 4 accounts.
  function pushEvent(event: TransactionEvent): void {
    if (!remember(event.messageId, seenEventIds, eventIdOrder, 2_000)) return
    events.value = [...events.value.slice(-(MAX_EVENTS - 1)), event]
  }

  // Newest first without re-sorting: an older duplicate that arrives late must
  // not jump the queue ahead of reviews the operator already saw.
  function pushAlert(alert: RiskAlert): void {
    if (!remember(alert.messageId, seenAlertIds, alertIdOrder, MAX_ALERTS)) return
    alerts.value = [alert, ...alerts.value.slice(0, MAX_ALERTS - 1)]
  }

  function clear(): void {
    events.value = []
    alerts.value = []
    seenEventIds.clear()
    eventIdOrder.length = 0
    seenAlertIds.clear()
    alertIdOrder.length = 0
  }

  return { events, alerts, pushEvent, pushAlert, clear }
})

// Kafka and the outbox are both at-least-once. Keep a bounded identity window so an ambiguous
// ACK can replay a message without drawing the same point or alert twice in the live UI.
export function remember(
  id: string,
  seen: Set<string>,
  order: string[],
  capacity: number,
): boolean {
  if (!id || seen.has(id)) return false

  seen.add(id)
  order.push(id)

  if (order.length > capacity) {
    const expired = order.shift()
    if (expired !== undefined) seen.delete(expired)
  }

  return true
}
