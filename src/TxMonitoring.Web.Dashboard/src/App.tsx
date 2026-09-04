import { useState, useCallback, useRef } from 'react'
import type { TransactionEvent, RiskAlert } from './types/transactions'
import { useRiskFeed } from './hooks/useRiskFeed'
import { TransactionChart } from './features/dashboard/TransactionChart'
import { RiskAlertsList } from './features/dashboard/RiskAlertsList'
import './App.css'

function App() {
  // For Kafka
  const [events, setEvents] = useState<TransactionEvent[]>([])
  const [alerts, setAlerts] = useState<RiskAlert[]>([])
  const seenEventIds = useRef(new Set<string>())
  const eventIdOrder = useRef<string[]>([])
  const seenAlertIds = useRef(new Set<string>())
  const alertIdOrder = useRef<string[]>([])

  const handleEvent = useCallback((event: TransactionEvent) => {
    if (!remember(event.messageId, seenEventIds.current, eventIdOrder.current, 2_000)) return
    // Shared across every account, so this is ~50 points each at 4 accounts.
    setEvents((prev) => [...prev.slice(-199), event])
  }, [])

  // Newest first without re-sorting: an older duplicate that arrives late must not jump
  // the queue ahead of reviews the operator already saw.
  const handleAlert = useCallback((alert: RiskAlert) => {
    if (!remember(alert.messageId, seenAlertIds.current, alertIdOrder.current, 50)) return
    setAlerts((prev) => [alert, ...prev.slice(0, 49)]) // Keep top 50
  }, [])

  // Kafka API updates happen here
  useRiskFeed(handleEvent, handleAlert)

  return (
    <div className="dashboard">
      <h1>Transaction Risk Monitor</h1>
      <p className="banner">
        Synthetic data for monitoring demonstration. This dashboard observes a generated
        feed; it moves no money and approves nothing.
      </p>
      <div className="grid">
        <section>
          <TransactionChart data={events} />
        </section>
        <aside>
          <RiskAlertsList alerts={alerts} />
        </aside>
      </div>
    </div>
  )
}

export default App

// Kafka and the outbox are both at-least-once. Keep a bounded identity window so an ambiguous
// ACK can replay a message without drawing the same point or alert twice in the live UI.
function remember(id: string, seen: Set<string>, order: string[], capacity: number): boolean {
  if (!id || seen.has(id)) return false

  seen.add(id)
  order.push(id)

  if (order.length > capacity) {
    const expired = order.shift()
    if (expired !== undefined) seen.delete(expired)
  }

  return true
}
