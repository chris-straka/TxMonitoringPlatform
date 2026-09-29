import { beforeEach, describe, expect, it } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { MAX_ALERTS, MAX_EVENTS, remember, useMonitoringStore } from './monitoring'
import { makeAlert, makeEvent } from '../test-fixtures'

describe('remember', () => {
  it('rejects empty and duplicate ids', () => {
    const seen = new Set<string>()
    const order: string[] = []
    expect(remember('', seen, order, 10)).toBe(false)
    expect(remember('a', seen, order, 10)).toBe(true)
    expect(remember('a', seen, order, 10)).toBe(false)
  })

  it('evicts the oldest id once over capacity', () => {
    const seen = new Set<string>()
    const order: string[] = []
    expect(remember('a', seen, order, 2)).toBe(true)
    expect(remember('b', seen, order, 2)).toBe(true)
    // Over capacity: 'c' arrives and 'a' expires, so the window holds {b, c}.
    expect(remember('c', seen, order, 2)).toBe(true)
    expect(seen.has('a')).toBe(false)
    expect(remember('b', seen, order, 2)).toBe(false)
    expect(remember('c', seen, order, 2)).toBe(false)
    // An expired id replays as new and evicts the then-oldest ('b').
    expect(remember('a', seen, order, 2)).toBe(true)
    expect(seen.has('b')).toBe(false)
    expect(remember('c', seen, order, 2)).toBe(false)
  })
})

describe('useMonitoringStore', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
  })

  it('ignores duplicate events from at-least-once redelivery', () => {
    const store = useMonitoringStore()
    store.pushEvent(makeEvent())
    store.pushEvent(makeEvent())
    expect(store.events).toHaveLength(1)
  })

  it('caps events at MAX_EVENTS, keeping the newest', () => {
    const store = useMonitoringStore()
    for (let i = 0; i < MAX_EVENTS + 10; i++) {
      store.pushEvent(makeEvent({ messageId: `evt-${i}`, sequenceNumber: i + 1 }))
    }
    expect(store.events).toHaveLength(MAX_EVENTS)
    expect(store.events.at(-1)?.messageId).toBe(`evt-${MAX_EVENTS + 9}`)
  })

  it('keeps alerts newest-first and caps at MAX_ALERTS', () => {
    const store = useMonitoringStore()
    for (let i = 0; i < MAX_ALERTS + 5; i++) {
      store.pushAlert(makeAlert({ messageId: `alert-${i}` }))
    }
    expect(store.alerts).toHaveLength(MAX_ALERTS)
    expect(store.alerts[0]?.messageId).toBe(`alert-${MAX_ALERTS + 4}`)
  })

  it('drops late duplicate alerts without reordering the queue', () => {
    const store = useMonitoringStore()
    store.pushAlert(makeAlert({ messageId: 'alert-1' }))
    store.pushAlert(makeAlert({ messageId: 'alert-2' }))
    store.pushAlert(makeAlert({ messageId: 'alert-1' }))
    expect(store.alerts.map((a) => a.messageId)).toEqual(['alert-2', 'alert-1'])
  })

  it('clear resets state and the identity window', () => {
    const store = useMonitoringStore()
    store.pushEvent(makeEvent())
    store.pushAlert(makeAlert())
    store.clear()
    expect(store.events).toHaveLength(0)
    expect(store.alerts).toHaveLength(0)
    store.pushEvent(makeEvent())
    expect(store.events).toHaveLength(1)
  })
})
