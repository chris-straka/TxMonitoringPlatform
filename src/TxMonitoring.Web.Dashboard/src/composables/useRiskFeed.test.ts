import { describe, expect, it } from 'vitest'
import { isRiskAlert, isTransactionEvent, parsePayload } from './useRiskFeed'
import { makeAlert, makeEvent } from '../test-fixtures'

describe('parsePayload', () => {
  it('parses a valid transaction event', () => {
    expect(parsePayload(JSON.stringify(makeEvent()), isTransactionEvent)?.messageId).toBe('evt-1')
  })

  it('parses a valid risk alert', () => {
    expect(parsePayload(JSON.stringify(makeAlert()), isRiskAlert)?.messageId).toBe('alert-1')
  })

  it('discards unreadable payloads instead of throwing', () => {
    expect(parsePayload('not-json{{{', isTransactionEvent)).toBeNull()
  })

  it('discards payloads with the wrong shape', () => {
    expect(parsePayload(JSON.stringify({ hello: 'world' }), isTransactionEvent)).toBeNull()
    // An event is not alert-shaped (no detectedAt, reasons, or policy fields).
    expect(parsePayload(JSON.stringify(makeEvent()), isRiskAlert)).toBeNull()
  })

  it('rejects pre-cutover PascalCase payloads', () => {
    const pascal = { ...makeEvent(), MessageId: 'evt-1' } as unknown as Record<string, unknown>
    delete pascal['messageId']
    expect(parsePayload(JSON.stringify(pascal), isTransactionEvent)).toBeNull()
  })

  it('rejects unsafe integers and bad clocks', () => {
    expect(
      parsePayload(
        JSON.stringify(makeEvent({ sequenceNumber: Number.MAX_SAFE_INTEGER + 1 })),
        isTransactionEvent,
      ),
    ).toBeNull()
    expect(
      parsePayload(JSON.stringify(makeEvent({ occurredAt: 'not-a-date' })), isTransactionEvent),
    ).toBeNull()
    expect(parsePayload(JSON.stringify(makeEvent({ amountMinor: 0 })), isTransactionEvent)).toBeNull()
  })

  it('rejects alerts with malformed velocity evidence', () => {
    expect(
      parsePayload(
        JSON.stringify(makeAlert({ velocity: { count1m: '12.5', amount5mMinor: '9' } })),
        isRiskAlert,
      ),
    ).toBeNull()
    // Base-10 aggregates beyond the safe integer are valid on the wire.
    expect(
      parsePayload(
        JSON.stringify(
          makeAlert({ velocity: { count1m: '7', amount5mMinor: '9007199254740993' } }),
        ),
        isRiskAlert,
      ),
    ).not.toBeNull()
  })
})
