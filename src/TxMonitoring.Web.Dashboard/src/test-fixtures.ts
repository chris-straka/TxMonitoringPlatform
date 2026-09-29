import type { RiskAlert, TransactionEvent } from './types/transactions'

export function makeEvent(overrides: Partial<TransactionEvent> = {}): TransactionEvent {
  return {
    schemaVersion: 1,
    messageId: 'evt-1',
    accountId: 'acct-1',
    sequenceNumber: 1,
    occurredAt: '2026-09-15T10:00:00.000Z',
    receivedAt: '2026-09-15T10:00:01.000Z',
    amountMinor: 1_000,
    currency: 'USD',
    ...overrides,
  }
}

export function makeAlert(overrides: Partial<RiskAlert> = {}): RiskAlert {
  return {
    schemaVersion: 1,
    messageId: 'alert-1',
    accountId: 'acct-1',
    sequenceNumber: 1,
    occurredAt: '2026-09-15T10:00:00.000Z',
    receivedAt: '2026-09-15T10:00:01.000Z',
    detectedAt: '2026-09-15T10:00:02.000Z',
    amountMinor: 99_000,
    currency: 'USD',
    riskScore: 87,
    reviewRecommended: true,
    reasons: [
      {
        code: 'amount.absolute_high',
        metric: 'amountMinor',
        observedValue: '99000',
        threshold: '50000',
        points: 60,
      },
    ],
    velocityStatus: 'Available',
    velocity: null,
    detector: {
      isSpike: true,
      rawScore: 0.99,
      pValue: 0.001,
      historyCount: 120,
      detectorVersion: 'v3',
    },
    policyVersion: 'p1',
    policyConfigurationHash: 'abc123',
    ...overrides,
  }
}
