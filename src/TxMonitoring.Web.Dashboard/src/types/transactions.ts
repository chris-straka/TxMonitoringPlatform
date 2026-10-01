// Mirrors the TransactionEvent the ingestion API produces to transaction-events, with
// camel-case wire names.
export interface TransactionEvent {
  schemaVersion: number
  messageId: string
  accountId: string
  // Monotonic per account within one generator run. A JSON number because the contract keeps
  // it below 2^53. Validate before use.
  sequenceNumber: number
  // Origin clock. Chart against this, not receivedAt.
  occurredAt: string
  // Ingestion clock.
  receivedAt: string
  // Integer minor units, bounded below 2^53.
  amountMinor: number
  currency: string
}

export interface RiskReason {
  code: string
  metric: string
  // Base-10 strings because aggregates can exceed the safe integer range.
  observedValue: string
  threshold: string
  points: number
}

// Mirrors the RiskAlert the detection worker produces to risk-alerts.
export interface RiskAlert {
  schemaVersion: number
  messageId: string
  accountId: string
  sequenceNumber: number
  occurredAt: string
  receivedAt: string
  detectedAt: string
  amountMinor: number
  currency: string
  riskScore: number
  reviewRecommended: boolean
  reasons: RiskReason[]
  velocityStatus: 'Available' | 'Unavailable' | 'OutOfOrder' | 'Rebuilding'
  velocity: { count1m: string; amount5mMinor: string } | null
  detector: {
    isSpike: boolean
    rawScore: number
    pValue: number
    historyCount: number
    detectorVersion: string
  }
  policyVersion: string
  policyConfigurationHash: string
}

/**
 * Fixed phrases for reason codes. The server sends only codes, and an unmapped code is
 * shown as unknown rather than hidden.
 */
export const REASON_PHRASES: Record<string, string> = {
  'amount.absolute_high': 'Amount above the high-value bound',
  'amount.account_spike': 'Unusual amount for this account',
  'velocity.count_1m': 'Too many transactions in one minute',
  'velocity.amount_5m': 'Too much money in five minutes',
}

export function reasonPhrase(code: string): string {
  return REASON_PHRASES[code] ?? `Unknown reason (${code})`
}

/** Formats integer minor units with the currency code. */
export function formatAmount(amountMinor: number, currency: string): string {
  return `${amountMinor.toLocaleString('en-US')} ${currency} minor`
}
