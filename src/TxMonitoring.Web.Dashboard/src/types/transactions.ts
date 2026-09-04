// Mirrors the TransactionEvent the ingestion API produces to transaction-events
// (camel-case wire names, shared serializer configuration on the server).
export interface TransactionEvent {
  schemaVersion: number
  messageId: string
  accountId: string
  // Monotonic per account within one generator run. Stays a JSON number because the
  // contract bounds it below 2^53; validate before use.
  sequenceNumber: number
  // Origin clock. Chart against this, not receivedAt.
  occurredAt: string
  // Ingestion clock.
  receivedAt: string
  // Integer minor units. Stays a JSON number for the same safe-integer reason.
  amountMinor: number
  currency: string
}

export interface RiskReason {
  code: string
  metric: string
  // Base-10 strings: aggregates can exceed JavaScript's safe integer.
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

/// Deterministic human phrases for reason codes. The server never sends free-form
/// advice; the UI maps codes so a new rule without a phrase is visible, not silent.
export const REASON_PHRASES: Record<string, string> = {
  'amount.absolute_high': 'Amount above the high-value bound',
  'amount.account_spike': 'Unusual amount for this account',
  'velocity.count_1m': 'Too many transactions in one minute',
  'velocity.amount_5m': 'Too much money in five minutes',
}

export function reasonPhrase(code: string): string {
  return REASON_PHRASES[code] ?? `Unknown reason (${code})`
}

/// Minor units formatted with the currency code. Amounts stay integers until display.
export function formatAmount(amountMinor: number, currency: string): string {
  return `${amountMinor.toLocaleString('en-US')} ${currency} minor`
}
