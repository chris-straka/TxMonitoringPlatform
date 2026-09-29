import { describe, expect, it } from 'vitest'
import { formatAmount, reasonPhrase } from './transactions'

describe('reasonPhrase', () => {
  it('maps known reason codes to human phrases', () => {
    expect(reasonPhrase('amount.absolute_high')).toBe('Amount above the high-value bound')
    expect(reasonPhrase('amount.account_spike')).toBe('Unusual amount for this account')
    expect(reasonPhrase('velocity.count_1m')).toBe('Too many transactions in one minute')
    expect(reasonPhrase('velocity.amount_5m')).toBe('Too much money in five minutes')
  })

  it('surfaces unknown codes instead of staying silent', () => {
    expect(reasonPhrase('rules.something_new')).toBe('Unknown reason (rules.something_new)')
  })
})

describe('formatAmount', () => {
  it('keeps integer minor units until display', () => {
    expect(formatAmount(12_345, 'USD')).toBe('12,345 USD minor')
  })
})
