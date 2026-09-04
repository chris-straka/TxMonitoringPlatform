import { Chart, Title, XAxis, YAxis } from '@highcharts/react'
import { LineSeries } from '@highcharts/react/series/Line'
import type { TransactionEvent } from '../../types/transactions'

interface Props {
  data: TransactionEvent[]
}

export function TransactionChart({ data }: Props) {
  // One series per account. A single series over a mixed feed draws a line that
  // jumps between accounts, which looks like activity no account produced.
  const byAccount = new Map<string, [number, number][]>()

  for (const event of data) {
    const points = byAccount.get(event.accountId) ?? []
    points.push([Date.parse(event.occurredAt), event.amountMinor])
    byAccount.set(event.accountId, points)
  }

  return (
    <Chart>
      <Title>Live Transaction Amounts (minor units)</Title>
      {/* Event time, so transactions drained after an outage land where they happened
          rather than bunching up at the moment the buffer flushed. */}
      <XAxis type="datetime" />
      <YAxis>Amount (minor units)</YAxis>
      {[...byAccount].map(([accountId, points]) => (
        <LineSeries key={accountId} name={accountId} data={points} />
      ))}
    </Chart>
  )
}
