import type { RiskAlert } from '../../types/transactions'
import { formatAmount, reasonPhrase } from '../../types/transactions'

interface Props {
  alerts: RiskAlert[]
}

export function RiskAlertsList({ alerts }: Props) {
  return (
    <table className="alerts">
      <caption>Transactions Recommended for Review</caption>
      <thead>
        <tr>
          <th>Detected</th>
          <th>Account</th>
          <th>Amount</th>
          <th>Score</th>
          <th>Why</th>
        </tr>
      </thead>
      <tbody>
        {alerts.map((alert) => (
          <tr key={alert.messageId}>
            <td>{new Date(alert.detectedAt).toLocaleTimeString()}</td>
            <td>{alert.accountId}</td>
            <td>{formatAmount(alert.amountMinor, alert.currency)}</td>
            <td>{alert.riskScore}</td>
            <td>
              {alert.reasons.map((reason) => reasonPhrase(reason.code)).join('; ')}
              {alert.velocityStatus !== 'Available' && ` (velocity ${alert.velocityStatus})`}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
