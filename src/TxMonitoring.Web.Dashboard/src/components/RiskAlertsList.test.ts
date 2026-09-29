import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import RiskAlertsList from './RiskAlertsList.vue'
import { makeAlert } from '../test-fixtures'

describe('RiskAlertsList', () => {
  it('renders one row per alert with human-readable reasons', () => {
    const wrapper = mount(RiskAlertsList, {
      props: {
        alerts: [
          makeAlert(),
          makeAlert({
            messageId: 'alert-2',
            accountId: 'acct-9',
            velocityStatus: 'OutOfOrder',
            reasons: [
              {
                code: 'rules.brand_new',
                metric: 'm',
                observedValue: '1',
                threshold: '2',
                points: 5,
              },
            ],
          }),
        ],
      },
    })
    const rows = wrapper.findAll('tbody tr')
    expect(rows).toHaveLength(2)
    expect(rows[0]?.text()).toContain('acct-1')
    expect(rows[0]?.text()).toContain('99,000 USD minor')
    expect(rows[0]?.text()).toContain('Amount above the high-value bound')
    // Unknown codes stay visible; degraded velocity is annotated.
    expect(rows[1]?.text()).toContain('Unknown reason (rules.brand_new)')
    expect(rows[1]?.text()).toContain('(velocity OutOfOrder)')
  })

  it('renders an empty body when there is nothing to review', () => {
    const wrapper = mount(RiskAlertsList, { props: { alerts: [] } })
    expect(wrapper.find('caption').text()).toBe('Transactions Recommended for Review')
    expect(wrapper.findAll('tbody tr')).toHaveLength(0)
  })
})
