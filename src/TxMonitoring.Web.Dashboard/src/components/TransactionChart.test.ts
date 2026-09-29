import { describe, expect, it } from 'vitest'
import { shallowMount } from '@vue/test-utils'
import type Highcharts from 'highcharts'
import { Chart } from 'highcharts-vue'
import TransactionChart from './TransactionChart.vue'
import { makeEvent } from '../test-fixtures'

describe('TransactionChart', () => {
  it('builds one datetime series per account from event time', () => {
    const wrapper = shallowMount(TransactionChart, {
      props: {
        data: [
          makeEvent({ messageId: 'e1', accountId: 'acct-1', amountMinor: 100 }),
          makeEvent({ messageId: 'e2', accountId: 'acct-2', amountMinor: 200 }),
          makeEvent({ messageId: 'e3', accountId: 'acct-1', amountMinor: 300 }),
        ],
      },
    })
    const options = wrapper.findComponent(Chart).props('options') as Highcharts.Options
    expect(options.title?.text).toBe('Live Transaction Amounts (minor units)')
    expect(options.xAxis).toMatchObject({ type: 'datetime' })
    const series = (options.series ?? []) as Highcharts.SeriesLineOptions[]
    expect(series.map((s) => s.name).sort()).toEqual(['acct-1', 'acct-2'])
    const acct1 = series.find((s) => s.name === 'acct-1')
    expect(acct1?.data).toHaveLength(2)
    // Points chart against occurredAt, not receivedAt.
    expect((acct1?.data as [number, number][])[0]?.[0]).toBe(
      Date.parse('2026-09-15T10:00:00.000Z'),
    )
  })

  it('renders no series for an empty feed', () => {
    const wrapper = shallowMount(TransactionChart, { props: { data: [] } })
    const options = wrapper.findComponent(Chart).props('options') as Highcharts.Options
    expect(options.series).toEqual([])
  })
})
