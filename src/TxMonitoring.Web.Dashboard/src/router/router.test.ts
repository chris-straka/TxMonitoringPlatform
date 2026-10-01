import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import App from '../App.vue'
import { routes } from './index'
import { useMonitoringStore } from '../stores/monitoring'
import { makeAlert } from '../test-fixtures'

// The app shell opens the live SignalR feed; router-flow tests stub the socket
// and assert on navigation instead.
vi.mock('../composables/useRiskFeed', () => ({
  useRiskFeed: vi.fn(),
}))

async function mountApp(initialRoute: string) {
  const router = createRouter({ history: createMemoryHistory(), routes })
  await router.push(initialRoute)
  const wrapper = mount(App, {
    global: {
      plugins: [createPinia(), router],
      // Highcharts needs a real SVG host, so stub the chart.
      stubs: { Chart: true },
    },
  })
  return { router, wrapper }
}

describe('router', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
  })

  it('resolves the dashboard, alerts, and not-found routes', async () => {
    const router = createRouter({ history: createMemoryHistory(), routes })
    await router.push('/')
    expect(router.currentRoute.value.name).toBe('dashboard')
    await router.push('/alerts')
    expect(router.currentRoute.value.name).toBe('alerts')
    await router.push('/no/such/page')
    expect(router.currentRoute.value.name).toBe('not-found')
  })

  it('renders the dashboard shell with navigation links', async () => {
    const { router, wrapper } = await mountApp('/')
    await router.isReady()
    expect(wrapper.find('h1').text()).toBe('Transaction Risk Monitor')
    expect(wrapper.find('.banner').text()).toContain('moves no money')
    const links = wrapper.findAll('.tabs a')
    expect(links.map((a) => a.attributes('href'))).toEqual(['/', '/alerts'])
  })

  it('flows from the live dashboard to the review queue', async () => {
    const { router, wrapper } = await mountApp('/')
    await router.isReady()
    expect(wrapper.find('table.alerts').exists()).toBe(true)

    await router.push('/alerts')
    expect(wrapper.find('h2').text()).toBe('Review queue (0)')

    const store = useMonitoringStore()
    store.pushAlert(makeAlert())
    await wrapper.vm.$nextTick()
    expect(wrapper.find('h2').text()).toBe('Review queue (1)')
    expect(wrapper.find('tbody tr').text()).toContain('acct-1')
  })

  it('renders the not-found view for unknown routes', async () => {
    const { router, wrapper } = await mountApp('/does/not/exist')
    await router.isReady()
    expect(router.currentRoute.value.name).toBe('not-found')
    expect(wrapper.find('h2').text()).toBe('Page not found')
  })
})
