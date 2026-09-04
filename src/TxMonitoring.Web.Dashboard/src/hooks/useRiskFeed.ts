import { useEffect, useRef } from 'react'
import { HubConnectionBuilder, HttpTransportType, LogLevel } from '@microsoft/signalr'
import type { TransactionEvent, RiskAlert } from '../types/transactions'

export function useRiskFeed(
    onEvent?: (data: TransactionEvent) => void,
    onAlert?: (data: RiskAlert) => void,
) {
    const eventRef = useRef(onEvent)
    const alertRef = useRef(onAlert)

    // Update box every render so SignalR listener always pulls latest callbacks
    useEffect(() => {
        eventRef.current = onEvent
        alertRef.current = onAlert
    })

    useEffect(() => {
        // Production routes the static site and API through one origin. Development can still
        // override it because Vite and the API normally use different localhost ports.
        const apiUrl = (import.meta.env.VITE_API_URL || window.location.origin).replace(/\/$/, '')

        const connection = new HubConnectionBuilder()
            .withUrl(`${apiUrl}/riskHub`, {
                transport: HttpTransportType.WebSockets,
                skipNegotiation: true,
            })
            .configureLogging(LogLevel.Information)
            .withAutomaticReconnect({
                // The built-in default stops after four tries. An ingestion outage can be
                // much longer, so cap the delay without ever giving up on an open dashboard.
                nextRetryDelayInMilliseconds: ({ previousRetryCount }) =>
                    Math.min(1_000 * (2 ** Math.min(previousRetryCount, 5)), 30_000),
            })
            .build()

        let disposed = false
        let retryTimer: number | undefined
        let retryDelayMs = 1_000

        connection.on('transaction_events', (payload: string) => {
            const parsed = parse(payload, isTransactionEvent)
            if (parsed) eventRef.current?.(parsed)
        })

        connection.on('risk_alerts', (payload: string) => {
            const parsed = parse(payload, isRiskAlert)
            if (parsed) alertRef.current?.(parsed)
        })

        // Automatic reconnect starts only after one successful connection. This loop covers the
        // common Compose race where the browser loads before Web.Api has finished starting.
        const start = async () => {
            try {
                await connection.start()
                retryDelayMs = 1_000
            } catch (error) {
                if (disposed) return
                console.error('SignalR initial connection failed; retrying', error)
                retryTimer = window.setTimeout(() => { void start() }, retryDelayMs)
                retryDelayMs = Math.min(retryDelayMs * 2, 30_000)
            }
        }

        void start()

        return () => {
            disposed = true
            if (retryTimer !== undefined) window.clearTimeout(retryTimer)
            void connection.stop()
        }
    }, []) // Empty array = One connection for life of component
}

// A throw inside a SignalR handler kills the whole connection, so one bad payload
// would take the dashboard down until it reconnects.
function parse<T>(payload: string, isExpected: (value: unknown) => value is T): T | null {
    try {
        const value: unknown = JSON.parse(payload)
        if (isExpected(value)) return value
        console.error('Discarding payload with the wrong shape', value)
        return null
    } catch {
        console.error('Discarding unreadable payload', payload)
        return null
    }
}

function isTransactionEvent(value: unknown): value is TransactionEvent {
    if (!isRecord(value)) return false
    // A PascalCase MessageId means a pre-cutover sender; surface the mismatch instead
    // of silently accepting a half-parsed contract.
    if ('MessageId' in value && !('messageId' in value)) {
        console.error('Discarding pre-cutover PascalCase event payload', value)
        return false
    }
    return value.schemaVersion === 1
        && typeof value.messageId === 'string'
        && value.messageId.length > 0
        && typeof value.accountId === 'string'
        && value.accountId.length > 0
        && isSafeInteger(value.sequenceNumber)
        && (value.sequenceNumber as number) > 0
        && isDateString(value.occurredAt)
        && isDateString(value.receivedAt)
        && isSafeInteger(value.amountMinor)
        && (value.amountMinor as number) > 0
        && typeof value.currency === 'string'
        && value.currency.length > 0
}

function isRiskAlert(value: unknown): value is RiskAlert {
    if (!isRecord(value)) return false
    if ('MessageId' in value && !('messageId' in value)) {
        console.error('Discarding pre-cutover PascalCase alert payload', value)
        return false
    }
    return value.schemaVersion === 1
        && typeof value.messageId === 'string'
        && value.messageId.length > 0
        && typeof value.accountId === 'string'
        && value.accountId.length > 0
        && isSafeInteger(value.sequenceNumber)
        && isDateString(value.occurredAt)
        && isDateString(value.receivedAt)
        && isDateString(value.detectedAt)
        && isSafeInteger(value.amountMinor)
        && typeof value.currency === 'string'
        && typeof value.riskScore === 'number'
        && Number.isFinite(value.riskScore)
        && typeof value.reviewRecommended === 'boolean'
        && Array.isArray(value.reasons)
        && (value.reasons as unknown[]).every(isRiskReason)
        && typeof value.velocityStatus === 'string'
        && (value.velocity === null || isVelocityEvidence(value.velocity))
        && typeof value.policyVersion === 'string'
        && typeof value.policyConfigurationHash === 'string'
}

function isRiskReason(value: unknown): value is object {
    if (!isRecord(value)) return false
    return typeof value.code === 'string'
        && typeof value.metric === 'string'
        && typeof value.observedValue === 'string'
        && typeof value.threshold === 'string'
        && isSafeInteger(value.points)
}

function isVelocityEvidence(value: unknown): boolean {
    if (!isRecord(value)) return false
    // Aggregates travel as base-10 strings because they can exceed the safe integer.
    return typeof value.count1m === 'string'
        && /^\d+$/.test(value.count1m)
        && typeof value.amount5mMinor === 'string'
        && /^\d+$/.test(value.amount5mMinor)
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null
}

function isSafeInteger(value: unknown): value is number {
    return typeof value === 'number' && Number.isSafeInteger(value)
}

function isDateString(value: unknown): value is string {
    return typeof value === 'string' && !Number.isNaN(Date.parse(value))
}
