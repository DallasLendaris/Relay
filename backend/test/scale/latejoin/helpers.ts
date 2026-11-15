import { randomUUID } from 'node:crypto';
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../../src/adapters/memory/alert-store.js';
import { MemoryCheckInStore } from '../../../src/adapters/memory/checkin-store.js';
import { MemoryDeviceStore } from '../../../src/adapters/memory/device-store.js';
import { MemoryIdempotencyStore } from '../../../src/adapters/memory/idempotency-store.js';
import { MemoryPlaceStore } from '../../../src/adapters/memory/place-store.js';
import { createApi, type ApiEvent } from '../../../src/api/index.js';
import type { Logger } from '../../../src/delivery/types.js';
import { cellForPoint } from '../../../src/geo/index.js';
import { MemoryCellIndex } from '../../../src/scale/cell-index/index.js';
import type { CellIndex } from '../../../src/scale/contracts.js';
import { MemorySendQueue } from '../../../src/scale/fanout/queues.js';
import {
  MemoryLateJoinQueue,
  processLateJoin,
  type LateJoinDeps,
  type LateJoinResult,
} from '../../../src/scale/latejoin/index.js';
import { createCheckInToken } from '../../../src/tokens/index.js';

export const START = new Date('2026-11-09T12:00:00Z');
export const TOKEN_KEY = { id: 't1', secret: 'token-secret-not-real' };
export const CREDENTIAL_KEY = { id: 'c1', secret: Buffer.alloc(32, 6).toString('base64') };
export const APNS = 'ab12'.repeat(16);
export const SJ = { label: 'Home', latitude: 37.3352, longitude: -121.8811 };
export const SJ_CELL = cellForPoint(SJ.latitude, SJ.longitude);

export function makeAlert(over: Partial<Alert> = {}, cells: string[] = [SJ_CELL]): Alert {
  return {
    id: randomUUID(),
    source: 'nws',
    sourceId: `urn:${randomUUID()}`,
    event: 'Flash Flood Warning',
    eventCode: 'FFW',
    relaySeverity: 'warning',
    capSeverity: 'Severe',
    urgency: 'Immediate',
    certainty: 'Likely',
    status: 'Actual',
    messageType: 'Alert',
    state: 'active',
    references: [],
    area: { description: 'Test', geometry: null, ugcCodes: [], sameCodes: [], h3Cells: cells },
    sent: '2026-11-09T11:00:00Z',
    effective: '2026-11-09T11:00:00Z',
    onset: null,
    expires: '2026-11-09T18:00:00Z',
    ends: null,
    official: {
      headline: 'h',
      description: 'd',
      instruction: null,
      senderName: 'NWS',
      language: 'en',
    },
    summaries: [],
    signature: 'v1.k1.sig',
    ...over,
  };
}

export function captureLogger(): { logger: Logger; lines: string[] } {
  const lines: string[] = [];
  const at =
    (level: string) =>
    (msg: string, fields?: Record<string, unknown>): void => {
      lines.push(JSON.stringify({ level, msg, ...fields }));
    };
  return { logger: { info: at('info'), warn: at('warn'), error: at('error') }, lines };
}

/**
 * The whole path in memory: the API (which only enqueues intents), the late-join core (which
 * does the sending) and the check-in route, all on shared stores and one movable clock.
 */
export function harness(
  opts: {
    verifyAlert?: (a: Alert) => boolean;
    paused?: boolean;
    /** The index the API writes through to (default: a fresh in-memory one). */
    cellIndex?: CellIndex;
  } = {},
) {
  const time = { now: START };
  const clock = { now: () => time.now };
  const devices = new MemoryDeviceStore(clock);
  const places = new MemoryPlaceStore();
  const alerts = new InMemoryAlertStore();
  const checkins = new MemoryCheckInStore();
  const idempotency = new MemoryIdempotencyStore(clock);
  const sendQueue = new MemorySendQueue();
  const intents = new MemoryLateJoinQueue();
  const cellIndex = opts.cellIndex ?? new MemoryCellIndex();
  const { logger, lines } = captureLogger();
  const control = { paused: opts.paused ?? false, enqueueFails: false };

  const deps: LateJoinDeps = {
    devices,
    places,
    alerts,
    verifyAlert: opts.verifyAlert ?? (() => true),
    idempotency,
    sendQueue: {
      enqueue: async (queue, batches, o) => {
        if (control.enqueueFails) throw new Error('sqs down');
        await sendQueue.enqueue(queue, batches, o);
      },
    },
    makeCheckInToken: (alertId, deviceId, h3Cell, expiresAt) =>
      createCheckInToken({ alertId, deviceId, h3Cell, expiresAt }, TOKEN_KEY),
    isDeliveryPaused: async () => control.paused,
    clock,
    logger,
  };

  const route = createApi({
    devices,
    places,
    alerts,
    geocoder: { geocode: async () => null },
    checkins,
    cellIndex,
    lateJoin: { queue: intents },
    tokenKeys: new Map([[TOKEN_KEY.id, TOKEN_KEY.secret]]),
    deviceCredentialKeys: new Map([[CREDENTIAL_KEY.id, CREDENTIAL_KEY.secret]]),
    deviceCredentialKey: CREDENTIAL_KEY,
    clock,
    newId: randomUUID,
  });

  async function call(method: string, path: string, body?: unknown, credential?: string) {
    const event: ApiEvent = {
      rawPath: path,
      headers: credential ? { authorization: `Bearer ${credential}` } : {},
      body: body === undefined ? null : JSON.stringify(body),
      requestContext: { http: { method } },
    };
    const res = await route(event);
    return {
      status: res.statusCode,
      body: res.body ? (JSON.parse(res.body) as Record<string, any>) : null,
    };
  }

  /** Runs every queued intent through the late-join core, as the Lambda would. */
  async function drain(): Promise<LateJoinResult[]> {
    const results: LateJoinResult[] = [];
    while (intents.intents.length > 0) {
      const intent = intents.intents.shift();
      if (intent) results.push(await processLateJoin(intent, deps));
    }
    return results;
  }

  const register = async () =>
    (await call('POST', '/devices', {})).body as { deviceId: string; credential: string };
  const setToken = (credential: string, token = APNS) =>
    call('PUT', '/devices/me/push-token', { platform: 'ios', token, language: 'en' }, credential);
  const addPlace = (credential: string, spot: object = SJ) =>
    call('POST', '/places', spot, credential);

  return {
    time,
    clock,
    devices,
    places,
    alerts,
    checkins,
    idempotency,
    sendQueue,
    intents,
    cellIndex,
    deps,
    control,
    lines,
    route,
    call,
    drain,
    register,
    setToken,
    addPlace,
  };
}
