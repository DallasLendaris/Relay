import type { SQSEvent } from 'aws-lambda';
import { randomUUID } from 'node:crypto';
import type { Alert } from '@relay/shared';
import { cellToChildren, cellToParent } from 'h3-js';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { InMemoryAlertStore } from '../../src/adapters/memory/alert-store.js';
import { MemoryCheckInStore } from '../../src/adapters/memory/checkin-store.js';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
import { createApi, type ApiEvent } from '../../src/api/index.js';
import { cellForPoint } from '../../src/geo/index.js';
import { MemoryCellIndex, MemoryIndexRepairQueue } from '../../src/scale/cell-index/index.js';
import { applyRepair, createRepairHandler } from '../../src/scale/cell-index/repair.js';
import type { CellIndex, SendQueue } from '../../src/scale/contracts.js';
import { MemorySendQueue } from '../../src/scale/fanout/queues.js';
import { verifyCheckInToken } from '../../src/tokens/index.js';

const NOW = new Date('2026-11-09T12:00:00Z');
const APNS = 'ab12'.repeat(16);
const KEY = { id: 't1', secret: 'token-secret-not-real' };
const SJ = { label: 'Home', latitude: 37.3352, longitude: -121.8811 };
const SJ_CELL = cellForPoint(SJ.latitude, SJ.longitude);

function makeAlert(over: Partial<Alert> = {}, cells: string[] = [SJ_CELL]): Alert {
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

class FailingIndex extends MemoryCellIndex {
  failPut = false;
  failRemove = false;
  failUpdate = false;
  onPut?: () => Promise<void>;
  override async put(entries: Parameters<CellIndex['put']>[0]) {
    if (this.failPut) throw new Error('dynamo down');
    await super.put(entries);
    await this.onPut?.();
  }
  override async remove(...a: Parameters<CellIndex['remove']>) {
    if (this.failRemove) throw new Error('dynamo down');
    return super.remove(...a);
  }
  override async updateDevice(...a: Parameters<CellIndex['updateDevice']>) {
    if (this.failUpdate) throw new Error('dynamo down');
    return super.updateDevice(...a);
  }
}

let devices: MemoryDeviceStore;
let places: MemoryPlaceStore;
let alerts: InMemoryAlertStore;
let index: FailingIndex;
let repairs: MemoryIndexRepairQueue;
let sendQueue: MemorySendQueue;
let logs: string[];

function build(opts: { lateJoin?: boolean; sendQueue?: SendQueue; repair?: boolean } = {}) {
  return createApi({
    devices,
    places,
    alerts,
    geocoder: { geocode: async () => null },
    checkins: new MemoryCheckInStore(),
    cellIndex: index,
    ...(opts.repair === false ? {} : { repairQueue: repairs }),
    ...(opts.lateJoin === false
      ? {}
      : { lateJoin: { sendQueue: opts.sendQueue ?? sendQueue, tokenKey: KEY } }),
    tokenKeys: new Map([[KEY.id, KEY.secret]]),
    clock: { now: () => NOW },
    newId: randomUUID,
  });
}
let route: ReturnType<typeof build>;

beforeEach(() => {
  logs = [];
  vi.spyOn(console, 'warn').mockImplementation((...a: unknown[]) => void logs.push(a.join(' ')));
  vi.spyOn(console, 'error').mockImplementation((...a: unknown[]) => void logs.push(a.join(' ')));
  devices = new MemoryDeviceStore();
  places = new MemoryPlaceStore();
  alerts = new InMemoryAlertStore();
  index = new FailingIndex();
  repairs = new MemoryIndexRepairQueue();
  sendQueue = new MemorySendQueue();
  route = build();
});
afterEach(() => vi.restoreAllMocks());

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
async function register() {
  return (await call('POST', '/devices', {})).body as { deviceId: string; credential: string };
}
const setToken = (credential: string) =>
  call(
    'PUT',
    '/devices/me/push-token',
    { platform: 'ios', token: APNS, language: 'en' },
    credential,
  );
const addPlace = (credential: string) => call('POST', '/places', SJ, credential);

describe('index repair outbox', () => {
  it('a failed put queues {op:put,h3Cell,deviceId,placeId} and the request still succeeds', async () => {
    const { deviceId, credential } = await register();
    index.failPut = true;
    const res = await addPlace(credential);
    expect(res.status).toBe(201);
    expect(repairs.messages).toEqual([
      { op: 'put', h3Cell: SJ_CELL, deviceId, placeId: res.body?.['id'] },
    ]);
  });

  it('a failed remove queues a remove message', async () => {
    const { deviceId, credential } = await register();
    const place = (await addPlace(credential)).body as { id: string };
    index.failRemove = true;
    expect((await call('DELETE', `/places/${place.id}`, undefined, credential)).status).toBe(204);
    expect(repairs.messages).toEqual([
      { op: 'remove', h3Cell: SJ_CELL, deviceId, placeId: place.id },
    ]);
  });

  it('a failed token sync queues an all-places update for the displaced device', async () => {
    const a = await register();
    const b = await register();
    await setToken(a.credential);
    await addPlace(a.credential);
    index.failUpdate = true;
    expect((await setToken(b.credential)).status).toBe(204);
    // b has no places (nothing to sync); a lost the token and has one entry to clear.
    expect(repairs.messages).toEqual([
      { op: 'update', h3Cell: '', deviceId: a.deviceId, placeId: '' },
    ]);
  });

  it('a repair enqueue that fails too is logged and never fails the request', async () => {
    const { credential } = await register();
    index.failPut = true;
    repairs.enqueue = async () => {
      throw new Error('sqs down');
    };
    expect((await addPlace(credential)).status).toBe(201);
    expect(logs.some((l) => l.includes('cellindex.repair_enqueue_failed'))).toBe(true);
    expect(logs.join('\n')).not.toContain(APNS);
  });

  it('puts the device target read AFTER the place is saved (token registered concurrently)', async () => {
    const { deviceId, credential } = await register();
    // The token arrives after authentication but before/while the place is saved.
    const add = places.add.bind(places);
    places.add = async (p) => {
      await add(p);
      await devices.assignPushTarget(deviceId, { platform: 'ios', token: APNS }, 'es');
    };
    await addPlace(credential);
    const [entry] = (await index.listForCell(SJ_CELL)).entries;
    expect(entry?.target).toEqual({ platform: 'ios', token: APNS });
    expect(entry?.language).toBe('es');
    expect(repairs.messages).toEqual([]); // nothing changed after the read
  });

  it('residual race: a target change after the read but before the put lands is repaired', async () => {
    const { deviceId, credential } = await register();
    index.onPut = async () => {
      index.onPut = undefined;
      await devices.assignPushTarget(deviceId, { platform: 'ios', token: APNS }, 'en');
    };
    const res = await addPlace(credential);
    expect(repairs.messages).toEqual([
      { op: 'update', h3Cell: SJ_CELL, deviceId, placeId: res.body?.['id'] },
    ]);
  });
});

describe('repair Lambda', () => {
  // Only the fields the repair Lambda reads; cast to the full SQS event type.
  const event = (...msgs: unknown[]) =>
    ({
      Records: msgs.map((m, i) => ({
        messageId: `m${i}`,
        body: typeof m === 'string' ? m : JSON.stringify(m),
      })),
    }) as unknown as SQSEvent;
  let deviceId: string;
  let placeId: string;
  beforeEach(async () => {
    const r = await register();
    deviceId = r.deviceId;
    await setToken(r.credential);
    placeId = ((await addPlace(r.credential)).body as { id: string }).id;
  });
  const deps = () => ({ devices, places, cellIndex: index });

  it('put: rebuilds a lost entry from Aurora with the current target, and is idempotent', async () => {
    index = new FailingIndex(); // the entry was lost
    const handle = createRepairHandler(deps());
    const msg = { op: 'put', h3Cell: SJ_CELL, deviceId, placeId };
    expect(await handle(event(msg))).toEqual({ batchItemFailures: [] });
    const first = (await index.listForCell(SJ_CELL)).entries;
    expect(first).toHaveLength(1);
    expect(first[0]).toMatchObject({ deviceId, placeId, target: { platform: 'ios', token: APNS } });
    await handle(event(msg, msg));
    expect((await index.listForCell(SJ_CELL)).entries).toEqual(first);
  });

  it('put for a place deleted since removes the entry instead of resurrecting it', async () => {
    const r = await places.listForDevice(deviceId);
    await places.remove(deviceId, placeId);
    expect(r).toHaveLength(1);
    await createRepairHandler(deps())(event({ op: 'put', h3Cell: SJ_CELL, deviceId, placeId }));
    expect((await index.listForCell(SJ_CELL)).entries).toEqual([]);
  });

  it('remove while Aurora still has the place restores the entry (truth wins)', async () => {
    await index.remove(SJ_CELL, deviceId, placeId);
    await createRepairHandler(deps())(event({ op: 'remove', h3Cell: SJ_CELL, deviceId, placeId }));
    expect((await index.listForCell(SJ_CELL)).entries).toHaveLength(1);
  });

  it('update with empty place id refreshes every place with the new target', async () => {
    await devices.assignPushTarget(deviceId, { platform: 'android', token: 'x'.repeat(120) }, 'es');
    await createRepairHandler(deps())(event({ op: 'update', h3Cell: '', deviceId, placeId: '' }));
    const [entry] = (await index.listForCell(SJ_CELL)).entries;
    expect(entry?.target?.platform).toBe('android');
    expect(entry?.language).toBe('es');
  });

  it('reports only the failing message in batchItemFailures and drops unparseable ones', async () => {
    const flaky = createRepairHandler({
      ...deps(),
      cellIndex: {
        ...index,
        put: async () => {
          throw new Error('dynamo down');
        },
        remove: async () => {},
      } as unknown as CellIndex,
    });
    const res = await flaky(
      event(
        'not json',
        { op: 'put', h3Cell: SJ_CELL, deviceId, placeId },
        { op: 'bogus' },
        { op: 'remove', h3Cell: SJ_CELL, deviceId, placeId: 'gone' },
      ),
    );
    expect(res).toEqual({ batchItemFailures: [{ itemIdentifier: 'm1' }] });
  });

  it('a deleted device only loses the entry the message names', async () => {
    const ghost = randomUUID();
    await index.put([
      { h3Cell: SJ_CELL, deviceId: ghost, placeId: 'p1', target: null, language: 'en' },
    ]);
    await applyRepair(deps(), { op: 'put', h3Cell: SJ_CELL, deviceId: ghost, placeId: 'p1' });
    expect((await index.listForCell(SJ_CELL)).entries.map((e) => e.deviceId)).toEqual([deviceId]);
  });
});

describe('late joiners', () => {
  const batchesOf = (q: 'urgent' | 'standard') => sendQueue.batches(q);

  it('POST /places sends the active alert to a device that already has a target', async () => {
    const alert = makeAlert();
    await alerts.save(alert);
    const { deviceId, credential } = await register();
    await setToken(credential);
    const res = await addPlace(credential);
    expect(res.status).toBe(201);
    const [batch] = batchesOf('urgent');
    expect(batchesOf('urgent')).toHaveLength(1);
    expect(batchesOf('standard')).toEqual([]);
    expect(batch).toMatchObject({
      alertId: alert.id,
      sourceId: alert.sourceId,
      relaySeverity: 'warning',
      batchKey: `${alert.id}:warning:late:${deviceId}`,
      attempt: 0,
    });
    expect(batch?.recipients).toHaveLength(1);
    expect(batch?.recipients[0]).toMatchObject({
      deviceId,
      target: { platform: 'ios', token: APNS },
      language: 'en',
    });
    // A v2 token with the place's cell signed in, valid until the alert ends.
    const verified = verifyCheckInToken(
      batch?.recipients[0]?.checkInToken ?? '',
      { alertId: alert.id, deviceId, now: NOW },
      new Map([[KEY.id, KEY.secret]]),
    );
    expect(verified).toEqual({ ok: true, h3Cell: SJ_CELL });
  });

  it('watch and advisory alerts go to the standard queue', async () => {
    await alerts.save(makeAlert({ relaySeverity: 'watch', event: 'Flood Watch' }));
    const { credential } = await register();
    await setToken(credential);
    await addPlace(credential);
    expect(batchesOf('standard')).toHaveLength(1);
    expect(batchesOf('urgent')).toEqual([]);
  });

  it('skips statements, expired, cancelled and other-area alerts, and devices with no target', async () => {
    await alerts.save(makeAlert({ relaySeverity: 'statement' }));
    await alerts.save(makeAlert({ expires: '2026-11-09T10:00:00Z' }));
    await alerts.save(makeAlert({ state: 'cancelled' }));
    const far = cellToChildren(cellToParent(SJ_CELL, 3), 7).find((c) => c !== SJ_CELL) as string;
    await alerts.save(makeAlert({}, [far]));
    const withTarget = await register();
    await setToken(withTarget.credential);
    await addPlace(withTarget.credential);
    expect(batchesOf('urgent')).toEqual([]);
    expect(batchesOf('standard')).toEqual([]);

    await alerts.save(makeAlert());
    const noTarget = await register();
    await addPlace(noTarget.credential);
    expect(batchesOf('urgent')).toEqual([]);
  });

  it('PUT push-token sends active alerts for the places the device already has', async () => {
    const alert = makeAlert();
    await alerts.save(alert);
    const { deviceId, credential } = await register();
    await addPlace(credential); // no target yet: nothing to send
    expect(batchesOf('urgent')).toEqual([]);
    expect((await setToken(credential)).status).toBe(204);
    expect(batchesOf('urgent').map((b) => b.batchKey)).toEqual([
      `${alert.id}:warning:late:${deviceId}`,
    ]);
  });

  it('a send queue failure never fails the API call', async () => {
    await alerts.save(makeAlert());
    route = build({
      sendQueue: {
        enqueue: async () => {
          throw new Error('sqs down');
        },
      },
    });
    const { credential } = await register();
    expect((await setToken(credential)).status).toBe(204);
    expect((await addPlace(credential)).status).toBe(201);
    expect(logs.some((l) => l.includes('latejoin.failed'))).toBe(true);
    // Nothing sensitive in the log line.
    expect(logs.join('\n')).not.toContain(APNS);
  });

  it('an alert store failure never fails the API call', async () => {
    alerts.listActiveForCells = async () => {
      throw new Error('aurora down');
    };
    const { credential } = await register();
    await setToken(credential);
    expect((await addPlace(credential)).status).toBe(201);
  });

  it('without lateJoin deps nothing is sent', async () => {
    await alerts.save(makeAlert());
    route = build({ lateJoin: false });
    const { credential } = await register();
    await setToken(credential);
    await addPlace(credential);
    expect(batchesOf('urgent')).toEqual([]);
  });
});
