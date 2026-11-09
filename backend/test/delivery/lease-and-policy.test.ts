import { describe, expect, it } from 'vitest';
import type { SQSEvent } from 'aws-lambda';
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../src/adapters/memory/alert-store.js';
import { MemoryDeliveryQueue } from '../../src/adapters/memory/delivery-queue.js';
import { MemoryIdempotencyStore } from '../../src/adapters/memory/idempotency-store.js';
import { MemoryPushSender } from '../../src/adapters/memory/push-sender.js';
import { ExpoPushSender } from '../../src/adapters/expo/expo-push-sender.js';
import {
  LEASE_MS,
  VISIBILITY_TIMEOUT_MS,
  WORKER_TIMEOUT_MS,
  PUSH_POLICY,
  buildMessage,
  deliveryKey,
  deliveryWorker,
  planDelivery,
  redactPushTokens,
  type WorkerDeps,
} from '../../src/delivery/index.js';
import { resolveExpoAccessToken } from '../../src/delivery/handler.js';
import type { DeliveryJob, PushMessage } from '../../src/ports.js';
import { verifyCheckInToken } from '../../src/tokens/index.js';
import { FakeDevices, FakePlaces, SJ, device, place, signed, verifyAlert } from './fakes.js';

const silent = { info() {}, warn() {}, error() {} };
const job = (alert: Alert, n: number): DeliveryJob => ({
  alertId: alert.id,
  deviceId: `d${n}`,
  expoPushToken: `tok${n}`,
  language: 'en',
  checkInToken: `ct${n}`,
  expiresAt: alert.expires,
  groupingKey: alert.id,
});
const event = (jobs: DeliveryJob[]): SQSEvent =>
  ({
    Records: jobs.map((j, i) => ({ messageId: `m${i}`, body: JSON.stringify(j) })),
  }) as unknown as SQSEvent;
const failed = (r: { batchItemFailures: Array<{ itemIdentifier: string }> }) =>
  r.batchItemFailures.map((f) => f.itemIdentifier);

function setup() {
  const state = { now: new Date('2026-11-10T10:05:00Z') };
  const clock = { now: () => state.now };
  const alerts = new InMemoryAlertStore();
  const sender = new MemoryPushSender();
  const idempotency = new MemoryIdempotencyStore(clock);
  const sleeps: number[] = [];
  const deps: WorkerDeps = {
    sender,
    alerts,
    idempotency,
    verifyAlert,
    isDeliveryPaused: async () => false,
    onDeadToken: async () => {},
    clock,
    logger: silent,
    sleep: async (ms) => void sleeps.push(ms),
  };
  return { state, alerts, sender, idempotency, deps, sleeps };
}

describe('timing constants', () => {
  it('lease outlasts the worker timeout and stays under 2x the visibility timeout', () => {
    expect(LEASE_MS).toBeGreaterThan(WORKER_TIMEOUT_MS);
    expect(LEASE_MS).toBeLessThan(2 * VISIBILITY_TIMEOUT_MS);
  });
});

describe('leases', () => {
  it('a crash after claiming is retried after the lease and sent exactly once', async () => {
    const t = setup();
    const alert = signed();
    await t.alerts.save(alert);
    // The first attempt claimed, then the Lambda died: no send, no release.
    const leaseEnd = new Date(t.state.now.getTime() + LEASE_MS);
    expect(await t.idempotency.claim(deliveryKey(alert, 'd0'), leaseEnd)).toBe('claimed');

    // Redelivery while the lease is live: fail the record (never ack), send nothing.
    expect(failed(await deliveryWorker(event([job(alert, 0)]), t.deps))).toEqual(['m0']);
    expect(t.sender.sent).toHaveLength(0);

    // After the lease: claim, send, mark sent.
    t.state.now = new Date(leaseEnd.getTime() + 1000);
    expect(failed(await deliveryWorker(event([job(alert, 0)]), t.deps))).toEqual([]);
    expect(t.sender.sent).toHaveLength(1);

    // Any further redelivery is a duplicate and is acked.
    expect(failed(await deliveryWorker(event([job(alert, 0)]), t.deps))).toEqual([]);
    expect(t.sender.sent).toHaveLength(1);
  });

  it('claims with a 45 second lease and marks sent with the long expiry on success', async () => {
    const t = setup();
    const calls: string[] = [];
    t.deps.idempotency = {
      claim: async (_k, until) => (calls.push(`claim ${until.toISOString()}`), 'claimed'),
      markSent: async (_k, at) => void calls.push(`sent ${at.toISOString()}`),
      release: async () => void calls.push('release'),
    };
    const alert = signed();
    await t.alerts.save(alert);
    await deliveryWorker(event([job(alert, 0)]), t.deps);
    expect(calls).toEqual(['claim 2026-11-10T10:05:45.000Z', 'sent 2026-11-11T14:00:00.000Z']);
  });

  it('a retryable failure releases, so the retry sends immediately', async () => {
    const t = setup();
    const alert = signed();
    await t.alerts.save(alert);
    t.sender.failTokens.set('tok0', 'MessageRateExceeded');
    expect(failed(await deliveryWorker(event([job(alert, 0)]), t.deps))).toEqual(['m0']);
    t.sender.failTokens.clear();
    expect(failed(await deliveryWorker(event([job(alert, 0)]), t.deps))).toEqual([]);
    expect(t.sender.sent).toHaveLength(1);
  });

  it('the same key twice in one batch sends once and acks both', async () => {
    const t = setup();
    const alert = signed();
    await t.alerts.save(alert);
    expect(failed(await deliveryWorker(event([job(alert, 0), job(alert, 0)]), t.deps))).toEqual([]);
    expect(t.sender.sent).toHaveLength(1);
  });

  it('a send that throws releases the claim', async () => {
    const t = setup();
    const alert = signed();
    await t.alerts.save(alert);
    t.deps.sender = {
      send: async () => {
        throw new Error('boom');
      },
    };
    expect(failed(await deliveryWorker(event([job(alert, 0)]), t.deps))).toEqual(['m0']);
    expect(await t.idempotency.claim(deliveryKey(alert, 'd0'), new Date('2030-01-01'))).toBe(
      'claimed',
    );
  });
});

describe('Expo errors', () => {
  const msg = (n: number): PushMessage => ({ to: `t${n}`, title: 'T', body: 'B', data: {} });

  it('times out a hung request with AbortSignal, reports Timeout, and the worker retries', async () => {
    let signal: AbortSignal | undefined;
    let calls = 0;
    const hang = (async (_u: string, init: RequestInit) => {
      calls++;
      signal = init.signal ?? undefined;
      return new Promise<Response>((_res, rej) => {
        init.signal?.addEventListener('abort', () => rej(init.signal?.reason));
      });
    }) as unknown as typeof fetch;
    const sender = new ExpoPushSender({ fetch: hang, timeoutMs: 20, sleep: async () => {} });
    expect(await sender.send([msg(0)])).toEqual([{ ok: false, error: 'Timeout' }]);
    expect(signal).toBeInstanceOf(AbortSignal);
    expect(calls).toBe(1); // no in-process retry of a hang

    const t = setup();
    t.deps.sender = sender;
    const alert = signed();
    await t.alerts.save(alert);
    expect(failed(await deliveryWorker(event([job(alert, 0)]), t.deps))).toEqual(['m0']);
    // The claim was released, so the retry (Expo healthy again) sends.
    t.deps.sender = new MemoryPushSender();
    expect(failed(await deliveryWorker(event([job(alert, 0)]), t.deps))).toEqual([]);
  });

  it('uses an 8 second timeout by default', async () => {
    const spy = AbortSignal.timeout;
    const asked: number[] = [];
    AbortSignal.timeout = (ms: number) => (asked.push(ms), spy.call(AbortSignal, ms));
    try {
      const fetchFn = (async () =>
        new Response(JSON.stringify({ data: [{ status: 'ok' }] }))) as unknown as typeof fetch;
      await new ExpoPushSender({ fetch: fetchFn }).send([msg(0)]);
    } finally {
      AbortSignal.timeout = spy;
    }
    expect(asked).toEqual([8000]);
  });

  for (const status of [401, 403]) {
    it(`HTTP ${status} is retryable, logs delivery.expo_auth_failed, and never logs the token`, async () => {
      const logs: string[] = [];
      const logger = {
        ...silent,
        error: (m: string, f?: Record<string, unknown>) => logs.push(m + JSON.stringify(f ?? {})),
      };
      const fetchFn = (async () => new Response('nope', { status })) as unknown as typeof fetch;
      const sender = new ExpoPushSender({ fetch: fetchFn, accessToken: 'SECRET-TOKEN', logger });
      expect(await sender.send([msg(0)])).toEqual([{ ok: false, error: `HttpError${status}` }]);
      expect(logs).toHaveLength(1);
      expect(logs[0]).toContain('delivery.expo_auth_failed');
      expect(logs[0]).not.toContain('SECRET-TOKEN');

      // Not a permanent error: the worker releases the claim and fails the record.
      const t = setup();
      t.deps.sender = sender;
      const alert = signed();
      await t.alerts.save(alert);
      expect(failed(await deliveryWorker(event([job(alert, 0)]), t.deps))).toEqual(['m0']);
      expect(await t.idempotency.claim(deliveryKey(alert, 'd0'), new Date('2030-01-01'))).toBe(
        'claimed',
      );
    });
  }

  it('redacts push tokens in thrown errors and ticket messages', async () => {
    const boom = (async () => {
      throw new Error('bad ExponentPushToken[abc-123] here');
    }) as unknown as typeof fetch;
    const [r1] = await new ExpoPushSender({ fetch: boom, sleep: async () => {} }).send([msg(0)]);
    expect(JSON.stringify(r1)).not.toContain('abc-123');

    const ticket = (async () =>
      new Response(
        JSON.stringify({ data: [{ status: 'error', message: 'ExpoPushToken[xyz] is bad' }] }),
      )) as unknown as typeof fetch;
    const [r2] = await new ExpoPushSender({ fetch: ticket }).send([msg(0)]);
    expect(JSON.stringify(r2)).not.toContain('xyz');
  });
});

describe('redactPushTokens', () => {
  it('replaces both token spellings, everywhere in the string', () => {
    const out = redactPushTokens('a ExponentPushToken[abc] b ExpoPushToken[d-e_f] c');
    expect(out).toBe('a [redacted-push-token] b [redacted-push-token] c');
    expect(redactPushTokens('nothing here')).toBe('nothing here');
  });
});

describe('resolveExpoAccessToken', () => {
  it('treats empty and REPLACE_ME values as no token, warning each time it is called', () => {
    const warns: string[] = [];
    const log = { ...silent, warn: (m: string) => void warns.push(m) };
    expect(resolveExpoAccessToken('', log)).toBeUndefined();
    expect(resolveExpoAccessToken('  \n', log)).toBeUndefined();
    expect(resolveExpoAccessToken('REPLACE_ME_LATER', log)).toBeUndefined();
    expect(warns).toHaveLength(3);
    expect(warns.join()).not.toContain('REPLACE_ME');
  });

  it('keeps a real token, trimmed, without warning', () => {
    const warns: string[] = [];
    expect(resolveExpoAccessToken(' abc\n', { ...silent, warn: (m) => void warns.push(m) })).toBe(
      'abc',
    );
    expect(warns).toEqual([]);
  });
});

describe('torn reads', () => {
  it('re-reads once after 200 ms when the signature fails, and sends if the second read is good', async () => {
    const t = setup();
    const good = signed();
    const bad = { ...good, event: 'Mid-update' }; // content changed, signature stale
    let reads = 0;
    t.deps.alerts = { get: async () => (++reads === 1 ? bad : good) };
    expect(failed(await deliveryWorker(event([job(good, 0)]), t.deps))).toEqual([]);
    expect(reads).toBe(2);
    expect(t.sleeps).toEqual([200]);
    expect(t.sender.sent).toHaveLength(1);
  });

  it('fails the record (SQS retries) when the second read is also bad, logging delivery.signature_failed', async () => {
    const t = setup();
    const errors: Array<[string, unknown]> = [];
    t.deps.logger = { ...silent, error: (m, f) => void errors.push([m, f]) };
    const good = signed();
    let reads = 0;
    t.deps.alerts = {
      get: async () => (reads++, { ...good, event: 'Evil' }),
    };
    expect(failed(await deliveryWorker(event([job(good, 0)]), t.deps))).toEqual(['m0']);
    expect(reads).toBe(2);
    expect(t.sender.sent).toHaveLength(0);
    expect(errors.map((e) => e[0])).toContain('delivery.signature_failed');
  });

  it('reloads the keyring once after a signature failure, and sends if a rotated key fixes it', async () => {
    const t = setup();
    const alert = signed();
    await t.alerts.save(alert);
    let keysLoaded = false;
    let refreshes = 0;
    t.deps.verifyAlert = (a) => keysLoaded && verifyAlert(a); // stale cache: key unknown
    t.deps.refreshKeys = async () => {
      refreshes++;
      keysLoaded = true;
    };
    // Two jobs for the same alert still cause only one refresh.
    expect(failed(await deliveryWorker(event([job(alert, 0), job(alert, 1)]), t.deps))).toEqual([]);
    expect(refreshes).toBe(1);
    expect(t.sender.sent).toHaveLength(2);
  });

  it('keeps failing the record when the refreshed keyring still cannot verify', async () => {
    const t = setup();
    const alert = signed();
    await t.alerts.save(alert);
    let refreshes = 0;
    t.deps.verifyAlert = () => false;
    t.deps.refreshKeys = async () => void refreshes++;
    expect(failed(await deliveryWorker(event([job(alert, 0)]), t.deps))).toEqual(['m0']);
    expect(refreshes).toBe(1);
  });

  it('does not refresh keys for other refusals, which stay final acks', async () => {
    const t = setup();
    let refreshes = 0;
    t.deps.refreshKeys = async () => void refreshes++;
    const cancelled = signed({ state: 'cancelled' });
    await t.alerts.save(cancelled);
    expect(failed(await deliveryWorker(event([job(cancelled, 0)]), t.deps))).toEqual([]);
    expect(refreshes).toBe(0);
  });

  it('does not re-read for other refusals', async () => {
    const t = setup();
    const cancelled = signed({ state: 'cancelled' });
    let reads = 0;
    t.deps.alerts = { get: async () => (reads++, cancelled) };
    await deliveryWorker(event([job(cancelled, 0)]), t.deps);
    expect(reads).toBe(1);
  });

  it('a DB error fails only that alert records, not the whole batch', async () => {
    const t = setup();
    const ok = signed();
    const broken = signed({ id: '22222222-2222-4222-8222-222222222222', sourceId: 'urn:oid:2' });
    t.deps.alerts = {
      get: async (id) => {
        if (id === broken.id) throw new Error('db down');
        return ok;
      },
    };
    const res = await deliveryWorker(event([job(broken, 0), job(ok, 1)]), t.deps);
    expect(failed(res)).toEqual(['m0']);
    expect(t.sender.sent.map((m) => m.to)).toEqual(['tok1']);
  });
});

describe('PUSH_POLICY', () => {
  const expected = {
    emergency: { push: true, priority: 'high', sound: 'default' },
    warning: { push: true, priority: 'high', sound: 'default' },
    watch: { push: true, priority: 'normal', sound: null },
    advisory: { push: true, priority: 'normal', sound: null },
    statement: { push: false, priority: 'normal', sound: null },
  } as const;

  for (const [severity, policy] of Object.entries(expected)) {
    it(`${severity}: table, built message, and Expo request body`, async () => {
      const sev = severity as Alert['relaySeverity'];
      expect(PUSH_POLICY[sev]).toEqual(policy);
      const alert = signed({ relaySeverity: sev });
      const message = buildMessage(job(alert, 0), alert);
      expect(message.priority).toBe(policy.priority);
      expect(message.sound).toBe(policy.sound ?? undefined);

      let body: Array<Record<string, unknown>> = [];
      const fetchFn = (async (_u: string, init: RequestInit) => {
        body = JSON.parse(String(init.body)) as Array<Record<string, unknown>>;
        return new Response(JSON.stringify({ data: [{ status: 'ok' }] }));
      }) as unknown as typeof fetch;
      await new ExpoPushSender({ fetch: fetchFn }).send([message]);
      expect(body[0]?.['priority']).toBe(policy.priority);
      expect(body[0]?.['sound']).toBe(policy.sound ?? undefined);
      expect('sound' in (body[0] ?? {})).toBe(policy.sound !== null);
    });

    it(`${severity}: planner ${policy.push ? 'enqueues' : "refuses 'informational'"}, worker agrees`, async () => {
      const sev = severity as Alert['relaySeverity'];
      const alert = signed({ relaySeverity: sev });
      const places = new FakePlaces();
      const devices = new FakeDevices();
      await devices.create(device('d0'));
      await places.add(place('p0', 'd0', SJ.lat, SJ.lng));
      const queue = new MemoryDeliveryQueue();
      const t = setup();
      const plan = await planDelivery(alert, {
        places,
        devices,
        queue,
        tokenKey: { id: 'k1', secret: 's' },
        verifyAlert,
        isDeliveryPaused: async () => false,
        clock: t.deps.clock,
        logger: silent,
      });
      if (policy.push) {
        expect(plan.refused).toBeUndefined();
        expect(queue.jobs).toHaveLength(1);
      } else {
        expect(plan.refused).toBe('informational');
        expect(plan.enqueued).toBe(0);
        expect(queue.jobs).toHaveLength(0);
      }

      // A job that slipped through (old queue message) is acked without a push.
      await t.alerts.save(alert);
      expect(failed(await deliveryWorker(event([job(alert, 0)]), t.deps))).toEqual([]);
      expect(t.sender.sent).toHaveLength(policy.push ? 1 : 0);
    });
  }
});

describe('check-in token expiry', () => {
  it('lasts until the later of expires and ends', async () => {
    const alert = signed({ expires: '2026-11-10T14:00:00Z', ends: '2026-11-10T16:00:00Z' });
    const places = new FakePlaces();
    const devices = new FakeDevices();
    await devices.create(device('d0'));
    await places.add(place('p0', 'd0', SJ.lat, SJ.lng));
    const queue = new MemoryDeliveryQueue();
    const key = { id: 'k1', secret: 's' };
    await planDelivery(alert, {
      places,
      devices,
      queue,
      tokenKey: key,
      verifyAlert,
      isDeliveryPaused: async () => false,
      clock: { now: () => new Date('2026-11-10T10:05:00Z') },
      logger: silent,
    });
    const token = queue.jobs[0]?.checkInToken;
    const keys = new Map([[key.id, key.secret]]);
    const check = (iso: string) =>
      verifyCheckInToken(token, { alertId: alert.id, deviceId: 'd0', now: new Date(iso) }, keys);
    expect(check('2026-11-10T15:00:00Z')).toEqual({ ok: true, h3Cell: null }); // past expires, before ends
    expect(check('2026-11-10T16:05:00Z')).toEqual({ ok: false, reason: 'expired' });
  });
});
