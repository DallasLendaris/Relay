import { describe, expect, it } from 'vitest';
import type { SQSEvent } from 'aws-lambda';
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../src/adapters/memory/alert-store.js';
import { MemoryDeliveryQueue } from '../../src/adapters/memory/delivery-queue.js';
import { MemoryIdempotencyStore } from '../../src/adapters/memory/idempotency-store.js';
import { MemoryPushSender } from '../../src/adapters/memory/push-sender.js';
import { deliveryWorker, planDelivery, type WorkerDeps } from '../../src/delivery/index.js';
import type { DeliveryJob, DeliveryQueue } from '../../src/ports.js';
import {
  FakeDevices,
  FakePlaces,
  SJ,
  device,
  place,
  signed,
  tampered,
  verifyAlert,
} from './fakes.js';

const logger = { info() {}, warn() {}, error() {} };
const clock = { now: () => new Date('2026-11-10T10:05:00Z') };
const tokenKey = { id: 'k1', secret: 'secret' };

function setup(opts: { paused?: boolean } = {}) {
  const alerts = new InMemoryAlertStore();
  const sender = new MemoryPushSender();
  const idempotency = new MemoryIdempotencyStore(clock);
  const dead: string[] = [];
  const state = { paused: opts.paused ?? false };
  const workerDeps: WorkerDeps = {
    sender,
    alerts,
    idempotency,
    verifyAlert,
    isDeliveryPaused: async () => state.paused,
    onDeadToken: async (t: string) => void dead.push(t),
    clock,
    logger,
  };
  const run = (jobs: DeliveryJob[]) => deliveryWorker(event(jobs), workerDeps);
  return { alerts, sender, idempotency, dead, state, run, workerDeps };
}

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

describe('deliveryWorker', () => {
  it('sends, and treats an already-claimed key as a successful duplicate', async () => {
    const t = setup();
    const alert = signed();
    await t.alerts.save(alert);
    expect(failed(await t.run([job(alert, 0)]))).toEqual([]);
    expect(failed(await t.run([job(alert, 0)]))).toEqual([]);
    expect(t.sender.sent).toHaveLength(1);
  });

  it('releases the claim on a retryable failure, so the retry sends', async () => {
    const t = setup();
    const alert = signed();
    await t.alerts.save(alert);
    t.sender.failTokens.set('tok0', 'MessageRateExceeded');
    expect(failed(await t.run([job(alert, 0), job(alert, 1)]))).toEqual(['m0']);
    expect(t.sender.sent.map((m) => m.to)).toEqual(['tok1']);

    t.sender.failTokens.clear();
    expect(failed(await t.run([job(alert, 0)]))).toEqual([]);
    expect(t.sender.sent.map((m) => m.to)).toEqual(['tok1', 'tok0']);
  });

  it('calls onDeadToken for DeviceNotRegistered and never retries it', async () => {
    const t = setup();
    const alert = signed();
    await t.alerts.save(alert);
    t.sender.failTokens.set('tok0', 'DeviceNotRegistered');
    expect(failed(await t.run([job(alert, 0)]))).toEqual([]);
    expect(t.dead).toEqual(['tok0']);
    // A redelivery is a duplicate: the claim was kept, so nothing is sent or reported again.
    t.sender.failTokens.clear();
    expect(failed(await t.run([job(alert, 0)]))).toEqual([]);
    expect(t.sender.sent).toHaveLength(0);
    expect(t.dead).toEqual(['tok0']);
  });

  it('skips an update at the same severity, but notifies on an upgrade or a new device', async () => {
    const t = setup();
    const watch = signed({ relaySeverity: 'watch', event: 'Flood Watch' });
    await t.alerts.save(watch);
    await t.run([job(watch, 0)]);
    expect(t.sender.sent).toHaveLength(1);

    // Routine NWS update, same alert id and severity.
    const update = signed({
      relaySeverity: 'watch',
      event: 'Flood Watch',
      state: 'updated',
      messageType: 'Update',
      sourceId: 'urn:oid:2',
    });
    await t.alerts.save(update);
    await t.run([job(update, 0)]);
    expect(t.sender.sent).toHaveLength(1);

    // A newly covered device is notified.
    await t.run([job(update, 0), job(update, 1)]);
    expect(t.sender.sent.map((m) => m.to)).toEqual(['tok0', 'tok1']);

    // Upgrade watch -> warning notifies everyone again.
    const warning = signed({
      relaySeverity: 'warning',
      state: 'updated',
      messageType: 'Update',
      sourceId: 'urn:oid:3',
    });
    await t.alerts.save(warning);
    await t.run([job(warning, 0), job(warning, 1)]);
    expect(t.sender.sent).toHaveLength(4);
  });

  it('claims until a day after the alert ends', async () => {
    const claims: Date[] = [];
    const t = setup();
    t.workerDeps.idempotency = {
      claim: async (_k: string, at: Date) => (claims.push(at), true),
      release: async () => {},
    };
    const alert = signed({ ends: '2026-11-10T16:00:00Z' });
    await t.alerts.save(alert);
    await deliveryWorker(event([job(alert, 0)]), t.workerDeps);
    expect(claims).toEqual([new Date('2026-11-11T16:00:00Z')]);
  });

  describe('refuses to send', () => {
    const good = signed();
    const cases: Array<[string, Alert]> = [
      ['a tampered row (signature mismatch)', tampered(good, { event: 'Evil' })],
      ['an unsigned row', { ...good, signature: null }],
      ['status Test', signed({ status: 'Test' })],
      ['a cancelled alert', signed({ state: 'cancelled' })],
      ['an expired alert', signed({ state: 'expired' })],
      ['a past-expiry alert', signed({ expires: '2026-11-10T10:00:00Z' })],
    ];
    for (const [name, alert] of cases) {
      it(name, async () => {
        const t = setup();
        await t.alerts.save(alert);
        // Acknowledged (not retried), nothing sent, nothing claimed.
        expect(failed(await t.run([job(alert, 0)]))).toEqual([]);
        expect(t.sender.sent).toHaveLength(0);
        const key = `${alert.id}:${alert.relaySeverity}:d0`;
        expect(await t.idempotency.claim(key, new Date('2030-01-01'))).toBe(true);
      });
    }
  });

  it('refuses while the kill switch is paused, then sends after resume', async () => {
    const t = setup({ paused: true });
    const alert = signed();
    await t.alerts.save(alert);
    expect(failed(await t.run([job(alert, 0), job(alert, 1)]))).toEqual(['m0', 'm1']);
    expect(t.sender.sent).toHaveLength(0);
    t.state.paused = false;
    expect(failed(await t.run([job(alert, 0), job(alert, 1)]))).toEqual([]);
    expect(t.sender.sent).toHaveLength(2);
  });

  it('retries jobs whose alert is missing and fails unparseable records', async () => {
    const t = setup();
    const alert = signed();
    const ev = {
      Records: [
        { messageId: 'bad', body: '{nope' },
        { messageId: 'gone', body: JSON.stringify(job(alert, 0)) },
      ],
    } as unknown as SQSEvent;
    expect(failed(await deliveryWorker(ev, t.workerDeps))).toEqual(['bad', 'gone']);
  });

  it('fails the record when the claim store throws', async () => {
    const t = setup();
    const alert = signed();
    await t.alerts.save(alert);
    t.workerDeps.idempotency = {
      claim: async () => {
        throw new Error('dynamo down');
      },
      release: async () => {},
    };
    expect(failed(await deliveryWorker(event([job(alert, 0)]), t.workerDeps))).toEqual(['m0']);
    expect(t.sender.sent).toHaveLength(0);
  });
});

describe('plan then work', () => {
  it('an enqueue failure halfway, then a re-plan, gives every device exactly one push', async () => {
    const places = new FakePlaces();
    const devices = new FakeDevices();
    for (let i = 0; i < 25; i++) {
      await devices.create(device(`d${i}`));
      await places.add(place(`p${i}`, `d${i}`, SJ.lat, SJ.lng));
    }
    const inner = new MemoryDeliveryQueue();
    let calls = 0;
    const flaky: DeliveryQueue = {
      enqueue: async (jobs) => {
        if (++calls === 2) throw new Error('SQS down'); // batch 2 of 3 fails once
        await inner.enqueue(jobs);
      },
    };
    const alert = signed();
    const t = setup();
    await t.alerts.save(alert);
    const deps = {
      places,
      devices,
      queue: flaky,
      tokenKey,
      verifyAlert,
      isDeliveryPaused: async () => false,
      clock,
      logger,
    };

    await expect(planDelivery(alert, deps)).rejects.toThrow('SQS down');
    expect(inner.jobs).toHaveLength(10); // only the first batch made it
    await planDelivery(alert, deps); // re-plan: harmless, nothing was claimed

    // Drain the queue in SQS-sized batches.
    for (let i = 0; i < inner.jobs.length; i += 10) {
      expect(failed(await t.run(inner.jobs.slice(i, i + 10)))).toEqual([]);
    }
    const counts = new Map<string, number>();
    for (const m of t.sender.sent) counts.set(m.to, (counts.get(m.to) ?? 0) + 1);
    expect(counts.size).toBe(25);
    expect([...counts.values()].every((n) => n === 1)).toBe(true);
  });
});
