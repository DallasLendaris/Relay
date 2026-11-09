import type { Alert } from '@relay/shared';
import { describe, expect, it } from 'vitest';
import { PUSH_POLICY } from '../../../src/delivery/policy.js';
import { SENDER_CHUNK_SIZE, type NativePushMessage } from '../../../src/scale/contracts.js';
import {
  MAX_ATTEMPT,
  SENDER_LEASE_MS,
  baseBatchKey,
  processBatch,
} from '../../../src/scale/sender/sender.js';
import { tampered } from '../../delivery/fakes.js';
import { FakePush, makeBatch, makeDeps, recordingLogger, seedAlert } from './fakes.js';

describe('processBatch: claim states', () => {
  it('acks without sending when the batch is already sent', async () => {
    const t = makeDeps();
    await seedAlert(t.alerts);
    const batch = makeBatch();
    await t.idempotency.claim(batch.batchKey, new Date(t.state.now.getTime() + 1000));
    await t.idempotency.markSent(batch.batchKey, new Date('2026-11-12T00:00:00Z'));
    const out = await processBatch(batch, t.deps);
    expect(out).toMatchObject({ ack: true, reason: 'duplicate' });
    expect(t.push.calls).toHaveLength(0);
  });

  it('leaves the message (no ack) while another attempt holds a live lease', async () => {
    const t = makeDeps();
    await seedAlert(t.alerts);
    const batch = makeBatch();
    await t.idempotency.claim(batch.batchKey, new Date(t.state.now.getTime() + 30_000));
    const out = await processBatch(batch, t.deps);
    expect(out).toMatchObject({ ack: false, reason: 'leased' });
    expect(t.push.calls).toHaveLength(0);
  });

  it('claims with a 45 s lease', async () => {
    const t = makeDeps();
    await seedAlert(t.alerts);
    const seen: number[] = [];
    const real = t.idempotency.claim.bind(t.idempotency);
    t.idempotency.claim = async (key, until) => {
      seen.push(until.getTime() - t.state.now.getTime());
      return real(key, until);
    };
    await processBatch(makeBatch(), t.deps);
    expect(seen).toEqual([SENDER_LEASE_MS]);
    expect(SENDER_LEASE_MS).toBe(45_000);
  });
});

describe('processBatch: eligibility', () => {
  const cases: Array<[string, Partial<Alert>, boolean, string]> = [
    ['test status', { status: 'Test' }, true, 'not_actual'],
    ['cancelled', { state: 'cancelled' }, true, 'inactive'],
    ['expired', { expires: '2026-11-10T10:01:00Z' }, true, 'expired'],
    ['informational', { relaySeverity: 'statement' }, true, 'informational'],
  ];
  it.each(cases)('%s: releases the claim, never marks sent, ack=%s', async (_n, over, ack, why) => {
    const t = makeDeps();
    await seedAlert(t.alerts, over);
    const batch = makeBatch(over.relaySeverity ? { relaySeverity: over.relaySeverity } : {});
    const out = await processBatch(batch, t.deps);
    expect(out).toMatchObject({ ack, reason: why });
    expect(t.push.calls).toHaveLength(0);
    // Released, not sent: the key can be claimed again.
    expect(await t.idempotency.claim(batch.batchKey, new Date(t.state.now.getTime() + 1000))).toBe(
      'claimed',
    );
  });

  it('missing alert is retried (read lag), not acked', async () => {
    const t = makeDeps();
    const out = await processBatch(makeBatch(), t.deps);
    expect(out).toMatchObject({ ack: false, reason: 'alert_missing' });
  });

  it('a bad signature is retried, after re-reading once', async () => {
    const t = makeDeps();
    const good = await seedAlert(t.alerts);
    await t.alerts.save(tampered(good, { event: 'Tsunami Warning' }));
    let reads = 0;
    const get = t.alerts.get.bind(t.alerts);
    t.alerts.get = async (id) => (reads++, get(id));
    const sleeps: number[] = [];
    let refreshed = 0;
    const out = await processBatch(makeBatch(), {
      ...t.deps,
      sleep: async (ms) => void sleeps.push(ms),
      refreshKeys: async () => void refreshed++,
    });
    expect(out).toMatchObject({ ack: false, reason: 'unsigned' });
    expect(reads).toBe(2);
    expect(sleeps).toEqual([200]);
    expect(refreshed).toBe(1);
    expect(t.push.calls).toHaveLength(0);
  });

  it('a torn read that heals on the re-read sends normally', async () => {
    const t = makeDeps();
    const good = await seedAlert(t.alerts);
    const bad = tampered(good, { event: 'Torn' });
    let n = 0;
    t.alerts.get = async () => (n++ === 0 ? bad : good);
    const out = await processBatch(makeBatch(), t.deps);
    expect(out).toMatchObject({ ack: true, reason: 'sent', sent: 3 });
  });

  it('drops a stale batch whose severity no longer matches the alert', async () => {
    const t = makeDeps();
    await seedAlert(t.alerts, { relaySeverity: 'watch' });
    const out = await processBatch(makeBatch({ relaySeverity: 'warning' }), t.deps);
    expect(out).toMatchObject({ ack: true, reason: 'stale_severity' });
    expect(t.push.calls).toHaveLength(0);
  });
});

describe('processBatch: paused', () => {
  it('releases and retries without sending', async () => {
    const t = makeDeps({ isDeliveryPaused: async () => true });
    await seedAlert(t.alerts);
    const batch = makeBatch();
    const out = await processBatch(batch, t.deps);
    expect(out).toMatchObject({ ack: false, reason: 'paused' });
    expect(t.push.calls).toHaveLength(0);
    expect(await t.idempotency.claim(batch.batchKey, new Date(t.state.now.getTime() + 1000))).toBe(
      'claimed',
    );
  });
});

describe('processBatch: building pushes', () => {
  it('uses PUSH_POLICY, collapseKey = alertId, ttl to the end time, es/en text', async () => {
    const t = makeDeps();
    const alert = await seedAlert(t.alerts, {
      summaries: [{ language: 'es', whatToDo: 'x', text: 'Inundación repentina' }],
    });
    await processBatch(makeBatch(), t.deps);
    const msgs = t.push.calls.flat();
    expect(msgs).toHaveLength(3);
    const policy = PUSH_POLICY.warning;
    for (const m of msgs) {
      expect(m.priority).toBe(policy.priority);
      expect(m.sound).toBe(true);
      expect(m.collapseKey).toBe(alert.id);
      expect(m.category).toBe('alert-checkin');
      // now 10:05, expires 14:00 -> 3 h 55 m
      expect(m.ttlSeconds).toBe(3 * 3600 + 55 * 60);
      expect(m.data['alertId']).toBe(alert.id);
      expect(m.data['expiresAt']).toBe('2026-11-10T14:00:00.000Z');
    }
    expect(msgs[0]?.title).toBe('Warning: Flash Flood Warning');
    expect(msgs[0]?.body).toBe('Flash Flood Warning until 6 PM');
    expect(msgs[0]?.data['checkInToken']).toBe('ct0');
    expect(msgs[1]?.title).toBe('Aviso: Flash Flood Warning');
    expect(msgs[1]?.body).toBe('Inundación repentina');
  });

  it('quiet policy for watch: normal priority, no sound', async () => {
    const t = makeDeps();
    await seedAlert(t.alerts, { relaySeverity: 'watch' });
    await processBatch(makeBatch({ relaySeverity: 'watch' }), t.deps);
    const m = t.push.calls.flat()[0] as NativePushMessage;
    expect(m.priority).toBe('normal');
    expect(m.sound).toBe(false);
  });

  it('sends all sub-chunks concurrently, each at most SENDER_CHUNK_SIZE', async () => {
    const push = new FakePush(() => ({ ok: true }), 20);
    const t = makeDeps({ push });
    await seedAlert(t.alerts);
    const out = await processBatch(makeBatch({}, 350), t.deps);
    expect(push.calls.map((c) => c.length)).toEqual([100, 100, 100, 50]);
    expect(SENDER_CHUNK_SIZE).toBe(100);
    expect(push.maxInFlight).toBe(4);
    expect(out.sent).toBe(350);
  });
});

describe('processBatch: failures', () => {
  const decide = (m: NativePushMessage) =>
    m.data['checkInToken'] === 'ct0'
      ? ({ ok: false, kind: 'permanent', error: 'Unregistered' } as const)
      : m.data['checkInToken'] === 'ct1'
        ? ({ ok: false, kind: 'retryable', error: 'Unavailable' } as const)
        : ({ ok: true } as const);

  it('clears permanent failures, re-queues retryable ones as a new batch, markSent last', async () => {
    const order: string[] = [];
    const deadSeen: Array<{ deviceId: string; target: unknown }> = [];
    const push = new FakePush((m) => (order.push('push'), decide(m)));
    const t = makeDeps({
      push,
      onDeadTarget: async (deviceId, target) => {
        order.push('dead');
        deadSeen.push({ deviceId, target });
      },
    });
    const realSent = t.idempotency.markSent.bind(t.idempotency);
    t.idempotency.markSent = async (k, e) => (order.push('markSent'), realSent(k, e));
    t.sendQueue.onEnqueue = () => void order.push('enqueue');
    await seedAlert(t.alerts);
    const batch = makeBatch();
    const out = await processBatch(batch, t.deps);

    expect(out).toMatchObject({ ack: true, sent: 1, permanentFailed: 1, retried: 1, gaveUp: 0 });
    expect(order.at(-1)).toBe('markSent');
    expect(order.indexOf('enqueue')).toBeLessThan(order.indexOf('markSent'));
    expect(order.indexOf('dead')).toBeLessThan(order.indexOf('markSent'));

    expect(t.sendQueue.enqueued).toHaveLength(1);
    const q = t.sendQueue.enqueued[0];
    expect(q?.queue).toBe('urgent');
    const retry = q?.batches[0];
    expect(retry?.batchKey).toBe(`${batch.batchKey}:r1`);
    expect(retry?.attempt).toBe(1);
    expect(retry?.recipients.map((r) => r.deviceId)).toEqual(['dev1']);
    expect(retry?.recipients[0]?.checkInToken).toBe('ct1');

    expect(deadSeen).toEqual([{ deviceId: 'dev0', target: batch.recipients[0]?.target }]);
    // markSent really happened
    expect(await t.idempotency.claim(batch.batchKey, new Date(t.state.now.getTime() + 1))).toBe(
      'sent',
    );
  });

  it('retry batch keys do not stack suffixes and go to the standard queue for watch', async () => {
    const t = makeDeps({
      push: new FakePush(() => ({ ok: false, kind: 'retryable', error: 'x' })),
    });
    await seedAlert(t.alerts, { relaySeverity: 'watch' });
    const base = makeBatch({ relaySeverity: 'watch' });
    const second = { ...base, batchKey: `${base.batchKey}:r1`, attempt: 1 };
    await processBatch(second, t.deps);
    const q = t.sendQueue.enqueued[0];
    expect(q?.queue).toBe('standard');
    expect(q?.batches[0]?.batchKey).toBe(`${base.batchKey}:r2`);
    expect(q?.batches[0]?.attempt).toBe(2);
    expect(baseBatchKey(`${base.batchKey}:r4`)).toBe(base.batchKey);
  });

  it('retries up to attempt 5, then gives up with counts only', async () => {
    const { lines, logger } = recordingLogger();
    const t = makeDeps({
      push: new FakePush(() => ({ ok: false, kind: 'retryable', error: 'x' })),
      logger,
    });
    await seedAlert(t.alerts);
    const base = makeBatch();
    // attempt 4 still re-queues (as r5)
    let out = await processBatch(
      { ...base, batchKey: `${base.batchKey}:r4`, attempt: MAX_ATTEMPT - 1 },
      t.deps,
    );
    expect(out.retried).toBe(3);
    expect(t.sendQueue.enqueued.at(-1)?.batches[0]?.batchKey).toBe(`${base.batchKey}:r5`);
    // attempt 5 gives up
    const before = t.sendQueue.enqueued.length;
    out = await processBatch(
      { ...base, batchKey: `${base.batchKey}:r5`, attempt: MAX_ATTEMPT },
      t.deps,
    );
    expect(out).toMatchObject({ ack: true, retried: 0, gaveUp: 3 });
    expect(t.sendQueue.enqueued).toHaveLength(before);
    const gaveUp = lines.find((l) => l.msg === 'delivery.gave_up');
    expect(gaveUp?.fields).toMatchObject({ count: 3 });
    const text = JSON.stringify(lines);
    expect(text).not.toContain('tok0');
    expect(text).not.toContain('ct0');
    expect(text).not.toContain('dev0');
  });

  it('a throwing push chunk is retryable, not a crash', async () => {
    const t = makeDeps({
      push: {
        send: async () => {
          throw new Error('boom tok0');
        },
      },
    });
    await seedAlert(t.alerts);
    const out = await processBatch(makeBatch(), t.deps);
    expect(out).toMatchObject({ ack: true, sent: 0, retried: 3 });
  });

  it('a failing onDeadTarget does not fail the batch', async () => {
    const t = makeDeps({
      push: new FakePush(() => ({ ok: false, kind: 'permanent', error: 'gone' })),
      onDeadTarget: async () => {
        throw new Error('db down');
      },
    });
    await seedAlert(t.alerts);
    const out = await processBatch(makeBatch(), t.deps);
    expect(out).toMatchObject({ ack: true, permanentFailed: 3 });
  });

  it('re-enqueue failure: releases the claim, throws, never marks sent', async () => {
    const t = makeDeps({ push: new FakePush(decide) });
    t.sendQueue.fail = true;
    await seedAlert(t.alerts);
    const batch = makeBatch();
    await expect(processBatch(batch, t.deps)).rejects.toThrow('sqs down');
    // The claim was released (a redelivery can take it at once) and is not marked sent.
    expect(await t.idempotency.claim(batch.batchKey, new Date(t.state.now.getTime() + 1000))).toBe(
      'claimed',
    );
  });

  it('after a re-enqueue failure the redelivered batch is sent again (documented duplicate)', async () => {
    const push = new FakePush(decide);
    const t = makeDeps({ push });
    t.sendQueue.fail = true;
    await seedAlert(t.alerts);
    const batch = makeBatch();
    await expect(processBatch(batch, t.deps)).rejects.toThrow();
    t.sendQueue.fail = false;
    const out = await processBatch(batch, t.deps);
    expect(out.ack).toBe(true);
    // dev2 was pushed in both attempts: the accepted rare duplicate.
    const dev2 = push.calls.flat().filter((m) => m.data['checkInToken'] === 'ct2');
    expect(dev2).toHaveLength(2);
  });
});
