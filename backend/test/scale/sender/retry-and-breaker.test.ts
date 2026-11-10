import { describe, expect, it } from 'vitest';
import { PermanentBreaker, ROLLING_WINDOW } from '../../../src/scale/sender/breaker.js';
import { DeadTargetQueue } from '../../../src/scale/sender/dead-target-queue.js';
import {
  RETRY_MAX_DELAY_SECONDS,
  processBatch,
  retryDelaySeconds,
} from '../../../src/scale/sender/sender.js';
import type {
  NativePushMessage,
  NativePushResult,
  SendBatch,
  SendQueue,
  SendQueueName,
} from '../../../src/scale/contracts.js';
import {
  FakePush,
  clearAtOnce,
  makeBatch,
  makeDeps,
  recordingLogger,
  seedAlert,
  waitFor,
} from './fakes.js';

/** A queue that honors delaySeconds against the test's fake clock, like SQS does. */
class DelayQueue implements SendQueue {
  items: Array<{ at: number; queue: SendQueueName; batch: SendBatch; delay: number }> = [];
  constructor(private readonly now: () => number) {}
  async enqueue(queue: SendQueueName, batches: SendBatch[], opts?: { delaySeconds?: number }) {
    expect(opts?.delaySeconds ?? 0).toBeLessThanOrEqual(900); // SQS cap
    for (const batch of batches) {
      const delay = opts?.delaySeconds ?? 0;
      this.items.push({ at: this.now() + delay * 1000, queue, batch, delay });
    }
  }
  /** Removes and returns the next message that would become visible. */
  next() {
    this.items.sort((a, b) => a.at - b.at);
    return this.items.shift();
  }
}

describe('retry backoff', () => {
  it('a sustained 429 for 3 minutes of fake time recovers and delivers every recipient exactly once', async () => {
    const t = makeDeps();
    const queue = new DelayQueue(() => t.state.now.getTime());
    t.deps.sendQueue = queue;
    await seedAlert(t.alerts);
    const start = t.state.now.getTime();
    const delivered = new Map<string, number>();
    let attemptsSeen = 0;
    const push = new FakePush((m: NativePushMessage): NativePushResult => {
      if (t.state.now.getTime() < start + 180_000) {
        return { ok: false, kind: 'retryable', error: 'Fcm429:QUOTA_EXCEEDED' };
      }
      delivered.set(m.target.token, (delivered.get(m.target.token) ?? 0) + 1);
      return { ok: true };
    });
    t.deps.push = push;

    const first = makeBatch({}, 30);
    let out = await processBatch(first, t.deps);
    const delays: number[] = [out.retried > 0 ? (queue.items[0]?.delay ?? -1) : -1];
    let guard = 0;
    let gaveUp = out.gaveUp;
    for (let item = queue.next(); item; item = queue.next()) {
      if (++guard > 100) throw new Error('did not converge');
      t.state.now = new Date(Math.max(t.state.now.getTime(), item.at));
      attemptsSeen = Math.max(attemptsSeen, item.batch.attempt);
      out = await processBatch(item.batch, t.deps);
      gaveUp += out.gaveUp;
      const last = queue.items.at(-1);
      if (last) delays.push(last.delay);
    }

    expect(gaveUp).toBe(0);
    // Recovered only after the outage ended, and did not burn through 5 attempts in seconds.
    expect(t.state.now.getTime() - start).toBeGreaterThanOrEqual(180_000);
    expect(attemptsSeen).toBeGreaterThan(5);
    // Backoff grows (jitter pinned at 0.5 => 1, 2, 4, 8 ... seconds) and never passes the SQS cap.
    expect(delays.slice(0, 6)).toEqual([1, 2, 4, 8, 16, 32]);
    expect(Math.max(...delays)).toBeLessThanOrEqual(RETRY_MAX_DELAY_SECONDS);
    // Exactly once: every token succeeded one time, none twice, none lost.
    expect(delivered.size).toBe(30);
    expect([...delivered.values()].every((n) => n === 1)).toBe(true);
  });

  it('retries carry firstAttemptAt so the 30 minute budget is shared across attempts', async () => {
    const t = makeDeps({
      push: new FakePush(() => ({ ok: false, kind: 'retryable', error: 'x' })),
    });
    await seedAlert(t.alerts);
    const t0 = t.state.now.toISOString();
    await processBatch(makeBatch(), t.deps);
    const r1 = t.sendQueue.enqueued[0]?.batches[0];
    expect(r1?.firstAttemptAt).toBe(t0);
    t.state.now = new Date(t.state.now.getTime() + 60_000);
    await processBatch(r1 as SendBatch, t.deps);
    expect(t.sendQueue.enqueued[1]?.batches[0]?.firstAttemptAt).toBe(t0);
  });

  it('honors Retry-After: the delay is at least the largest one in the batch, capped at 900', async () => {
    const t = makeDeps({
      random: () => 0, // no jitter at all: only Retry-After can raise the delay
      push: new FakePush((m) => ({
        ok: false,
        kind: 'retryable',
        error: 'Fcm429',
        ...(m.data['checkInToken'] === 'ct1'
          ? { retryAfterSeconds: 120 }
          : { retryAfterSeconds: 7 }),
      })),
    });
    await seedAlert(t.alerts);
    await processBatch(makeBatch(), t.deps);
    expect(t.sendQueue.enqueued[0]?.delaySeconds).toBe(120);

    const huge = makeDeps({
      random: () => 0,
      push: new FakePush(() => ({
        ok: false,
        kind: 'retryable',
        error: 'x',
        retryAfterSeconds: 5000,
      })),
    });
    await seedAlert(huge.alerts);
    await processBatch(makeBatch(), huge.deps);
    expect(huge.sendQueue.enqueued[0]?.delaySeconds).toBe(900);
  });

  it('retryDelaySeconds is full jitter over min(900, 2 x 2^attempt)', () => {
    expect(retryDelaySeconds(0, 0, () => 0)).toBe(0);
    expect(retryDelaySeconds(0, 0, () => 0.999)).toBe(2);
    expect(retryDelaySeconds(3, 0, () => 0.999)).toBe(16);
    expect(retryDelaySeconds(30, 0, () => 0.999)).toBe(900);
    expect(retryDelaySeconds(1, 50, () => 0.1)).toBe(50);
  });

  it('gives up (GaveUp, logged with counts) once the alert would be over before the retry runs', async () => {
    const { lines, logger } = recordingLogger();
    const t = makeDeps({
      logger,
      push: new FakePush(() => ({ ok: false, kind: 'retryable', error: 'x' })),
    });
    // Alert ends 20 s from now; a retry at attempt 8 would wait about 256 s.
    await seedAlert(t.alerts, { expires: '2026-11-10T10:05:20Z' });
    const base = makeBatch();
    const out = await processBatch(
      { ...base, batchKey: `${base.batchKey}:r8`, attempt: 8 },
      t.deps,
    );
    expect(out).toMatchObject({ ack: true, retried: 0, gaveUp: 3 });
    expect(t.sendQueue.enqueued).toHaveLength(0);
    expect(lines.find((l) => l.msg === 'delivery.gave_up')?.fields).toMatchObject({ count: 3 });
  });
});

describe('PermanentBreaker', () => {
  it('opens on a batch only above 25% AND with at least 20 permanents', () => {
    const open = (total: number, perm: number) => new PermanentBreaker().record(total, perm).open;
    expect(open(60, 19)).toBe(false); // 31%, but fewer than 20
    expect(open(60, 20)).toBe(true);
    expect(open(200, 24)).toBe(false); // 12%
    expect(open(100, 20)).toBe(false); // 20%
    expect(open(80, 20)).toBe(false); // exactly 25%: not "more than"
    expect(open(79, 20)).toBe(true);
  });

  it('opens on the rolling rate over the last 1,000 results when no single batch trips', () => {
    const b = new PermanentBreaker();
    const verdicts = [];
    // 15 of every 50 = 30%: each batch has <20 permanents, so only the rolling rate can trip.
    for (let i = 0; i < 20; i++) verdicts.push(b.record(50, 15));
    // Not before the window holds 200 results (batches 1-3 = 150 results).
    expect(verdicts.slice(0, 3).some((v) => v.open)).toBe(false);
    const first = verdicts.find((v) => v.open);
    expect(first?.reason).toBe('rolling_rate');
    expect(verdicts.at(-1)?.open).toBe(true);
    // Exactly 20% never trips (10 of 50): the rule is "more than".
    const edge = new PermanentBreaker();
    for (let i = 0; i < 40; i++) expect(edge.record(50, 10).open).toBe(false);
  });

  it('really is a rolling window: old failures age out after 1,000 more results', () => {
    const b = new PermanentBreaker();
    for (let i = 0; i < 20; i++) b.record(50, 15);
    expect(b.record(50, 15).open).toBe(true);
    for (let i = 0; i < ROLLING_WINDOW / 50; i++) b.record(50, 0);
    expect(b.record(50, 0).open).toBe(false);
  });

  it('does not trip on a small first batch with a few dead tokens', () => {
    expect(new PermanentBreaker().record(3, 1).open).toBe(false);
    expect(new PermanentBreaker().record(19, 19).open).toBe(false); // 100%, but under 20 permanents and 200 samples
  });

  it('a steady 5% dead-token rate over 10,000 results never opens the breaker', () => {
    const b = new PermanentBreaker();
    let total = 0;
    let maxRolling = 0;
    for (let i = 0; total < 10_000; i++) {
      const size = 50 + ((i * 37) % 200); // batches of 50-249
      const v = b.record(size, Math.round(size * 0.05));
      total += size;
      maxRolling = Math.max(maxRolling, v.rollingRate);
      expect(v.open).toBe(false);
    }
    expect(total).toBeGreaterThanOrEqual(10_000);
    expect(maxRolling).toBeLessThan(0.1);
  });

  it('a 100% permanent misconfiguration opens in the first batch of 50', () => {
    const v = new PermanentBreaker().record(50, 50);
    expect(v).toMatchObject({ open: true, reason: 'batch_rate', batchRate: 1 });
  });
});

describe('circuit breaker in processBatch', () => {
  const dead = (_m: NativePushMessage): NativePushResult => ({
    ok: false,
    kind: 'permanent',
    error: 'Apns400:BadDeviceToken',
  });

  it('opens when more than 25% (and 20+) of a batch is permanent: nothing is cleared, all are retried', async () => {
    const { lines, logger } = recordingLogger();
    const metrics: string[] = [];
    const cleared: string[] = [];
    const t = makeDeps({
      logger,
      writeMetric: (l) => metrics.push(l),
      push: new FakePush(dead),
      onDeadTarget: async (id) => void cleared.push(id),
    });
    await seedAlert(t.alerts);
    const out = await processBatch(makeBatch({}, 20), t.deps);
    await new Promise((r) => setTimeout(r, 20)); // give a (wrong) async cleanup time to run
    expect(cleared).toEqual([]);
    expect(out).toMatchObject({ ack: true, permanentFailed: 0, retried: 20 });
    expect(t.sendQueue.enqueued[0]?.batches[0]?.recipients).toHaveLength(20);
    const log = lines.find((l) => l.msg === 'delivery.permanent_breaker_open');
    expect(log?.level).toBe('error');
    expect(log?.fields).toMatchObject({ permanent: 20, total: 20, reason: 'batch_rate' });
    const emf = metrics.map((m) => JSON.parse(m) as Record<string, unknown>);
    expect(emf).toHaveLength(1);
    expect(emf[0]?.['PermanentBreakerOpen']).toBe(1);
    expect(JSON.stringify(lines)).not.toContain('tok0');
  });

  it('below the thresholds dead targets are still cleared, asynchronously', async () => {
    const cleared: string[] = [];
    const t = makeDeps({
      push: new FakePush((m) => (m.data['checkInToken'] === 'ct0' ? dead(m) : { ok: true })),
      onDeadTarget: async (id) => void cleared.push(id),
    });
    await seedAlert(t.alerts);
    await processBatch(makeBatch({}, 20), t.deps); // 1 permanent: under the 20 minimum
    await new Promise((r) => setTimeout(r, 20));
    // Held: the breaker has seen 20 results, too few to tell a dead token from a misconfiguration.
    expect(cleared).toEqual([]);
    for (let i = 1; i < 10; i++) await processBatch(makeBatch({ batchKey: `k:${i}` }, 20), t.deps);
    // 200 results with a 5% permanent rate: the held clear is released.
    await waitFor(() => cleared.length === 10);
    expect(cleared).toEqual(Array.from({ length: 10 }, () => 'dev0'));
  });

  it('opens on the process-wide rolling rate across many small batches', async () => {
    const { lines, logger } = recordingLogger();
    const cleared: string[] = [];
    const t = makeDeps({
      logger,
      writeMetric: () => {},
      // 2 permanent per 5 recipients = 40%: each batch is far under the 20-permanent minimum.
      push: new FakePush((m) =>
        m.data['checkInToken'] === 'ct0' || m.data['checkInToken'] === 'ct1'
          ? dead(m)
          : { ok: true },
      ),
      onDeadTarget: async (id) => void cleared.push(id),
    });
    await seedAlert(t.alerts);
    for (let i = 0; i < 60; i++) {
      const b = makeBatch({ batchKey: `k:${i}` }, 5);
      await processBatch(b, t.deps);
    }
    await new Promise((r) => setTimeout(r, 20));
    // The window reaches 200 results at batch 40 and opens. Before the fix the 78 dead targets of
    // batches 1-39 had already been cleared; now they were held, and are re-queued as retries.
    expect(cleared).toEqual([]);
    const opened = lines.filter((l) => l.msg === 'delivery.permanent_breaker_open');
    expect(opened).toHaveLength(21); // batches 40-60
    expect(opened[0]?.fields).toMatchObject({ reason: 'rolling_rate' });
    const requeued = lines.filter((l) => l.msg === 'sender.dead_targets_requeued');
    expect(requeued).toHaveLength(1);
    expect(requeued[0]?.fields).toMatchObject({ count: 39 * 2, reason: 'breaker_open' });
    await waitFor(
      () =>
        t.sendQueue.enqueued
          .flatMap((e) => e.batches)
          .filter((b) => b.batchKey.endsWith(':held:r1'))
          .reduce((n, b) => n + b.recipients.length, 0) ===
        39 * 2,
    );
  });

  it('a message-scoped permanent failure (payload too large) is never cleared, retried or counted toward the breaker', async () => {
    const { lines, logger } = recordingLogger();
    const cleared: string[] = [];
    const t = makeDeps({
      logger,
      push: new FakePush(() => ({
        ok: false,
        kind: 'permanent',
        scope: 'message',
        error: 'Apns413:PayloadTooLarge',
      })),
      onDeadTarget: async (id) => void cleared.push(id),
    });
    await seedAlert(t.alerts);
    const out = await processBatch(makeBatch({}, 10), t.deps);
    await new Promise((r) => setTimeout(r, 20));
    expect(out).toMatchObject({ ack: true, retried: 0, gaveUp: 10, permanentFailed: 0 });
    expect(cleared).toEqual([]);
    expect(t.sendQueue.enqueued).toHaveLength(0);
    expect(lines.some((l) => l.msg === 'delivery.permanent_breaker_open')).toBe(false);
    expect(lines.find((l) => l.msg === 'delivery.payload_too_large')?.fields).toMatchObject({
      count: 10,
    });
  });
});

describe('asynchronous dead-target cleanup', () => {
  const gate = () => {
    let open!: () => void;
    const promise = new Promise<void>((r) => (open = r));
    return { promise, open };
  };

  it('markSent happens before cleanup completes, and a slow cleanup never delays the next batch', async () => {
    const g = gate();
    const started: string[] = [];
    const order: string[] = [];
    const t = makeDeps({
      push: new FakePush((m) =>
        m.data['checkInToken'] === 'ct0'
          ? { ok: false, kind: 'permanent', error: 'x' }
          : { ok: true },
      ),
      onDeadTarget: async (id) => {
        started.push(id);
        await g.promise; // a very slow cleanup (3 Data API calls per token)
        order.push('cleanup-done');
      },
    });
    clearAtOnce(t.deps); // about the cleanup queue, not the hold
    const realSent = t.idempotency.markSent.bind(t.idempotency);
    t.idempotency.markSent = async (k, e) => (order.push('markSent'), realSent(k, e));
    await seedAlert(t.alerts);

    const b1 = makeBatch({ batchKey: 'k:1' }, 20);
    await processBatch(b1, t.deps); // resolves although cleanup is stuck
    expect(order).toEqual(['markSent']);
    expect(await t.idempotency.claim('k:1', new Date(t.state.now.getTime() + 1))).toBe('sent');
    await waitFor(() => started.length === 1);

    // The next batch still completes at once while the cleanup is blocked.
    const b2 = makeBatch({ batchKey: 'k:2' }, 20);
    const out2 = await processBatch(b2, t.deps);
    expect(out2.ack).toBe(true);
    expect(order).toEqual(['markSent', 'markSent']);

    g.open();
    await waitFor(() => order.filter((o) => o === 'cleanup-done').length === 2);
  });

  it('runs 4 cleanups at a time, queues up to 10,000, drops the rest with a counted metric, and flushes', async () => {
    const g = gate();
    let running = 0;
    let peak = 0;
    let done = 0;
    const metrics: string[] = [];
    const q = new DeadTargetQueue({
      logger: { info() {}, warn() {}, error() {} },
      writeMetric: (l) => metrics.push(l),
      process: async () => {
        running++;
        peak = Math.max(peak, running);
        await g.promise;
        running--;
        done++;
      },
    });
    const target = { platform: 'ios', token: 't' } as const;
    let accepted = 0;
    for (let i = 0; i < 10_050; i++) if (q.enqueue(`d${i}`, target)) accepted++;
    expect(accepted).toBe(10_000);
    q.flushMetrics();
    expect(JSON.parse(metrics[0] ?? '{}')).toMatchObject({ DeadTargetDropped: 50 });

    await waitFor(() => running === 4);
    expect(peak).toBe(4);
    // A flush that cannot finish in time reports what is left rather than hanging.
    expect(await q.flush(30)).toBeGreaterThan(0);
    g.open();
    await waitFor(() => done === 10_000, 10_000);
    expect(peak).toBe(4);
    expect(q.queued).toBe(0);
  });

  it('flush on graceful shutdown waits for pending cleanups', async () => {
    const cleared: string[] = [];
    const q = new DeadTargetQueue({
      logger: { info() {}, warn() {}, error() {} },
      writeMetric: () => {},
      process: async (id) => {
        await new Promise((r) => setTimeout(r, 5));
        cleared.push(id);
      },
    });
    for (let i = 0; i < 20; i++) q.enqueue(`d${i}`, { platform: 'ios', token: 't' });
    expect(await q.flush(5000)).toBe(0);
    expect(cleared).toHaveLength(20);
  });

  it('a failing cleanup is logged and does not stop the queue', async () => {
    const { lines, logger } = recordingLogger();
    const cleared: string[] = [];
    const q = new DeadTargetQueue({
      logger,
      writeMetric: () => {},
      process: async (id) => {
        if (id === 'bad') throw new Error('db down tok123');
        cleared.push(id);
      },
    });
    q.enqueue('bad', { platform: 'ios', token: 't' });
    q.enqueue('good', { platform: 'ios', token: 't' });
    await q.flush(1000);
    expect(cleared).toEqual(['good']);
    expect(lines.find((l) => l.msg === 'sender.clear_dead_target_failed')).toBeTruthy();
    expect(JSON.stringify(lines)).not.toContain('tok123');
  });
});
