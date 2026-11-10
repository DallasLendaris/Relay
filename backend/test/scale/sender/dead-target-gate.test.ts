import { describe, expect, it } from 'vitest';
import type { NativePushResult, PushTarget, SendBatch } from '../../../src/scale/contracts.js';
import { PermanentBreaker } from '../../../src/scale/sender/breaker.js';
import { DEAD_TARGET_HOLD_MS } from '../../../src/scale/sender/dead-target-gate.js';
import { DeadTargetQueue } from '../../../src/scale/sender/dead-target-queue.js';
import {
  RETRY_BUDGET_MS,
  createDeadTargetGate,
  heldRetryKey,
  processBatch,
} from '../../../src/scale/sender/sender.js';
import {
  FakePush,
  makeBatch,
  makeDeps,
  manualTimer,
  recordingLogger,
  seedAlert,
  waitFor,
} from './fakes.js';

/** Deterministic PRNG (mulberry32) so the batch sizes and dead tokens are reproducible. */
function rng(seed: number): () => number {
  let a = seed;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const BAD: NativePushResult = { ok: false, kind: 'permanent', error: 'Apns400:BadDeviceToken' };

/**
 * Sender deps with a real breaker, cleanup queue and the production gate (manual hold timer,
 * re-queueing through t.sendQueue), recording what is cleared and every successful push.
 */
async function setup(isDead: (token: string) => boolean) {
  const { lines, logger } = recordingLogger();
  const metrics: string[] = [];
  const cleared: string[] = [];
  const delivered = new Map<string, number>();
  const timer = manualTimer();
  const t = makeDeps({
    logger,
    writeMetric: (l) => metrics.push(l),
    push: new FakePush((m) => {
      if (isDead(m.target.token)) return BAD;
      delivered.set(m.target.token, (delivered.get(m.target.token) ?? 0) + 1);
      return { ok: true };
    }),
    onDeadTarget: async (id) => void cleared.push(id),
  });
  const breaker = new PermanentBreaker();
  const queue = new DeadTargetQueue({
    process: t.deps.onDeadTarget,
    logger,
    writeMetric: () => {},
  });
  t.deps.breaker = breaker;
  t.deps.deadTargets = queue;
  const gate = createDeadTargetGate(t.deps, { setTimer: timer.setTimer });
  t.deps.deadTargetGate = gate;
  await seedAlert(t.alerts);
  return { t, lines, metrics, cleared, delivered, timer, breaker, queue, gate };
}

/** Small batches (2-5 recipients) with unique devices, as most real batches are. */
function smallBatches(count: number, random: () => number): SendBatch[] {
  const out: SendBatch[] = [];
  let dev = 0;
  for (let b = 0; b < count; b++) {
    const size = 2 + Math.floor(random() * 4);
    const base = makeBatch({ batchKey: `a:warning:cell${b}:0` }, size);
    out.push({
      ...base,
      recipients: base.recipients.map((r) => {
        const id = `d${dev++}`;
        return { ...r, deviceId: id, target: { platform: 'ios', token: id } as PushTarget };
      }),
    });
  }
  return out;
}

type Setup = Awaited<ReturnType<typeof setup>>;

/** Processes everything on the send queue (retries included) until it is empty. */
async function drain(s: Setup): Promise<number> {
  let n = 0;
  while (s.t.sendQueue.enqueued.length > 0) {
    const e = s.t.sendQueue.enqueued.shift();
    for (const b of e?.batches ?? []) {
      await processBatch(b, s.t.deps);
      n++;
    }
    await s.gate.settle(1000);
  }
  return n;
}

const heldBatches = (s: Setup) =>
  s.t.sendQueue.enqueued.flatMap((e) =>
    e.batches.filter((b) => b.batchKey.includes(':held:')).map((b) => ({ ...e, batch: b })),
  );

describe('dead-target hold: misconfiguration with small batches', () => {
  it('40% BadDeviceToken, batches of 2-5: zero cleared, every held recipient re-queued, and delivered exactly once after the fix', async () => {
    const decide = rng(99);
    const verdicts = new Map<string, boolean>(); // each token decided once, at its first send
    let misconfigured = true;
    const s = await setup((tok) => {
      if (!verdicts.has(tok)) verdicts.set(tok, decide() < 0.4);
      return misconfigured && verdicts.get(tok) === true;
    });
    const batches = smallBatches(600, rng(7)); // about 2,100 results
    for (const b of batches) await processBatch(b, s.t.deps);
    s.gate.shutdown();
    expect(await s.gate.settle(1000)).toBe(0);
    expect(await s.queue.flush(1000)).toBe(0);

    const devices = batches.flatMap((b) => b.recipients);
    const badDevices = devices.filter((r) => verdicts.get(r.target.token));
    expect(badDevices.length).toBeGreaterThan(500);
    // 1. Not one token cleared.
    expect(s.cleared).toEqual([]);

    // 2. Everything held was re-queued as a retry batch, none dropped.
    const requeuedLog = s.lines.filter((l) => l.msg === 'sender.dead_targets_requeued');
    expect(requeuedLog).toHaveLength(1);
    expect(requeuedLog[0]?.fields).toMatchObject({ reason: 'breaker_open' });
    const heldCount = Number(requeuedLog[0]?.fields?.['count']);
    expect(heldCount).toBeGreaterThan(50);
    expect(Number(requeuedLog[0]?.fields?.['samples'])).toBeGreaterThanOrEqual(200);
    const held = heldBatches(s);
    expect(held.reduce((n, h) => n + h.batch.recipients.length, 0)).toBe(heldCount);
    const emf = s.metrics.map((m) => JSON.parse(m) as Record<string, unknown>);
    expect(emf.find((m) => 'DeadTargetHeldRequeued' in m)?.['DeadTargetHeldRequeued']).toBe(
      heldCount,
    );
    expect(emf.some((m) => 'DeadTargetHeldDropped' in m)).toBe(false);
    for (const h of held) {
      // The normal retry scheme on its own `:held` base, same queue and context, budget kept.
      const src = batches.find((b) => h.batch.batchKey.startsWith(`${b.batchKey}:held:`));
      expect(src, h.batch.batchKey).toBeDefined();
      expect(h.batch.batchKey).toBe(`${src?.batchKey}:held:r1`);
      expect(h.batch.attempt).toBe(1);
      expect(h.batch).toMatchObject({
        alertId: src?.alertId,
        sourceId: src?.sourceId,
        relaySeverity: src?.relaySeverity,
        firstAttemptAt: s.t.state.now.toISOString(),
      });
      expect(h.queue).toBe('urgent');
      expect(h.delaySeconds).toBeGreaterThan(0);
      expect(h.batch.recipients.every((r) => verdicts.get(r.target.token))).toBe(true);
    }
    // Every "dead" recipient is on the queue: held ones, or retried by the open breaker.
    const queued = new Set(
      s.t.sendQueue.enqueued
        .flatMap((e) => e.batches.flatMap((b) => b.recipients))
        .map((r) => r.deviceId),
    );
    for (const r of badDevices) expect(queued.has(r.deviceId), r.deviceId).toBe(true);
    expect(s.gate.heldCount).toBe(0);

    // 3. The misconfiguration is fixed: draining the retries delivers everyone exactly once.
    expect([...s.delivered.values()].every((n) => n === 1)).toBe(true);
    misconfigured = false;
    expect(await drain(s)).toBeGreaterThan(0);
    for (const r of devices) expect(s.delivered.get(r.target.token), r.deviceId).toBe(1);
    expect(s.cleared).toEqual([]);
  });

  it('a 100% misconfiguration in tiny batches is re-queued at 200 results, never cleared', async () => {
    const s = await setup(() => true);
    for (const b of smallBatches(100, rng(3))) await processBatch(b, s.t.deps);
    await s.gate.settle(1000);
    await new Promise((r) => setTimeout(r, 20));
    expect(s.cleared).toEqual([]);
    expect(s.lines.find((l) => l.msg === 'sender.dead_targets_requeued')?.fields).toMatchObject({
      reason: 'breaker_open',
    });
    expect(heldBatches(s).length).toBeGreaterThan(0);
  });

  it('a held retry is itself retried on the :held base, never colliding with the batch’s own :r<n>', () => {
    const b = makeBatch({ batchKey: 'a:warning:c:0' });
    expect(heldRetryKey(b)).toBe('a:warning:c:0:held:r1');
    expect(heldRetryKey({ ...b, batchKey: 'a:warning:c:0:r2', attempt: 2 })).toBe(
      'a:warning:c:0:held:r3',
    );
    expect(heldRetryKey({ ...b, batchKey: 'a:warning:c:0:held:r1', attempt: 1 })).toBe(
      'a:warning:c:0:held:r2',
    );
  });

  it('when the re-queue cannot be written, it is retried, then logged and counted as dropped', async () => {
    const s = await setup(() => true);
    s.t.sendQueue.fail = true;
    // Once the breaker opens, those batches throw too (their own retries cannot be written), so
    // the held recipients are decided at shutdown.
    for (const b of smallBatches(100, rng(3))) await processBatch(b, s.t.deps).catch(() => {});
    s.gate.shutdown();
    await s.gate.settle(1000);
    expect(s.cleared).toEqual([]);
    expect(s.lines.some((l) => l.msg === 'sender.held_requeue_failed')).toBe(true);
    const emf = s.metrics.map((m) => JSON.parse(m) as Record<string, unknown>);
    expect(emf.some((m) => Number(m['DeadTargetHeldDropped']) > 0)).toBe(true);
  });

  it('held recipients whose 30-minute retry budget is spent give up (tokens kept)', async () => {
    const s = await setup(() => true);
    const start = s.t.state.now.getTime();
    const batches = smallBatches(100, rng(3)).map((b) => ({
      ...b,
      firstAttemptAt: new Date(start - RETRY_BUDGET_MS + 1000).toISOString(),
    }));
    for (const b of batches) await processBatch(b, s.t.deps);
    await s.gate.settle(1000);
    expect(s.cleared).toEqual([]);
    expect(heldBatches(s)).toHaveLength(0);
    expect(s.lines.some((l) => l.msg === 'delivery.gave_up' && l.fields?.['held'] === true)).toBe(
      true,
    );
  });
});

describe('dead-target hold: normal dead-token churn', () => {
  it('5% dead tokens, batches of 2-5: every dead token is cleared and the breaker never opens', async () => {
    const isDead = (tok: string) => Number(tok.slice(1)) % 20 === 7; // exactly 5%
    const s = await setup(isDead);
    const batches = smallBatches(800, rng(11)); // about 2,800 results
    let results = 0;
    let checkedBound = false;
    for (const b of batches) {
      await processBatch(b, s.t.deps);
      results += b.recipients.length;
      if (!checkedBound && results >= 200) {
        // Bound: once the breaker has 200 results everything held so far is released.
        checkedBound = true;
        expect(s.gate.heldCount).toBe(0);
        const deadSoFar = batches
          .slice(0, batches.indexOf(b) + 1)
          .flatMap((x) => x.recipients)
          .filter((r) => isDead(r.target.token)).length;
        await waitFor(() => s.cleared.length === deadSoFar);
        expect(s.timer.armed).toBe(false);
      }
    }
    expect(await s.queue.flush(2000)).toBe(0);
    const want = batches
      .flatMap((b) => b.recipients)
      .filter((r) => isDead(r.target.token))
      .map((r) => r.deviceId);
    expect(want.length).toBeGreaterThan(100);
    expect(s.cleared.sort()).toEqual(want.sort());
    expect(s.lines.some((l) => l.msg === 'delivery.permanent_breaker_open')).toBe(false);
    expect(s.lines.some((l) => l.msg === 'sender.dead_targets_requeued')).toBe(false);
    expect(heldBatches(s)).toHaveLength(0);
  });

  it('little traffic: held clears are released when the 60 s hold runs out', async () => {
    const s = await setup((tok) => tok === 'd0' || tok === 'd5');
    for (const b of smallBatches(3, () => 0.6)) await processBatch(b, s.t.deps); // 3 x 4 results
    await new Promise((r) => setTimeout(r, 20));
    expect(s.cleared).toEqual([]);
    expect(s.gate.heldCount).toBe(2);
    expect(s.timer.scheduled).toEqual([DEAD_TARGET_HOLD_MS]); // one timer, from the first hold
    s.timer.fire();
    await waitFor(() => s.cleared.length === 2);
    expect(s.cleared.sort()).toEqual(['d0', 'd5']);
    expect(s.lines.find((l) => l.msg === 'sender.dead_targets_released')?.fields).toMatchObject({
      count: 2,
      reason: 'hold_timeout',
    });
    // The next dead target starts a new hold: the breaker still has too few results.
    expect(s.timer.armed).toBe(false);
  });

  it('little traffic that looks systemic (>= 20 permanents, > 20%) is re-queued on timeout', async () => {
    // 8 batches of 4 with 3 dead each: 24 of 32 permanent, but no batch reaches 20 permanents.
    const s = await setup((tok) => Number(tok.slice(1)) % 4 !== 0);
    for (const b of smallBatches(8, () => 0.6)) await processBatch(b, s.t.deps);
    expect(s.lines.some((l) => l.msg === 'delivery.permanent_breaker_open')).toBe(false);
    s.timer.fire();
    expect(await s.gate.settle(1000)).toBe(0);
    expect(s.cleared).toEqual([]);
    expect(s.lines.find((l) => l.msg === 'sender.dead_targets_requeued')?.fields).toMatchObject({
      count: 24,
      reason: 'hold_timeout',
    });
    expect(heldBatches(s).reduce((n, h) => n + h.batch.recipients.length, 0)).toBe(24);
  });

  it('shutdown decides at once, so the final queue flush includes the released clears', async () => {
    const s = await setup((tok) => tok === 'd1');
    for (const b of smallBatches(2, () => 0)) await processBatch(b, s.t.deps);
    expect(s.gate.heldCount).toBe(1);
    s.gate.shutdown();
    expect(s.timer.armed).toBe(false);
    expect(await s.queue.flush(1000)).toBe(0);
    expect(s.cleared).toEqual(['d1']);
  });
});
