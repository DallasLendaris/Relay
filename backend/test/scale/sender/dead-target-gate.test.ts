import { describe, expect, it } from 'vitest';
import type { NativePushResult, PushTarget, SendBatch } from '../../../src/scale/contracts.js';
import { PermanentBreaker } from '../../../src/scale/sender/breaker.js';
import { DEAD_TARGET_HOLD_MS, DeadTargetGate } from '../../../src/scale/sender/dead-target-gate.js';
import { DeadTargetQueue } from '../../../src/scale/sender/dead-target-queue.js';
import { processBatch } from '../../../src/scale/sender/sender.js';
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

/** Sender deps with a real breaker, queue and gate (manual hold timer), recording what is cleared. */
async function setup(isDead: (token: string) => boolean) {
  const { lines, logger } = recordingLogger();
  const metrics: string[] = [];
  const cleared: string[] = [];
  const timer = manualTimer();
  const t = makeDeps({
    logger,
    writeMetric: (l) => metrics.push(l),
    push: new FakePush((m) => (isDead(m.target.token) ? BAD : { ok: true })),
    onDeadTarget: async (id) => void cleared.push(id),
  });
  const breaker = new PermanentBreaker();
  const queue = new DeadTargetQueue({
    process: t.deps.onDeadTarget,
    logger,
    writeMetric: () => {},
  });
  const gate = new DeadTargetGate({
    breaker,
    queue,
    logger,
    writeMetric: (l) => metrics.push(l),
    setTimer: timer.setTimer,
  });
  t.deps.breaker = breaker;
  t.deps.deadTargets = queue;
  t.deps.deadTargetGate = gate;
  await seedAlert(t.alerts);
  return { t, lines, metrics, cleared, timer, breaker, queue, gate };
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

describe('dead-target hold: misconfiguration with small batches', () => {
  it('40% BadDeviceToken from the very first result, batches of 2-5: zero tokens cleared', async () => {
    const random = rng(7);
    const decide = rng(99);
    const verdicts = new Map<string, boolean>(); // each token decided once, at its first send
    const dead = new Set<string>();
    const s = await setup((tok) => {
      if (!verdicts.has(tok)) verdicts.set(tok, decide() < 0.4);
      if (verdicts.get(tok)) dead.add(tok);
      return verdicts.get(tok) === true;
    });
    const batches = smallBatches(600, random); // about 2,100 results
    for (const b of batches) await processBatch(b, s.t.deps);
    s.gate.shutdown();
    expect(await s.queue.flush(1000)).toBe(0);

    expect(dead.size).toBeGreaterThan(500);
    expect(s.cleared).toEqual([]);
    // The held clears were dropped as soon as the breaker had its 200 results.
    const dropped = s.lines.filter((l) => l.msg === 'sender.dead_targets_dropped');
    expect(dropped.length).toBeGreaterThanOrEqual(1);
    expect(dropped[0]?.fields).toMatchObject({ reason: 'breaker_open' });
    expect(Number(dropped[0]?.fields?.['count'])).toBeGreaterThan(50);
    expect(Number(dropped[0]?.fields?.['samples'])).toBeGreaterThanOrEqual(200);
    expect(s.metrics.some((m) => m.includes('"DeadTargetHeldDropped"'))).toBe(true);
    // From then on the breaker is open and the "dead" recipients are retried instead.
    expect(s.lines.some((l) => l.msg === 'delivery.permanent_breaker_open')).toBe(true);
    expect(s.t.sendQueue.enqueued.length).toBeGreaterThan(0);
    expect(s.gate.heldCount).toBe(0);
  });

  it('a 100% misconfiguration in tiny batches is dropped at 200 results, never cleared', async () => {
    const s = await setup(() => true);
    for (const b of smallBatches(100, rng(3))) await processBatch(b, s.t.deps);
    await new Promise((r) => setTimeout(r, 20));
    expect(s.cleared).toEqual([]);
    expect(s.lines.find((l) => l.msg === 'sender.dead_targets_dropped')?.fields).toMatchObject({
      reason: 'breaker_open',
    });
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
    expect(s.lines.some((l) => l.msg === 'sender.dead_targets_dropped')).toBe(false);
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

  it('little traffic that looks systemic (>= 20 permanents, > 20%) is dropped on timeout', async () => {
    // 8 batches of 4 with 3 dead each: 24 of 32 permanent, but no batch reaches 20 permanents.
    const s = await setup((tok) => Number(tok.slice(1)) % 4 !== 0);
    for (const b of smallBatches(8, () => 0.6)) await processBatch(b, s.t.deps);
    expect(s.lines.some((l) => l.msg === 'delivery.permanent_breaker_open')).toBe(false);
    s.timer.fire();
    await new Promise((r) => setTimeout(r, 20));
    expect(s.cleared).toEqual([]);
    expect(s.lines.find((l) => l.msg === 'sender.dead_targets_dropped')?.fields).toMatchObject({
      count: 24,
      reason: 'hold_timeout',
    });
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
