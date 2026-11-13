import { describe, expect, it } from 'vitest';
import type { NativePushResult, PushTarget, SendBatch } from '../../../src/scale/contracts.js';
import {
  PermanentBreaker,
  isSystemic,
  platformKeyOf,
  wilsonLower,
} from '../../../src/scale/sender/breaker.js';
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
  clockScheduler,
  makeBatch,
  makeDeps,
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
const HOLD = DEAD_TARGET_HOLD_MS;

/**
 * Sender deps with a real breaker, cleanup queue and the production gate on the test clock
 * (re-queueing through t.sendQueue), recording what is cleared (and when) and every success.
 */
async function setup(isDead: (target: PushTarget) => boolean, opts: { maxHeld?: number } = {}) {
  const { lines, logger } = recordingLogger();
  const metrics: string[] = [];
  const cleared: string[] = [];
  const clearedAt = new Map<string, number>();
  const delivered = new Map<string, number>();
  const t = makeDeps({
    logger,
    writeMetric: (l) => metrics.push(l),
    push: new FakePush((m) => {
      if (isDead(m.target)) return BAD;
      delivered.set(m.target.token, (delivered.get(m.target.token) ?? 0) + 1);
      return { ok: true };
    }),
    onDeadTarget: async (id) => {
      cleared.push(id);
      clearedAt.set(id, t.state.now.getTime());
    },
  });
  const sched = clockScheduler(t.state);
  const breaker = new PermanentBreaker();
  const queue = new DeadTargetQueue({
    process: t.deps.onDeadTarget,
    logger,
    writeMetric: () => {},
  });
  t.deps.breaker = breaker;
  t.deps.deadTargets = queue;
  const gate = createDeadTargetGate(t.deps, {
    setTimer: sched.setTimer,
    ...(opts.maxHeld !== undefined ? { maxHeld: opts.maxHeld } : {}),
  });
  t.deps.deadTargetGate = gate;
  await seedAlert(t.alerts);
  return { t, lines, metrics, cleared, clearedAt, delivered, sched, breaker, queue, gate };
}
type Setup = Awaited<ReturnType<typeof setup>>;

let devSeq = 0;
/** Batches of the given sizes, unique devices on `platform` (token = device id). */
function batchesOf(
  sizes: number[],
  prefix = 'd',
  target?: (id: string) => PushTarget,
): SendBatch[] {
  let n = 0;
  return sizes.map((size, b) => {
    const base = makeBatch({ batchKey: `a:warning:${prefix}cell${b}:${devSeq++}` }, size);
    return {
      ...base,
      recipients: base.recipients.map((r) => {
        const id = `${prefix}${n++}`;
        return {
          ...r,
          deviceId: id,
          target: target?.(id) ?? ({ platform: 'ios', token: id } as PushTarget),
        };
      }),
    };
  });
}
const smallSizes = (count: number, random: () => number) =>
  Array.from({ length: count }, () => 2 + Math.floor(random() * 4));

/** Processes everything on the send queue (retries included) until it is empty. */
async function drain(s: Setup): Promise<void> {
  for (let guard = 0; guard < 10_000; guard++) {
    await s.sched.advance(HOLD + 1);
    await s.gate.settle(1000);
    const next = s.t.sendQueue.enqueued.splice(0).flatMap((e) => e.batches);
    if (next.length === 0 && s.gate.heldCount === 0) return;
    for (const b of next) await processBatch(b, s.t.deps);
  }
  throw new Error('drain did not finish');
}

const queuedDevices = (s: Setup) =>
  new Set(
    s.t.sendQueue.enqueued.flatMap((e) =>
      e.batches.flatMap((b) => b.recipients.map((r) => r.deviceId)),
    ),
  );
const heldBatches = (s: Setup) =>
  s.t.sendQueue.enqueued.flatMap((e) =>
    e.batches.filter((b) => /:h\d+$/.test(b.batchKey)).map((b) => ({ ...e, batch: b })),
  );
const logged = (s: Setup, msg: string) => s.lines.filter((l) => l.msg === msg);

describe('isSystemic: the small-sample rule', () => {
  it('matches the documented examples', () => {
    expect(isSystemic(5, 5)).toBe(true);
    expect(isSystemic(60, 60)).toBe(true);
    expect(isSystemic(8, 20)).toBe(true);
    expect(isSystemic(55, 200)).toBe(true);
    expect(isSystemic(4, 4)).toBe(false); // too few permanents
    expect(isSystemic(7, 20)).toBe(false);
    expect(isSystemic(3, 60)).toBe(false);
    expect(isSystemic(10, 200)).toBe(false);
    expect(wilsonLower(0, 10)).toBe(0);
    expect(wilsonLower(10, 10)).toBeGreaterThan(0.7);
  });

  it('normal churn (5%) essentially never looks systemic in either window', () => {
    const random = rng(1);
    const b = new PermanentBreaker();
    let hits = 0;
    for (let i = 0; i < 100_000; i++) {
      b.platforms.record('ios', random() < 0.05);
      if (b.platforms.systemic('ios')) hits++;
    }
    expect(hits).toBe(0);
  });

  it('platform keys: ios, expo, and android per FCM project', () => {
    expect(platformKeyOf({ platform: 'ios', token: 't' })).toBe('ios');
    expect(platformKeyOf({ platform: 'expo', token: 't' })).toBe('expo');
    expect(platformKeyOf({ platform: 'android', token: 't' })).toBe('android:default');
    expect(platformKeyOf({ platform: 'android', token: 't', project: 'p2' })).toBe('android:p2');
  });
});

describe('review 2 probes', () => {
  it('P-gate-1: 100% misconfiguration on low traffic (60 one-recipient batches): 0 cleared, all 60 re-queued', async () => {
    const s = await setup(() => true);
    const batches = batchesOf(Array.from({ length: 60 }, () => 1));
    for (const b of batches) await processBatch(b, s.t.deps);
    await s.sched.advance(HOLD + 1); // the hold elapses
    await s.gate.settle(1000);
    await s.queue.flush(1000);
    expect(s.cleared).toEqual([]);
    // The first 4 were held (no evidence yet) and re-queued by the gate; from the 5th the
    // platform is systemic and the sender retries them itself.
    expect(heldBatches(s).flatMap((h) => h.batch.recipients)).toHaveLength(4);
    const queued = queuedDevices(s);
    for (const b of batches) expect(queued.has(b.recipients[0]!.deviceId)).toBe(true);
    expect(queued.size).toBe(60);
  });

  it('P-gate-1, slow: one result per minute, each decided before the next arrives: at most 4 cleared (the cold-start bound)', async () => {
    const s = await setup(() => true);
    for (const b of batchesOf(
      Array.from({ length: 60 }, () => 1),
      's',
    )) {
      await processBatch(b, s.t.deps);
      await s.sched.advance(60_000);
    }
    await s.queue.flush(1000);
    expect(s.cleared).toHaveLength(4);
  });

  it('P-gate-2: misconfiguration after a healthy window of 1,000, one-recipient batches: 0 cleared', async () => {
    let bad = false;
    const s = await setup((t) => bad && t.token.startsWith('m'));
    await processBatch(batchesOf([1000], 'o')[0]!, s.t.deps);
    bad = true;
    for (const b of batchesOf(
      Array.from({ length: 300 }, () => 1),
      'm',
    )) {
      await processBatch(b, s.t.deps);
    }
    await s.sched.advance(HOLD + 1);
    await s.gate.settle(1000);
    await s.queue.flush(1000);
    expect(s.cleared).toEqual([]);
    // 7 were held (the short window needs 8 of 20) and re-queued; the rest the sender retried.
    expect(heldBatches(s).flatMap((h) => h.batch.recipients)).toHaveLength(7);
    expect(queuedDevices(s).size).toBe(300);
  });

  it('P-gate-2, slow onset: one result per minute after the healthy window: at most 7 cleared (the stated bound)', async () => {
    let bad = false;
    const s = await setup((t) => bad && t.token.startsWith('n'));
    await processBatch(batchesOf([1000], 'o')[0]!, s.t.deps);
    bad = true;
    for (const b of batchesOf(
      Array.from({ length: 40 }, () => 1),
      'n',
    )) {
      await processBatch(b, s.t.deps);
      await s.sched.advance(60_000);
    }
    await s.queue.flush(1000);
    expect(s.cleared.length).toBeLessThanOrEqual(7);
    expect(s.cleared).toHaveLength(7);
  });
});

describe('a misconfiguration fixed before the hold ends', () => {
  it('held clears are still re-queued, not cleared, although the window has healed by then', async () => {
    let bad = true;
    const s = await setup((t) => bad && t.token.startsWith('h'));
    // 4 "dead" (held: below the 5-permanent minimum), then 6 more make iOS systemic.
    for (const b of batchesOf(
      Array.from({ length: 10 }, () => 1),
      'h',
    )) {
      await processBatch(b, s.t.deps);
    }
    expect(s.gate.heldCount).toBe(4);
    // The fix lands within the hold; 400 healthy results heal both windows.
    bad = false;
    await processBatch(batchesOf([400], 'k')[0]!, s.t.deps);
    expect(s.breaker.platforms.systemic('ios')).toBe(false);
    await s.sched.advance(HOLD + 1);
    await s.gate.settle(1000);
    await s.queue.flush(1000);
    expect(s.cleared).toEqual([]);
    expect(heldBatches(s).flatMap((h) => h.batch.recipients)).toHaveLength(4);
  });
});

describe('per-platform breaker', () => {
  it('iOS-only misconfiguration at 10% of traffic: the iOS gate trips, FCM is unaffected', async () => {
    // Batches of 10: one iOS recipient (APNs key wrong: always "dead"), nine Android (2% truly dead).
    const androidDead = (tok: string) => Number(tok.slice(1)) % 50 === 3;
    const s = await setup((t) => t.platform === 'ios' || androidDead(t.token));
    const batches = batchesOf(
      Array.from({ length: 200 }, () => 10),
      'x',
      (id) =>
        Number(id.slice(1)) % 10 === 0
          ? { platform: 'ios', token: id }
          : { platform: 'android', token: id },
    );
    for (const b of batches) await processBatch(b, s.t.deps);
    await s.sched.advance(HOLD + 1);
    await s.gate.settle(1000);
    await s.queue.flush(2000);

    const all = batches.flatMap((b) => b.recipients);
    const ios = all.filter((r) => r.target.platform === 'ios');
    const fcmDead = all.filter(
      (r) => r.target.platform === 'android' && androidDead(r.target.token),
    );
    expect(ios).toHaveLength(200);
    expect(fcmDead.length).toBeGreaterThan(20);
    // The process-wide rules never opened (about 12% permanent overall)...
    expect(logged(s, 'delivery.permanent_breaker_open')).toHaveLength(0);
    // ...but the iOS one did: no iOS token cleared, every iOS recipient queued for retry.
    expect(logged(s, 'delivery.platform_breaker_open').length).toBeGreaterThan(0);
    const cleared = new Set(s.cleared);
    for (const r of ios) expect(cleared.has(r.deviceId), r.deviceId).toBe(false);
    const queued = queuedDevices(s);
    for (const r of ios) expect(queued.has(r.deviceId), r.deviceId).toBe(true);
    // FCM: every genuinely dead token cleared, nothing else.
    expect([...cleared].sort()).toEqual(fcmDead.map((r) => r.deviceId).sort());
    expect(s.breaker.platforms.systemic('ios')).toBe(true);
    expect(s.breaker.platforms.systemic('android:default')).toBe(false);
  });

  it('one misconfigured FCM project does not stop clears for another', async () => {
    const s = await setup((t) => t.project === 'p2' || t.token.endsWith('7'));
    const batches = batchesOf(
      Array.from({ length: 50 }, () => 4),
      'f',
      (id) => ({
        platform: 'android',
        token: id,
        project: Number(id.slice(1)) % 2 === 0 ? 'p1' : 'p2',
      }),
    );
    for (const b of batches) await processBatch(b, s.t.deps);
    await s.sched.advance(HOLD + 1);
    await s.gate.settle(1000);
    await s.queue.flush(1000);
    const all = batches.flatMap((b) => b.recipients);
    const p1Dead = all.filter((r) => r.target.project === 'p1' && r.target.token.endsWith('7'));
    const p2 = all.filter((r) => r.target.project === 'p2');
    const cleared = new Set(s.cleared);
    for (const r of p2) expect(cleared.has(r.deviceId)).toBe(false);
    expect([...cleared].sort()).toEqual(p1Dead.map((r) => r.deviceId).sort());
  });
});

describe('normal dead-token churn', () => {
  it('5% dead on low traffic (one push a second): every dead token cleared within the hold + 1 s, no breaker', async () => {
    const isDead = (t: PushTarget) => Number(t.token.slice(1)) % 20 === 7;
    const s = await setup(isDead);
    const failedAt = new Map<string, number>();
    const batches = batchesOf(
      Array.from({ length: 400 }, () => 1),
      'l',
    );
    for (const b of batches) {
      const r = b.recipients[0]!;
      if (isDead(r.target)) failedAt.set(r.deviceId, s.t.state.now.getTime());
      await processBatch(b, s.t.deps);
      await s.sched.advance(1000);
    }
    await s.sched.advance(HOLD + 1000);
    await s.queue.flush(1000);
    expect(failedAt.size).toBe(20);
    expect(s.cleared.sort()).toEqual([...failedAt.keys()].sort());
    for (const [id, at] of failedAt) {
      const delay = (s.clearedAt.get(id) ?? Infinity) - at;
      expect(delay, id).toBeGreaterThanOrEqual(HOLD);
      expect(delay, id).toBeLessThanOrEqual(HOLD + 1000);
    }
    expect(logged(s, 'delivery.permanent_breaker_open')).toHaveLength(0);
    expect(logged(s, 'delivery.platform_breaker_open')).toHaveLength(0);
    expect(logged(s, 'sender.dead_targets_requeued')).toHaveLength(0);
  });

  it('5% dead, batches of 2-5: every dead token cleared after the hold, nothing re-queued', async () => {
    const isDead = (t: PushTarget) => Number(t.token.slice(1)) % 20 === 7;
    const s = await setup(isDead);
    const batches = batchesOf(smallSizes(800, rng(11)), 'c');
    for (const b of batches) await processBatch(b, s.t.deps);
    await new Promise((r) => setTimeout(r, 10));
    expect(s.cleared).toEqual([]); // held: nothing is cleared before the hold elapses
    await s.sched.advance(HOLD + 1);
    await s.queue.flush(2000);
    const want = batches.flatMap((b) => b.recipients).filter((r) => isDead(r.target));
    expect(want.length).toBeGreaterThan(50);
    expect(s.cleared.sort()).toEqual(want.map((r) => r.deviceId).sort());
    expect(heldBatches(s)).toHaveLength(0);
  });

  it('held memory is bounded: past maxHeld the oldest are decided early, none lost', async () => {
    const isDead = (t: PushTarget) => Number(t.token.slice(1)) % 20 === 7;
    const s = await setup(isDead, { maxHeld: 25 });
    const batches = batchesOf(smallSizes(800, rng(12)), 'b');
    for (const b of batches) {
      await processBatch(b, s.t.deps);
      expect(s.gate.heldCount).toBeLessThanOrEqual(25 + 5); // the bound plus one batch
    }
    expect(s.gate.peakHeld).toBeLessThanOrEqual(30);
    expect(
      logged(s, 'sender.dead_targets_released').some((l) => l.fields?.['reason'] === 'overflow'),
    ).toBe(true);
    await s.sched.advance(HOLD + 1);
    await s.queue.flush(2000);
    const want = batches.flatMap((b) => b.recipients).filter((r) => isDead(r.target));
    expect(s.cleared.sort()).toEqual(want.map((r) => r.deviceId).sort());
  });

  it('shutdown decides everything held at once', async () => {
    const s = await setup((t) => t.token === 'z1');
    for (const b of batchesOf([2, 2], 'z')) await processBatch(b, s.t.deps);
    expect(s.gate.heldCount).toBe(1);
    s.gate.shutdown();
    expect(s.gate.heldCount).toBe(0);
    expect(await s.queue.flush(1000)).toBe(0);
    expect(s.cleared).toEqual(['z1']);
  });
});

describe('misconfiguration with small batches', () => {
  it('40% BadDeviceToken, batches of 2-5: zero cleared, all queued, delivered exactly once after the fix', async () => {
    const decide = rng(99);
    const verdicts = new Map<string, boolean>(); // each token decided once, at its first send
    let misconfigured = true;
    const s = await setup((t) => {
      if (!verdicts.has(t.token)) verdicts.set(t.token, decide() < 0.4);
      return misconfigured && verdicts.get(t.token) === true;
    });
    const batches = batchesOf(smallSizes(600, rng(7)), 'q');
    for (const b of batches) await processBatch(b, s.t.deps);
    await s.sched.advance(HOLD + 1);
    expect(await s.gate.settle(1000)).toBe(0);
    await s.queue.flush(1000);

    const devices = batches.flatMap((b) => b.recipients);
    const bad = devices.filter((r) => verdicts.get(r.target.token));
    expect(bad.length).toBeGreaterThan(500);
    expect(s.cleared).toEqual([]);
    const held = heldBatches(s);
    expect(held.length).toBeGreaterThan(0);
    const emf = s.metrics.map((m) => JSON.parse(m) as Record<string, unknown>);
    const heldCount = held.reduce((n, h) => n + h.batch.recipients.length, 0);
    expect(emf.find((m) => 'DeadTargetHeldRequeued' in m)?.['DeadTargetHeldRequeued']).toBe(
      heldCount,
    );
    expect(emf.some((m) => 'DeadTargetHeldDropped' in m)).toBe(false);
    for (const h of held) {
      const src = batches.find((b) => h.batch.batchKey === `${b.batchKey}:h1`);
      expect(src, h.batch.batchKey).toBeDefined();
      expect(h.batch).toMatchObject({
        attempt: 1,
        alertId: src?.alertId,
        sourceId: src?.sourceId,
        relaySeverity: src?.relaySeverity,
        firstAttemptAt: new Date(s.t.state.now.getTime() - HOLD - 1).toISOString(),
      });
      expect(h.queue).toBe('urgent');
      expect(h.delaySeconds).toBeGreaterThan(0);
    }
    const queued = queuedDevices(s);
    for (const r of bad) expect(queued.has(r.deviceId), r.deviceId).toBe(true);

    misconfigured = false;
    await drain(s);
    for (const r of devices) expect(s.delivered.get(r.target.token), r.deviceId).toBe(1);
    expect(s.cleared).toEqual([]);
  });

  it('a stale closed verdict: the platform turns systemic while a batch waits in markSent (t5 d)', async () => {
    let misconfigured = true;
    const looksDead = (t: PushTarget) =>
      t.token.startsWith('A')
        ? t.token === 'A0' || t.token === 'A1'
        : Number(t.token.slice(1)) % 20 < 9;
    const s = await setup((t) => misconfigured && looksDead(t));
    const a = batchesOf([5], 'A')[0]!;
    let unblock!: () => void;
    const blocked = new Promise<void>((r) => (unblock = r));
    let aAtMarkSent = false;
    const realMarkSent = s.t.idempotency.markSent.bind(s.t.idempotency);
    s.t.idempotency.markSent = async (k, e) => {
      if (k === a.batchKey) {
        aAtMarkSent = true;
        await blocked;
      }
      return realMarkSent(k, e);
    };
    const pa = processBatch(a, s.t.deps);
    await waitFor(() => aAtMarkSent);
    const others = batchesOf(smallSizes(70, rng(21)), 'o');
    for (const b of others) await processBatch(b, s.t.deps);
    expect(s.breaker.platforms.systemic('ios')).toBe(true);
    unblock();
    expect(await pa).toMatchObject({ ack: true, permanentFailed: 2 }); // A's own (stale) view
    await s.sched.advance(HOLD + 1);
    expect(await s.gate.settle(1000)).toBe(0);
    await s.queue.flush(1000);
    expect(s.cleared).toEqual([]);
    const aHeld = heldBatches(s).filter((h) => h.batch.batchKey === `${a.batchKey}:h1`);
    expect(aHeld.flatMap((h) => h.batch.recipients.map((r) => r.deviceId))).toEqual(['A0', 'A1']);
    misconfigured = false;
    await drain(s);
    for (const r of [a, ...others].flatMap((b) => b.recipients)) {
      expect(s.delivered.get(r.target.token), r.deviceId).toBe(1);
    }
  });

  it('a held re-queue key appends `:h<attempt+1>` to the whole key, never stripping anything', () => {
    const b = makeBatch({ batchKey: 'a:warning:c:0' });
    expect(heldRetryKey(b)).toBe('a:warning:c:0:h1');
    expect(heldRetryKey({ ...b, batchKey: 'a:warning:c:0:r2', attempt: 2 })).toBe(
      'a:warning:c:0:r2:h3',
    );
    expect(heldRetryKey({ ...b, batchKey: 'a:warning:c:0:h1', attempt: 1 })).toBe(
      'a:warning:c:0:h1:h2',
    );
  });

  it('when the re-queue cannot be written, it is retried, then logged and counted as dropped', async () => {
    const s = await setup(() => true);
    // 4 dead (below the 5-permanent minimum) are held; the 5th makes iOS systemic, so at release
    // the 4 are re-queued, and the send queue is down by then.
    for (const b of batchesOf([4], 'v')) await processBatch(b, s.t.deps);
    s.t.sendQueue.fail = true;
    for (const b of batchesOf([1], 'u')) await processBatch(b, s.t.deps).catch(() => {});
    await s.sched.advance(HOLD + 1);
    await s.gate.settle(1000);
    expect(s.cleared).toEqual([]);
    expect(logged(s, 'sender.held_requeue_failed').length).toBeGreaterThan(0);
    const emf = s.metrics.map((m) => JSON.parse(m) as Record<string, unknown>);
    expect(emf.some((m) => Number(m['DeadTargetHeldDropped']) > 0)).toBe(true);
  });

  it('held recipients whose 30-minute retry budget is spent give up (tokens kept)', async () => {
    const s = await setup(() => true);
    const start = s.t.state.now.getTime();
    const batches = batchesOf(
      Array.from({ length: 10 }, () => 1),
      'g',
    ).map((b) => ({
      ...b,
      firstAttemptAt: new Date(start - RETRY_BUDGET_MS + HOLD + 1000).toISOString(),
    }));
    for (const b of batches) await processBatch(b, s.t.deps);
    await s.sched.advance(HOLD + 1);
    await s.gate.settle(1000);
    expect(s.cleared).toEqual([]);
    expect(heldBatches(s)).toHaveLength(0);
    expect(logged(s, 'delivery.gave_up').some((l) => l.fields?.['held'] === true)).toBe(true);
  });
});
