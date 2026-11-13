/**
 * Reproduction of the t5 c (full scale) held-clear loop, in process: memory adapters, a fake
 * clock, many concurrent batches of ~14 recipients over 10 platform keys (ios, expo and 8 FCM
 * projects), per-project FCM throttling that answers 429 (so many recipients go through `:r`
 * retries), 5% dead tokens chosen by a hash, then fresh traffic ends.
 *
 * Expected: every dead token is cleared about DEAD_TARGET_HOLD_MS after it is found, so all of
 * them within the hold plus a retry of the end of traffic; no live token is cleared; every live
 * device gets exactly one push.
 */
import { describe, expect, it } from 'vitest';
import type {
  NativePushMessage,
  NativePushResult,
  PushTarget,
  SendBatch,
  SendQueue,
  SendQueueName,
} from '../../../src/scale/contracts.js';
import { PermanentBreaker, PlatformWindows } from '../../../src/scale/sender/breaker.js';
import { DEAD_TARGET_HOLD_MS } from '../../../src/scale/sender/dead-target-gate.js';
import { DeadTargetQueue } from '../../../src/scale/sender/dead-target-queue.js';
import { createDeadTargetGate, processBatch } from '../../../src/scale/sender/sender.js';
import { clockScheduler, makeDeps, seedAlert } from './fakes.js';

const HOLD = DEAD_TARGET_HOLD_MS;
const TICK_MS = 100;
const PROJECTS = Array.from({ length: 8 }, (_, i) => `p${i}`);
const KEYS = ['ios', 'expo', ...PROJECTS.map((p) => `android:${p}`)];

/**
 * FNV-1a plus the murmur3 finalizer, scaled to [0, 1): the "hash" that decides which tokens are
 * dead. The finalizer matters here: plain FNV-1a of sequential ids (d1230, d1231, ...) puts runs of
 * consecutive ids on the same side of 0.05, i.e. dead tokens clustered in batches. (The t5 tokens
 * are random, so plain FNV-1a is fine there.)
 */
function hash01(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) h = Math.imul(h ^ s.charCodeAt(i), 0x01000193);
  h ^= h >>> 16;
  h = Math.imul(h, 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  h ^= h >>> 16;
  return (h >>> 0) / 4294967296;
}

function rng(seed: number): () => number {
  let a = seed;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const targetFor = (key: string, token: string): PushTarget =>
  key.startsWith('android:')
    ? { platform: 'android', token, project: key.slice('android:'.length) }
    : ({ platform: key, token } as PushTarget);

/** SQS on the fake clock: a message becomes visible `delaySeconds` after it was sent. */
class DelayQueue implements SendQueue {
  items: Array<{ at: number; batch: SendBatch }> = [];
  constructor(private readonly now: () => number) {}
  async enqueue(_q: SendQueueName, batches: SendBatch[], opts?: { delaySeconds?: number }) {
    for (const batch of batches) {
      this.items.push({ at: this.now() + (opts?.delaySeconds ?? 0) * 1000, batch });
    }
  }
  add(at: number, batch: SendBatch): void {
    this.items.push({ at, batch });
  }
  takeDue(now: number): SendBatch[] {
    const due = this.items.filter((i) => i.at <= now);
    this.items = this.items.filter((i) => i.at > now);
    return due.map((i) => i.batch);
  }
}

interface RunOptions {
  batches: number;
  /** Fresh batches arrive evenly over this long. */
  freshMs: number;
  /** Per FCM project, messages per second (a token bucket of one tick). */
  fcmRatePerSecond: number;
  /** Batches processed at once per tick (7 sender processes x a few in flight). */
  concurrency: number;
  /** Optional extra dead tokens (by device index) on top of the 5% hash. */
  extraDead?: (deviceIndex: number, key: string) => boolean;
  /** Optional: put device `i` on this key instead of the hashed one. */
  keyOf?: (deviceIndex: number) => string | undefined;
  horizonMs: number;
}

async function run(o: RunOptions) {
  const t = makeDeps({ writeMetric: () => {} });
  const start = t.state.now.getTime();
  const now = () => t.state.now.getTime();
  const sched = clockScheduler(t.state);
  const queue = new DelayQueue(now);
  t.deps.sendQueue = queue;
  t.deps.random = rng(5);
  const alert = await seedAlert(t.alerts);

  // Devices and their fate.
  const dead = new Set<string>();
  const live = new Set<string>();
  const delivered = new Map<string, number>();
  const cleared = new Map<string, number>(); // deviceId -> clear time
  let lastLiveDelivery = start;

  // Per-project FCM quota on the fake clock.
  const bucket = new Map<string, { tokens: number; at: number }>();
  const cap = (o.fcmRatePerSecond * TICK_MS) / 1000;
  const take = (project: string): boolean => {
    const b = bucket.get(project) ?? { tokens: cap, at: now() };
    b.tokens = Math.min(cap, b.tokens + ((now() - b.at) * o.fcmRatePerSecond) / 1000);
    b.at = now();
    bucket.set(project, b);
    if (b.tokens < 1) return false;
    b.tokens -= 1;
    return true;
  };
  t.deps.push = {
    async send(messages: NativePushMessage[]): Promise<NativePushResult[]> {
      return messages.map((m): NativePushResult => {
        const tgt = m.target;
        if (tgt.platform === 'android' && !take(tgt.project ?? 'default')) {
          return { ok: false, kind: 'retryable', error: 'Fcm429', retryAfterSeconds: 1 };
        }
        if (dead.has(tgt.token)) {
          return { ok: false, kind: 'permanent', error: 'Fcm404:UNREGISTERED' };
        }
        delivered.set(tgt.token, (delivered.get(tgt.token) ?? 0) + 1);
        lastLiveDelivery = now();
        return { ok: true };
      });
    },
  };
  t.deps.onDeadTarget = async (id) => void cleared.set(id, now());

  const breaker = new PermanentBreaker();
  const cleanup = new DeadTargetQueue({
    process: t.deps.onDeadTarget,
    logger: t.deps.logger,
    writeMetric: () => {},
    concurrency: 64,
  });
  t.deps.breaker = breaker;
  t.deps.deadTargets = cleanup;
  const gate = createDeadTargetGate(t.deps, { setTimer: sched.setTimer });
  t.deps.deadTargetGate = gate;

  // How often each platform window looked systemic (the seed).
  const systemicMoments: Array<{ key: string; at: number }> = [];
  const record = PlatformWindows.prototype.record;
  const windows = breaker.platforms;
  const wasSystemic = new Map<string, boolean>();
  windows.record = function (key: string, permanent: boolean, nowMs?: number) {
    record.call(this, key, permanent, nowMs);
    const s = windows.systemic(key);
    if (s && !wasSystemic.get(key)) systemicMoments.push({ key, at: now() - start });
    wasSystemic.set(key, s);
  };

  let device = 0;
  for (let b = 0; b < o.batches; b++) {
    const recipients: SendBatch['recipients'] = [];
    for (let r = 0; r < 14; r++) {
      const i = device++;
      const id = `d${i}`;
      const key = o.keyOf?.(i) ?? (KEYS[Math.floor(hash01(`k${id}`) * KEYS.length)] as string);
      if (hash01(id) < 0.05 || o.extraDead?.(i, key)) dead.add(id);
      else live.add(id);
      recipients.push({
        deviceId: id,
        target: targetFor(key, id),
        language: 'en',
        checkInToken: `ct${id}`,
      });
    }
    queue.add(start + Math.floor((b * o.freshMs) / o.batches), {
      alertId: alert.id,
      sourceId: alert.sourceId,
      relaySeverity: 'warning',
      batchKey: `${alert.id}:warning:cell${b}:0`,
      recipients,
      attempt: 0,
    });
  }

  let held = 0;
  let heldBatches = 0;
  let allClearedAt: number | undefined;
  while (now() - start < o.horizonMs) {
    const due = queue.takeDue(now());
    for (const b of due) {
      if (/:[hv]\d+/.test(b.batchKey)) {
        heldBatches++;
        held += b.recipients.length;
      }
    }
    for (let i = 0; i < due.length; i += o.concurrency) {
      await Promise.all(due.slice(i, i + o.concurrency).map((b) => processBatch(b, t.deps)));
    }
    await gate.settle(1000);
    await cleanup.flush(1000);
    if (allClearedAt === undefined && cleared.size >= dead.size) allClearedAt = now();
    if (allClearedAt !== undefined && queue.items.length === 0 && gate.heldCount === 0) break;
    await sched.advance(TICK_MS);
  }
  gate.shutdown(); // only so nothing is left armed; whatever it decides now is not counted
  return {
    start,
    dead,
    live,
    delivered,
    cleared,
    lastLiveDelivery,
    allClearedAt,
    held,
    heldBatches,
    systemicMoments,
    pending: queue.items.length,
  };
}

function expectHealthy(r: Awaited<ReturnType<typeof run>>) {
  for (const id of r.live) expect(r.delivered.get(id), id).toBe(1);
  for (const id of r.cleared.keys()) expect(r.dead.has(id), `live ${id} cleared`).toBe(true);
  const missing = [...r.dead].filter((id) => !r.cleared.has(id));
  expect(missing.length, `dead not cleared (held re-queues: ${r.held})`).toBe(0);
  // Within the hold plus a retry of the end of traffic (the last live delivery).
  expect(r.allClearedAt).toBeDefined();
  expect((r.allClearedAt ?? 0) - r.lastLiveDelivery).toBeLessThanOrEqual(HOLD + 5_000);
}

describe('t5 c reproduction: 5% random dead tokens at scale, with 429 retries', () => {
  it('the seed: at 5% churn the short window (8 of 20) looks systemic about once per million results', () => {
    // The 1M run records ~1M results over 7 processes x 10 platform keys (~14k per window).
    const random = rng(11);
    let moments = 0;
    for (let w = 0; w < 70; w++) {
      const p = new PlatformWindows();
      let was = false;
      for (let i = 0; i < 14_300; i++) {
        p.record('k', random() < 0.05);
        const s = p.systemic('k');
        if (s && !was) moments++;
        was = s;
      }
    }
    // Expected ~0.8 per run (Binomial(20, 0.05) >= 8 is ~2.8e-6 per result); smoke scale
    // (5,000 devices) sees ~0.004. This seed is harmless only if it cannot feed itself.
    expect(moments).toBeGreaterThanOrEqual(1);
  });

  it('one unlucky stretch (8 dead in a row on one FCM project) does not keep dead tokens cycling', async () => {
    // Device indices 7000-7013 are one batch; put 10 of them on android:p3, 8 of those dead.
    // At 5% churn this stretch happens about once per 1M-result run (see the test above).
    const stretch = (i: number) => i >= 7000 && i < 7010;
    const r = await run({
      batches: 1500,
      freshMs: 20_000,
      fcmRatePerSecond: 70,
      concurrency: 32,
      keyOf: (i) => (stretch(i) ? 'android:p3' : undefined),
      extraDead: (i) => stretch(i) && i < 7008,
      horizonMs: 6 * 60_000,
    });
    expect(r.systemicMoments.length).toBeGreaterThan(0); // the seed did happen
    expectHealthy(r);
  }, 120_000);

  it('no stretch: plain 5% churn with 429 retries clears every dead token after the hold', async () => {
    const r = await run({
      batches: 1500,
      freshMs: 20_000,
      fcmRatePerSecond: 70,
      concurrency: 32,
      horizonMs: 6 * 60_000,
    });
    expectHealthy(r);
  }, 120_000);
});

describe('re-check batches are not breaker evidence', () => {
  const DEAD: NativePushResult = { ok: false, kind: 'permanent', error: 'Fcm404:UNREGISTERED' };

  async function setup(isDead: (token: string) => boolean) {
    const cleared: string[] = [];
    const t = makeDeps({
      writeMetric: () => {},
      push: {
        send: async (ms: NativePushMessage[]) =>
          ms.map((m): NativePushResult => (isDead(m.target.token) ? DEAD : { ok: true })),
      },
      onDeadTarget: async (id) => void cleared.push(id),
    });
    const sched = clockScheduler(t.state);
    const breaker = new PermanentBreaker();
    const cleanup = new DeadTargetQueue({
      process: t.deps.onDeadTarget,
      logger: t.deps.logger,
      writeMetric: () => {},
    });
    t.deps.breaker = breaker;
    t.deps.deadTargets = cleanup;
    const gate = createDeadTargetGate(t.deps, { setTimer: sched.setTimer });
    t.deps.deadTargetGate = gate;
    const alert = await seedAlert(t.alerts);
    const batch = (key: string, ids: string[], attempt = 0): SendBatch => ({
      alertId: alert.id,
      sourceId: alert.sourceId,
      relaySeverity: 'warning',
      batchKey: key,
      attempt,
      recipients: ids.map((id) => ({
        deviceId: id,
        target: { platform: 'ios', token: id },
        language: 'en',
        checkInToken: `ct${id}`,
      })),
    });
    const settleAll = async () => {
      await sched.advance(HOLD + 1);
      await gate.settle(1000);
      await cleanup.flush(1000);
    };
    return { t, sched, breaker, gate, batch, cleared, settleAll };
  }

  const ids = (prefix: string, n: number) => Array.from({ length: n }, (_, i) => `${prefix}${i}`);
  const queuedKeys = (s: Awaited<ReturnType<typeof setup>>) =>
    s.t.sendQueue.enqueued.flatMap((e) => e.batches.map((b) => b.batchKey));

  it('a held re-queue of 30 dead tokens records nothing, and is cleared on fresh evidence after its suspicion', async () => {
    const s = await setup((tok) => tok.startsWith('x'));
    const suspectedAt = { ios: s.t.state.now.getTime() };
    await s.sched.advance(1);
    // Fresh, healthy iOS traffic after the suspicion, then a held re-queue (`:h1`) and a retry
    // below one (`:h1:r2`), both carrying the suspicion.
    await processBatch(s.batch('a:warning:c0:0', ids('ok', 300)), s.t.deps);
    const seq = s.breaker.platforms.seq('ios');
    const samples = s.breaker.samples;
    await processBatch({ ...s.batch('a:warning:c1:0:h1', ids('x', 20), 1), suspectedAt }, s.t.deps);
    await processBatch(
      { ...s.batch('a:warning:c2:0:h1:r2', ids('xr', 10), 2), suspectedAt },
      s.t.deps,
    );
    // Before G6 these 30 permanents went into both breakers: iOS looked systemic (20 of the last
    // 20), so they were re-queued again instead of cleared.
    expect(s.breaker.platforms.seq('ios')).toBe(seq);
    expect(s.breaker.samples).toBe(samples);
    expect(s.breaker.platforms.systemic('ios')).toBe(false);
    await s.settleAll();
    expect(s.cleared.sort()).toEqual([...ids('x', 20), ...ids('xr', 10)].sort());
    expect(queuedKeys(s)).toEqual([]);
  });

  it('a re-check is never cleared on healthy evidence from before its suspicion (G6b)', async () => {
    const s = await setup((tok) => tok.startsWith('x'));
    await processBatch(s.batch('a:warning:c0:0', ids('ok', 300)), s.t.deps);
    await s.sched.advance(1);
    // Suspected after the healthy traffic (by another process, say), or carrying no suspicion
    // at all (a legacy message): the healthy past proves nothing about the push.
    const suspectedAt = { ios: s.t.state.now.getTime() };
    await processBatch({ ...s.batch('a:warning:c1:0:v1', ids('x', 20), 1), suspectedAt }, s.t.deps);
    await processBatch(s.batch('a:warning:c2:0:h1', ids('xh', 5), 1), s.t.deps);
    await s.settleAll();
    expect(s.cleared).toEqual([]);
    expect(queuedKeys(s).sort()).toEqual(['a:warning:c1:0:v1:u2', 'a:warning:c2:0:h1:u2']);
    const kept = s.t.sendQueue.enqueued.flatMap((e) => e.batches) as Array<
      SendBatch & { suspectedAt?: Record<string, number> }
    >;
    expect(kept.find((b) => b.batchKey.startsWith('a:warning:c1'))?.suspectedAt).toEqual(
      suspectedAt,
    );
  });

  it('breaker-open permanents are re-checked as `:v`, not `:r`, carry the suspicion, and the re-check records nothing', async () => {
    const s = await setup((tok) => tok.startsWith('x'));
    // 20 of 20 dead in one fresh batch: the batch rule opens.
    const at = s.t.state.now.getTime();
    const out = await processBatch(s.batch('a:warning:c0:0', ids('x', 20)), s.t.deps);
    expect(out).toMatchObject({ permanentFailed: 0, retried: 20 });
    expect(queuedKeys(s)).toEqual(['a:warning:c0:0:v1']);
    const recheck = s.t.sendQueue.enqueued.splice(0)[0]!.batches[0]!;
    expect((recheck as { suspectedAt?: unknown }).suspectedAt).toEqual({ ios: at });
    // Healthy fresh traffic after the suspicion heals the windows; the re-check's permanents are
    // then cleared.
    await s.sched.advance(1);
    await processBatch(s.batch('a:warning:c1:0', ids('ok', 400)), s.t.deps);
    expect(s.breaker.platforms.systemic('ios')).toBe(false);
    const seq = s.breaker.platforms.seq('ios');
    await processBatch(recheck, s.t.deps);
    expect(s.breaker.platforms.seq('ios')).toBe(seq);
    await s.settleAll();
    expect(s.cleared.sort()).toEqual(ids('x', 20).sort());
  });

  it('a re-check while fresh traffic still says misconfigured is re-queued within the budget, never cleared', async () => {
    let broken = true;
    const s = await setup(() => broken);
    await processBatch(s.batch('a:warning:c0:0', ids('m', 20)), s.t.deps);
    const recheck = s.t.sendQueue.enqueued.splice(0)[0]!.batches[0]!;
    expect(recheck.batchKey).toBe('a:warning:c0:0:v1');
    await processBatch(recheck, s.t.deps); // still broken: its permanents go to the gate
    expect(queuedKeys(s)).toEqual([]);
    await processBatch(s.batch('a:warning:c1:0', ids('n', 10)), s.t.deps); // fresh, still broken
    await s.settleAll();
    expect(queuedKeys(s).sort()).toEqual(['a:warning:c0:0:v1:h2', 'a:warning:c1:0:v1']);
    broken = false;
    expect(s.cleared).toEqual([]);
  });
});
