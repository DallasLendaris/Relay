/**
 * G6b: a re-check permanent is cleared only on positive, recent fresh evidence that its platform
 * is healthy, in the process judging it (dead-target-gate.ts "Re-check verdicts").
 *
 * The review probes after G6: breakers are per process, and SQS hands a re-check to any sender
 * task. A task whose windows are healthy (it never saw the misconfiguration) or cold (just
 * started) used to clear every live token in the re-check (P1: 20 of 20, P1b: 50 of 50). And a
 * chance systemic verdict left as the last state of fresh traffic kept dead tokens cycling until
 * the 30-minute budget (P3: 12 pushes per dead token).
 */
import { describe, expect, it } from 'vitest';
import type {
  NativePushMessage,
  NativePushResult,
  SendBatch,
} from '../../../src/scale/contracts.js';
import { PermanentBreaker, PLATFORM_EVIDENCE_TTL_MS } from '../../../src/scale/sender/breaker.js';
import { DEAD_TARGET_HOLD_MS } from '../../../src/scale/sender/dead-target-gate.js';
import { DeadTargetQueue } from '../../../src/scale/sender/dead-target-queue.js';
import {
  BLIND_RECHECK_DELAY_SECONDS,
  MAX_PUSHES_PER_DEAD_TOKEN,
  blindRecheckDelaySeconds,
  createDeadTargetGate,
  processBatch,
  type Batch,
  type SenderDeps,
} from '../../../src/scale/sender/sender.js';
import { clockScheduler, makeDeps, recordingLogger, seedAlert } from './fakes.js';

const HOLD = DEAD_TARGET_HOLD_MS;
const BAD: NativePushResult = { ok: false, kind: 'permanent', error: 'Apns400:BadDeviceToken' };

/** One push service, one send queue, any number of sender processes (breaker + gate each). */
async function world() {
  const state = { misconfigured: false, dead: (_tok: string) => false };
  const cleared: string[] = [];
  const delivered = new Map<string, number>();
  const pushes = new Map<string, number>();
  const { lines, logger } = recordingLogger();
  const metrics: string[] = [];
  const t = makeDeps({
    logger,
    writeMetric: (l) => void metrics.push(l),
    push: {
      send: async (ms: NativePushMessage[]) =>
        ms.map((m): NativePushResult => {
          const tok = m.target.token;
          pushes.set(tok, (pushes.get(tok) ?? 0) + 1);
          if (state.misconfigured || state.dead(tok)) return BAD;
          delivered.set(tok, (delivered.get(tok) ?? 0) + 1);
          return { ok: true };
        }),
    },
    onDeadTarget: async (id) => void cleared.push(id),
  });
  const sched = clockScheduler(t.state);
  /** A sender task: its own breaker, gate and cleanup queue, the shared queue and stores. */
  const proc = () => {
    const breaker = new PermanentBreaker();
    const cleanup = new DeadTargetQueue({
      process: t.deps.onDeadTarget,
      logger,
      writeMetric: () => {},
    });
    const deps: SenderDeps = { ...t.deps, breaker, deadTargets: cleanup };
    const gate = createDeadTargetGate(deps, { setTimer: sched.setTimer });
    deps.deadTargetGate = gate;
    return {
      deps,
      breaker,
      gate,
      run: (b: Batch) => processBatch(b, deps),
      settle: async () => {
        await gate.settle(1000);
        await cleanup.flush(1000);
      },
    };
  };
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
  const taken: Array<{ batch: Batch; delaySeconds: number }> = [];
  /** Moves what was enqueued since the last call out of the shared queue. */
  const take = (): Batch[] =>
    t.sendQueue.enqueued.splice(0).flatMap((e) =>
      e.batches.map((b) => {
        taken.push({ batch: b, delaySeconds: e.delaySeconds ?? 0 });
        return b;
      }),
    );
  const advance = (ms: number) => sched.advance(ms);
  const logged = (msg: string) => lines.filter((l) => l.msg === msg);
  /** Sum of an EMF metric written so far. */
  const metric = (name: string) =>
    metrics
      .map((m) => JSON.parse(m) as Record<string, unknown>)
      .reduce((n, m) => n + (typeof m[name] === 'number' ? (m[name] as number) : 0), 0);
  return {
    t,
    state,
    cleared,
    delivered,
    pushes,
    proc,
    batch,
    take,
    taken,
    advance,
    logged,
    metric,
  };
}

const ids = (p: string, n: number) => Array.from({ length: n }, (_, i) => `${p}${i}`);

describe('re-checks judged by another sender process (G6b, review P1/P1b)', () => {
  it('P1: the re-check lands on a process that saw only healthy traffic before the misconfiguration: nothing cleared', async () => {
    const w = await world();
    const A = w.proc();
    const B = w.proc();
    await A.run(w.batch('a:warning:c0:0', ids('okA', 300)));
    await B.run(w.batch('a:warning:c1:0', ids('okB', 300)));
    await w.advance(1);
    // The misconfiguration starts as fresh traffic ends; A takes the last fresh batch.
    w.state.misconfigured = true;
    await A.run(w.batch('a:warning:c2:0', ids('L', 20)));
    const v = w.take();
    expect(v.map((b) => b.batchKey)).toEqual(['a:warning:c2:0:v1']);
    // Still misconfigured; SQS hands the re-check to B, whose windows say healthy.
    await w.advance(2000);
    await B.run(v[0]!);
    await w.advance(HOLD + 1);
    await A.settle();
    await B.settle();
    expect(w.cleared).toEqual([]); // before: all 20 live tokens cleared
    const again = w.take();
    expect(again.map((b) => b.batchKey)).toEqual(['a:warning:c2:0:v1:u2']);
    // The operator fixes it; the unconfirmed re-check delivers.
    w.state.misconfigured = false;
    await w.advance(w.taken.at(-1)!.delaySeconds * 1000);
    await A.run(again[0]!);
    for (const id of ids('L', 20)) expect(w.delivered.get(id), id).toBe(1);
    expect(w.cleared).toEqual([]);
  });

  it('P1b: the re-check lands on a freshly started (cold) process: nothing cleared', async () => {
    const w = await world();
    const A = w.proc();
    await A.run(w.batch('a:warning:c0:0', ids('ok', 300)));
    await w.advance(1);
    w.state.misconfigured = true;
    await A.run(w.batch('a:warning:c2:0', ids('L', 50)));
    const v = w.take();
    const B = w.proc(); // a replacement task: no evidence at all
    await w.advance(2000);
    await B.run(v[0]!);
    await w.advance(HOLD + 1);
    await B.settle();
    expect(w.cleared).toEqual([]); // before: all 50 cleared
    expect(w.take().map((b) => b.batchKey)).toEqual(['a:warning:c2:0:v1:u2']);
  });

  it('P1b with fresh traffic: the cold process sees the misconfiguration itself and confirms', async () => {
    const w = await world();
    const A = w.proc();
    await A.run(w.batch('a:warning:c0:0', ids('ok', 300)));
    await w.advance(1);
    w.state.misconfigured = true;
    await A.run(w.batch('a:warning:c2:0', ids('L', 50)));
    const v = w.take();
    const B = w.proc(); // a replacement task
    await w.advance(2000);
    await B.run(v[0]!);
    await B.run(w.batch('a:warning:c3:0', ids('f', 10))); // B's fresh traffic: still broken
    await w.advance(HOLD + 1);
    await B.settle();
    expect(w.cleared).toEqual([]); // before: all 50 cleared
    expect(
      w
        .take()
        .map((b) => b.batchKey)
        .sort(),
    ).toEqual(['a:warning:c2:0:v1:h2', 'a:warning:c3:0:v1']); // confirmed: full budget
  });

  it('the fix race: the misconfiguration is fixed after the re-check push, then B sees only healthy traffic: nothing cleared', async () => {
    const w = await world();
    const A = w.proc();
    const B = w.proc();
    await B.run(w.batch('a:warning:c1:0', ids('okB', 300)));
    await w.advance(1);
    w.state.misconfigured = true;
    await A.run(w.batch('a:warning:c2:0', ids('L', 20)));
    const v = w.take();
    await w.advance(2000);
    await B.run(v[0]!); // pushed while still misconfigured: 20 permanents
    w.state.misconfigured = false;
    await w.advance(100);
    await B.run(w.batch('a:warning:c3:0', ids('okC', 300))); // healthy, after the push
    await w.advance(HOLD + 1);
    await B.settle();
    expect(w.cleared).toEqual([]);
  });

  it('positive evidence clears: dead tokens re-checked on a process with recent healthy traffic after the suspicion', async () => {
    const w = await world();
    const A = w.proc();
    const B = w.proc();
    w.state.dead = (tok) => tok.startsWith('x');
    await A.run(w.batch('a:warning:c0:0', ids('x', 20))); // 20 of 20: A's breakers open
    const v = w.take();
    expect(v[0]?.suspectedAt).toEqual({ ios: w.t.state.now.getTime() });
    await w.advance(1);
    await B.run(w.batch('a:warning:c1:0', ids('ok', 300))); // after the suspicion
    await w.advance(2000);
    await B.run(v[0]!);
    await w.advance(HOLD + 1);
    await B.settle();
    expect(w.cleared.sort()).toEqual(ids('x', 20).sort());
    expect(w.take()).toEqual([]);
  });

  it('healthy evidence older than a minute before the push does not count', async () => {
    const w = await world();
    const A = w.proc();
    const B = w.proc();
    w.state.dead = (tok) => tok.startsWith('x');
    await A.run(w.batch('a:warning:c0:0', ids('x', 20)));
    const v = w.take();
    await w.advance(1);
    await B.run(w.batch('a:warning:c1:0', ids('ok', 300)));
    await w.advance(61_000); // B's platform has been idle a minute when the re-check arrives
    await B.run(v[0]!);
    await w.advance(HOLD + 1);
    await B.settle();
    expect(w.cleared).toEqual([]);
    expect(w.take().map((b) => b.batchKey)).toEqual(['a:warning:c0:0:v1:u2']);
  });
});

describe('a chance systemic verdict as the last state fresh traffic leaves (G6b, review P3)', () => {
  it('dead tokens are pushed at most MAX_PUSHES_PER_DEAD_TOKEN times in all, on a doubling backoff, then kept', async () => {
    const w = await world();
    const A = w.proc();
    w.state.dead = (tok) => tok.startsWith('x');
    const start = w.t.state.now.getTime();
    await A.run(w.batch('a:warning:c0:0', ids('ok', 300)));
    // The last fresh batch: 8 dead of 12 (the short window: 8 of its last 20 → systemic).
    await A.run(w.batch('a:warning:c1:0', [...ids('x', 8), ...ids('ok2_', 4)]));
    expect(A.breaker.platforms.systemic('ios')).toBe(true);
    let rechecks = 0;
    for (let i = 0; i < 50; i++) {
      let due = w.take();
      if (due.length === 0) {
        await w.advance(HOLD + 1);
        await A.settle();
        due = w.take();
        if (due.length === 0) break;
      }
      for (const b of due) {
        rechecks++;
        await w.advance(1000 * w.taken.at(-1)!.delaySeconds + 1);
        await A.run(b);
      }
      await A.settle();
    }
    const minutes = (w.t.state.now.getTime() - start) / 60_000;
    expect(w.cleared).toEqual([]); // no fresh evidence either way: kept
    expect(rechecks).toBe(MAX_PUSHES_PER_DEAD_TOKEN - 1); // G6: 11; G6b: 3
    for (const id of ids('x', 8)) expect(w.pushes.get(id), id).toBe(MAX_PUSHES_PER_DEAD_TOKEN);
    expect(MAX_PUSHES_PER_DEAD_TOKEN).toBeLessThanOrEqual(6);
    expect(w.taken.map((e) => e.batch.batchKey)).toEqual([
      'a:warning:c1:0:v1',
      'a:warning:c1:0:v1:u2',
      'a:warning:c1:0:v1:u2:u3',
      'a:warning:c1:0:v1:u2:u3:u4',
      'a:warning:c1:0:v1:u2:u3:u4:u5',
    ]);
    expect(w.taken[1]!.delaySeconds).toBeGreaterThanOrEqual(BLIND_RECHECK_DELAY_SECONDS);
    expect(w.taken[2]!.delaySeconds).toBeGreaterThanOrEqual(2 * BLIND_RECHECK_DELAY_SECONDS);
    expect(w.taken[3]!.delaySeconds).toBeGreaterThanOrEqual(4 * BLIND_RECHECK_DELAY_SECONDS);
    const gaveUp = w.logged('delivery.gave_up');
    expect(gaveUp.map((l) => [l.fields?.['reason'], l.fields?.['count']])).toEqual([
      ['unconfirmed', 8],
    ]);
    // The last re-check lands inside the 30-minute budget; nothing runs past it.
    expect(minutes).toBeLessThan(31);
    expect(w.metric('DeadTargetUnconfirmedKept')).toBe(8);
    expect(w.metric('DeadTargetHeldDropped')).toBe(0);
    // Only the fresh batch raised an alarm-level breaker log; the re-checks raised none.
    expect(w.logged('delivery.platform_breaker_open')).toHaveLength(1);
    expect(w.logged('delivery.permanent_breaker_open')).toHaveLength(0);
    expect(w.logged('sender.dead_targets_requeued')).toHaveLength(0);
  });

  it('a stale window is reset after PLATFORM_EVIDENCE_TTL_MS: the next alert’s first dead token is cleared, not suspected', async () => {
    const w = await world();
    const A = w.proc();
    w.state.dead = (tok) => tok.startsWith('x');
    await A.run(w.batch('a:warning:c0:0', ids('ok', 300)));
    await A.run(w.batch('a:warning:c1:0', [...ids('x', 8), ...ids('ok2_', 4)]));
    expect(A.breaker.platforms.systemic('ios')).toBe(true);
    w.take(); // the chance alarm's re-check is not the point here
    await w.advance(PLATFORM_EVIDENCE_TTL_MS + 1);
    await A.settle();
    w.take();
    w.cleared.length = 0;
    // Much later, fresh traffic again: one dead token among 49 healthy.
    await A.run(w.batch('a:warning:c9:0', ['xlate', ...ids('ok3_', 49)]));
    expect(A.breaker.platforms.systemic('ios')).toBe(false);
    await w.advance(HOLD + 1);
    await A.settle();
    expect(w.cleared).toEqual(['xlate']); // without the reset: held as systemic and re-queued
    expect(w.take()).toEqual([]);
  });
});

type World = Awaited<ReturnType<typeof world>>;
type Proc = ReturnType<World['proc']>;

/**
 * Runs the shared send queue on the test clock in 5 s steps until `untilMin`: each step settles
 * every process, calls `tick(minutes)`, then runs every batch whose delay has passed on the
 * process `pick` chooses.
 */
async function runQueue(
  w: World,
  procs: Proc[],
  pick: (b: Batch) => number,
  tick: (min: number) => Promise<void>,
  untilMin: number,
): Promise<string[]> {
  const start = w.t.state.now.getTime();
  const q: Array<{ at: number; b: Batch }> = [];
  const log: string[] = [];
  const pull = () => {
    const n = w.taken.length;
    w.take();
    for (const e of w.taken.slice(n))
      q.push({ at: w.t.state.now.getTime() + 1000 * e.delaySeconds, b: e.batch });
  };
  pull();
  while (w.t.state.now.getTime() - start < untilMin * 60_000) {
    await w.advance(5000);
    for (const p of procs) await p.settle();
    const min = (w.t.state.now.getTime() - start) / 60_000;
    await tick(min);
    pull();
    q.sort((a, b) => a.at - b.at);
    while (q.length > 0 && q[0]!.at <= w.t.state.now.getTime()) {
      const { b } = q.shift()!;
      log.push(`${b.batchKey}@${min.toFixed(1)}m`);
      await procs[pick(b)]!.run(b);
      pull();
    }
  }
  return log;
}

describe('a misconfiguration that outlasts fresh evidence is re-checked until the budget (G6c, review N2/N3)', () => {
  for (const fixAt of [8, 20]) {
    it(`N2: one task, fresh traffic over, still misconfigured; the operator fixes it at minute ${fixAt}: all delivered`, async () => {
      const w = await world();
      const A = w.proc();
      await A.run(w.batch('a:warning:c0:0', ids('ok', 300)));
      w.state.misconfigured = true;
      await A.run(w.batch('a:warning:c2:0', ids('L', 20)));
      const log = await runQueue(
        w,
        [A],
        () => 0,
        async (min) => {
          if (min >= fixAt) w.state.misconfigured = false;
        },
        32,
      );
      expect(w.cleared).toEqual([]);
      for (const id of ids('L', 20)) expect(w.delivered.get(id), `${id} ${log.join(' ')}`).toBe(1); // G6b: 0
      expect(w.logged('delivery.gave_up')).toEqual([]);
    });
  }

  it('N3: broken fresh traffic only on task A, re-checks land on task B; fixed at minute 10: all delivered', async () => {
    const w = await world();
    const A = w.proc();
    const B = w.proc();
    await A.run(w.batch('a:warning:c0:0', ids('ok', 300)));
    await B.run(w.batch('a:warning:c0b:0', ids('okB', 300)));
    w.state.misconfigured = true;
    await A.run(w.batch('a:warning:c2:0', ids('L', 20)));
    let n = 0;
    const log = await runQueue(
      w,
      [A, B],
      (b) => (b.batchKey.startsWith('a:warning:c2:') ? 1 : 0),
      async (min) => {
        if (min < 10) {
          n++;
          await A.run(w.batch(`a:warning:f${n}:0`, ids(`F${n}_`, 20)));
        } else w.state.misconfigured = false;
      },
      32,
    );
    const chain = log.filter((l) => l.startsWith('a:warning:c2:'));
    // B never sees fresh iOS traffic: every re-check there is unconfirmed, and they keep coming.
    expect(
      chain.some((l) => /:u4@/.test(l)),
      chain.join(' '),
    ).toBe(true);
    expect(w.cleared.filter((id) => id.startsWith('L'))).toEqual([]);
    for (const id of ids('L', 20)) expect(w.delivered.get(id), chain.join(' ')).toBe(1); // G6b: 0
  });

  it('the blind backoff never pushes a dead token more than MAX_PUSHES_PER_DEAD_TOKEN times, whatever the jitter or hold', () => {
    const budget = 30 * 60_000;
    for (const holdMs of [0, 1, HOLD]) {
      for (const r of [0, 0.5, 0.999999]) {
        // The fresh push and the `:v1` re-check at t = 0 (the shortest possible), then blind ones.
        let pushes = 2;
        let now = holdMs;
        for (let blind = 0; ; blind++) {
          const d = blindRecheckDelaySeconds(blind, blind + 1, now, budget, () => r);
          if (d === undefined) break;
          expect(d).toBeGreaterThanOrEqual(BLIND_RECHECK_DELAY_SECONDS);
          now += d * 1000;
          expect(now).toBeLessThan(budget); // every re-check lands before the deadline
          pushes++;
          now += holdMs;
        }
        expect(pushes, `hold ${holdMs} r ${r}`).toBeLessThanOrEqual(MAX_PUSHES_PER_DEAD_TOKEN);
      }
    }
    expect(MAX_PUSHES_PER_DEAD_TOKEN).toBe(6);
  });
});

describe('shutdown never clears a re-check (G6c, review N6)', () => {
  it('a primed re-check held at shutdown is re-queued with its tokens, not cleared before its after-window', async () => {
    const w = await world();
    const A = w.proc();
    const B = w.proc();
    await A.run(w.batch('a:warning:c0:0', ids('okA', 300)));
    w.state.misconfigured = true;
    await A.run(w.batch('a:warning:c2:0', ids('L', 20)));
    const v = w.take();
    await w.advance(1000);
    w.state.misconfigured = false;
    await B.run(w.batch('a:warning:c3:0', ids('okC', 20))); // healthy evidence after the suspicion
    w.state.misconfigured = true; // a new onset nobody has reported yet
    await B.run(v[0]!);
    B.gate.shutdown(); // before the 30 s after-window
    await B.settle();
    expect(w.cleared).toEqual([]); // G6b: all 20 cleared
    expect(w.take().map((b) => b.batchKey)).toEqual(['a:warning:c2:0:v1:u2']);
  });

  it('fresh holds keep their shutdown behaviour: decided at once', async () => {
    const w = await world();
    const A = w.proc();
    w.state.dead = (tok) => tok === 'x0';
    await A.run(w.batch('a:warning:c0:0', [...ids('ok', 49), 'x0']));
    A.gate.shutdown();
    await A.settle();
    expect(w.cleared).toEqual(['x0']);
  });
});
