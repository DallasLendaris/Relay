import { describe, expect, it } from 'vitest';
import type { NativePushResult, PushTarget, SendBatch } from '../../../src/scale/contracts.js';
import { PermanentBreaker } from '../../../src/scale/sender/breaker.js';
import { DeadTargetQueue } from '../../../src/scale/sender/dead-target-queue.js';
import {
  blindRecheckKey,
  blindRechecks,
  createDeadTargetGate,
  carriesEvidence,
  heldRetryKey,
  processBatch,
  recheckKey,
  retryKey,
} from '../../../src/scale/sender/sender.js';
import {
  FakePush,
  makeBatch,
  makeDeps,
  clockScheduler,
  recordingLogger,
  seedAlert,
} from './fakes.js';
import { splitToFit } from '../../../src/scale/fanout/pack.js';

/** Deterministic PRNG (mulberry32). */
function rng(seed: number): () => number {
  let a = seed;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

type Node = Pick<SendBatch, 'batchKey' | 'attempt'> & {
  r: boolean;
  h: boolean;
  v: boolean;
  b: boolean;
};

/** Planned keys of every shape the fan-out produces (normal, dense part, late-join). */
const PLANNED = [
  'a1:warning:8928308280fffff:7',
  'a1:warning:8928308280fffff:12.3/16',
  'a1:watch:late:0f3c2a91d4e5b6a7',
  'a1:warning:8928308280fffff:1',
  'a2:warning:8928308280fffff:1',
];

describe('derived batch keys are unique by construction', () => {
  it('the review collision: retry of a held re-queue, held of a held, held of a retry', () => {
    const x = { batchKey: 'X', attempt: 0 };
    const r1 = { batchKey: retryKey(x as SendBatch), attempt: 1 };
    const h1 = { batchKey: heldRetryKey(x), attempt: 1 };
    const keys = [
      r1.batchKey,
      h1.batchKey,
      retryKey(h1 as SendBatch),
      heldRetryKey(h1),
      heldRetryKey(r1),
      retryKey(r1 as SendBatch),
    ];
    expect(keys).toEqual(['X:r1', 'X:h1', 'X:h1:r2', 'X:h1:h2', 'X:r1:h2', 'X:r2']);
    expect(new Set(keys).size).toBe(keys.length);
  });

  it('random derivation trees (each batch retried, held-re-queued, re-checked and blind-re-queued once, at most): all keys distinct', () => {
    for (let seed = 1; seed <= 40; seed++) {
      const random = rng(seed);
      const nodes: Node[] = PLANNED.map((k) => ({
        batchKey: k,
        attempt: 0,
        r: false,
        h: false,
        v: false,
        b: false,
      }));
      const seen = new Map<string, string>();
      for (const n of nodes) seen.set(n.batchKey, 'planned');
      for (let step = 0; step < 3000; step++) {
        const n = nodes[Math.floor(random() * nodes.length)] as Node;
        const u = random();
        const kind = u < 1 / 4 ? 'r' : u < 2 / 4 ? 'h' : u < 3 / 4 ? 'v' : 'b';
        if (n[kind]) continue; // one processing of a batch derives each kind at most once
        n[kind] = true;
        const key =
          kind === 'r'
            ? retryKey(n as unknown as SendBatch)
            : kind === 'h'
              ? heldRetryKey(n)
              : kind === 'v'
                ? recheckKey(n)
                : blindRecheckKey(n);
        const how = `${kind}(${n.batchKey}@${n.attempt})`;
        expect(
          seen.get(key),
          `seed ${seed}: ${how} collides with ${seen.get(key)}`,
        ).toBeUndefined();
        seen.set(key, how);
        nodes.push({
          batchKey: key,
          attempt: n.attempt + 1,
          r: false,
          h: false,
          v: false,
          b: false,
        });
      }
      expect(seen.size).toBeGreaterThan(1000);
    }
  });

  it('every chain of up to 6 derivations from one key is distinct (exhaustive)', () => {
    const seen = new Set<string>(['X']);
    let frontier: Array<Pick<SendBatch, 'batchKey' | 'attempt'>> = [{ batchKey: 'X', attempt: 0 }];
    for (let depth = 1; depth <= 6; depth++) {
      const next: typeof frontier = [];
      for (const n of frontier) {
        const children = [retryKey(n as SendBatch), heldRetryKey(n), recheckKey(n)];
        children.push(blindRecheckKey(n));
        for (const key of children) {
          expect(seen.has(key), key).toBe(false);
          seen.add(key);
          next.push({ batchKey: key, attempt: n.attempt + 1 });
        }
      }
      frontier = next;
    }
    expect(seen.size).toBe((4 ** 7 - 1) / 3);
  });

  it('carriesEvidence: planned keys and their `:r` retries do; anything below an `:h`, `:v` or `:u` does not', () => {
    const random = rng(3);
    const hex = () =>
      Array.from({ length: 16 }, () => '0123456789abcdef'[Math.floor(random() * 16)]).join('');
    const planned = [
      ...PLANNED,
      'a1:warning:8928308280fffff:7:s01',
      ...Array.from({ length: 200 }, () => `a1:watch:late:${hex()}`),
    ];
    for (const k of planned) expect(carriesEvidence(k), k).toBe(true);
    for (const root of planned) {
      let frontier = [{ batchKey: root, attempt: 0, clean: true }];
      for (let depth = 1; depth <= 4; depth++) {
        frontier = frontier.flatMap((n) => [
          { batchKey: retryKey(n), attempt: n.attempt + 1, clean: n.clean },
          { batchKey: heldRetryKey(n), attempt: n.attempt + 1, clean: false },
          { batchKey: recheckKey(n), attempt: n.attempt + 1, clean: false },
          { batchKey: blindRecheckKey(n), attempt: n.attempt + 1, clean: false },
        ]);
        for (const n of frontier) expect(carriesEvidence(n.batchKey), n.batchKey).toBe(n.clean);
      }
    }
  });

  it('the review probe (P4): key classification, and blindRechecks counts only `:u` segments', () => {
    const planned = [
      'a:warning:8928308280fffff:12',
      'a:warning:8928308280fffff:12.3/16',
      'a:warning:late:0123456789abcdef',
      'a:warning:late:bb12bb34bb56bb78',
      'a:warning:late:b123456789012345',
      'a:warning:late:d123456789012345',
      'a:warning:8928308280fffff:12:s0101',
      'a:warning:8928308280fffff:12:r3',
      'a:warning:8928308280fffff:12.3/16:r2',
    ];
    for (const k of planned) {
      expect(carriesEvidence(k), k).toBe(true);
      expect(blindRechecks(k), k).toBe(0);
    }
    const re = [
      'K:h1',
      'K:v1',
      'K:u1',
      'K:v1:r2',
      'K:r1:v2',
      'K:r1:v2:r3',
      'K:h1:r2:v3',
      'K:v1:u2',
    ];
    for (const k of re) expect(carriesEvidence(`a:warning:c:1${k.slice(1)}`), k).toBe(false);
    expect(retryKey({ batchKey: 'K:v1:r2', attempt: 2 })).toBe('K:v1:r3');
    expect(retryKey({ batchKey: 'K:u1', attempt: 1 })).toBe('K:u1:r2');
    expect(blindRecheckKey({ batchKey: 'K:v1:r2', attempt: 2 })).toBe('K:v1:r2:u3');
    expect(blindRechecks('a:warning:late:b123456789012345:v1:u2:h3:u4:r5')).toBe(2);
  });

  it('G6c (review N4): a split anywhere in the chain keeps the classification of what it split', () => {
    const random = rng(11);
    const hex = () =>
      Array.from({ length: 16 }, () => '0123456789abcdef'[Math.floor(random() * 16)]).join('');
    const roots = [...PLANNED, 'a1:warning:8928308280fffff:7:s01', `a1:watch:late:${hex()}`];
    const split = (n: { batchKey: string; attempt: number }, path: string) => ({
      ...n,
      batchKey: `${n.batchKey}:s${path}`,
    });
    for (const root of roots) {
      let frontier = [{ batchKey: root, attempt: 0, clean: true }];
      for (let depth = 1; depth <= 4; depth++) {
        frontier = frontier.flatMap((n) => [
          { batchKey: retryKey(n), attempt: n.attempt + 1, clean: n.clean },
          { batchKey: recheckKey(n), attempt: n.attempt + 1, clean: false },
          { batchKey: blindRecheckKey(n), attempt: n.attempt + 1, clean: false },
          { ...split(n, '0'), clean: n.clean },
          { ...split(n, '10'), clean: n.clean },
        ]);
        for (const n of frontier) expect(carriesEvidence(n.batchKey), n.batchKey).toBe(n.clean);
      }
    }
    // The probe's keys.
    expect(carriesEvidence('a:warning:c:1:v1:s0')).toBe(false); // G6b: true
    expect(carriesEvidence('a:warning:c:1:v1:u2:s1')).toBe(false);
    expect(carriesEvidence('a:warning:c:1:h1:s01:r2')).toBe(false); // G6b: true
    expect(blindRechecks('a:warning:c:1:v1:u2:s1')).toBe(1);
  });

  it('G6c (review N4): the results of a split re-check (pack.ts splitToFit) never feed the breakers', async () => {
    const t = makeDeps({
      push: new FakePush(() => ({ ok: false, kind: 'permanent', error: 'BadDeviceToken' })),
    });
    const breaker = new PermanentBreaker();
    t.deps.breaker = breaker;
    t.deps.deadTargetGate = { submit: () => {} };
    const alert = await seedAlert(t.alerts);
    const big = {
      ...makeBatch({ batchKey: `${alert.id}:warning:c:1:v1`, attempt: 1 }, 40),
      suspectedAt: { ios: 1 },
    };
    const pieces = splitToFit(big, 4000);
    expect(pieces.length).toBeGreaterThan(1);
    for (const p of pieces) {
      expect(p.batchKey).toMatch(/:v1:s[01]+$/);
      await processBatch(p, t.deps);
    }
    expect(breaker.samples).toBe(0); // G6b: 40 permanents recorded as fresh evidence
    expect(breaker.platforms.seq('ios')).toBe(0);
  });
});

describe('cold start + misconfiguration + retryable errors (review scenario)', () => {
  it('every recipient is delivered exactly once, no token cleared', async () => {
    const decide = rng(5);
    const bad = new Map<string, boolean>(); // 40% look dead while misconfigured
    const flaky = new Map<string, number>(); // ~15% fail retryably on their first two sends
    let misconfigured = true;
    const delivered = new Map<string, number>();
    const cleared: string[] = [];
    const { lines, logger } = recordingLogger();
    const t = makeDeps({
      logger,
      writeMetric: () => {},
      push: new FakePush((m): NativePushResult => {
        const tok = m.target.token;
        if (!bad.has(tok)) {
          bad.set(tok, decide() < 0.4);
          flaky.set(tok, decide() < 0.15 ? 2 : 0);
        }
        const left = flaky.get(tok) ?? 0;
        if (left > 0) {
          flaky.set(tok, left - 1);
          return { ok: false, kind: 'retryable', error: 'Unavailable' };
        }
        if (misconfigured && bad.get(tok)) {
          return { ok: false, kind: 'permanent', error: 'Apns400:BadDeviceToken' };
        }
        delivered.set(tok, (delivered.get(tok) ?? 0) + 1);
        return { ok: true };
      }),
      onDeadTarget: async (id) => void cleared.push(id),
    });
    t.deps.breaker = new PermanentBreaker();
    t.deps.deadTargets = new DeadTargetQueue({
      process: t.deps.onDeadTarget,
      logger,
      writeMetric: () => {},
    });
    const sched = clockScheduler(t.state);
    const gate = createDeadTargetGate(t.deps, { setTimer: sched.setTimer });
    t.deps.deadTargetGate = gate;
    await seedAlert(t.alerts);

    // Small batches, all urgent; retries are processed as soon as they are queued, so retry
    // batches are sent during the cold start too, before the breakers have evidence.
    const random = rng(9);
    const queue: SendBatch[] = [];
    let dev = 0;
    for (let b = 0; b < 400; b++) {
      const base = makeBatch({ batchKey: `a:warning:c${b}:0` }, 2 + Math.floor(random() * 4));
      queue.push({
        ...base,
        recipients: base.recipients.map((r) => {
          const id = `d${dev++}`;
          return { ...r, deviceId: id, target: { platform: 'ios', token: id } as PushTarget };
        }),
      });
    }
    const all = queue.flatMap((b) => b.recipients.map((r) => r.target.token));
    const keys = new Set<string>();
    // Retries (and held re-queues) go to their own FIFO lane, served every other step, so they
    // interleave with the cold start (none waits behind all 400 originals) in arrival order.
    const retries: SendBatch[] = [];
    let steps = 0;
    for (;;) {
      const next =
        (steps % 2 === 1 ? retries.shift() : undefined) ?? queue.shift() ?? retries.shift();
      if (!next) {
        if (gate.heldCount === 0) break;
        gate.shutdown(); // decide what is still held, then keep draining
      } else {
        expect(keys.has(next.batchKey), `key reused: ${next.batchKey}`).toBe(false);
        keys.add(next.batchKey);
        await processBatch(next, t.deps);
        await sched.advance(1000); // one second per step: the 30 s holds elapse mid-run
        if (++steps === 900) misconfigured = false; // the fix is deployed, 15 minutes in
      }
      await gate.settle(1000);
      retries.push(...t.sendQueue.enqueued.splice(0).flatMap((e) => e.batches));
      expect(steps).toBeLessThan(20_000);
    }
    expect(misconfigured).toBe(false);
    expect(lines.filter((l) => l.msg === 'delivery.gave_up')).toEqual([]); // within the budget
    expect(cleared).toEqual([]);
    expect([...keys].some((k) => /:h\d+$/.test(k))).toBe(true); // held re-queues happened
    // ...and were re-queued again (confirmed) while fresh traffic still said misconfigured; a
    // re-check's permanents always go to the gate, so the sender never re-checks a re-check (`:v:v`)
    expect([...keys].some((k) => /:h\d+:h\d+$/.test(k))).toBe(true);
    expect([...keys].some((k) => /:r\d+:h\d+$/.test(k))).toBe(true); // retries were held
    expect([...keys].some((k) => /:v\d+:h\d+$/.test(k))).toBe(true); // re-checks re-queued
    expect([...keys].some((k) => /:v\d+:v\d+/.test(k))).toBe(false);
    for (const tok of all) expect(delivered.get(tok), tok).toBe(1);
  });
});
