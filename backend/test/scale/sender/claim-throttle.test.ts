import { describe, expect, it } from 'vitest';
import { IdempotencyThrottledError } from '../../../src/adapters/aws/dynamo-idempotency-store.js';
import type { IdempotencyStore } from '../../../src/ports.js';
import type {
  NativePushMessage,
  NativePushResult,
  PushTarget,
  SendBatch,
} from '../../../src/scale/contracts.js';
import { PermanentBreaker } from '../../../src/scale/sender/breaker.js';
import { DEAD_TARGET_HOLD_MS } from '../../../src/scale/sender/dead-target-gate.js';
import { DeadTargetQueue } from '../../../src/scale/sender/dead-target-queue.js';
import {
  createDeadTargetGate,
  isTransientError,
  newBatchControl,
  processBatch,
  type BatchOutcome,
} from '../../../src/scale/sender/sender.js';
import {
  FakePush,
  clockScheduler,
  makeBatch,
  makeDeps,
  recordingLogger,
  seedAlert,
} from './fakes.js';

/** Stable 0..99 per key, so every retry of the same key's claim fails the same way. */
const bucketOf = (key: string): number => {
  let h = 2166136261;
  for (let i = 0; i < key.length; i++) h = Math.imul(h ^ key.charCodeAt(i), 16777619);
  return (h >>> 0) % 100;
};

describe('isTransientError', () => {
  it('network errors, timeouts and 5xx are transient; bugs and permission errors are not', () => {
    expect(isTransientError(Object.assign(new Error('read'), { code: 'ECONNRESET' }))).toBe(true);
    expect(isTransientError(new Error('socket hang up'))).toBe(true);
    expect(isTransientError(Object.assign(new Error('x'), { name: 'TimeoutError' }))).toBe(true);
    expect(isTransientError({ name: 'X', $metadata: { httpStatusCode: 503 } })).toBe(true);
    expect(isTransientError(new Error('wrapped', { cause: { code: 'ETIMEDOUT' } }))).toBe(true);
    expect(isTransientError(new Error('boom'))).toBe(false);
    expect(
      isTransientError({ name: 'AccessDeniedException', $metadata: { httpStatusCode: 400 } }),
    ).toBe(false);
    expect(isTransientError(new TypeError('x is undefined'))).toBe(false);
    const loop: { cause?: unknown } = {};
    loop.cause = loop;
    expect(isTransientError(loop)).toBe(false); // a cyclic cause chain terminates
  });
});

describe('best-effort store calls never fail a batch', () => {
  it('progress read, progress writes, markSent and release all throwing: the batch still completes', async () => {
    const { lines, logger } = recordingLogger();
    const t = makeDeps({ logger, chunkConcurrency: 1 });
    await seedAlert(t.alerts);
    const real = t.idempotency;
    const throttled = () =>
      new IdempotencyThrottledError(
        5,
        Object.assign(new Error('t'), { name: 'ThrottlingException' }),
      );
    t.deps.idempotency = {
      claim: (k, l) => real.claim(k, l),
      extend: (k, l) => real.extend(k, l),
      claimedProgress: async () => {
        throw throttled();
      },
      recordChunk: async () => {
        throw new Error('anything at all');
      },
      markSent: async () => {
        throw throttled();
      },
      release: async () => {
        throw throttled();
      },
    };
    const out = await processBatch(makeBatch({}, 250), t.deps);
    expect(out).toMatchObject({ ack: true, reason: 'sent', sent: 250 });
    const msgs = lines.map((l) => l.msg);
    expect(msgs).toContain('sender.progress_read_failed');
    expect(msgs.filter((m) => m === 'sender.record_chunk_failed')).toHaveLength(3);
    expect(msgs).toContain('sender.mark_sent_failed');
  });
});

describe('claim throttling across every claim path (t5 scenario f regression)', () => {
  it('10% of keys throttled on first claims, re-claims with progress and :r/:h keys: no batch throws, everyone once', async () => {
    const { lines, logger } = recordingLogger();
    const delivered = new Map<string, number>();
    const flaky = new Set<string>(); // tokens that fail retryably once
    let misconfigured = true; // iOS looks dead for the first results (held, then :h keys)
    const t = makeDeps({
      logger,
      writeMetric: () => {},
      push: new FakePush((m: NativePushMessage): NativePushResult => {
        const tok = m.target.token;
        if (tok.endsWith('3') && !flaky.has(tok)) {
          flaky.add(tok);
          return { ok: false, kind: 'retryable', error: 'Unavailable' };
        }
        if (misconfigured && m.target.platform === 'ios') {
          return { ok: false, kind: 'permanent', error: 'Apns400:BadDeviceToken' };
        }
        delivered.set(tok, (delivered.get(tok) ?? 0) + 1);
        return { ok: true };
      }),
    });

    // The store: 10% of keys throttled out (the real store's error), 8% fail with a connection
    // reset, while the fault window is open. It hits first claims, re-claims (which inherit
    // progress) and every derived key alike.
    const real = t.idempotency;
    let faults = true;
    const claims = { throttled: 0, reset: 0, total: 0 };
    const store: IdempotencyStore = {
      claim: async (key, leaseUntil) => {
        claims.total++;
        const b = bucketOf(key);
        if (faults && b < 10) {
          claims.throttled++;
          throw new IdempotencyThrottledError(
            5,
            Object.assign(new Error('t'), { name: 'ThrottlingException' }),
          );
        }
        if (faults && b >= 10 && b < 18) {
          claims.reset++;
          throw Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' });
        }
        return real.claim(key, leaseUntil);
      },
      markSent: (k, e) => real.markSent(k, e),
      release: (k) => real.release(k),
      extend: (k, l) => real.extend(k, l),
      peek: (k) => real.peek(k),
      recordChunk: (k, p) => real.recordChunk(k, p),
      claimedProgress: (k) => real.claimedProgress(k),
    };
    t.deps.idempotency = store;
    const sched = clockScheduler(t.state);
    t.deps.breaker = new PermanentBreaker();
    t.deps.deadTargets = new DeadTargetQueue({
      process: t.deps.onDeadTarget,
      logger,
      writeMetric: () => {},
    });
    const gate = createDeadTargetGate(t.deps, { setTimer: sched.setTimer });
    t.deps.deadTargetGate = gate;
    await seedAlert(t.alerts);

    // 60 batches: multi-chunk ones (250 recipients) and small ones; iOS and Android mixed.
    let dev = 0;
    const queue: SendBatch[] = [];
    for (let b = 0; b < 60; b++) {
      const base = makeBatch({ batchKey: `a:warning:c${b}:0` }, b % 3 === 0 ? 250 : 4);
      queue.push({
        ...base,
        recipients: base.recipients.map((r) => {
          const id = `d${dev++}`;
          return {
            ...r,
            deviceId: id,
            target: { platform: dev % 5 === 0 ? 'ios' : 'android', token: id } as PushTarget,
          };
        }),
      });
    }
    const all = queue.flatMap((b) => b.recipients.map((r) => r.target.token));
    // The first attempt of every multi-chunk batch is cut after its first chunk (a shutdown
    // deadline: progress is kept and the claim released), so its re-claim resumes.
    const cut = new Set<string>();

    const reasons: Record<string, number> = {};
    const keys = new Set<string>();
    let thrown = 0;
    for (let step = 0; step < 5_000; step++) {
      const batch = queue.shift();
      if (!batch) {
        if (gate.heldCount === 0) break;
        await sched.advance(DEAD_TARGET_HOLD_MS + 1);
      } else {
        keys.add(batch.batchKey);
        const control = newBatchControl();
        const pushOf = t.deps.push;
        // Only keys whose claim is never faulted, so the cut attempt is a real claimed one.
        if (
          batch.recipients.length > 100 &&
          !cut.has(batch.batchKey) &&
          bucketOf(batch.batchKey) >= 18
        ) {
          cut.add(batch.batchKey);
          t.deps.push = {
            send: async (msgs) => {
              control.abort = true; // the drain deadline passes during the first chunk
              return pushOf.send(msgs);
            },
          };
        }
        let out: BatchOutcome | undefined;
        try {
          out = await processBatch(batch, t.deps, control);
        } catch {
          thrown++;
        } finally {
          t.deps.push = pushOf;
        }
        if (out) reasons[out.reason] = (reasons[out.reason] ?? 0) + 1;
        if (!out || !out.ack) queue.push(batch); // SQS redelivers an un-acked message
        await sched.advance(1000);
        if (step === 150) faults = false; // the throttle window ends
        if (step === 40) misconfigured = false; // the APNs fix lands
      }
      await gate.settle(1000);
      queue.push(...t.sendQueue.enqueued.splice(0).flatMap((e) => e.batches));
    }

    expect(thrown).toBe(0);
    expect(reasons['claim_throttled']).toBeGreaterThan(0);
    expect(reasons['claim_failed']).toBeGreaterThan(0);
    expect(reasons['shutdown']).toBeGreaterThan(0);
    expect(claims.throttled).toBeGreaterThan(0);
    // Every claim path ran under the faults: resumed re-claims, retry keys and held keys.
    expect(lines.some((l) => l.msg === 'sender.batch_resumed')).toBe(true);
    expect([...keys].some((k) => /:r\d+$/.test(k) && bucketOf(k) < 10)).toBe(true);
    expect([...keys].some((k) => /:h\d+$/.test(k))).toBe(true);
    for (const tok of all) expect(delivered.get(tok), tok).toBe(1);
  });
});
