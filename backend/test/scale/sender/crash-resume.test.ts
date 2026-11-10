import { describe, expect, it } from 'vitest';
import type { MemoryIdempotencyStore } from '../../../src/adapters/memory/idempotency-store.js';
import type { ChunkProgress } from '../../../src/ports.js';
import {
  SENDER_CHUNK_SIZE,
  type NativePushMessage,
  type NativePushResult,
} from '../../../src/scale/contracts.js';
import {
  SENDER_LEASE_MS,
  newBatchControl,
  processBatch,
  progressByChunk,
} from '../../../src/scale/sender/sender.js';
import {
  FakePush,
  clearAtOnce,
  makeBatch,
  makeDeps,
  recordingLogger,
  seedAlert,
  waitFor,
} from './fakes.js';

const idx = (m: NativePushMessage) => Number(m.data['checkInToken']?.slice(2));

/** Counts successful pushes per recipient index across every sender "process". */
function deliveries() {
  const ok = new Map<number, number>();
  return {
    ok,
    note(m: NativePushMessage, r: NativePushResult) {
      if (r.ok) ok.set(idx(m), (ok.get(idx(m)) ?? 0) + 1);
      return r;
    },
  };
}

describe('sender crash: re-claim skips sub-chunks already sent', () => {
  it('a sender dies mid-batch; the redelivery after lease expiry sends only what had not gone out', async () => {
    expect(SENDER_CHUNK_SIZE).toBe(100);
    const N = 350; // chunks of 100, 100, 100, 50
    const d = deliveries();
    // Recipient 5 hits a retryable error in chunk 0; recipient 150 has a dead token (chunk 1).
    const decide = (m: NativePushMessage): NativePushResult =>
      idx(m) === 150 ? { ok: false, kind: 'permanent', error: 'Unregistered' } : { ok: true };

    // ---- process A: chunks 0 and 1 finish, chunk 2 is in flight when the process dies ----
    const a = makeDeps({ chunkConcurrency: 1 });
    await seedAlert(a.alerts);
    let callsA = 0;
    a.deps.push = {
      send: async (msgs) => {
        callsA++;
        if (callsA === 3) return new Promise<never>(() => {}); // the process dies here
        return msgs.map((m) =>
          d.note(
            m,
            idx(m) === 5 ? { ok: false, kind: 'retryable', error: 'Unavailable' } : decide(m),
          ),
        );
      },
    };
    const batch = makeBatch({}, N);
    void processBatch(batch, a.deps);
    await waitFor(() => callsA === 3);
    expect(progressByChunk(recorded(a.idempotency, batch.batchKey))).toEqual(
      new Map([
        [0, new Set([5])],
        [1, new Set([50])], // offset 50 of chunk 1 = recipient 150
      ]),
    );

    // ---- process B: same table, after the lease ran out ----
    const { lines, logger } = recordingLogger();
    const cleared: string[] = [];
    const pushB = new FakePush((m) => d.note(m, decide(m)));
    const b = makeDeps({
      chunkConcurrency: 1,
      logger,
      push: pushB,
      idempotency: a.idempotency,
      alerts: a.alerts,
      clock: a.deps.clock,
      onDeadTarget: async (id) => void cleared.push(id),
    });
    clearAtOnce(b.deps);
    a.state.now = new Date(a.state.now.getTime() + SENDER_LEASE_MS + 1000);
    const out = await processBatch(batch, b.deps);

    // B re-sent recipient 5 (the retryable one) and 150 (to judge its dead token again), then
    // chunks 2 and 3 in full: 152 messages instead of 350.
    expect(pushB.calls.map((c) => c.map(idx))).toEqual([
      [5],
      [150],
      Array.from({ length: 100 }, (_, i) => 200 + i),
      Array.from({ length: 50 }, (_, i) => 300 + i),
    ]);
    expect(out).toMatchObject({
      ack: true,
      reason: 'sent',
      sent: N - 1,
      permanentFailed: 1,
      retried: 0,
    });
    await waitFor(() => cleared.length === 1);
    expect(cleared).toEqual(['dev150']);
    expect(lines.find((l) => l.msg === 'sender.batch_resumed')?.fields).toMatchObject({
      chunksDone: 2,
      redo: 2,
    });
    // Exactly one successful push per live recipient, none duplicated.
    for (let i = 0; i < N; i++) expect(d.ok.get(i) ?? 0, `recipient ${i}`).toBe(i === 150 ? 0 : 1);
    expect(await a.idempotency.claim(batch.batchKey, new Date(a.state.now.getTime() + 1))).toBe(
      'sent',
    );
  });

  it('shutdown abort keeps the progress: the redelivered message skips the sent chunks', async () => {
    const t = makeDeps({ chunkConcurrency: 1 });
    await seedAlert(t.alerts);
    const control = newBatchControl();
    const push = new FakePush((m) => {
      if (idx(m) === 199) control.abort = true; // the drain deadline passes during chunk 1
      return { ok: true };
    });
    t.deps.push = push;
    const batch = makeBatch({}, 400);
    expect(await processBatch(batch, t.deps, control)).toMatchObject({
      ack: false,
      reason: 'shutdown',
    });
    expect(push.calls).toHaveLength(2);

    const again = new FakePush();
    t.deps.push = again;
    // Released, so the redelivery claims at once, and sends chunks 2 and 3 only.
    expect(await processBatch(batch, t.deps)).toMatchObject({ ack: true, sent: 400 });
    expect(again.calls.map((c) => c.length)).toEqual([100, 100]);
    expect(again.calls[0]?.[0] && idx(again.calls[0][0])).toBe(200);
  });

  it('a refused progress write means the lease is lost: stop starting chunks', async () => {
    const t = makeDeps({ chunkConcurrency: 1 });
    await seedAlert(t.alerts);
    t.idempotency.recordChunk = async () => false;
    const out = await processBatch(makeBatch({}, 300), t.deps);
    expect(out).toMatchObject({ ack: false, reason: 'lease_lost' });
    expect(t.push.calls).toHaveLength(1);
  });

  it('a failing progress write is logged and sending goes on', async () => {
    const { lines, logger } = recordingLogger();
    const t = makeDeps({ chunkConcurrency: 1, logger });
    await seedAlert(t.alerts);
    t.idempotency.recordChunk = async () => {
      throw new Error('ddb down');
    };
    expect(await processBatch(makeBatch({}, 300), t.deps)).toMatchObject({ ack: true, sent: 300 });
    expect(lines.filter((l) => l.msg === 'sender.record_chunk_failed')).toHaveLength(3);
  });

  it('single-chunk batches (late-join, small cells) write no progress; retry batches resume on their own key', async () => {
    const t = makeDeps({ chunkConcurrency: 1 });
    await seedAlert(t.alerts);
    const keys: string[] = [];
    const real = t.idempotency.recordChunk.bind(t.idempotency);
    t.idempotency.recordChunk = async (k, p) => (keys.push(k), real(k, p));
    const base = makeBatch({}, 1);
    await processBatch(
      { ...base, batchKey: `${base.batchKey.split(':')[0]}:warning:late:abc` },
      t.deps,
    );
    await processBatch(makeBatch({ batchKey: 'k:small' }, SENDER_CHUNK_SIZE), t.deps);
    expect(keys).toEqual([]);

    // A retry batch (`:r<n>`) of 250 recipients records under its own key, and only there.
    const big = makeBatch({}, 250);
    const retry = { ...big, batchKey: `${big.batchKey}:r1`, attempt: 1 };
    expect(await processBatch(retry, t.deps)).toMatchObject({ ack: true, sent: 250 });
    expect(new Set(keys)).toEqual(new Set([retry.batchKey]));
    expect(keys).toHaveLength(3);
  });
});

/** Peeks at the progress the crashed attempt left on the key (the store's private state). */
function recorded(store: MemoryIdempotencyStore, key: string): ChunkProgress[] {
  const entries = (store as unknown as { entries: Map<string, { progress?: ChunkProgress[] }> })
    .entries;
  return entries.get(key)?.progress ?? [];
}
