import { describe, expect, it } from 'vitest';
import type { MemoryIdempotencyStore } from '../../../src/adapters/memory/idempotency-store.js';
import type { ChunkProgress } from '../../../src/ports.js';
import {
  SENDER_CHUNK_SIZE,
  type NativePushMessage,
  type NativePushResult,
  type SendBatch,
} from '../../../src/scale/contracts.js';
import {
  SENDER_LEASE_MS,
  newBatchControl,
  processBatch,
  progressByChunk,
  recipientFingerprint,
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

/**
 * Process A sends `first` with chunk 0 finished and dies in chunk 1. After the lease runs out,
 * process B gets `second`: the same batch key, maybe another recipient list (a re-plan).
 */
async function crashThenReplan(n: number, replan: (b: SendBatch) => SendBatch) {
  const d = deliveries();
  const a = makeDeps({ chunkConcurrency: 1 });
  await seedAlert(a.alerts);
  let callsA = 0;
  a.deps.push = {
    send: async (msgs) => {
      if (++callsA === 2) return new Promise<never>(() => {}); // dies in chunk 1
      return msgs.map((m) => d.note(m, { ok: true }));
    },
  };
  const first = makeBatch({}, n);
  void processBatch(first, a.deps);
  await waitFor(() => callsA === 2);

  const { lines, logger } = recordingLogger();
  const pushB = new FakePush((m) => d.note(m, { ok: true }));
  const b = makeDeps({
    chunkConcurrency: 1,
    logger,
    push: pushB,
    idempotency: a.idempotency,
    alerts: a.alerts,
    clock: a.deps.clock,
  });
  a.state.now = new Date(a.state.now.getTime() + SENDER_LEASE_MS + 1000);
  const second = replan(first);
  const out = await processBatch(second, b.deps);
  return { d, out, lines, pushB, first, second };
}

describe('sender crash: progress is bound to the recipient list (review probes)', () => {
  it('re-plan without dev5 after a crash in chunk 1: dev100 is still sent (was: never sent)', async () => {
    const r = await crashThenReplan(250, (b) => ({
      ...b,
      recipients: b.recipients.filter((x) => x.deviceId !== 'dev5'),
    }));
    expect(r.out).toMatchObject({ ack: true, sent: 249 });
    // Offsets shifted by one: progress for the old list is ignored, the batch goes out in full.
    for (const rec of r.second.recipients) {
      const i = Number(rec.checkInToken.slice(2));
      expect(r.d.ok.get(i) ?? 0, rec.deviceId).toBeGreaterThanOrEqual(1);
    }
    expect(r.d.ok.get(100)).toBeGreaterThanOrEqual(1);
    expect(r.pushB.calls.flat()).toHaveLength(249);
    expect(r.lines.find((l) => l.msg === 'sender.progress_ignored')?.fields).toMatchObject({
      reason: 'recipients_changed',
    });
  });

  it('a device added, or the same devices in another order, also ignores the progress', async () => {
    const added = await crashThenReplan(250, (b) => ({
      ...b,
      recipients: [
        { ...b.recipients[0]!, deviceId: 'new', checkInToken: 'ct999' },
        ...b.recipients,
      ],
    }));
    expect(added.pushB.calls.flat()).toHaveLength(251);
    const reordered = await crashThenReplan(250, (b) => ({
      ...b,
      recipients: [...b.recipients].reverse(),
    }));
    expect(reordered.pushB.calls.flat()).toHaveLength(250);
    for (let i = 0; i < 250; i++) expect(reordered.d.ok.get(i) ?? 0).toBeGreaterThanOrEqual(1);
  });

  it('a changed push token for one device ignores the progress (no skip to a token never sent)', async () => {
    const r = await crashThenReplan(250, (b) => ({
      ...b,
      recipients: b.recipients.map((x) =>
        x.deviceId === 'dev7' ? { ...x, target: { ...x.target, token: 'rotated' } } : x,
      ),
    }));
    expect(r.pushB.calls.flat()).toHaveLength(250);
  });

  it('the same list resumes as before (control)', async () => {
    const r = await crashThenReplan(250, (b) => b);
    expect(r.pushB.calls.flat()).toHaveLength(150); // chunk 0 skipped
    for (let i = 0; i < 250; i++) expect(r.d.ok.get(i), `recipient ${i}`).toBe(1);
  });

  it('the fingerprint depends on order, devices, tokens and nothing else', () => {
    const b = makeBatch({}, 5);
    const fp = recipientFingerprint(b.recipients);
    expect(recipientFingerprint(structuredClone(b.recipients))).toBe(fp);
    expect(recipientFingerprint([...b.recipients].reverse())).not.toBe(fp);
    expect(recipientFingerprint(b.recipients.slice(1))).not.toBe(fp);
    // Language and check-in token do not move offsets: same fingerprint.
    expect(recipientFingerprint(b.recipients.map((r) => ({ ...r, language: 'es' as const })))).toBe(
      fp,
    );
  });
});

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
    const fp = recipientFingerprint(batch.recipients);
    expect(progressByChunk(recorded(a.idempotency, batch.batchKey), fp)).toEqual(
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
