/**
 * Broker process: stands in for the two SQS send queues (urgent, standard) and the DynamoDB
 * idempotency table, shared by every shard-worker and sender process over TCP (wire.ts).
 *
 * SQS behaviour kept: standard-queue semantics (at-least-once, roughly FIFO), receive up to 10,
 * long polling, visibility timeout (30 s, as infra's SEND_QUEUE_VISIBILITY_SECONDS) with
 * redelivery, ChangeMessageVisibility, DelaySeconds, receive count and a DLQ after 100 receives
 * (SEND_QUEUE_MAX_RECEIVE_COUNT), SentTimestamp. Not kept: SQS is effectively unbounded, this
 * process is not, so producers (shard workers) wait while the backlog is over `capBytes`
 * (sender retries never wait). That only delays the shard phase, never the send rate.
 *
 * Idempotency keeps MemoryIdempotencyStore's semantics with the attempt passed explicitly, so a
 * release or markSent from a process whose lease ran out never touches another process's claim.
 */
import net from 'node:net';
import { serve } from './proc.js';
import { FrameReader, encodeFrame, type Frame } from './wire.js';

const VISIBILITY_MS = 30_000;
const MAX_RECEIVE_COUNT = 100;

interface Msg {
  mid: string;
  body: Buffer;
  sent: number;
  rc: number;
  rh: string;
  visibleAt: number;
}

interface Waiter {
  max: number;
  done(msgs: Msg[]): void;
}

class Queue {
  visible: Msg[] = [];
  head = 0;
  readonly inflight = new Map<string, Msg>();
  delayed = 0;
  waiters: Waiter[] = [];

  get visibleCount(): number {
    return this.visible.length - this.head;
  }

  take(max: number): Msg[] {
    const out: Msg[] = [];
    while (out.length < max && this.head < this.visible.length) {
      const m = this.visible[this.head++] as Msg;
      out.push(m);
    }
    if (this.head > 4096 && this.head * 2 > this.visible.length) {
      this.visible = this.visible.slice(this.head);
      this.head = 0;
    }
    return out;
  }
}

const state = {
  queues: { urgent: new Queue(), standard: new Queue() } as Record<string, Queue>,
  bytes: 0,
  peakBytes: 0,
  capBytes: 256 * 1048576,
  nextMid: 0,
  sentMessages: 0,
  maxBodyBytes: 0,
  overBudgetMessages: 0,
  sentDelayed: 0,
  receives: 0,
  redeliveries: 0,
  dlq: 0,
  producerWaits: 0,
  producerWaitMs: 0,
  generation: 0,
  idem: new Map<string, { state: 'leased' | 'sent'; until: number; attempt: number }>(),
  nextAttempt: 1,
  idemOps: 0,
};
let producerQueue: Array<() => void> = [];

function addBytes(n: number): void {
  state.bytes += n;
  state.peakBytes = Math.max(state.peakBytes, state.bytes);
  if (n < 0 && state.bytes < state.capBytes && producerQueue.length > 0) {
    const q = producerQueue;
    producerQueue = [];
    for (const f of q) f();
  }
}

function makeVisible(q: Queue, m: Msg): void {
  const w = q.waiters.shift();
  if (w) {
    deliver(q, [m]);
    w.done([m]);
    return;
  }
  q.visible.push(m);
}

function deliver(q: Queue, msgs: Msg[]): void {
  const now = Date.now();
  for (const m of msgs) {
    m.rc++;
    if (m.rc > 1) state.redeliveries++;
    m.rh = `${m.mid}:${m.rc}`;
    m.visibleAt = now + VISIBILITY_MS;
    q.inflight.set(m.rh, m);
    state.receives++;
  }
}

// Visibility timeouts: return expired in-flight messages to the queue (or the DLQ).
setInterval(() => {
  const now = Date.now();
  for (const q of Object.values(state.queues)) {
    for (const [rh, m] of q.inflight) {
      if (m.visibleAt > now) continue;
      q.inflight.delete(rh);
      if (m.rc >= MAX_RECEIVE_COUNT) {
        state.dlq++;
        addBytes(-m.body.length);
      } else makeVisible(q, m);
    }
  }
}, 100).unref();

async function handle(f: Frame): Promise<{ h: Record<string, unknown>; p?: Buffer }> {
  const h = f.h;
  const q = state.queues[String(h['queue'])];
  switch (h['op']) {
    case 'send': {
      if (!q) throw new Error('bad queue');
      if (h['role'] === 'producer' && state.bytes >= state.capBytes) {
        const t0 = performance.now();
        state.producerWaits++;
        while (state.bytes >= state.capBytes) {
          await new Promise<void>((r) => producerQueue.push(r));
        }
        state.producerWaitMs += performance.now() - t0;
      }
      const entries = h['entries'] as Array<{ delay: number; len: number }>;
      let at = 0;
      const gen = state.generation;
      for (const e of entries) {
        // Copy: the frame buffer is shared with the rest of the socket read.
        const body = Buffer.from(f.p.subarray(at, at + e.len));
        at += e.len;
        const m: Msg = {
          mid: `m${++state.nextMid}`,
          body,
          sent: Date.now(),
          rc: 0,
          rh: '',
          visibleAt: 0,
        };
        state.sentMessages++;
        state.maxBodyBytes = Math.max(state.maxBodyBytes, body.length);
        if (body.length > 240 * 1024) state.overBudgetMessages++;
        addBytes(body.length);
        if (e.delay > 0) {
          state.sentDelayed++;
          q.delayed++;
          setTimeout(() => {
            if (gen !== state.generation) return;
            q.delayed--;
            makeVisible(q, m);
          }, e.delay * 1000);
        } else makeVisible(q, m);
      }
      return { h: {} };
    }
    case 'recv': {
      if (!q) throw new Error('bad queue');
      const max = Math.max(1, Math.min(10, Number(h['max'])));
      let msgs = q.take(max);
      if (msgs.length > 0) deliver(q, msgs);
      else if (Number(h['wait']) > 0) {
        msgs = await new Promise<Msg[]>((resolve) => {
          const w: Waiter = {
            max,
            done: (m) => {
              clearTimeout(t);
              resolve(m);
            },
          };
          const t = setTimeout(
            () => {
              q.waiters = q.waiters.filter((x) => x !== w);
              resolve([]);
            },
            Number(h['wait']) * 1000,
          );
          q.waiters.push(w);
        });
      }
      return {
        h: { msgs: msgs.map((m) => ({ mid: m.mid, rh: m.rh, sent: m.sent, len: m.body.length })) },
        p: Buffer.concat(msgs.map((m) => m.body)),
      };
    }
    case 'del': {
      if (!q) throw new Error('bad queue');
      for (const rh of h['rhs'] as string[]) {
        const m = q.inflight.get(rh);
        if (!m) continue; // stale receipt handle (redelivered meanwhile): ignored, like SQS
        q.inflight.delete(rh);
        addBytes(-m.body.length);
      }
      return { h: {} };
    }
    case 'vis': {
      if (!q) throw new Error('bad queue');
      const m = q.inflight.get(String(h['rh']));
      if (m) {
        const vt = Number(h['vt']);
        if (vt <= 0) {
          q.inflight.delete(m.rh);
          makeVisible(q, m);
        } else m.visibleAt = Date.now() + vt * 1000;
      }
      return { h: {} };
    }
    case 'claim': {
      state.idemOps++;
      const key = String(h['key']);
      const e = state.idem.get(key);
      if (e && e.until > Date.now()) return { h: { r: e.state } };
      const attempt = state.nextAttempt++;
      state.idem.set(key, { state: 'leased', until: Number(h['until']), attempt });
      return { h: { r: 'claimed', a: attempt } };
    }
    case 'markSent': {
      state.idemOps++;
      const key = String(h['key']);
      const a = Number(h['a']);
      const e = state.idem.get(key);
      const live = e && e.until > Date.now() ? e : undefined;
      if (!(live?.state === 'sent' && live.attempt !== a)) {
        state.idem.set(key, { state: 'sent', until: Number(h['until']), attempt: a });
      }
      return { h: {} };
    }
    case 'release': {
      state.idemOps++;
      const key = String(h['key']);
      const e = state.idem.get(key);
      if (e?.state === 'leased' && e.attempt === Number(h['a'])) state.idem.delete(key);
      return { h: {} };
    }
    case 'extend': {
      state.idemOps++;
      const e = state.idem.get(String(h['key']));
      if (e?.state !== 'leased' || e.attempt !== Number(h['a'])) return { h: { r: false } };
      e.until = Number(h['until']);
      return { h: { r: true } };
    }
    case 'peek': {
      state.idemOps++;
      const e = state.idem.get(String(h['key']));
      return { h: { r: e && e.until > Date.now() ? e.state : 'absent' } };
    }
    default:
      throw new Error(`unknown op ${String(h['op'])}`);
  }
}

const server = net.createServer((sock) => {
  sock.setNoDelay(true);
  sock.on('error', () => {});
  const reader = new FrameReader((f) => {
    handle(f).then(
      (r) => {
        if (!sock.destroyed) sock.write(encodeFrame({ ...r.h, id: f.h.id }, r.p));
      },
      (err: unknown) => {
        const error = err instanceof Error ? err.message : String(err);
        if (!sock.destroyed) sock.write(encodeFrame({ id: f.h.id, error }));
      },
    );
  });
  sock.on('data', (c: Buffer) => reader.push(c));
});

function stats() {
  const qs = Object.fromEntries(
    Object.entries(state.queues).map(([name, q]) => [
      name,
      { visible: q.visibleCount, inflight: q.inflight.size, delayed: q.delayed },
    ]),
  );
  const empty = Object.values(state.queues).every(
    (q) => q.visibleCount === 0 && q.inflight.size === 0 && q.delayed === 0,
  );
  return {
    queues: qs,
    empty,
    bytes: state.bytes,
    peakBytesMb: Math.round(state.peakBytes / 1048576),
    sentMessages: state.sentMessages,
    maxBodyBytes: state.maxBodyBytes,
    overBudgetMessages: state.overBudgetMessages,
    sentDelayed: state.sentDelayed,
    receives: state.receives,
    redeliveries: state.redeliveries,
    dlq: state.dlq,
    producerWaits: state.producerWaits,
    producerWaitMs: Math.round(state.producerWaitMs),
    idemKeys: state.idem.size,
    idemOps: state.idemOps,
  };
}

serve({
  start: async (p: { capMb: number }) => {
    state.capBytes = p.capMb * 1048576;
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(0, '127.0.0.1', resolve);
    });
    return { port: (server.address() as net.AddressInfo).port };
  },
  stats,
  reset: () => {
    state.generation++;
    state.queues = { urgent: new Queue(), standard: new Queue() };
    state.bytes = 0;
    state.peakBytes = 0;
    state.sentMessages = 0;
    state.maxBodyBytes = 0;
    state.overBudgetMessages = 0;
    state.sentDelayed = 0;
    state.receives = 0;
    state.redeliveries = 0;
    state.dlq = 0;
    state.producerWaits = 0;
    state.producerWaitMs = 0;
    state.idem = new Map();
    state.idemOps = 0;
    const q = producerQueue;
    producerQueue = [];
    for (const f of q) f();
    return true;
  },
});
