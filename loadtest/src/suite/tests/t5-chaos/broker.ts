/**
 * t5 child process: the broker. It plays the AWS services the real code talks to, so the senders can
 * run in separate processes: an SQS-like send queue (visibility timeout, delay, redelivery), the
 * idempotency store (the real MemoryIdempotencyStore, on the shared virtual clock), the kill switch
 * parameter, the cleared-dead-targets record, and the signed alert the senders read.
 *
 * Kept alive across scenarios; `POST /config` resets everything. Sends { control } to the parent.
 *
 * Routes (JSON over HTTP on 127.0.0.1): see the switch in `route`.
 */
import { MemoryIdempotencyStore } from '../../../../../backend/src/adapters/memory/idempotency-store.js';
import { runWorkerMain, sendMessage } from '../../lib/child.js';
import {
  HttpError,
  isDeadToken,
  json,
  startServer,
  unitHash,
  vnow,
  type ThrottleSpec,
  type VClock,
} from './common.js';

interface Message {
  id: string;
  body: string;
  queue: string;
  sentAt: number;
}

interface InFlight {
  msg: Message;
  /** Virtual ms at which the message becomes visible again. */
  expiresAt: number;
  sender: string;
}

export interface BrokerConfig {
  vclock: VClock;
  /** Visibility timeout of a received message, virtual seconds. */
  visibilityS: number;
  /** Producers are told to wait while this many messages are queued. */
  maxQueued: number;
  throttle?: ThrottleSpec;
  /** Tokens for which a clear is legitimate (deterministic hash below this rate); 0 = none. */
  deadRate: number;
}

runWorkerMain(async () => {
  let cfg: BrokerConfig = {
    vclock: { epochMs: Date.now(), scale: 1 },
    visibilityS: 60,
    maxQueued: 300,
    deadRate: 0,
  };
  let idem = new MemoryIdempotencyStore({ now: () => new Date(vnow(cfg.vclock)) });
  const lanes: Record<string, Message[]> = { urgent: [], standard: [] };
  const heads: Record<string, number> = { urgent: 0, standard: 0 };
  let delayed = 0;
  let inflight = new Map<string, InFlight>();
  let nextId = 1;
  let paused = false;
  let alert: { alert: unknown; publicKeyPem: string; kid: string } | null = null;
  let counters = fresh();
  let dead = new Map<string, number>();
  let firstClaimAt: number | null = null;
  let pauseLog: Array<{ paused: boolean; at: number }> = [];

  function fresh() {
    return {
      enqueued: 0,
      taken: 0,
      redelivered: 0,
      deleted: 0,
      maxInflight: 0,
      claims: 0,
      throttledClaims: 0,
      markSent: 0,
      releases: 0,
      extendOk: 0,
      extendRefused: 0,
      leasedAnswers: 0,
      sentAnswers: 0,
      claimedAnswers: 0,
    };
  }

  /** Messages a consumer could receive right now (delayed and in-flight ones are not). */
  const queued = (): number =>
    lanes['urgent']!.length - heads['urgent']! + (lanes['standard']!.length - heads['standard']!);

  const push = (m: Message): void => {
    lanes[m.queue]!.push(m);
  };

  /** Messages whose visibility ran out come back (a crashed or stalled consumer). */
  const reclaim = (): void => {
    const now = vnow(cfg.vclock);
    for (const [receipt, f] of inflight) {
      if (f.expiresAt <= now) {
        inflight.delete(receipt);
        counters.redelivered++;
        push(f.msg);
      }
    }
  };
  setInterval(reclaim, 100).unref();

  const compact = (q: string): void => {
    const l = lanes[q]!;
    if (heads[q]! > 4096 && heads[q]! * 2 > l.length) {
      lanes[q] = l.slice(heads[q]);
      heads[q] = 0;
    }
  };

  const recipientsOf = (body: string): number => {
    try {
      const batches = JSON.parse(body) as Array<{ recipients?: unknown[] }>;
      return batches.reduce((n, b) => n + (b.recipients?.length ?? 0), 0);
    } catch {
      return 0;
    }
  };

  const throttled = (key: string): boolean => {
    const t = cfg.throttle;
    if (!t) return false;
    const now = vnow(cfg.vclock);
    firstClaimAt ??= now;
    const s = (now - firstClaimAt) / 1000;
    if (s < t.fromS || s >= t.toS) return false;
    // Per key, so every retry of the same claim fails the same way (the store gives up).
    return unitHash(key, 'throttle:') < t.p;
  };

  const control = await startServer((method, path, query, body) => {
    switch (`${method} ${path}`) {
      case 'POST /config': {
        cfg = json<BrokerConfig>(body);
        idem = new MemoryIdempotencyStore({ now: () => new Date(vnow(cfg.vclock)) });
        lanes['urgent'] = [];
        lanes['standard'] = [];
        heads['urgent'] = 0;
        heads['standard'] = 0;
        delayed = 0;
        inflight = new Map();
        paused = false;
        alert = null;
        counters = fresh();
        dead = new Map();
        firstClaimAt = null;
        pauseLog = [];
        return { ok: true };
      }
      case 'POST /alert':
        alert = json(body);
        return { ok: true };
      case 'GET /alert':
        if (!alert) throw new HttpError(404, 'no alert', path);
        return alert;
      case 'POST /enqueue': {
        const queue = query.get('queue') ?? 'urgent';
        const delayS = Number(query.get('delay') ?? 0);
        const m: Message = {
          id: String(nextId++),
          body,
          queue,
          sentAt: vnow(cfg.vclock),
        };
        counters.enqueued++;
        if (delayS > 0) {
          delayed++;
          setTimeout(
            () => {
              delayed--;
              push(m);
            },
            (delayS * 1000) / cfg.vclock.scale,
          ).unref();
        } else push(m);
        return { queued: queued() };
      }
      case 'GET /depth':
        return { queued: queued(), delayed, inflight: inflight.size };
      case 'GET /take': {
        const queue = query.get('queue') ?? 'urgent';
        const max = Math.min(10, Number(query.get('max') ?? 1));
        const sender = query.get('sender') ?? '?';
        reclaim();
        const out: Array<{ id: string; receipt: string; body: string; sentAt: number }> = [];
        const lane = lanes[queue]!;
        while (out.length < max && heads[queue]! < lane.length) {
          const m = lane[heads[queue]!++]!;
          const receipt = `${m.id}.${nextId++}`;
          inflight.set(receipt, {
            msg: m,
            expiresAt: vnow(cfg.vclock) + cfg.visibilityS * 1000,
            sender,
          });
          counters.taken++;
          out.push({ id: m.id, receipt, body: m.body, sentAt: m.sentAt });
        }
        compact(queue);
        counters.maxInflight = Math.max(counters.maxInflight, inflight.size);
        return { messages: out };
      }
      case 'POST /delete': {
        const { receipts } = json<{ receipts: string[] }>(body);
        let n = 0;
        for (const r of receipts) if (inflight.delete(r)) n++;
        counters.deleted += n;
        return { deleted: n };
      }
      case 'POST /visibility': {
        const { receipt, seconds } = json<{ receipt: string; seconds: number }>(body);
        const f = inflight.get(receipt);
        if (!f) return { ok: false };
        if (seconds <= 0) {
          inflight.delete(receipt);
          counters.redelivered++;
          push(f.msg);
        } else f.expiresAt = vnow(cfg.vclock) + seconds * 1000;
        return { ok: true };
      }
      case 'POST /idem/claim': {
        const { key, leaseUntil } = json<{ key: string; leaseUntil: string }>(body);
        counters.claims++;
        if (throttled(key)) {
          counters.throttledClaims++;
          throw new HttpError(503, 'ThrottlingException', path);
        }
        return idem.claim(key, new Date(leaseUntil)).then((r) => {
          if (r === 'claimed') counters.claimedAnswers++;
          else if (r === 'sent') counters.sentAnswers++;
          else counters.leasedAnswers++;
          return { r };
        });
      }
      case 'POST /idem/markSent': {
        const { key, expiresAt } = json<{ key: string; expiresAt: string }>(body);
        counters.markSent++;
        return idem.markSent(key, new Date(expiresAt)).then(() => ({ ok: true }));
      }
      case 'POST /idem/release': {
        const { key } = json<{ key: string }>(body);
        counters.releases++;
        return idem.release(key).then(() => ({ ok: true }));
      }
      case 'POST /idem/extend': {
        const { key, leaseUntil } = json<{ key: string; leaseUntil: string }>(body);
        return idem.extend(key, new Date(leaseUntil)).then((ok) => {
          if (ok) counters.extendOk++;
          else counters.extendRefused++;
          return { ok };
        });
      }
      case 'GET /killswitch':
        return { value: paused ? 'paused' : 'running' };
      case 'POST /killswitch': {
        const b = json<{ paused: boolean }>(body);
        paused = b.paused;
        pauseLog.push({ paused, at: Date.now() });
        return { ok: true, at: Date.now() };
      }
      case 'POST /dead': {
        const { token } = json<{ token: string }>(body);
        dead.set(token, (dead.get(token) ?? 0) + 1);
        return { ok: true };
      }
      case 'GET /inflight': {
        const sender = query.get('sender') ?? '';
        let messages = 0;
        let recipients = 0;
        for (const f of inflight.values()) {
          if (f.sender !== sender) continue;
          messages++;
          recipients += recipientsOf(f.msg.body);
        }
        return { messages, recipients };
      }
      case 'GET /stats': {
        let cleared = 0;
        let clearedLive = 0;
        let clearedTwice = 0;
        for (const [token, n] of dead) {
          cleared++;
          if (n > 1) clearedTwice++;
          if (!isDeadToken(token, cfg.deadRate)) clearedLive++;
        }
        return {
          ...counters,
          queued: queued(),
          delayed,
          inflight: inflight.size,
          paused,
          pauseLog,
          cleared,
          clearedLive,
          clearedTwice,
        };
      }
      default:
        throw new HttpError(404, 'unknown route', path);
    }
  });

  await sendMessage({ control: `http://127.0.0.1:${control.port}` });
  await new Promise<void>(() => undefined);
  return {};
});
