/**
 * t8/t9 child: one sender (stands in for a Fargate task). The REAL SenderConsumer and processBatch,
 * wired like sender/main.ts: RoutingPushSender over a real ApnsSender (HTTP/2 + TLS to the mock APNs)
 * and a real FcmProjectSender (one FcmSender per Firebase project) to the mock FCM servers, the
 * shared idempotency store and SQS queues in the broker, the real breaker and dead-target queue,
 * real alert signature verification. It holds every alert of the run.
 *
 * It keeps a ledger for the exactly-once check: for every accepted push, a 64-bit hash of the token
 * under the alert's index (one list per alert), plus the tokens answered "unregistered" and the tokens
 * whose answer was lost (timeout, connection error: a retry may legitimately double those). Lists
 * are written to files on `finish`. Accepted pushes are also counted per alert in time slots, for
 * time-to-90%, and the push of every late-join batch (batch key `...:late:...`) is timed per token.
 *
 * Control: start, finish (collect the ledger), stop (the process).
 */
import { AsyncLocalStorage } from 'node:async_hooks';
import { generateKeyPairSync } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../../../../backend/src/adapters/memory/alert-store.js';
import { ApnsSender } from '../../../../../backend/src/adapters/push/apns-sender.js';
import { FcmProjectSender } from '../../../../../backend/src/adapters/push/fcm-projects.js';
import { FcmSender } from '../../../../../backend/src/adapters/push/fcm-sender.js';
import { RoutingPushSender } from '../../../../../backend/src/adapters/push/routing-push-sender.js';
import type { Logger } from '../../../../../backend/src/delivery/types.js';
import type {
  NativePushMessage,
  NativePushResult,
  NativePushSender,
  SendBatch,
} from '../../../../../backend/src/scale/contracts.js';
import { CachedAlertStore } from '../../../../../backend/src/scale/sender/cached-alerts.js';
import { SenderConsumer } from '../../../../../backend/src/scale/sender/consumer.js';
import { SqsSendQueue } from '../../../../../backend/src/scale/sender/sqs-send-queue.js';
import {
  SENDER_LEASE_MS,
  processBatch,
  type BatchControl,
  type SenderDeps,
} from '../../../../../backend/src/scale/sender/sender.js';
import type { AlertVerifier } from '../../../../../backend/src/signing/index.js';
import { verifierFromPem } from '../t2-fanout/alert.js';
import { HIST_BUCKETS, HashList, histAdd } from '../t2-fanout/verify.js';
import { BrokerClient, QUEUE_URLS, RemoteIdempotencyStore, fakeSqs } from '../t2-fanout/wire.js';
import { runCtlWorker } from './ctl.js';

const apnsKey = generateKeyPairSync('ec', { namedCurve: 'prime256v1' })
  .privateKey.export({ type: 'pkcs8', format: 'pem' })
  .toString();
const fcmKey = generateKeyPairSync('rsa', { modulusLength: 2048 })
  .privateKey.export({ type: 'pkcs8', format: 'pem' })
  .toString();

export interface SenderStartOptions {
  id: string;
  brokerPort: number;
  publicPem: string;
  alertsFile: string;
  apnsUrl: string;
  apnsCert: string;
  fcm: Array<{ projectId: string; url: string }>;
  fcmRatePerSec: number;
  concurrency: number;
  /** Time slot of the per-alert counters, ms. */
  slotMs: number;
  /** Directory the ledger files go to. */
  outDir: string;
  /** Cap on pushes this sender sends per second (0 or absent: unlimited). Smoke runs use it to stretch a fan-out over time. */
  maxPushesPerSec?: number;
}

export interface SenderStopReply {
  id: string;
  /** Per alert: [slot, accepted pushes] of batches that are not late-join. */
  timeline: Array<Array<[number, number]>>;
  /** Per alert: accepted pushes, "unregistered" answers, and the last accepted push (epoch ms). */
  perAlert: Array<{ ok: number; dead: number; lastOk: number; lateOk: number }>;
  slotMs: number;
  batchHist: number[];
  reasons: Record<string, number>;
  errors: Record<string, number>;
  logEvents: Record<string, number>;
  sent: number;
  retried: number;
  gaveUp: number;
  permanentFailed: number;
  batches: number;
  retryBatches: number;
  lostTokens: number;
  lateTimed: number;
  files: { ok: string[]; dead: string[]; lost: string; lateTokens: string; lateTimes: string };
}

class Run {
  readonly ok: HashList[] = [];
  readonly dead: HashList[] = [];
  readonly lost = new HashList();
  readonly lateTokens = new HashList();
  readonly lateTimes: number[] = [];
  readonly timeline: Array<Map<number, number>> = [];
  readonly perAlert: SenderStopReply['perAlert'] = [];
  readonly batchHist = new Uint32Array(HIST_BUCKETS);
  readonly reasons: Record<string, number> = {};
  readonly errors = new Map<string, number>();
  readonly logEvents = new Map<string, number>();
  sent = 0;
  retried = 0;
  gaveUp = 0;
  permanentFailed = 0;
  batches = 0;
  retryBatches = 0;
  rate = 0;
  private nextFree = 0;
  consumer: SenderConsumer | undefined;
  done: Promise<void> | undefined;
  closers: Array<() => void> = [];

  /** Waits until `n` more pushes fit under the cap. */
  async pace(n: number): Promise<void> {
    if (this.rate <= 0) return;
    const now = Date.now();
    const start = Math.max(now, this.nextFree);
    this.nextFree = start + (n * 1000) / this.rate;
    if (start > now) await new Promise<void>((r) => setTimeout(r, start - now));
  }
}

const batchContext = new AsyncLocalStorage<{ late: boolean }>();

/** Records every push result in the run's ledger, then passes the results through unchanged. */
class Recording implements NativePushSender {
  constructor(
    private readonly inner: NativePushSender,
    private readonly r: Run,
    private readonly idxOf: Map<string, number>,
    private readonly slotMs: number,
  ) {}

  async send(messages: NativePushMessage[]): Promise<NativePushResult[]> {
    await this.r.pace(messages.length);
    const out = await this.inner.send(messages);
    const t = Date.now();
    const slot = Math.floor(t / this.slotMs);
    const late = batchContext.getStore()?.late === true;
    out.forEach((res, i) => {
      const m = messages[i] as NativePushMessage;
      const idx = this.idxOf.get(m.data['alertId'] ?? '');
      if (idx === undefined) {
        this.r.errors.set(
          'push for an unknown alert',
          (this.r.errors.get('push for an unknown alert') ?? 0) + 1,
        );
        return;
      }
      const stat = this.r.perAlert[idx] as SenderStopReply['perAlert'][number];
      if (res.ok) {
        (this.r.ok[idx] as HashList).add(m.target.token);
        stat.ok++;
        if (late) stat.lateOk++;
        else {
          const tl = this.r.timeline[idx] as Map<number, number>;
          tl.set(slot, (tl.get(slot) ?? 0) + 1);
          stat.lastOk = t;
        }
      } else if (res.kind === 'permanent') {
        (this.r.dead[idx] as HashList).add(m.target.token);
        stat.dead++;
      } else if (res.error.startsWith('Timeout') || res.error.includes('ConnectionError')) {
        // The push may have been accepted although we never heard back.
        this.r.lost.add(m.target.token);
      }
      if (!res.ok) {
        const label = res.error.replace(/\s*retry-after=\d+/, '').slice(0, 60);
        this.r.errors.set(label, (this.r.errors.get(label) ?? 0) + 1);
      }
    });
    return out;
  }
}

runCtlWorker(async () => {
  let broker: BrokerClient | undefined;
  let verify: AlertVerifier | undefined;
  let run: Run | undefined;
  let outDir = '';
  let id = '';
  let slotMs = 50;

  const start = async (p: SenderStartOptions): Promise<void> => {
    if (run?.consumer) throw new Error('sender already running');
    broker ??= await BrokerClient.connect(p.brokerPort);
    verify = verifierFromPem(p.publicPem);
    const v = verify;
    id = p.id;
    outDir = p.outDir;
    slotMs = p.slotMs;
    const alertList = JSON.parse(readFileSync(p.alertsFile, 'utf8')) as Alert[];
    const idxOf = new Map(alertList.map((a, i) => [a.id, i]));
    const r = new Run();
    r.rate = p.maxPushesPerSec ?? 0;
    run = r;
    for (let i = 0; i < alertList.length; i++) {
      r.ok.push(new HashList());
      r.dead.push(new HashList());
      r.timeline.push(new Map());
      r.perAlert.push({ ok: 0, dead: 0, lastOk: 0, lateOk: 0 });
    }
    const count = (msg: string): void => void r.logEvents.set(msg, (r.logEvents.get(msg) ?? 0) + 1);
    const logger: Logger = { info: () => undefined, warn: count, error: count };
    const store = new InMemoryAlertStore();
    for (const a of alertList) await store.save(a);
    const alerts = new CachedAlertStore(store);

    const apns = new ApnsSender({
      privateKeyPem: apnsKey,
      keyId: 'T8KEY00001',
      teamId: 'T8TEAM0001',
      bundleId: 'org.relay.t8',
      authority: p.apnsUrl,
      connectOptions: { ca: p.apnsCert },
      logger,
    });
    const fcmSenders = p.fcm.map(
      (f) =>
        new FcmSender({
          serviceAccount: { client_email: 't8@relay-t8.iam', private_key: fcmKey },
          projectId: f.projectId,
          tokenUri: `${f.url}/token`,
          baseUrl: f.url,
          // Bounded sockets per project: 20 alerts at once must not open thousands of loopback connections.
          maxInFlight: 16,
          logger,
        }),
    );
    r.closers.push(() => apns.close(), ...fcmSenders.map((f) => () => f.close()));
    const android = new FcmProjectSender({
      projects: p.fcm.map((f, i) => ({
        projectId: f.projectId,
        sender: fcmSenders[i] as FcmSender,
      })),
      ratePerSecond: p.fcmRatePerSec,
      logger,
    });
    const ios = new Recording(apns, r, idxOf, p.slotMs);
    const push = new RoutingPushSender({
      ios,
      android: new Recording(android, r, idxOf, p.slotMs),
      expo: ios,
    });
    const idempotency = new RemoteIdempotencyStore(broker);
    const sqs = fakeSqs(broker, 'sender');
    const deps: SenderDeps = {
      idempotency,
      alerts,
      verifyAlert: (a) => v(a),
      isDeliveryPaused: async () => false,
      push,
      sendQueue: new SqsSendQueue(sqs, QUEUE_URLS),
      onDeadTarget: async () => undefined,
      clock: { now: () => new Date() },
      logger,
      writeMetric: () => undefined,
      invalidateAlert: (aid) => alerts.invalidate(aid),
    };
    const handle = async (batch: SendBatch, control: BatchControl) => {
      const t0 = performance.now();
      const late = batch.batchKey.includes(':late:');
      const o = await batchContext.run({ late }, () => processBatch(batch, deps, control));
      r.reasons[o.reason] = (r.reasons[o.reason] ?? 0) + 1;
      if (o.reason === 'sent') {
        histAdd(r.batchHist, performance.now() - t0);
        r.batches++;
        if (batch.attempt > 0) r.retryBatches++;
        // A late-join batch has one recipient: when it was sent, that device's push was accepted.
        if (late && o.sent > 0 && batch.recipients.length === 1) {
          r.lateTokens.add(batch.recipients[0]?.target.token ?? '');
          r.lateTimes.push(Date.now());
        }
      }
      r.sent += o.sent;
      r.retried += o.retried;
      r.gaveUp += o.gaveUp;
      r.permanentFailed += o.permanentFailed;
      return o;
    };
    r.consumer = new SenderConsumer({
      sqs,
      urgentQueueUrl: QUEUE_URLS.urgent,
      standardQueueUrl: QUEUE_URLS.standard,
      handle,
      extendLease: (batch) =>
        idempotency.extend(batch.batchKey, new Date(Date.now() + SENDER_LEASE_MS)),
      isPaused: async () => false,
      logger,
      concurrency: p.concurrency,
      writeMetric: () => undefined,
    });
    r.done = r.consumer.run();
  };

  const write = (name: string, words: BigUint64Array | Float64Array): string => {
    const file = join(outDir, `${id}-${name}.bin`);
    writeFileSync(file, Buffer.from(words.buffer, words.byteOffset, words.byteLength));
    return file;
  };

  const finish = async (): Promise<SenderStopReply> => {
    const r = run;
    if (!r?.consumer) throw new Error('sender not running');
    r.consumer.stop();
    await r.done;
    for (const c of r.closers) c();
    run = undefined;
    return {
      id,
      timeline: r.timeline.map((m) => [...m]),
      perAlert: r.perAlert,
      slotMs,
      batchHist: [...r.batchHist],
      reasons: r.reasons,
      errors: Object.fromEntries(r.errors),
      logEvents: Object.fromEntries(r.logEvents),
      sent: r.sent,
      retried: r.retried,
      gaveUp: r.gaveUp,
      permanentFailed: r.permanentFailed,
      batches: r.batches,
      retryBatches: r.retryBatches,
      lostTokens: r.lost.n,
      lateTimed: r.lateTimes.length,
      files: {
        ok: r.ok.map((l, i) => write(`a${i}-ok`, l.toArray())),
        dead: r.dead.map((l, i) => write(`a${i}-dead`, l.toArray())),
        lost: write('lost', r.lost.toArray()),
        lateTokens: write('late-tokens', r.lateTokens.toArray()),
        lateTimes: write('late-times', Float64Array.from(r.lateTimes)),
      },
    };
  };

  return { handlers: { start, finish } };
});
