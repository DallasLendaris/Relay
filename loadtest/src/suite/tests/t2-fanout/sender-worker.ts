/**
 * Sender process (stands in for one Fargate sender task, 1 vCPU): the REAL SenderConsumer and
 * processBatch, wired like sender/main.ts: RoutingPushSender over a real ApnsSender (HTTP/2 + TLS
 * to the mock APNs) and a real FcmProjectSender (one FcmSender per Firebase project, each with its
 * own token bucket) to the mock FCM servers, the shared idempotency store and SQS queues in the
 * broker, the real breaker and dead-target queue, and real alert signature verification.
 */
import { generateKeyPairSync } from 'node:crypto';
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
import { verifierFromPem } from './alert.js';
import { serve } from './proc.js';
import { HIST_BUCKETS, histAdd } from './verify.js';
import { BrokerClient, QUEUE_URLS, RemoteIdempotencyStore, fakeSqs } from './wire.js';

const apnsKey = generateKeyPairSync('ec', { namedCurve: 'prime256v1' })
  .privateKey.export({ type: 'pkcs8', format: 'pem' })
  .toString();
const fcmKey = generateKeyPairSync('rsa', { modulusLength: 2048 })
  .privateKey.export({ type: 'pkcs8', format: 'pem' })
  .toString();

let broker: BrokerClient | undefined;
let verify: AlertVerifier | undefined;

interface PlatformStats {
  ok: number;
  retryable: number;
  permanent: number;
  firstOk: number;
  lastOk: number;
}
const emptyPlatform = (): PlatformStats => ({
  ok: 0,
  retryable: 0,
  permanent: 0,
  firstOk: 0,
  lastOk: 0,
});

class Run {
  readonly ios = emptyPlatform();
  readonly android = emptyPlatform();
  readonly batchHist = new Uint32Array(HIST_BUCKETS);
  readonly reasons: Record<string, number> = {};
  readonly errors = new Map<string, number>();
  readonly logEvents = new Map<string, number>();
  /** Accepted pushes per 100 ms slot (epoch ms / 100). */
  readonly timeline = new Map<number, number>();
  sent = 0;
  retried = 0;
  gaveUp = 0;
  permanentFailed = 0;
  batches = 0;
  retryBatches = 0;
  deadTargets = 0;
  consumer: SenderConsumer | undefined;
  done: Promise<void> | undefined;
  closers: Array<() => void> = [];
}
let run: Run | undefined;

/** Counts results per platform with wall-clock times (all processes share the clock). */
class Counting implements NativePushSender {
  constructor(
    private readonly inner: NativePushSender,
    private readonly s: PlatformStats,
    private readonly r: Run,
  ) {}

  async send(messages: NativePushMessage[]): Promise<NativePushResult[]> {
    const out = await this.inner.send(messages);
    const t = Date.now();
    const slot = Math.floor(t / 100);
    for (const res of out) {
      if (res.ok) {
        this.r.timeline.set(slot, (this.r.timeline.get(slot) ?? 0) + 1);
        this.s.ok++;
        if (this.s.firstOk === 0) this.s.firstOk = t;
        this.s.lastOk = t;
      } else {
        if (res.kind === 'permanent') this.s.permanent++;
        else this.s.retryable++;
        const label = res.error.replace(/\s*retry-after=\d+/, '').slice(0, 60);
        this.r.errors.set(label, (this.r.errors.get(label) ?? 0) + 1);
      }
    }
    return out;
  }
}

interface StartOptions {
  alert: Alert;
  apnsUrl: string;
  apnsCert: string;
  fcm: Array<{ projectId: string; url: string }>;
  fcmRatePerSec: number;
  concurrency: number;
}

serve({
  init: async (p: { brokerPort: number; publicPem: string }) => {
    broker = await BrokerClient.connect(p.brokerPort);
    verify = verifierFromPem(p.publicPem);
    return true;
  },
  start: async (p: StartOptions) => {
    if (!broker || !verify) throw new Error('sender not initialised');
    if (run?.consumer) throw new Error('sender already running');
    const v = verify;
    const r = new Run();
    run = r;
    const count = (msg: string) => r.logEvents.set(msg, (r.logEvents.get(msg) ?? 0) + 1);
    const logger: Logger = { info: () => {}, warn: count, error: count };
    const store = new InMemoryAlertStore();
    await store.save(p.alert);
    const alerts = new CachedAlertStore(store);

    const apns = new ApnsSender({
      privateKeyPem: apnsKey,
      keyId: 'T2KEY00001',
      teamId: 'T2TEAM0001',
      bundleId: 'org.relay.t2',
      authority: p.apnsUrl,
      connectOptions: { ca: p.apnsCert },
      logger,
    });
    const fcmSenders = p.fcm.map(
      (f) =>
        new FcmSender({
          serviceAccount: { client_email: 't2@relay-t2.iam', private_key: fcmKey },
          projectId: f.projectId,
          tokenUri: `${f.url}/token`,
          baseUrl: f.url,
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
    const iosCounted = new Counting(apns, r.ios, r);
    const push = new RoutingPushSender({
      ios: iosCounted,
      android: new Counting(android, r.android, r),
      expo: iosCounted,
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
      onDeadTarget: async () => {
        r.deadTargets++;
      },
      clock: { now: () => new Date() },
      logger,
      writeMetric: () => {},
      invalidateAlert: (id) => alerts.invalidate(id),
    };
    const handle = async (batch: SendBatch, control: BatchControl) => {
      const t0 = performance.now();
      const o = await processBatch(batch, deps, control);
      r.reasons[o.reason] = (r.reasons[o.reason] ?? 0) + 1;
      if (o.reason === 'sent') {
        histAdd(r.batchHist, performance.now() - t0);
        r.batches++;
        if (batch.attempt > 0) r.retryBatches++;
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
      writeMetric: () => {},
    });
    r.done = r.consumer.run();
    return true;
  },
  stop: async () => {
    const r = run;
    if (!r?.consumer) throw new Error('sender not running');
    r.consumer.stop();
    await r.done;
    for (const c of r.closers) c();
    run = undefined;
    return {
      ios: r.ios,
      android: r.android,
      batchHist: r.batchHist,
      timeline: [...r.timeline],
      reasons: r.reasons,
      errors: Object.fromEntries(r.errors),
      logEvents: Object.fromEntries(r.logEvents),
      sent: r.sent,
      retried: r.retried,
      gaveUp: r.gaveUp,
      permanentFailed: r.permanentFailed,
      batches: r.batches,
      retryBatches: r.retryBatches,
      deadTargets: r.deadTargets,
    };
  },
});
