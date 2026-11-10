/**
 * t5 child process: one sender (what one Fargate task is). It runs the REAL `SenderConsumer`
 * (pause handling, heartbeat, lease extension, drain), the REAL `processBatch` (claim, breaker,
 * backoff, dead-target queue), `ApnsSender`, `FcmSender` and `SqsSendQueue`, against the mock
 * push servers; only the AWS clients are replaced by fakes that talk to the broker over HTTP.
 *
 * Kept alive across scenarios; the parent restarts it with POST /start and collects the tallies
 * with POST /stop. Killing the process (scenario e) is a sender crash.
 *
 * Control: POST /start { broker, apnsUrl, fcmUrl, vclock, ksScale, id, concurrency }, POST /stop.
 */
import { generateKeyPairSync, createPublicKey } from 'node:crypto';
import {
  ChangeMessageVisibilityCommand,
  DeleteMessageBatchCommand,
  ReceiveMessageCommand,
  SendMessageBatchCommand,
} from '@aws-sdk/client-sqs';
import type { Alert } from '@relay/shared';
import {
  IdempotencyThrottledError,
  isThrottleError,
} from '../../../../../backend/src/adapters/aws/dynamo-idempotency-store.js';
import { ApnsSender } from '../../../../../backend/src/adapters/push/apns-sender.js';
import { FcmSender } from '../../../../../backend/src/adapters/push/fcm-sender.js';
import { RoutingPushSender } from '../../../../../backend/src/adapters/push/routing-push-sender.js';
import { createKillSwitch } from '../../../../../backend/src/config/kill-switch.js';
import type { Logger } from '../../../../../backend/src/delivery/types.js';
import type { ChunkProgress, IdempotencyStore } from '../../../../../backend/src/ports.js';
import { createAlertVerifier } from '../../../../../backend/src/signing/index.js';
import { CachedAlertStore } from '../../../../../backend/src/scale/sender/cached-alerts.js';
import { SenderConsumer } from '../../../../../backend/src/scale/sender/consumer.js';
import { DeadTargetQueue } from '../../../../../backend/src/scale/sender/dead-target-queue.js';
import { DEAD_TARGET_HOLD_MS } from '../../../../../backend/src/scale/sender/dead-target-gate.js';
import {
  SENDER_LEASE_MS,
  createDeadTargetGate,
  processBatch,
  type BatchOutcome,
  type SenderDeps,
} from '../../../../../backend/src/scale/sender/sender.js';
import { SqsSendQueue } from '../../../../../backend/src/scale/sender/sqs-send-queue.js';
import { PermanentBreaker } from '../../../../../backend/src/scale/sender/breaker.js';
import { runWorkerMain, sendMessage } from '../../lib/child.js';
import {
  CHUNK_CONCURRENCY,
  HttpError,
  call,
  json,
  realMs,
  sleep,
  startServer,
  vclock,
  vnow,
  type VClock,
} from './common.js';

interface StartBody {
  broker: string;
  apnsUrl: string;
  fcmUrl: string;
  vclock: VClock;
  ksScale: number;
  id: string;
  concurrency: number;
}

export interface SenderTally {
  id: string;
  batches: number;
  retryBatches: number;
  thrown: number;
  /** Error name: message of thrown batches (first 80 chars), counted. */
  thrownErrors: Record<string, number>;
  byReason: Record<string, number>;
  sent: number;
  permanentFailed: number;
  retried: number;
  gaveUp: number;
  throttleErrors: number;
  /** Dead targets handed to the cleanup queue before / after this sender's breaker first opened. */
  deadEnqueuedBeforeBreaker: number;
  deadEnqueuedAfterBreaker: number;
  logEvents: Record<string, number>;
  deadFlushLeft: number;
  /** Recipients the dead-target gate re-queued (breaker found a misconfiguration): sum of `count`. */
  deadRequeuedRecipients: number;
  /** Every decision of the dead-target gate: re-queue or clear, why, how many, breaker state. */
  gateDecisions: Array<{
    kind: 'requeued' | 'released';
    reason: string;
    count: number;
    samples: number;
    rollingRate: number;
  }>;
  /** Retry batches of held recipients handled (key `<batchKey>:h<n>`). */
  heldRetryBatches: number;
}

const fresh = (id: string): SenderTally => ({
  id,
  batches: 0,
  retryBatches: 0,
  thrown: 0,
  thrownErrors: {},
  byReason: {},
  sent: 0,
  permanentFailed: 0,
  retried: 0,
  gaveUp: 0,
  throttleErrors: 0,
  deadEnqueuedBeforeBreaker: 0,
  deadEnqueuedAfterBreaker: 0,
  logEvents: {},
  deadFlushLeft: 0,
  deadRequeuedRecipients: 0,
  gateDecisions: [],
  heldRetryBatches: 0,
});

/** The idempotency store as the broker's HTTP API, with the real store's throttle-retry budget. */
class HttpIdempotencyStore implements IdempotencyStore {
  constructor(
    private readonly broker: string,
    private readonly onThrottle: () => void,
  ) {}

  /** Mirrors DynamoIdempotencyStore.send: 5 tries, full-jitter backoff 50 ms x 2^n, 2.5 s budget. */
  private async claimWithRetry(
    key: string,
    leaseUntil: Date,
  ): Promise<'claimed' | 'sent' | 'leased'> {
    let slept = 0;
    for (let attempt = 1; ; attempt++) {
      try {
        const r = await call<{ r: 'claimed' | 'sent' | 'leased' }>(
          this.broker,
          'POST',
          '/idem/claim',
          { key, leaseUntil: leaseUntil.toISOString() },
        );
        return r.r;
      } catch (err) {
        if (!(err instanceof HttpError) || err.status !== 503) throw err;
        const named = Object.assign(new Error('throttled'), { name: 'ThrottlingException' });
        if (!isThrottleError(named)) throw err;
        this.onThrottle();
        const delay = Math.floor(Math.random() * Math.min(1000, 50 * 2 ** (attempt - 1)));
        if (attempt >= 5 || slept + delay > 2500)
          throw new IdempotencyThrottledError(attempt, named);
        slept += delay;
        await sleep(delay);
      }
    }
  }

  claim(key: string, leaseUntil: Date) {
    return this.claimWithRetry(key, leaseUntil);
  }
  async markSent(key: string, expiresAt: Date): Promise<void> {
    await call(this.broker, 'POST', '/idem/markSent', { key, expiresAt: expiresAt.toISOString() });
  }
  async release(key: string): Promise<void> {
    await call(this.broker, 'POST', '/idem/release', { key });
  }
  async extend(key: string, leaseUntil: Date): Promise<boolean> {
    const r = await call<{ ok: boolean }>(this.broker, 'POST', '/idem/extend', {
      key,
      leaseUntil: leaseUntil.toISOString(),
    });
    return r.ok;
  }
  async recordChunk(key: string, progress: ChunkProgress): Promise<boolean> {
    const r = await call<{ ok: boolean }>(this.broker, 'POST', '/idem/recordChunk', {
      key,
      set: progress.set,
      chunk: progress.chunk,
      redo: progress.redo,
    });
    return r.ok;
  }
  async claimedProgress(key: string): Promise<ChunkProgress[]> {
    const r = await call<{ progress: ChunkProgress[] }>(this.broker, 'POST', '/idem/progress', {
      key,
    });
    return r.progress;
  }
}

// The real kill switch writes an EMF metric line to stdout on every read; keep the console quiet.
console.log = () => undefined;

runWorkerMain(async () => {
  /** Real ms of the last held-dead-target submit / the last gate decision, and the hold window. */
  const gateClock = { lastSubmitAt: 0, lastDecisionAt: 0, holdMs: 0 };
  let running: {
    consumer: SenderConsumer;
    run: Promise<void>;
    tally: SenderTally;
    deadTargets: DeadTargetQueue;
    gate: ReturnType<typeof createDeadTargetGate>;
    closers: Array<() => void>;
  } | null = null;

  // Keys are generated once per process (RSA keygen is slow); the mocks accept any key.
  const apnsKey = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }).privateKey.export({
    type: 'pkcs8',
    format: 'pem',
  }) as string;
  const fcmKey = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({
    type: 'pkcs8',
    format: 'pem',
  }) as string;

  const start = async (b: StartBody): Promise<void> => {
    const tally = fresh(b.id);
    const vc = b.vclock;
    let breakerOpened = false;
    const logger: Logger = {
      // Info lines are only counted when a scenario reports them.
      info: (m, fields) => {
        if (m === 'sender.batch_resumed' || m === 'sender.dead_targets_released') {
          tally.logEvents[m] = (tally.logEvents[m] ?? 0) + 1;
        }
        if (m === 'sender.dead_targets_released') {
          gateClock.lastDecisionAt = Date.now();
          tally.gateDecisions.push({
            kind: 'released',
            reason: String(fields?.['reason']),
            count: Number(fields?.['count'] ?? 0),
            samples: Number(fields?.['samples'] ?? 0),
            rollingRate: Number(fields?.['rollingRate'] ?? 0),
          });
        }
      },
      warn: (m) => void (tally.logEvents[m] = (tally.logEvents[m] ?? 0) + 1),
      error: (m, fields) => {
        tally.logEvents[m] = (tally.logEvents[m] ?? 0) + 1;
        if (m === 'delivery.permanent_breaker_open') breakerOpened = true;
        if (m === 'sender.dead_targets_requeued') {
          tally.deadRequeuedRecipients += Number(fields?.['count'] ?? 0);
          tally.gateDecisions.push({
            kind: 'requeued',
            reason: String(fields?.['reason']),
            count: Number(fields?.['count'] ?? 0),
            samples: Number(fields?.['samples'] ?? 0),
            rollingRate: Number(fields?.['rollingRate'] ?? 0),
          });
          gateClock.lastDecisionAt = Date.now();
        }
      },
    };

    // ----- fake SQS: the broker's queue -----
    const sqs = {
      async send(cmd: unknown): Promise<unknown> {
        if (cmd instanceof ReceiveMessageCommand) {
          const i = cmd.input;
          const r = await call<{
            messages: Array<{ id: string; receipt: string; body: string; sentAt: number }>;
          }>(
            b.broker,
            'GET',
            `/take?queue=${i.QueueUrl}&max=${i.MaxNumberOfMessages ?? 1}&sender=${b.id}`,
          );
          if (r.messages.length === 0 && (i.WaitTimeSeconds ?? 0) > 0) await sleep(100);
          return {
            Messages: r.messages.map((m) => ({
              MessageId: m.id,
              ReceiptHandle: m.receipt,
              Body: m.body,
              Attributes: { SentTimestamp: String(m.sentAt) },
            })),
          };
        }
        if (cmd instanceof DeleteMessageBatchCommand) {
          const entries = cmd.input.Entries ?? [];
          await call(b.broker, 'POST', '/delete', {
            receipts: entries.map((e) => e.ReceiptHandle),
          });
          return { Successful: entries.map((e) => ({ Id: e.Id })), Failed: [] };
        }
        if (cmd instanceof ChangeMessageVisibilityCommand) {
          await call(b.broker, 'POST', '/visibility', {
            receipt: cmd.input.ReceiptHandle,
            seconds: cmd.input.VisibilityTimeout,
          });
          return {};
        }
        if (cmd instanceof SendMessageBatchCommand) {
          const entries = cmd.input.Entries ?? [];
          await Promise.all(
            entries.map((e) =>
              call(
                b.broker,
                'POST',
                `/enqueue?queue=${cmd.input.QueueUrl}&delay=${e.DelaySeconds ?? 0}`,
                e.MessageBody ?? '',
              ),
            ),
          );
          return { Successful: entries.map((e) => ({ Id: e.Id })), Failed: [] };
        }
        throw new Error('unexpected SQS command');
      },
    } as unknown as ConstructorParameters<typeof SenderConsumer>[0]['sqs'];

    // ----- alert and signature -----
    const doc = await call<{ publicKeyPem: string; kid: string }>(b.broker, 'GET', '/alert');
    const verifyAlert = createAlertVerifier(
      new Map([[doc.kid, createPublicKey(doc.publicKeyPem)]]),
    );
    const alerts = new CachedAlertStore(
      {
        async get(id: string): Promise<Alert | null> {
          const r = await call<{ alert: Alert }>(b.broker, 'GET', '/alert');
          return r.alert.id === id ? r.alert : null;
        },
      },
      5000,
    );

    // ----- kill switch: the real one over a fake SSM client -----
    const ksNow = (): number => vc.epochMs + (Date.now() - vc.epochMs) * b.ksScale;
    const ssm = {
      send: async () => {
        const r = await call<{ value: string }>(b.broker, 'GET', '/killswitch');
        return { Parameter: { Value: r.value } };
      },
    } as unknown as Parameters<typeof createKillSwitch>[1];
    const isPaused = createKillSwitch('t5', ssm, ksNow);

    const idempotency = new HttpIdempotencyStore(b.broker, () => void tally.throttleErrors++);
    const apns = new ApnsSender({
      privateKeyPem: apnsKey,
      keyId: 'SIMKEY0001',
      teamId: 'SIMTEAM001',
      bundleId: 'org.relay.sim',
      authority: b.apnsUrl,
      sessions: 4,
    });
    const fcm = new FcmSender({
      serviceAccount: { client_email: 'sim@relay-sim.iam', private_key: fcmKey },
      projectId: 'relay-sim',
      tokenUri: `${b.fcmUrl}/token`,
      baseUrl: b.fcmUrl,
    });
    const onDead = async (_deviceId: string, target: { token: string }): Promise<void> => {
      await call(b.broker, 'POST', '/dead', { token: target.token });
    };
    const deadTargets = new DeadTargetQueue({
      process: onDead,
      logger,
      writeMetric: () => undefined,
    });
    const deps: SenderDeps = {
      idempotency,
      alerts,
      verifyAlert: (a) => verifyAlert(a),
      isDeliveryPaused: isPaused,
      push: new RoutingPushSender({ ios: apns, android: fcm, expo: apns }),
      sendQueue: new SqsSendQueue(sqs, { urgent: 'urgent', standard: 'standard' }),
      onDeadTarget: onDead,
      // Called by processBatch at the moment it decides a target is dead (never when the breaker
      // is open for that batch), so the count tells clears before and after the breaker opened.
      deadTargets: {
        enqueue: (deviceId, target) => {
          if (breakerOpened) tally.deadEnqueuedAfterBreaker++;
          else tally.deadEnqueuedBeforeBreaker++;
          return deadTargets.enqueue(deviceId, target);
        },
      },
      breaker: new PermanentBreaker(),
      chunkConcurrency: CHUNK_CONCURRENCY,
      clock: vclock(vc),
      logger,
      writeMetric: () => undefined,
      invalidateAlert: (id) => alerts.invalidate(id),
    };

    // The deployed wiring (sender/main.ts): the gate holds dead-target clears until the breaker
    // holds every dead-target clear (DEAD_TARGET_HOLD_MS) and, per platform, clears it or re-queues
    // it (never clears) when that platform looks misconfigured. The hold runs on the virtual clock.
    gateClock.lastSubmitAt = 0;
    gateClock.lastDecisionAt = 0;
    gateClock.holdMs = realMs(vc, DEAD_TARGET_HOLD_MS);
    const gate = createDeadTargetGate(deps, { holdMs: gateClock.holdMs });
    deps.deadTargetGate = {
      submit: (group) => {
        if (group.recipients.length > 0) gateClock.lastSubmitAt = Date.now();
        gate.submit(group);
      },
    };

    const consumer = new SenderConsumer({
      sqs,
      urgentQueueUrl: 'urgent',
      standardQueueUrl: 'standard',
      handle: async (batch, control): Promise<BatchOutcome> => {
        try {
          const o = await processBatch(batch, deps, control);
          tally.batches++;
          if (batch.attempt > 0) tally.retryBatches++;
          if (/:[hv]\d+($|:)/.test(batch.batchKey)) tally.heldRetryBatches++;
          tally.byReason[o.reason] = (tally.byReason[o.reason] ?? 0) + 1;
          tally.sent += o.sent;
          tally.permanentFailed += o.permanentFailed;
          tally.retried += o.retried;
          tally.gaveUp += o.gaveUp;
          return o;
        } catch (err) {
          tally.thrown++;
          const e = err instanceof Error ? `${err.name}: ${err.message}` : String(err);
          const k = e.slice(0, 80);
          tally.thrownErrors[k] = (tally.thrownErrors[k] ?? 0) + 1;
          throw err;
        }
      },
      extendLease: (batch) =>
        idempotency.extend(batch.batchKey, new Date(vnow(vc) + SENDER_LEASE_MS)),
      isPaused,
      logger,
      concurrency: b.concurrency,
      now: () => vnow(vc),
      // The consumer's real-time timers, compressed with the virtual clock.
      extendAfterMs: realMs(vc, 20_000),
      pausePollMs: realMs(vc, 5_000),
      pauseCacheMs: 5_000 / b.ksScale,
      drainTimeoutMs: Math.max(3_000, realMs(vc, 25_000)),
      abortGraceMs: Math.max(500, realMs(vc, 2_000)),
      writeMetric: () => undefined,
    });
    const run = consumer.run();
    running = {
      consumer,
      run,
      tally,
      deadTargets,
      gate,
      closers: [() => apns.close(), () => fcm.close()],
    };
  };

  const control = await startServer(async (method, path, _q, body) => {
    if (method === 'POST' && path === '/start') {
      if (running) throw new HttpError(409, 'already running', path);
      await start(json<StartBody>(body));
      return { ok: true };
    }
    if (method === 'GET' && path === '/gate') {
      // Held dead targets still undecided (the gate decides by sample count or after its hold).
      const g = gateClock;
      const undecided =
        running !== null &&
        g.lastSubmitAt > g.lastDecisionAt &&
        Date.now() - g.lastSubmitAt < g.holdMs + 1_000;
      return { undecided };
    }
    if (method === 'POST' && path === '/stop') {
      const r = running;
      if (!r) return { tally: null };
      running = null;
      r.consumer.stop();
      await r.run;
      r.gate.shutdown(); // decide on held clears now, so the flush below includes them
      await r.gate.settle(5_000); // re-queues of held recipients go first
      r.tally.deadFlushLeft = await r.deadTargets.flush(15_000);
      for (const c of r.closers) c();
      return { tally: r.tally };
    }
    return {};
  });

  await sendMessage({ control: `http://127.0.0.1:${control.port}` });
  await new Promise<void>(() => undefined);
  return {};
});
