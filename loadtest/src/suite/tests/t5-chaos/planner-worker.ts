/**
 * t5 child process: the planner side of the fan-out. It owns the seeded cell index and runs the REAL
 * planner handler, shard handler and paused-queue consumer (backend/src/scale/fanout) with a
 * simulated parking queue (the 60 s SQS delay, on the virtual clock), a real kill switch reading the
 * broker's parameter, and a send queue that is the broker's HTTP queue with back-pressure.
 *
 * Kept alive across scenarios. Args: --devices N --expected-dir DIR --seed N
 * Sends { control, ... } to the parent when the index is seeded and the expected-token files exist.
 *
 * Control: POST /prepare (new alert + fresh state), POST /plan (publish AlertReady to the planner),
 * GET /status.
 */
import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../../../../backend/src/adapters/memory/alert-store.js';
import { createKillSwitch } from '../../../../../backend/src/config/kill-switch.js';
import type { Logger } from '../../../../../backend/src/delivery/types.js';
import { cellsForAlertArea } from '../../../../../backend/src/geo/h3-cells.js';
import type {
  SendQueue,
  SendQueueName,
  ShardJob,
} from '../../../../../backend/src/scale/contracts.js';
import { CachedAlertReader } from '../../../../../backend/src/scale/fanout/alert-cache.js';
import {
  buildPlannerHandler,
  buildShardHandler,
  makeCheckInTokenFor,
} from '../../../../../backend/src/scale/fanout/handlers.js';
import type { ResumableShardJob } from '../../../../../backend/src/scale/fanout/messages.js';
import { packBatches, serializeMessage } from '../../../../../backend/src/scale/fanout/pack.js';
import { createPausedConsumer } from '../../../../../backend/src/scale/fanout/paused-consumer.js';
import {
  PARK_DELAY_SECONDS,
  type ParkedMessage,
  type PausedQueue,
} from '../../../../../backend/src/scale/fanout/paused-queue.js';
import type { ShardDispatcher } from '../../../../../backend/src/scale/fanout/queues.js';
import { createAlertVerifier, signAlertSync } from '../../../../../backend/src/signing/index.js';
import { numberArg, parseArgs } from '../../../mock-common.js';
import { generateEntries } from '../../../seed.js';
import {
  ALERT_SIGNING,
  LightCellIndex,
  TOKEN_KEY,
  countyPolygon,
} from '../../../sim/fanout-sim.js';
import { runWorkerMain, sendMessage } from '../../lib/child.js';
import { call, json, sleep, startServer, vclock, vnow, type VClock } from './common.js';

interface PrepareBody {
  vclock: VClock;
  /** Kill-switch clock speed (the real cache is 30 s). */
  ksScale: number;
  alertId: string;
  broker: string;
  shardConcurrency: number;
  /** Pause between starting two shard jobs, real ms (stretches planning in smoke runs). */
  jobIntervalMs: number;
  maxQueued: number;
}

// The real kill switch writes an EMF metric line to stdout on every read; keep the console quiet.
console.log = () => undefined;

runWorkerMain(async () => {
  const args = parseArgs(process.argv.slice(2));
  const devices = numberArg(args, 'devices') ?? 5000;
  const dir = args['expected-dir'] ?? '.';
  const seed = numberArg(args, 'seed') ?? 1;

  // ----- seed the index and write the expected tokens (independent of the system under test) -----
  const t0 = performance.now();
  const index = new LightCellIndex();
  for (const e of generateEntries({ devices, seed, deviceIds: 'uuid' })) index.add(e);
  index.finalize();
  const alertCells = await cellsForAlertArea(
    { geometry: countyPolygon(), ugcCodes: [] },
    {
      getZoneGeometry: () => {
        throw new Error('zone lookup not expected');
      },
    },
  );
  const apnsTokens = new Set<string>();
  const fcmTokens = new Set<string>();
  for (const cell of alertCells) {
    for (const e of index.cells.get(cell) ?? []) {
      if (!e.target) continue;
      (e.target.platform === 'android' ? fcmTokens : apnsTokens).add(e.target.token);
    }
  }
  const apnsFile = join(dir, 't5-expected-apns.txt');
  const fcmFile = join(dir, 't5-expected-fcm.txt');
  writeFileSync(apnsFile, [...apnsTokens].join('\n') + '\n');
  writeFileSync(fcmFile, [...fcmTokens].join('\n') + '\n');
  const expected = { apns: apnsTokens.size, fcm: fcmTokens.size };
  apnsTokens.clear();
  fcmTokens.clear();
  console.error(
    `[t5-planner] seeded ${devices} devices, ${index.entries} entries, ${alertCells.length} alert cells in ${Math.round(performance.now() - t0)} ms`,
  );

  const verify = createAlertVerifier(ALERT_SIGNING.publicKeys);
  const publicKeyPem = ALERT_SIGNING.publicKey.export({ type: 'spki', format: 'pem' }) as string;

  // ----- per-scenario state -----
  let prep: PrepareBody | null = null;
  const logCounts = new Map<string, number>();
  const logger: Logger = {
    info: () => undefined,
    warn: (m) => void logCounts.set(m, (logCounts.get(m) ?? 0) + 1),
    error: (m) => void logCounts.set(m, (logCounts.get(m) ?? 0) + 1),
  };

  const stats = {
    batches: 0,
    recipients: 0,
    maxBatchRecipients: 0,
    messages: 0,
    jobsStarted: 0,
    jobErrors: 0,
    parkedTotal: 0,
    planParkedTotal: 0,
    unparkedJobs: 0,
    planRuns: 0,
    planRunsDone: 0,
    planReturnedAtMs: 0,
    lastEnqueueAtMs: 0,
    backpressureWaits: 0,
  };
  const resetStats = (): void => {
    for (const k of Object.keys(stats) as Array<keyof typeof stats>) stats[k] = 0;
    logCounts.clear();
  };

  const shardIds = new Set<string>();
  const completed = new Set<string>();
  const parkedThisRun = new Set<string>();
  let parkedList: Array<{ message: ParkedMessage; dueAt: number; id: string }> = [];
  let pending: ResumableShardJob[] = [];
  let active = 0;
  let lastQueued = 0;
  let nextParkId = 1;
  let ticker: NodeJS.Timeout | undefined;

  const state = {
    plannerHandler: null as null | ReturnType<typeof buildPlannerHandler>,
  };

  // ----- send queue: the broker, with back-pressure like the in-process sim -----
  const producerQueue: SendQueue = {
    async enqueue(name: SendQueueName, batches) {
      const p = prep!;
      for (const m of packBatches(batches)) {
        while (lastQueued >= p.maxQueued) {
          stats.backpressureWaits++;
          await sleep(20);
          lastQueued = (await call<{ queued: number }>(p.broker, 'GET', '/depth')).queued;
        }
        const r = await call<{ queued: number }>(
          p.broker,
          'POST',
          `/enqueue?queue=${name}`,
          serializeMessage(m),
        );
        lastQueued = r.queued;
        stats.messages++;
        stats.lastEnqueueAtMs = Date.now();
        stats.batches += m.length;
        for (const b of m) {
          stats.recipients += b.recipients.length;
          stats.maxBatchRecipients = Math.max(stats.maxBatchRecipients, b.recipients.length);
        }
      }
    },
  };

  const runJob = async (job: ResumableShardJob): Promise<void> => {
    const p = prep!;
    if (p.jobIntervalMs > 0) await sleep(p.jobIntervalMs);
    stats.jobsStarted++;
    parkedThisRun.delete(job.shardId);
    const handler = shardHandlerRef!;
    // Lambda's asynchronous invoke retries a failed run twice.
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        await handler(job);
        break;
      } catch (err) {
        stats.jobErrors++;
        console.error(`[t5-planner] shard ${job.shardId} failed: ${String(err)}`);
        await sleep(300);
      }
    }
    if (!parkedThisRun.has(job.shardId)) completed.add(job.shardId);
  };
  const pump = (): void => {
    const p = prep!;
    while (active < p.shardConcurrency && pending.length > 0) {
      const job = pending.shift()!;
      active++;
      void runJob(job).finally(() => {
        active--;
        pump();
      });
    }
  };
  const dispatcher: ShardDispatcher = {
    async dispatch(jobs: ShardJob[]) {
      for (const j of jobs) {
        shardIds.add(j.shardId);
        pending.push(j);
      }
      pump();
    },
  };

  // ----- the parking queue (SQS with a 60 s delivery delay, on the virtual clock) -----
  const pausedQueue: PausedQueue = {
    async park(message) {
      const p = prep!;
      if (message.kind === 'shard') {
        parkedThisRun.add(message.job.shardId);
        stats.parkedTotal++;
      } else stats.planParkedTotal++;
      parkedList.push({
        message: structuredClone(message),
        dueAt: Date.now() + (PARK_DELAY_SECONDS * 1000) / p.vclock.scale,
        id: String(nextParkId++),
      });
    },
  };

  let shardHandlerRef: ReturnType<typeof buildShardHandler> | null = null;

  const planEvent = (ready: object) => ({
    Records: [{ messageId: 'plan', body: JSON.stringify(ready) }],
  });
  type Event = Parameters<NonNullable<typeof state.plannerHandler>>[0];

  const runPlanner = async (ready: object): Promise<void> => {
    stats.planRuns++;
    try {
      const res = await state.plannerHandler!(planEvent(ready) as unknown as Event);
      if (res.batchItemFailures.length > 0) throw new Error('planner record failed');
    } finally {
      stats.planRunsDone++;
      stats.planReturnedAtMs = Date.now();
    }
  };

  const control = await startServer(async (method, path, _q, body) => {
    if (method === 'POST' && path === '/prepare') {
      clearInterval(ticker);
      prep = json<PrepareBody>(body);
      const vc = prep.vclock;
      resetStats();
      shardIds.clear();
      completed.clear();
      parkedThisRun.clear();
      parkedList = [];
      pending = [];
      active = 0;
      lastQueued = 0;

      const now = vnow(vc);
      const alert: Alert = signAlertSync(
        {
          id: prep.alertId,
          source: 'nws',
          sourceId: `urn:oid:t5-${prep.alertId}`,
          event: 'Flash Flood Warning',
          eventCode: 'FFW',
          relaySeverity: 'warning',
          capSeverity: 'Severe',
          urgency: 'Immediate',
          certainty: 'Likely',
          status: 'Actual',
          messageType: 'Alert',
          state: 'active',
          references: [],
          area: {
            description: 'Santa Clara County (simulated)',
            geometry: countyPolygon(),
            ugcCodes: [],
            sameCodes: [],
            h3Cells: alertCells,
          },
          sent: new Date(now - 60_000).toISOString(),
          effective: new Date(now - 60_000).toISOString(),
          onset: null,
          expires: new Date(now + 4 * 3600_000).toISOString(),
          ends: null,
          official: {
            headline: 'Flash Flood Warning for the county',
            description: 'Simulated alert for the failure-under-load test.',
            instruction: null,
            senderName: 'NWS',
            language: 'en-US',
          },
          summaries: [],
          signature: 'unsigned',
        },
        ALERT_SIGNING.signer,
      );
      const store = new InMemoryAlertStore();
      await store.save(alert);
      await call(prep.broker, 'POST', '/alert', {
        alert,
        publicKeyPem,
        kid: ALERT_SIGNING.signer.kid,
      });

      // The real kill switch, reading the broker's parameter through a fake SSM client.
      const ksEpoch = vc.epochMs;
      const ksNow = (): number => ksEpoch + (Date.now() - ksEpoch) * prep!.ksScale;
      const ssm = {
        send: async () => {
          const r = await call<{ value: string }>(prep!.broker, 'GET', '/killswitch');
          return { Parameter: { Value: r.value } };
        },
      } as unknown as Parameters<typeof createKillSwitch>[1];
      const isDeliveryPaused = createKillSwitch('t5', ssm, ksNow);
      const clock = vclock(vc);
      const alerts = new CachedAlertReader(store, 10_000, () => vnow(vc));
      const eligibility = {
        alerts,
        verifyAlert: (a: Alert) => verify(a),
        isDeliveryPaused,
        clock,
        logger,
      };
      const shardDeps = {
        ...eligibility,
        cellIndex: index,
        sendQueue: producerQueue,
        makeCheckInToken: makeCheckInTokenFor(TOKEN_KEY),
      };
      state.plannerHandler = buildPlannerHandler(
        { ...eligibility, shardDispatcher: dispatcher },
        undefined,
        pausedQueue,
      );
      shardHandlerRef = buildShardHandler(shardDeps, undefined, pausedQueue) as never;

      // The paused-queue consumer, polled like an SQS trigger.
      const consumer = createPausedConsumer({
        isDeliveryPaused: createKillSwitch('t5', ssm, ksNow),
        paused: pausedQueue,
        shards: {
          async dispatch(jobs) {
            stats.unparkedJobs += jobs.length;
            return dispatcher.dispatch(jobs);
          },
        },
        plans: {
          async publish(ready) {
            void runPlanner(ready);
          },
        },
        now: () => new Date(vnow(vc)),
        logger,
      });
      ticker = setInterval(() => {
        const due = parkedList.filter((x) => x.dueAt <= Date.now());
        if (due.length === 0) return;
        parkedList = parkedList.filter((x) => x.dueAt > Date.now());
        void consumer({
          Records: due.map((x) => ({ messageId: x.id, body: JSON.stringify(x.message) })),
        } as never).then((r) => {
          const failed = new Set(r.batchItemFailures.map((f) => f.itemIdentifier));
          for (const x of due)
            if (failed.has(x.id)) parkedList.push({ ...x, dueAt: Date.now() + 500 });
        });
      }, 250);
      ticker.unref();
      return { alertCells: alertCells.length, alertId: prep.alertId };
    }

    if (method === 'POST' && path === '/plan') {
      const a = prep!.alertId;
      const sourceId = `urn:oid:t5-${a}`;
      void runPlanner({ alertId: a, sourceId, relaySeverity: 'warning' }).catch((e: unknown) => {
        console.error(`[t5-planner] planner failed: ${String(e)}`);
        stats.jobErrors++;
      });
      return { ok: true };
    }

    if (method === 'GET' && path === '/status') {
      const done =
        stats.planRuns > 0 &&
        stats.planRuns === stats.planRunsDone &&
        parkedList.length === 0 &&
        pending.length === 0 &&
        active === 0 &&
        completed.size === shardIds.size &&
        shardIds.size > 0;
      return {
        done,
        jobsTotal: shardIds.size,
        jobsCompleted: completed.size,
        parkedNow: parkedList.length,
        activeJobs: active,
        pendingJobs: pending.length,
        ...stats,
        log: Object.fromEntries(logCounts),
      };
    }
    return {};
  });

  await sendMessage({
    control: `http://127.0.0.1:${control.port}`,
    expected,
    expectedFiles: { apns: apnsFile, fcm: fcmFile },
    entries: index.entries,
    alertCells: alertCells.length,
  });
  await new Promise<void>(() => undefined);
  return {};
});
