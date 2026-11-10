/**
 * t5: failure under load. One county alert (1,000,000 devices at full scale) is fanned out by the
 * real planner/shard/sender code in separate processes (planner, broker, two mock push servers and
 * several senders), while one fault at a time is injected:
 *
 *   a  kill switch: pause at ~30% sent, hold, unpause (real paused-queue parking and sender pause)
 *   b  APNs 429 (Retry-After) storm, then 503 bursts: the real backoff must ride it out
 *   c  5% dead tokens (410 / UNREGISTERED): the breaker must stay closed, only dead tokens cleared
 *   d  misconfiguration (40% BadDeviceToken, fixed after 90 s): the breaker must open, no live token
 *      cleared, held recipients re-queued (`<batchKey>:h<n>`) and delivered after the fix
 *   e  a sender process killed at ~50%: the others finish after lease expiry, duplicates bounded
 *      by the in-flight chunks of the crashed sender (per-chunk progress resume)
 *   f  idempotency store throttling for 10% of claims: claim_throttled, batch re-queued
 *
 * A clean run comes first as the time reference. Every scenario verifies missed devices == 0 and
 * reports duplicates. See common.ts for the process layout and the virtual clock used in smoke runs.
 */
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnWorker, type ChildHandle } from '../../lib/child.js';
import { fmt, writeTestReport } from '../../lib/report.js';
import { RssSampler } from '../../lib/rss.js';
import type { Metric, ScenarioResult, SuiteContext, SuiteTest, TestResult } from '../../types.js';
import { SENDER_CHUNK_SIZE } from '../../../../../backend/src/scale/contracts.js';
import { ROLLING_MIN_SAMPLES } from '../../../../../backend/src/scale/sender/breaker.js';
import { call, sleep, type FaultRule, type ThrottleSpec, type VClock } from './common.js';
import type { SenderTally } from './sender-worker.js';

const file = (name: string): string => fileURLToPath(new URL(`./${name}`, import.meta.url));

// ---------- Sizes ----------

interface Sizes {
  devices: number;
  /** Virtual seconds per real second (see common.ts). */
  scale: number;
  /** Kill-switch clock speed (its real cache is 30 s). */
  ksScale: number;
  senders: number;
  concurrency: number;
  shardConcurrency: number;
  holdMs: number;
  /** Real-time limit per scenario. */
  limitMs: number;
  throttleWindowS: number;
  heap: { broker: number; planner: number; mock: number; sender: number };
  /** Mock server latency (p50, p99 ms). Smoke uses a fast mock so a run fits in seconds. */
  mockLatency: [number, number];
}

function sizesFor(ctx: SuiteContext): Sizes {
  const base = sizesForScale(ctx);
  const c = Number(process.env['T5_SENDER_CONCURRENCY']);
  return Number.isFinite(c) && c >= 1 ? { ...base, concurrency: Math.floor(c) } : base;
}

function sizesForScale(ctx: SuiteContext): Sizes {
  if (ctx.scale === 'smoke') {
    return {
      devices: 5_000,
      scale: 10,
      ksScale: 60,
      senders: 2,
      concurrency: 8,
      shardConcurrency: 8,
      holdMs: 2_000,
      limitMs: 120_000,
      throttleWindowS: 40,
      heap: { broker: 256, planner: 512, mock: 384, sender: 384 },
      mockLatency: [3, 12],
    };
  }
  return {
    devices: 1_000_000,
    scale: 1,
    ksScale: 1,
    senders: Math.max(2, Math.min(8, Math.floor(ctx.workers / 3))),
    concurrency: 24,
    shardConcurrency: 32,
    holdMs: 120_000,
    limitMs: 45 * 60_000,
    throttleWindowS: 20,
    heap: { broker: 1536, planner: 4096, mock: 2560, sender: 1024 },
    mockLatency: [20, 150],
  };
}

// ---------- Harness ----------

interface Proc {
  handle: ChildHandle;
  control: string;
  url?: string;
}

interface ReadyMessage {
  control: string;
  url?: string;
  expected?: { apns: number; fcm: number };
  entries?: number;
  alertCells?: number;
}

function spawnProc(path: string, args: string[], heapMb: number): Promise<Proc> {
  const handle = spawnWorker(path, args, { heapMb });
  return new Promise<Proc>((resolve, reject) => {
    handle.onMessage((v) => {
      const m = v as ReadyMessage;
      if (m && typeof m.control === 'string') {
        resolve({ handle, control: m.control, ...(m.url ? { url: m.url } : {}) });
      }
    });
    handle.result.then(
      () => reject(new Error(`${path} exited before it was ready`)),
      (e: unknown) => reject(e instanceof Error ? e : new Error(String(e))),
    );
  });
}

interface MockStats {
  ok: number;
  injected: Record<string, number>;
  firstOkAt: number | null;
  lastOkAt: number | null;
  t0Real: number | null;
  marks: Record<string, { at: number; firstOkAfter: number | null }>;
  okTimeline: number[];
}

interface MockReport {
  expected: number;
  deliveredExpected: number;
  missing: number;
  missingExcused: number;
  duplicateTokens: number;
  extraDeliveries: number;
  unexpectedDelivered: number;
  deadAnswered: number;
  injected: Record<string, number>;
  ok: number;
}

interface BrokerStats {
  enqueued: number;
  taken: number;
  redelivered: number;
  maxInflight: number;
  claims: number;
  throttledClaims: number;
  queued: number;
  delayed: number;
  inflight: number;
  cleared: number;
  clearedLive: number;
  clearedTwice: number;
  pauseLog: Array<{ paused: boolean; at: number }>;
  extendRefused: number;
  leasedAnswers: number;
}

interface PlannerStatus {
  done: boolean;
  jobsTotal: number;
  jobsCompleted: number;
  jobsStarted: number;
  parkedNow: number;
  parkedTotal: number;
  parkedJobs: number;
  planParkedTotal: number;
  unparkedJobs: number;
  jobErrors: number;
  batches: number;
  recipients: number;
  maxBatchRecipients: number;
  messages: number;
  backpressureWaits: number;
  log: Record<string, number>;
}

class Harness {
  broker!: Proc;
  planner!: Proc;
  apns!: Proc;
  fcm!: Proc;
  senders: Array<{ id: string; proc: Proc; alive: boolean }> = [];
  expected = { apns: 0, fcm: 0 };
  alertCells = 0;
  private nextSender = 1;
  readonly rss = new RssSampler();

  constructor(
    private readonly ctx: SuiteContext,
    readonly sizes: Sizes,
  ) {}

  private track(p: Proc): Proc {
    this.rss.addSource(() => p.handle.rssNow);
    return p;
  }

  async boot(): Promise<void> {
    const { sizes, ctx } = this;
    const apnsFile = join(ctx.dataDir, 't5-expected-apns.txt');
    const fcmFile = join(ctx.dataDir, 't5-expected-fcm.txt');
    const lat = ['--p50', String(sizes.mockLatency[0]), '--p99', String(sizes.mockLatency[1])];
    ctx.log(`t5: starting broker, planner (seeding ${fmt(sizes.devices)} devices), mocks, senders`);
    const plannerHandle = spawnWorker(
      file('planner-worker.ts'),
      ['--devices', String(sizes.devices), '--expected-dir', ctx.dataDir],
      { heapMb: sizes.heap.planner },
    );
    const planner = new Promise<ReadyMessage>((resolve, reject) => {
      plannerHandle.onMessage((v) => {
        const m = v as ReadyMessage;
        if (m && typeof m.control === 'string') resolve(m);
      });
      plannerHandle.result.then(
        () => reject(new Error('planner exited early')),
        (e: unknown) => reject(e instanceof Error ? e : new Error(String(e))),
      );
    });
    const [broker, apns, fcm, plannerReady] = await Promise.all([
      spawnProc(file('broker.ts'), [], sizes.heap.broker),
      spawnProc(
        file('mock-worker.ts'),
        ['--kind', 'apns', '--expected', apnsFile, ...lat],
        sizes.heap.mock,
      ),
      spawnProc(
        file('mock-worker.ts'),
        ['--kind', 'fcm', '--expected', fcmFile, ...lat],
        sizes.heap.mock,
      ),
      planner,
    ]);
    this.broker = this.track(broker);
    this.apns = this.track(apns);
    this.fcm = this.track(fcm);
    this.planner = this.track({ handle: plannerHandle, control: plannerReady.control });
    this.expected = plannerReady.expected ?? { apns: 0, fcm: 0 };
    this.alertCells = plannerReady.alertCells ?? 0;
    await this.ensureSenders();
  }

  /** Spawns senders until `sizes.senders` are alive (a killed sender is replaced). */
  async ensureSenders(): Promise<void> {
    this.senders = this.senders.filter((s) => s.alive);
    const missing = this.sizes.senders - this.senders.length;
    const spawned = await Promise.all(
      Array.from({ length: Math.max(0, missing) }, () => {
        const id = `s${this.nextSender++}`;
        return spawnProc(file('sender-worker.ts'), [], this.sizes.heap.sender).then((proc) => {
          this.track(proc);
          return { id, proc, alive: true };
        });
      }),
    );
    this.senders.push(...spawned);
  }

  dispose(): void {
    this.rss.stop();
    const all = [
      this.broker,
      this.planner,
      this.apns,
      this.fcm,
      ...this.senders.map((s) => s.proc),
    ];
    for (const p of all) p?.handle.kill();
  }
}

// ---------- Scenario definitions ----------

interface RunData {
  vc: VClock;
  /** Real seconds from the plan request to a drained system. */
  elapsedS: number;
  drivenNotes: Notes;
  apns: MockReport;
  fcm: MockReport;
  apnsStats: MockStats;
  fcmStats: MockStats;
  broker: BrokerStats;
  planner: PlannerStatus;
  tallies: SenderTally[];
  sum: SenderTally;
  expectedTotal: number;
  sizes: Sizes;
  sendersAtStart: number;
  /** Seconds the clean run took (0 for the clean run itself). */
  cleanElapsedS: number;
}

type Notes = Record<string, number | string | boolean>;

interface Driver {
  h: Harness;
  vc: VClock;
  sizes: Sizes;
  expectedTotal: number;
  progress(): Promise<number>;
  waitProgress(fraction: number, timeoutMs: number): Promise<number>;
  plannerStatus(): Promise<PlannerStatus>;
  setPaused(paused: boolean): Promise<number>;
  markMocks(name: string): Promise<void>;
  mockStats(): Promise<[MockStats, MockStats]>;
}

interface Evaluation {
  metrics: Metric[];
  checks: ScenarioResult['checks'];
  notes: string[];
}

interface Def {
  key: string;
  name: string;
  apnsRules: FaultRule[];
  fcmRules: FaultRule[];
  deadRate: number;
  throttle?: ThrottleSpec;
  jobIntervalMs?: number;
  shardConcurrency?: number;
  drive?(d: Driver): Promise<Notes>;
  evaluate(r: RunData): Evaluation;
}

const sumLog = (r: RunData, key: string): number => r.sum.logEvents[key] ?? 0;
const dupTotal = (r: RunData): number => r.apns.extraDeliveries + r.fcm.extraDeliveries;
const missedTotal = (r: RunData): number => r.apns.missing + r.fcm.missing;

function mergeTallies(list: SenderTally[]): SenderTally {
  const out: SenderTally = {
    id: 'all',
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
  };
  for (const t of list) {
    out.gateDecisions.push(...t.gateDecisions);
    out.deadRequeuedRecipients += t.deadRequeuedRecipients;
    out.heldRetryBatches += t.heldRetryBatches;
    out.batches += t.batches;
    out.retryBatches += t.retryBatches;
    out.thrown += t.thrown;
    out.sent += t.sent;
    out.permanentFailed += t.permanentFailed;
    out.retried += t.retried;
    out.gaveUp += t.gaveUp;
    out.throttleErrors += t.throttleErrors;
    out.deadEnqueuedBeforeBreaker += t.deadEnqueuedBeforeBreaker;
    out.deadEnqueuedAfterBreaker += t.deadEnqueuedAfterBreaker;
    out.deadFlushLeft += t.deadFlushLeft;
    for (const [k, v] of Object.entries(t.thrownErrors ?? {}))
      out.thrownErrors[k] = (out.thrownErrors[k] ?? 0) + v;
    for (const [k, v] of Object.entries(t.byReason)) out.byReason[k] = (out.byReason[k] ?? 0) + v;
    for (const [k, v] of Object.entries(t.logEvents))
      out.logEvents[k] = (out.logEvents[k] ?? 0) + v;
  }
  return out;
}

/** Checks every scenario shares. `dupBound`: undefined means duplicates must be 0. */
function baseChecks(
  r: RunData,
  dupBound?: number,
  /** Scenario d: devices whose token the sender cleared are the only ones allowed to be missed. */
  missedExplainedBy?: number,
): ScenarioResult['checks'] {
  const dups = dupTotal(r);
  const unexpected = r.apns.unexpectedDelivered + r.fcm.unexpectedDelivered;
  return [
    missedExplainedBy === undefined
      ? {
          name: 'missed devices == 0',
          ok: missedTotal(r) === 0,
          detail: `${missedTotal(r)} missed`,
        }
      : {
          name: 'every missed device is one whose token was cleared (nobody else lost)',
          ok: missedTotal(r) <= missedExplainedBy,
          detail: `${missedTotal(r)} missed, ${missedExplainedBy} tokens cleared`,
        },
    dupBound === undefined
      ? { name: 'duplicate pushes == 0', ok: dups === 0, detail: `${dups} duplicates` }
      : {
          name: `duplicate pushes <= ${fmt(dupBound)} (the crashed sender's in-flight batches)`,
          ok: dups <= dupBound,
          detail: `${dups} duplicates`,
        },
    { name: 'no push to an unexpected token', ok: unexpected === 0, detail: `${unexpected}` },
    {
      name: 'planner: every shard job completed, nothing left parked',
      ok:
        r.planner.done &&
        r.planner.jobsCompleted === r.planner.jobsTotal &&
        r.planner.parkedNow === 0 &&
        r.planner.jobErrors === 0,
      detail: `${r.planner.jobsCompleted}/${r.planner.jobsTotal} jobs, ${r.planner.jobErrors} errors`,
    },
    {
      name: 'send queue drained, no retry given up',
      ok: r.broker.queued + r.broker.delayed + r.broker.inflight === 0 && r.sum.gaveUp === 0,
      detail: `gave up on ${r.sum.gaveUp} recipients`,
    },
    {
      name: 'finished inside the 30 minute retry budget (virtual time)',
      ok: r.elapsedS * r.vc.scale < 30 * 60,
      detail: `${fmt(r.elapsedS * r.vc.scale, 0)} s`,
    },
  ];
}

function baseMetrics(r: RunData): Metric[] {
  const pushes = r.apns.ok + r.fcm.ok;
  return [
    { name: 'devices expected', value: r.expectedTotal, unit: 'devices' },
    { name: 'time to completion', value: r.elapsedS, unit: 's' },
    {
      name: 'missed devices',
      value: missedTotal(r),
      unit: 'devices',
      target: { op: '==', value: 0 },
    },
    { name: 'duplicate pushes', value: dupTotal(r), unit: 'pushes' },
    { name: 'pushes accepted', value: pushes, unit: 'pushes' },
    { name: 'batches handled', value: r.sum.batches, unit: 'batches' },
    { name: 'retry batches handled', value: r.sum.retryBatches, unit: 'batches' },
    { name: 'recipients re-queued', value: r.sum.retried, unit: 'recipients' },
  ];
}

const extraTime = (r: RunData): number => (r.cleanElapsedS > 0 ? r.elapsedS - r.cleanElapsedS : 0);

function acceptedBetween(stats: MockStats[], fromMs: number, toMs: number): number {
  let n = 0;
  for (const s of stats) {
    if (s.t0Real === null) continue;
    s.okTimeline.forEach((count, i) => {
      const start = s.t0Real! + i * 1000;
      if (start >= fromMs && start + 1000 <= toMs) n += count;
    });
  }
  return n;
}

function buildDefs(sizes: Sizes): Def[] {
  const smoke = sizes.scale > 1;
  const both = (rule: Omit<FaultRule, 'platform'>): { apns: FaultRule[]; fcm: FaultRule[] } => ({
    apns: [{ ...rule, platform: 'apns' }],
    fcm: [{ ...rule, platform: 'fcm' }],
  });
  const noFaults: Pick<Def, 'apnsRules' | 'fcmRules' | 'deadRate'> = {
    apnsRules: [],
    fcmRules: [],
    deadRate: 0,
  };

  const clean: Def = {
    key: '0',
    name: 'clean run (time reference, no faults)',
    ...noFaults,
    evaluate(r) {
      return {
        metrics: [
          ...baseMetrics(r),
          {
            name: 'pushes per second (mean)',
            value: r.elapsedS > 0 ? (r.apns.ok + r.fcm.ok) / r.elapsedS : 0,
            unit: 'pushes/s',
          },
        ],
        checks: baseChecks(r),
        notes: ['The time reference for the extra-time numbers of the other scenarios.'],
      };
    },
  };

  const killSwitch: Def = {
    key: 'a',
    name: 'a: kill switch at ~30% sent',
    ...noFaults,
    jobIntervalMs: smoke ? 100 : 0,
    shardConcurrency: smoke ? 2 : 6,
    async drive(d) {
      const before = await d.waitProgress(0.3, d.sizes.limitMs);
      const status = await d.plannerStatus();
      const tPause = await d.setPaused(true);
      await sleep(d.sizes.holdMs);
      await d.markMocks('resume');
      const tResume = await d.setPaused(false);
      // Wait for the first accepted push after the unpause on either mock.
      const deadline = Date.now() + d.sizes.limitMs;
      let firstAfter: number | null = null;
      while (Date.now() < deadline && firstAfter === null) {
        const [a, f] = await d.mockStats();
        const times = [a.marks['resume']?.firstOkAfter, f.marks['resume']?.firstOkAfter].filter(
          (t): t is number => typeof t === 'number',
        );
        if (times.length > 0) firstAfter = Math.min(...times);
        else await sleep(100);
      }
      return {
        tPause,
        tResume,
        tFirstAfter: firstAfter ?? 0,
        sentAtPause: before,
        shardsNotStartedAtPause: status.jobsTotal - status.jobsStarted,
        shardsTotal: status.jobsTotal,
      };
    },
    evaluate(r) {
      const n = r.drivenNotes as Record<string, number>;
      const tPause = n['tPause'] ?? 0;
      const tResume = n['tResume'] ?? 0;
      const timeToResumeS = n['tFirstAfter'] ? (n['tFirstAfter'] - tResume) / 1000 : -1;
      const holdS = (tResume - tPause) / 1000;
      // The switch is cached (30 s) and so is the sender's pause check (5 s); give both time, plus
      // a few seconds for batches already sending to finish, before demanding silence.
      const allowanceMs = ((30 + 5) / r.sizes.ksScale) * 1000 + 4000;
      const silentFrom = tPause + allowanceMs;
      const silentWindowMs = tResume - silentFrom;
      const during = acceptedBetween([r.apnsStats, r.fcmStats], silentFrom, tResume);
      const before = acceptedBetween([r.apnsStats, r.fcmStats], tPause - 3000, tPause);
      const checks = baseChecks(r);
      checks.push({
        name: 'senders saw the pause (sender.receiving_paused)',
        ok: sumLog(r, 'sender.receiving_paused') >= 1,
        detail: `${sumLog(r, 'sender.receiving_paused')} senders logged it`,
      });
      checks.push({
        name: 'resumed after the unpause',
        ok: timeToResumeS >= 0,
        detail: timeToResumeS >= 0 ? `${timeToResumeS.toFixed(1)} s` : 'no push seen',
      });
      if (silentWindowMs >= 1500) {
        checks.push({
          name: 'no push accepted while paused (after the cache delay)',
          ok: during === 0,
          detail: `${during} pushes in the last ${(silentWindowMs / 1000).toFixed(0)} s of the hold`,
        });
      }
      checks.push({
        name: 'every parked shard job was resumed',
        // A job still paused when its 60 s park delay ends is parked again, so park events
        // (parkedTotal) exceed distinct jobs; each distinct job must be resumed exactly once.
        ok: r.planner.unparkedJobs === r.planner.parkedJobs,
        detail: `${r.planner.parkedJobs} jobs parked (${r.planner.parkedTotal} park events), ${r.planner.unparkedJobs} resumed`,
      });
      return {
        metrics: [
          ...baseMetrics(r),
          { name: 'sent when paused', value: n['sentAtPause'] ?? 0, unit: 'pushes' },
          { name: 'hold time', value: holdS, unit: 's' },
          { name: 'time to resume (unpause to next push)', value: timeToResumeS, unit: 's' },
          { name: 'pushes accepted just before the pause (3 s)', value: before, unit: 'pushes' },
          { name: 'shard jobs parked while paused', value: r.planner.parkedTotal, unit: 'jobs' },
          {
            name: 'shard jobs not yet started at the pause',
            value: n['shardsNotStartedAtPause'] ?? 0,
            unit: 'jobs',
          },
          { name: 'extra time over the clean run', value: extraTime(r), unit: 's' },
          { name: 'total time without the hold', value: r.elapsedS - holdS, unit: 's' },
        ],
        checks,
        notes: [
          silentWindowMs < 1500
            ? 'The hold is shorter than the kill switch cache plus a margin, so the silence check is skipped at this scale.'
            : `Silence is demanded from ${(allowanceMs / 1000).toFixed(0)} s after the pause: the switch is cached for 30 s and the sender's own check for 5 s, so batches keep sending until both caches expire.`,
          `Parked shard jobs only appear when jobs start after the switch cache has seen the pause (here ${r.planner.parkedTotal}); the parking path itself is covered by unit tests in backend/src/scale/fanout.`,
        ],
      };
    },
  };

  const storm: Def = {
    key: 'b',
    name: 'b: APNs 429/503 storms',
    apnsRules: [
      // 20% of requests get 429 with Retry-After for 30 s ...
      { fromS: 0, toS: 30, p: 0.2, kind: 'throttle', retryAfterS: 2, platform: 'apns' },
      // ... then three 5 s bursts of 503 (30% of requests).
      { fromS: 30, toS: 35, p: 0.3, kind: 'unavailable', retryAfterS: 2, platform: 'apns' },
      { fromS: 40, toS: 45, p: 0.3, kind: 'unavailable', retryAfterS: 2, platform: 'apns' },
      { fromS: 50, toS: 55, p: 0.3, kind: 'unavailable', retryAfterS: 2, platform: 'apns' },
    ],
    fcmRules: [],
    deadRate: 0,
    evaluate(r) {
      const t429 = r.apnsStats.injected['throttle'] ?? 0;
      const t503 = r.apnsStats.injected['unavailable'] ?? 0;
      const checks = baseChecks(r);
      checks.push({
        name: 'the storm happened and was retried',
        ok: t429 > 0 && r.sum.retried > 0,
        detail: `${t429} x 429, ${t503} x 503, ${r.sum.retried} recipients re-queued`,
      });
      checks.push({
        name: 'no dead token cleared by a 429/503',
        ok: r.broker.cleared === 0,
        detail: `${r.broker.cleared} cleared`,
      });
      return {
        metrics: [
          ...baseMetrics(r),
          { name: 'extra time over the clean run', value: extraTime(r), unit: 's' },
          { name: 'APNs 429 answers', value: t429, unit: 'responses' },
          { name: 'APNs 503 answers', value: t503, unit: 'responses' },
          {
            name: 'retry attempts (re-queued batches handled)',
            value: r.sum.retryBatches,
            unit: 'batches',
          },
        ],
        checks,
        notes: [
          'Retry-After is 2 s on every injected answer; the sender never retries sooner than that and backs off with jitter (2 s x 2^attempt, capped at 900 s).',
        ],
      };
    },
  };

  const dead: Def = {
    key: 'c',
    name: 'c: 5% dead tokens',
    apnsRules: both({ fromS: 0, toS: 1e9, p: 0.05, byToken: true, kind: 'unregistered' }).apns,
    fcmRules: both({ fromS: 0, toS: 1e9, p: 0.05, byToken: true, kind: 'unregistered' }).fcm,
    deadRate: 0.05,
    evaluate(r) {
      const answered = r.apns.deadAnswered + r.fcm.deadAnswered;
      const opens = sumLog(r, 'delivery.permanent_breaker_open');
      const checks = baseChecks(r);
      checks.push(
        {
          name: 'circuit breaker did not open',
          ok: opens === 0,
          detail: `${opens} breaker-open events`,
        },
        {
          name: 'every dead target was cleared',
          ok: r.broker.cleared === answered && r.sum.deadFlushLeft === 0,
          detail: `${r.broker.cleared} cleared, ${answered} answered dead, ${r.sum.deadFlushLeft} left in queue`,
        },
        {
          name: 'no live token cleared',
          ok: r.broker.clearedLive === 0,
          detail: `${r.broker.clearedLive} live tokens cleared`,
        },
        {
          name: 'dead-target cleanup queue dropped nothing',
          ok: sumLog(r, 'sender.dead_target_dropped') === 0,
        },
      );
      return {
        metrics: [
          ...baseMetrics(r),
          { name: 'tokens answered dead (410 / UNREGISTERED)', value: answered, unit: 'tokens' },
          { name: 'targets cleared', value: r.broker.cleared, unit: 'tokens' },
          {
            name: 'live tokens cleared',
            value: r.broker.clearedLive,
            unit: 'tokens',
            target: { op: '==', value: 0 },
          },
          {
            name: 'breaker-open events',
            value: opens,
            unit: 'events',
            target: { op: '==', value: 0 },
          },
        ],
        checks,
        notes: [
          'Dead tokens are not "missed": the devices whose token the mock reported dead are excused, as in the single-process run.',
        ],
      };
    },
  };

  const misconfigEndS = smoke ? 30 : 90;
  const misconfig: Def = {
    key: 'd',
    name: 'd: misconfiguration (40% BadDeviceToken)',
    // A wrong APNs topic / FCM project until the misconfiguration is fixed at a defined point:
    // 90 s after the start (smoke: 30 virtual s, 3 s real). The held recipients are re-queued with
    // backoff (2, 4, 8, ... s), so their retries land after the fix and succeed.
    apnsRules: both({ fromS: 0, toS: misconfigEndS, p: 0.4, byToken: true, kind: 'badtoken' }).apns,
    fcmRules: both({ fromS: 0, toS: misconfigEndS, p: 0.4, byToken: true, kind: 'badtoken' }).fcm,
    deadRate: 0,
    evaluate(r) {
      const opens = sumLog(r, 'delivery.permanent_breaker_open');
      const bad = (r.apnsStats.injected['badtoken'] ?? 0) + (r.fcmStats.injected['badtoken'] ?? 0);
      const requeuedEvents = sumLog(r, 'sender.dead_targets_requeued');
      const requeued = r.sum.deadRequeuedRecipients;
      const dropped = sumLog(r, 'sender.dead_targets_dropped') + sumLog(r, 'delivery.gave_up');
      // Normal-path (not misconfigured) clears can only be dead tokens; here there are none, so
      // every cleared token is a live one.
      const checks = baseChecks(r);
      checks.push(
        { name: 'circuit breaker opened', ok: opens >= 1, detail: `${opens} breaker-open events` },
        {
          name: '0 live tokens cleared',
          ok: r.broker.clearedLive === 0 && r.broker.cleared === 0,
          detail: `${r.broker.clearedLive} live tokens cleared, ${r.broker.cleared} clears in all`,
        },
        {
          name: 'held recipients were re-queued (sender.dead_targets_requeued)',
          ok: requeuedEvents >= 1 && requeued > 0 && r.sum.heldRetryBatches >= 1,
          detail: `${requeuedEvents} events, ${requeued} recipients, ${r.sum.heldRetryBatches} retry batches handled`,
        },
        {
          name: 'no held recipient dropped or given up; 0 missed devices after the fix',
          ok: dropped === 0 && missedTotal(r) === 0,
          detail: `${dropped} dropped or given up, ${missedTotal(r)} missed`,
        },
      );
      return {
        metrics: [
          ...baseMetrics(r),
          { name: 'BadDeviceToken / INVALID_ARGUMENT answers', value: bad, unit: 'responses' },
          {
            name: 'breaker-open events',
            value: opens,
            unit: 'events',
            target: { op: '>=', value: 1 },
          },
          {
            name: 'live tokens cleared',
            value: r.broker.clearedLive,
            unit: 'tokens',
            target: { op: '==', value: 0 },
          },
          {
            name: 'permanent failures handed to the dead-target gate (verdict closed)',
            value: r.sum.permanentFailed,
            unit: 'recipients',
          },
          { name: 'dead-target gate re-queue events', value: requeuedEvents, unit: 'events' },
          {
            name: 'dead-target gate release events (cleared as dead)',
            value: sumLog(r, 'sender.dead_targets_released'),
            unit: 'events',
          },
          {
            name: "dead targets cleared before / after a sender's breaker first opened",
            value: r.sum.deadEnqueuedBeforeBreaker,
            unit: `tokens (after: ${r.sum.deadEnqueuedAfterBreaker})`,
          },
          {
            name: 'recipients re-queued by the gate (held, not cleared)',
            value: requeued,
            unit: 'recipients',
          },
          {
            name: 'held retry batches handled (<batchKey>:h<n> or :v<n>)',
            value: r.sum.heldRetryBatches,
            unit: 'batches',
          },
          { name: 'misconfiguration ended at', value: misconfigEndS, unit: 's (virtual)' },
          { name: 'extra time over the clean run', value: extraTime(r), unit: 's' },
        ],
        checks,
        notes: [
          `Gate decisions: ${r.sum.gateDecisions.map((g) => `${g.kind} ${g.count} (${g.reason}, ${g.samples} samples, rolling ${g.rollingRate})`).join('; ') || 'none'}.`,
          `The dead-target gate holds clears until the breaker has seen ${ROLLING_MIN_SAMPLES} results (or 60 s pass); when the breaker finds a misconfiguration it re-queues the held recipients as retry batches (\`<batchKey>:h<n>\`) and clears nobody. The misconfiguration ends ${misconfigEndS} virtual s after the start, so the retries (backoff 2, 4, 8, ... s) succeed and no device is missed.`,
        ],
      };
    },
  };

  const crash: Def = {
    key: 'e',
    name: 'e: sender process killed at ~50%',
    ...noFaults,
    async drive(d) {
      await d.waitProgress(0.5, d.sizes.limitMs);
      const victim = d.h.senders[0];
      if (!victim) throw new Error('no sender to kill');
      const tKill = Date.now();
      victim.proc.handle.kill();
      await victim.proc.handle.exited;
      victim.alive = false;
      const inflight = await call<{
        messages: number;
        recipients: number;
        batches: number;
        multiChunkBatches: number;
        chunkBound: number;
      }>(d.h.broker.control, 'GET', `/inflight?sender=${victim.id}`);
      return {
        tKill,
        inflightMessagesAtCrash: inflight.messages,
        inflightRecipientsAtCrash: inflight.recipients,
        inflightBatchesAtCrash: inflight.batches,
        multiChunkBatchesAtCrash: inflight.multiChunkBatches,
        chunkBoundAtCrash: inflight.chunkBound,
        survivors: d.h.senders.filter((s) => s.alive).length,
      };
    },
    evaluate(r) {
      const n = r.drivenNotes as Record<string, number>;
      const multi = n['multiChunkBatchesAtCrash'] ?? 0;
      const inflightBatches = n['inflightBatchesAtCrash'] ?? 0;
      const oldBound = n['inflightRecipientsAtCrash'] ?? 0;
      // Per-chunk progress: a re-claim skips the chunks the crashed sender finished, so at most the
      // chunks it was sending can be sent again: up to chunk concurrency x SENDER_CHUNK_SIZE per
      // multi-chunk batch; a single-chunk batch records nothing and can repeat once, whole.
      const bound = n['chunkBoundAtCrash'] ?? 0;
      const strict = multi * SENDER_CHUNK_SIZE;
      const resumed = sumLog(r, 'sender.batch_resumed');
      const dups = dupTotal(r);
      const checks = baseChecks(r, bound);
      checks[1] = {
        name: `duplicates <= in-flight chunks of the crashed sender x ${SENDER_CHUNK_SIZE} (${fmt(bound)})`,
        ok: dups <= bound,
        detail: `${dups} duplicates; ${multi} multi-chunk batches in flight (x ${SENDER_CHUNK_SIZE} = ${fmt(strict)}), ${inflightBatches} batches, ${fmt(oldBound)} in-flight recipients`,
      };
      checks.push({
        name: 'remaining senders completed the alert after lease expiry',
        ok: missedTotal(r) === 0 && (n['survivors'] ?? 0) >= 1,
        detail: `${n['survivors']} senders left`,
      });
      return {
        metrics: [
          ...baseMetrics(r),
          {
            name: 'in-flight messages of the crashed sender',
            value: n['inflightMessagesAtCrash'] ?? 0,
            unit: 'messages',
          },
          {
            name: 'in-flight batches of the crashed sender',
            value: inflightBatches,
            unit: 'batches',
          },
          {
            name: `in-flight multi-chunk batches (more than ${SENDER_CHUNK_SIZE} recipients)`,
            value: multi,
            unit: 'batches',
          },
          {
            name: 'in-flight recipients of the crashed sender (the old duplicate bound)',
            value: oldBound,
            unit: 'recipients',
          },
          {
            name: `duplicate bound (in-flight chunks x ${SENDER_CHUNK_SIZE})`,
            value: bound,
            unit: 'recipients',
          },
          {
            name: `brief formula: multi-chunk batches x ${SENDER_CHUNK_SIZE}`,
            value: strict,
            unit: 'recipients',
          },
          {
            name: 'duplicate pushes after the crash',
            value: dups,
            unit: 'pushes',
            target: { op: '<=', value: bound },
          },
          {
            name: 'batches resumed from recorded chunk progress (sender.batch_resumed)',
            value: resumed,
            unit: 'batches',
          },
          {
            name: 'messages redelivered after visibility timeout',
            value: r.broker.redelivered,
            unit: 'messages',
          },
          { name: 'extra time over the clean run', value: extraTime(r), unit: 's' },
        ],
        checks,
        notes: [
          'A killed sender cannot release its claims, so its batches wait for the 45 s lease and the 60 s queue visibility. Whoever re-claims a multi-chunk batch reads the progress the dead sender recorded per chunk (sender.batch_resumed) and sends only the chunks it had not finished, so duplicates are limited to the chunks that were in flight at the kill (same collapse key, so the phone shows one notification). A single-chunk batch records no progress and is sent again whole.',
          'The idempotency stand-in implements recordChunk and claimedProgress (the real MemoryIdempotencyStore behind the broker), so the per-chunk resume path is the one that runs.',
        ],
      };
    },
  };

  const throttle: Def = {
    key: 'f',
    name: 'f: idempotency store throttling (10% of claims)',
    ...noFaults,
    throttle: { fromS: 0, toS: sizes.throttleWindowS, p: 0.1 },
    evaluate(r) {
      const reasons = r.sum.byReason;
      const checks = baseChecks(r);
      checks.push(
        {
          name: 'claims were throttled and the real claim_throttled path ran',
          ok: (reasons['claim_throttled'] ?? 0) > 0 && r.broker.throttledClaims > 0,
          detail: `${reasons['claim_throttled'] ?? 0} batches, ${r.broker.throttledClaims} throttled claim calls`,
        },
        {
          name: 'no sender crash or failed batch',
          ok: r.sum.thrown === 0 && sumLog(r, 'sender.message_failed') === 0,
          detail: `${r.sum.thrown} batches threw${
            r.sum.thrown > 0
              ? ` (${Object.entries(r.sum.thrownErrors)
                  .map(([k, v]) => `${v}x ${k}`)
                  .join('; ')})`
              : ''
          }; claim_failed ${reasons['claim_failed'] ?? 0}`,
        },
      );
      return {
        metrics: [
          ...baseMetrics(r),
          {
            name: 'batches answered claim_throttled',
            value: reasons['claim_throttled'] ?? 0,
            unit: 'batches',
          },
          {
            name: 'throttled claim calls (with the store retries)',
            value: r.broker.throttledClaims,
            unit: 'calls',
          },
          {
            name: 'messages redelivered after visibility timeout',
            value: r.broker.redelivered,
            unit: 'messages',
          },
          { name: 'extra time over the clean run', value: extraTime(r), unit: 's' },
        ],
        checks,
        notes: [
          'A throttled claim is retried inside the store (5 tries, 2.5 s budget); a batch whose key keeps failing is left un-acked and its message comes back after the 60 s visibility timeout.',
        ],
      };
    },
  };

  return [clean, killSwitch, storm, dead, misconfig, crash, throttle];
}

// ---------- Running one scenario ----------

async function runDef(
  h: Harness,
  def: Def,
  index: number,
  cleanElapsedS: number,
  log: SuiteContext['log'],
): Promise<ScenarioResult> {
  const { sizes } = h;
  const t0 = Date.now();
  const vc: VClock = { epochMs: Date.now(), scale: sizes.scale };
  const expectedTotal = h.expected.apns + h.expected.fcm;
  const alertId = `00000000-0000-4000-8000-0000000005${String(index).padStart(2, '0')}`;
  const sendersAtStart = h.senders.length;

  await call(h.broker.control, 'POST', '/config', {
    vclock: vc,
    visibilityS: 60,
    maxQueued: 300,
    ...(def.throttle ? { throttle: def.throttle } : {}),
    deadRate: def.deadRate,
  });
  await Promise.all([
    call(h.apns.control, 'POST', '/reset', {
      rules: def.apnsRules,
      vclock: vc,
      deadRate: def.deadRate,
    }),
    call(h.fcm.control, 'POST', '/reset', {
      rules: def.fcmRules,
      vclock: vc,
      deadRate: def.deadRate,
    }),
  ]);
  await call(h.planner.control, 'POST', '/prepare', {
    vclock: vc,
    ksScale: sizes.ksScale,
    alertId,
    broker: h.broker.control,
    shardConcurrency: def.shardConcurrency ?? sizes.shardConcurrency,
    jobIntervalMs: def.jobIntervalMs ?? 0,
    maxQueued: 300,
  });
  await h.ensureSenders();
  const live = h.senders.filter((s) => s.alive);
  await Promise.all(
    live.map((s) =>
      call(s.proc.control, 'POST', '/start', {
        broker: h.broker.control,
        apnsUrl: h.apns.url,
        fcmUrl: h.fcm.url,
        vclock: vc,
        ksScale: sizes.ksScale,
        id: s.id,
        concurrency: sizes.concurrency,
      }),
    ),
  );

  const mockStats = async (): Promise<[MockStats, MockStats]> =>
    Promise.all([
      call<MockStats>(h.apns.control, 'GET', '/stats'),
      call<MockStats>(h.fcm.control, 'GET', '/stats'),
    ]);
  const progress = async (): Promise<number> => {
    const [a, f] = await mockStats();
    return a.ok + f.ok;
  };
  const driver: Driver = {
    h,
    vc,
    sizes,
    expectedTotal,
    progress,
    async waitProgress(fraction, timeoutMs) {
      const deadline = Date.now() + timeoutMs;
      for (;;) {
        const p = await progress();
        if (p >= fraction * expectedTotal) return p;
        if (Date.now() > deadline) throw new Error(`progress ${fraction} not reached`);
        await sleep(100);
      }
    },
    plannerStatus: () => call<PlannerStatus>(h.planner.control, 'GET', '/status'),
    async setPaused(paused) {
      const r = await call<{ at: number }>(h.broker.control, 'POST', '/killswitch', { paused });
      return r.at;
    },
    async markMocks(name) {
      await Promise.all([
        call(h.apns.control, 'POST', `/mark?name=${name}`),
        call(h.fcm.control, 'POST', `/mark?name=${name}`),
      ]);
    },
    mockStats,
  };

  log(`t5 ${def.name}: running`);
  const tPlan = Date.now();
  await call(h.planner.control, 'POST', '/plan');
  const driving = def.drive ? def.drive(driver) : Promise.resolve({} as Notes);
  driving.catch(() => undefined);

  // Wait for a drained system: planner done, queue empty (incl. delayed and in-flight), twice in a row.
  let stable = 0;
  let lastLog = 0;
  for (;;) {
    if (Date.now() - tPlan > sizes.limitMs) throw new Error(`${def.name}: time limit reached`);
    await sleep(300);
    const [status, depth] = await Promise.all([
      driver.plannerStatus(),
      call<{ queued: number; delayed: number; inflight: number }>(
        h.broker.control,
        'GET',
        '/depth',
      ),
    ]);
    if (Date.now() - lastLog > 15_000) {
      lastLog = Date.now();
      log(
        `t5 ${def.name}: ${fmt(await progress())}/${fmt(expectedTotal)} pushes, queued ${depth.queued}, delayed ${depth.delayed}, in flight ${depth.inflight}`,
      );
    }
    // Held dead-target clears are decided by the gate (sample count or its 60 s hold); a re-queue
    // lands in the queue, so do not call the system drained while one is still undecided.
    const gates = await Promise.all(
      h.senders
        .filter((s) => s.alive)
        .map((s) =>
          // A sender killed by the scenario answers with a reset: nothing undecided there.
          call<{ undecided: boolean }>(s.proc.control, 'GET', '/gate').catch(() => ({
            undecided: false,
          })),
        ),
    );
    const drained =
      status.done &&
      depth.queued + depth.delayed + depth.inflight === 0 &&
      gates.every((g) => !g.undecided);
    stable = drained ? stable + 1 : 0;
    if (stable >= 3) break;
  }
  const elapsedS = (Date.now() - tPlan) / 1000;
  const drivenNotes = await driving;

  const tallies: SenderTally[] = [];
  await Promise.all(
    h.senders
      .filter((s) => s.alive)
      .map(async (s) => {
        const r = await call<{ tally: SenderTally | null }>(s.proc.control, 'POST', '/stop');
        if (r.tally) tallies.push(r.tally);
      }),
  );
  const [apns, fcm] = await Promise.all([
    call<MockReport>(h.apns.control, 'GET', '/report'),
    call<MockReport>(h.fcm.control, 'GET', '/report'),
  ]);
  const [apnsStats, fcmStats] = await mockStats();
  const broker = await call<BrokerStats>(h.broker.control, 'GET', '/stats');
  const planner = await driver.plannerStatus();

  const data: RunData = {
    vc,
    elapsedS,
    drivenNotes,
    apns,
    fcm,
    apnsStats,
    fcmStats,
    broker,
    planner,
    tallies,
    sum: mergeTallies(tallies),
    expectedTotal,
    sizes,
    sendersAtStart,
    cleanElapsedS,
  };
  const ev = def.evaluate(data);
  const ok = ev.checks.every((c) => c.ok);
  log(`t5 ${def.name}: ${ok ? 'pass' : 'FAIL'} in ${elapsedS.toFixed(1)} s`);
  return {
    name: def.name,
    verdict: ok ? 'pass' : 'fail',
    durationMs: Date.now() - t0,
    metrics: ev.metrics,
    checks: ev.checks,
    notes: ev.notes,
    // for the caller: the run's elapsed seconds (clean run reference)
    ...({ elapsedS } as object),
  } as ScenarioResult;
}

// ---------- The test ----------

export const t5Chaos: SuiteTest = {
  id: 't5',
  title: 'Failure under load',
  description:
    '1,000,000-device county alert in separate processes with one fault at a time: kill switch, APNs 429/503 storms, dead tokens, misconfiguration, a killed sender, idempotency throttling.',
  estimatedMinutes: 35,
  async run(ctx: SuiteContext): Promise<TestResult> {
    const startedAt = new Date().toISOString();
    const t0 = Date.now();
    const sizes = sizesFor(ctx);
    const h = new Harness(ctx, sizes);
    const scenarios: ScenarioResult[] = [];
    let error: string | undefined;
    let peak = 0;
    try {
      await h.boot();
      // T5_SCENARIOS=d,e reruns only those scenarios (the clean run always goes first).
      const only = (process.env['T5_SCENARIOS'] ?? '')
        .split(',')
        .map((x) => x.trim())
        .filter(Boolean);
      const defs = buildDefs(sizes).filter(
        (d) => d.key === '0' || only.length === 0 || only.includes(d.key),
      );
      let cleanElapsedS = 0;
      for (const [i, def] of defs.entries()) {
        const s = await runDef(h, def, i, cleanElapsedS, ctx.log);
        if (i === 0) cleanElapsedS = (s as unknown as { elapsedS: number }).elapsedS;
        delete (s as unknown as { elapsedS?: number }).elapsedS;
        scenarios.push(s);
      }
    } catch (e) {
      error = e instanceof Error ? (e.stack ?? e.message) : String(e);
      ctx.log(`t5 failed: ${error}`);
    } finally {
      peak = Math.max(peak, Math.round(h.rss.peakMb));
      h.dispose();
    }
    const verdict =
      !error && scenarios.length > 0 && scenarios.every((s) => s.verdict === 'pass')
        ? 'pass'
        : 'fail';
    const result: TestResult = {
      id: 't5',
      title: t5Chaos.title,
      verdict,
      startedAt,
      durationMs: Date.now() - t0,
      scenarios,
      peakRssMb: peak,
      ...(error ? { error } : {}),
    };
    writeTestReport(
      ctx,
      result,
      `One signed county alert for ${fmt(sizes.devices)} seeded devices is fanned out by the real planner, shard, paused-queue and sender code (\`buildPlannerHandler\`, \`buildShardHandler\`, \`createPausedConsumer\`, \`SenderConsumer\`, \`processBatch\`, \`ApnsSender\`, \`FcmSender\`) in separate processes: a planner (owns the cell index), a broker standing in for SQS, the idempotency table, the kill-switch parameter and the dead-target clean-up, two mock push servers with scripted faults, and ${sizes.senders} senders. Exactly one fault is injected per scenario; the clean run first gives the time reference. ${ctx.scale === 'smoke' ? `Smoke scale: all clocks run ${sizes.scale} times faster than the wall clock so the 45 s lease, the 60 s visibility timeout and the retry backoff fit in seconds (the kill switch cache runs ${sizes.ksScale} times faster).` : 'All clocks are real time.'} Verification compares the mocks' per-token delivery counts with the independently computed expected-token files.`,
    );
    return result;
  },
};
