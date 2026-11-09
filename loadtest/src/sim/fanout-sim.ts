/**
 * Local fan-out simulation: one signed alert, N seeded devices, the REAL planner, shard worker,
 * processBatch and push adapters (ApnsSender over h2c, FcmSender over HTTP/1.1) against the mock
 * Apple and Google servers, an in-memory send queue and an in-memory idempotency store.
 *
 * Everything runs in one Node process (mock servers included), so on a small machine the mocks
 * compete with the senders for CPU. The numbers are a local measurement, not an AWS prediction.
 */
import { generateKeyPairSync } from 'node:crypto';
import type { Alert } from '@relay/shared';
import { MemoryIdempotencyStore } from '../../../backend/src/adapters/memory/idempotency-store.js';
import { InMemoryAlertStore } from '../../../backend/src/adapters/memory/alert-store.js';
import { ApnsSender } from '../../../backend/src/adapters/push/apns-sender.js';
import { FcmSender } from '../../../backend/src/adapters/push/fcm-sender.js';
import { RoutingPushSender } from '../../../backend/src/adapters/push/routing-push-sender.js';
import type { Logger } from '../../../backend/src/delivery/types.js';
import { cellsForAlertArea } from '../../../backend/src/geo/h3-cells.js';
import {
  createAlertVerifier,
  createMemoryAlertSigning,
  signAlertSync,
} from '../../../backend/src/signing/index.js';
import { createCheckInToken } from '../../../backend/src/tokens/checkInToken.js';
import {
  type CellIndex,
  type CellIndexEntry,
  type CellIndexPage,
  type NativePushMessage,
  type NativePushResult,
  type NativePushSender,
  type SendQueue,
  type SendQueueName,
  type ShardJob,
} from '../../../backend/src/scale/contracts.js';
import { planShards } from '../../../backend/src/scale/fanout/planner.js';
import { processShard } from '../../../backend/src/scale/fanout/shard-worker.js';
import { packBatches, serializeMessage } from '../../../backend/src/scale/fanout/pack.js';
import type { ShardDispatcher } from '../../../backend/src/scale/fanout/queues.js';
import { CachedAlertStore } from '../../../backend/src/scale/sender/cached-alerts.js';
import { parseBatches } from '../../../backend/src/scale/sender/consumer.js';
import { processBatch, type BatchReason } from '../../../backend/src/scale/sender/sender.js';
import { startMockApns } from '../mock-apns.js';
import { startMockFcm } from '../mock-fcm.js';
import { COUNTY_BOUNDS, generateEntries } from '../seed.js';
import {
  LatencyRecorder,
  checkExactlyOnce,
  formatExactlyOnce,
  type ExactlyOnceResult,
  type LatencySummary,
} from '../report.js';

export const ALERT_ID = '5f0c2a7e-1b3d-4c8e-9a6f-0d2e4b6a8c10';
/** In-memory ECDSA P-256 alert key (same algorithm as the KMS key in AWS). */
export const ALERT_SIGNING = createMemoryAlertSigning('sim-alert');
export const TOKEN_KEY = { id: 'sim-token', secret: 'sim-token-secret' };
export const DEVICE_CRED_KEY = { id: 'sim-cred', secret: 'sim-cred-secret' };

export interface FanoutSimOptions {
  devices: number;
  seed?: number;
  /** FCM project quota, messages per second. Default 10,000. */
  fcmRate?: number;
  /** In-process sender consumers. Default 4. */
  senders?: number;
  /** HTTP/2 sessions per ApnsSender. Default 4. */
  apnsSessions?: number;
  /** SQS messages each sender consumer works on at once. Default 12. */
  messagesPerSender?: number;
  /** Shard workers running at once (the in-process dispatcher's concurrency). Default 32. */
  shardConcurrency?: number;
  /** Keep every delivered check-in token (needed for the check-in burst). */
  collectTokens?: boolean;
  log?: (line: string) => void;
}

export interface WindowStat {
  startSec: number;
  ok: number;
  okPerSec: number;
  retryable: number;
  permanent: number;
}

export interface PassCounts {
  messagesProcessed: number;
  batchesProcessed: number;
  retryBatchesProcessed: number;
  /** Outcome reasons of processBatch, e.g. sent, duplicate, leased. */
  byReason: Record<string, number>;
  sent: number;
  permanentFailed: number;
  retried: number;
  gaveUp: number;
  redeliveries: number;
}

export interface FanoutSimResult {
  options: Required<Omit<FanoutSimOptions, 'log' | 'collectTokens'>>;
  seedMs: number;
  heapAfterSeedMb: number;
  peakHeapMb: number;
  peakRssMb: number;
  counts: {
    devices: number;
    entries: number;
    cellsUsed: number;
    alertCells: number;
    devicesInArea: number;
    devicesWithTarget: number;
    devicesInTwoOrMoreCells: number;
    shards: number;
    plannedBatches: number;
    plannedRecipients: number;
    skippedNoTarget: number;
    sameCellDuplicates: number;
    /** Entries skipped because the device is sent from another of its cells (primary-cell rule). */
    skippedNonPrimary: number;
  };
  timing: {
    planMs: number;
    firstPushMs: number | null;
    lastPushMs: number | null;
    wallMs: number;
  };
  throughput: {
    meanOkPerSec: number;
    peakWindowOkPerSec: number;
    windows: WindowStat[];
  };
  pushLatency: LatencySummary;
  /** Failed push attempts by error label, first pass and re-plan together. */
  pushErrors: Record<string, number>;
  pass1: PassCounts;
  retryDelays: { delayedMessages: number; maxDelaySeconds: number };
  /** Sender log events by name (warnings and errors), e.g. delivery.permanent_breaker_open. */
  senderLogEvents: Record<string, number>;
  breakerOpened: boolean;
  /** Fields of the first breaker-open events (no tokens, ids or keys). */
  breakerEvents: Array<Record<string, unknown>>;
  deliveries: {
    totalOkPushes: number;
    apnsAccepted: number;
    fcmAccepted: number;
    apnsUnregistered: number;
    fcmUnregistered: number;
    apnsRequests: number;
    fcmRequests: number;
    fcmRateLimited: number;
    injected429or500: number;
  };
  exactlyOnce: {
    ok: boolean;
    expectedDevices: number;
    deliveredOnce: number;
    excusedPermanent: number;
    missing: number;
    /** Devices with places in two alert cells delivered once per cell (the documented gap). */
    knownTwoCellDuplicates: number;
    /** Any other duplicate: a failure. */
    /** Any duplicate other than the cross-cell case (the two numbers below add up to it). */
    unexpectedDuplicates: number;
    /** Duplicates of a device whose first push got a timeout or connection error (answer lost). */
    duplicatesAfterLostResponse: number;
    duplicatesUnexplained: number;
    /** Devices the mock accepted a push for whose success the sender never saw. */
    deliveredWithoutAck: number;
    unexpectedTokens: number;
    summary: string;
  };
  replan: {
    planMs: number;
    shards: number;
    batchesProcessed: number;
    byReason: Record<string, number>;
    newPushRequests: number;
    newAcceptedPushes: number;
    newPushAttemptsInSender: number;
  };
  /** Present when collectTokens was set. */
  tokens?: { checkInTokens: Iterable<string> };
  /** Distinct devices with no successful delivery (permanent failures). */
  permanentFailureDevices: number;
  /** Devices delivered per the mock but never seen as a success by the sender (no check-in token). */
  deliveredWithoutAck: number;
}

// ---------- A light, Map-backed cell index (no cloning, sorted once) ----------

export class LightCellIndex implements Pick<CellIndex, 'listForCell'> {
  readonly cells = new Map<string, CellIndexEntry[]>();
  entries = 0;

  add(e: CellIndexEntry): void {
    let list = this.cells.get(e.h3Cell);
    if (!list) this.cells.set(e.h3Cell, (list = []));
    list.push(e);
    this.entries++;
  }

  /** Sort every cell by the DynamoDB sort key (`deviceId#placeId`), like the real index. */
  finalize(): void {
    for (const list of this.cells.values()) {
      list.sort((a, b) => {
        const ka = `${a.deviceId}#${a.placeId}`;
        const kb = `${b.deviceId}#${b.placeId}`;
        return ka < kb ? -1 : ka > kb ? 1 : 0;
      });
    }
  }

  async listForCell(h3Cell: string, cursor?: string, limit = 1000): Promise<CellIndexPage> {
    const all = this.cells.get(h3Cell) ?? [];
    const start = cursor === undefined ? 0 : Number(cursor);
    const entries = all.slice(start, start + limit);
    const next = start + limit;
    return next < all.length ? { entries, cursor: String(next) } : { entries };
  }
}

// ---------- In-memory send queue ----------

/** Holds serialized SQS-style messages (the real packer and JSON wire format). */
class SimQueue {
  private readonly lanes: Record<SendQueueName, string[]> = { urgent: [], standard: [] };
  private readonly heads: Record<SendQueueName, number> = { urgent: 0, standard: 0 };
  private waiters: Array<() => void> = [];
  private spaceWaiters: Array<() => void> = [];
  /** Messages hidden until `at` (performance.now ms): SQS DelaySeconds / visibility timeout. */
  private delayed: Array<{ at: number; lane: SendQueueName; body: string }> = [];
  closed = false;
  active = 0;
  /** Messages enqueued with a delay, and the longest delay seen (seconds). */
  delayedCount = 0;
  maxDelaySeconds = 0;

  constructor(private readonly maxQueued: number) {}

  get queued(): number {
    return (
      this.lanes.urgent.length -
      this.heads.urgent +
      (this.lanes.standard.length - this.heads.standard) +
      this.delayed.length
    );
  }

  /** Enqueue a message that consumers cannot see for `delaySeconds`. */
  pushDelayed(queue: SendQueueName, body: string, delaySeconds: number): void {
    this.delayedCount++;
    this.maxDelaySeconds = Math.max(this.maxDelaySeconds, delaySeconds);
    this.delayed.push({ at: performance.now() + delaySeconds * 1000, lane: queue, body });
    setTimeout(() => this.wake(), delaySeconds * 1000 + 5).unref();
  }

  private promoteDue(): void {
    if (this.delayed.length === 0) return;
    const t = performance.now();
    const still: typeof this.delayed = [];
    for (const d of this.delayed) {
      if (d.at <= t) this.lanes[d.lane].push(d.body);
      else still.push(d);
    }
    this.delayed = still;
  }

  private wake(): void {
    const w = this.waiters;
    this.waiters = [];
    for (const f of w) f();
  }

  push(queue: SendQueueName, body: string): void {
    this.lanes[queue].push(body);
    this.wake();
  }

  take(): string | undefined {
    this.promoteDue();
    for (const lane of ['urgent', 'standard'] as const) {
      const list = this.lanes[lane];
      const h = this.heads[lane];
      if (h < list.length) {
        const body = list[h];
        this.heads[lane] = h + 1;
        if (this.heads[lane] > 2048 && this.heads[lane] * 2 > list.length) {
          this.lanes[lane] = list.slice(this.heads[lane]);
          this.heads[lane] = 0;
        }
        const s = this.spaceWaiters;
        if (this.queued < this.maxQueued && s.length > 0) {
          this.spaceWaiters = [];
          for (const f of s) f();
        }
        return body;
      }
    }
    return undefined;
  }

  /** Producer back-pressure: wait until the queue is below its cap. */
  async waitForSpace(): Promise<void> {
    while (this.queued >= this.maxQueued) {
      await new Promise<void>((r) => this.spaceWaiters.push(r));
    }
  }

  /** Resolves when something changed (new message, a worker finished, or the queue closed). */
  changed(): Promise<void> {
    return new Promise<void>((r) => this.waiters.push(r));
  }

  notify(): void {
    this.wake();
  }
}

/** Strings of ASCII tokens packed into 1 MiB Buffers (2-byte length prefix): no per-string heap cost. */
export class TokenStore implements Iterable<string> {
  private readonly chunks: Buffer[] = [];
  private cur: Buffer | undefined;
  private used = 0;
  count = 0;

  add(token: string): void {
    const need = 2 + token.length;
    if (!this.cur || this.used + need > this.cur.length) {
      this.cur = Buffer.alloc(1 << 20);
      this.chunks.push(this.cur);
      this.used = 0;
    }
    this.cur.writeUInt16BE(token.length, this.used);
    this.cur.write(token, this.used + 2, 'latin1');
    this.used += need;
    this.count++;
  }

  *[Symbol.iterator](): Iterator<string> {
    for (const [i, chunk] of this.chunks.entries()) {
      const end = i === this.chunks.length - 1 ? this.used : chunk.length;
      for (let at = 0; at + 2 <= end;) {
        const len = chunk.readUInt16BE(at);
        if (len === 0) break;
        yield chunk.toString('latin1', at + 2, at + 2 + len);
        at += 2 + len;
      }
    }
  }
}

// ---------- Recording wrapper: one timed call per push ----------

interface PassClock {
  t0: number;
}

class Recorder {
  readonly latency = new LatencyRecorder();
  readonly checkInTokens = new TokenStore();
  readonly windows: Array<{ ok: number; retryable: number; permanent: number }> = [];
  firstOkMs: number | null = null;
  lastOkMs: number | null = null;
  okTotal = 0;
  /** Failed pushes by error label (no tokens in labels). */
  readonly errors = new Map<string, number>();
  /** Push tokens for which the sender saw at least one success. */
  readonly okTokens = new Set<string>();
  /** Push tokens with a timeout or connection error (the answer may have been lost). */
  readonly lostResponseTokens = new Set<string>();
  /** Messages handed to the push sender (all outcomes). */
  attempts = 0;
  clock: PassClock = { t0: performance.now() };

  constructor(private readonly collect: boolean) {}

  record(m: NativePushMessage, r: NativePushResult, startedAt: number, endedAt: number): void {
    this.attempts++;
    this.latency.record(endedAt - startedAt);
    const sec = Math.floor((endedAt - this.clock.t0) / 5000);
    let w = this.windows[sec];
    if (!w) {
      for (let i = this.windows.length; i <= sec; i++) {
        this.windows[i] = { ok: 0, retryable: 0, permanent: 0 };
      }
      w = this.windows[sec];
    }
    if (!w) return;
    if (r.ok) {
      w.ok++;
      this.okTotal++;
      const rel = endedAt - this.clock.t0;
      if (this.firstOkMs === null) this.firstOkMs = rel;
      this.lastOkMs = rel;
      this.okTokens.add(m.target.token);
      if (this.collect) this.checkInTokens.add(m.data['checkInToken'] ?? '');
    } else {
      const label = r.error.replace(/\s*retry-after=\d+/, '').slice(0, 80);
      this.errors.set(label, (this.errors.get(label) ?? 0) + 1);
      // The request may have been accepted although the sender never saw the answer.
      if (label.startsWith('Timeout') || label.includes('ConnectionError')) {
        this.lostResponseTokens.add(m.target.token);
      }
      if (r.kind === 'permanent') w.permanent++;
      else w.retryable++;
    }
  }
}

class RecordingSender implements NativePushSender {
  constructor(
    private readonly inner: NativePushSender,
    private readonly rec: Recorder,
  ) {}

  async send(messages: NativePushMessage[]): Promise<NativePushResult[]> {
    return Promise.all(
      messages.map(async (m) => {
        const t0 = performance.now();
        const out = await this.inner.send([m]);
        const t1 = performance.now();
        const r: NativePushResult = out[0] ?? { ok: false, kind: 'retryable', error: 'NoResult' };
        this.rec.record(m, r, t0, t1);
        return r;
      }),
    );
  }
}

// ---------- The simulation ----------

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
const mb = (n: number) => Math.round((n / 1024 / 1024) * 10) / 10;

function emptyCounts(): PassCounts {
  return {
    messagesProcessed: 0,
    batchesProcessed: 0,
    retryBatchesProcessed: 0,
    byReason: {},
    sent: 0,
    permanentFailed: 0,
    retried: 0,
    gaveUp: 0,
    redeliveries: 0,
  };
}

/** The county-sized polygon: the full seed bounding box, as GeoJSON [lng, lat]. */
export function countyPolygon() {
  const { south, north, west, east } = COUNTY_BOUNDS;
  return {
    type: 'Polygon' as const,
    coordinates: [
      [
        [west, south],
        [east, south],
        [east, north],
        [west, north],
        [west, south],
      ],
    ],
  };
}

export async function runFanoutSim(options: FanoutSimOptions): Promise<FanoutSimResult> {
  const log = options.log ?? (() => {});
  const opts = {
    devices: options.devices,
    seed: options.seed ?? 1,
    fcmRate: options.fcmRate ?? 10_000,
    senders: options.senders ?? 4,
    apnsSessions: options.apnsSessions ?? 4,
    messagesPerSender: options.messagesPerSender ?? 12,
    shardConcurrency: options.shardConcurrency ?? 32,
  };

  let peakHeap = 0;
  let peakRss = 0;
  const memTimer = setInterval(() => {
    const m = process.memoryUsage();
    peakHeap = Math.max(peakHeap, m.heapUsed);
    peakRss = Math.max(peakRss, m.rss);
  }, 500);
  memTimer.unref();

  // ----- seed -----
  const seedStart = performance.now();
  const index = new LightCellIndex();
  for (const e of generateEntries({
    devices: opts.devices,
    seed: opts.seed,
    deviceIds: 'uuid',
  })) {
    index.add(e);
  }
  index.finalize();
  const seedMs = performance.now() - seedStart;
  const heapAfterSeedMb = mb(process.memoryUsage().heapUsed);
  log(
    `seeded ${opts.devices} devices, ${index.entries} entries in ${index.cells.size} cells, ${Math.round(seedMs)} ms, heap ${heapAfterSeedMb} MB`,
  );

  // ----- the alert -----
  const alertCells = await cellsForAlertArea(
    { geometry: countyPolygon(), ugcCodes: [] },
    {
      getZoneGeometry: () => {
        throw new Error('zone lookup not expected');
      },
    },
  );
  const now = Date.now();
  const alert: Alert = signAlertSync(
    {
      id: ALERT_ID,
      source: 'nws',
      sourceId: 'urn:oid:sim-1',
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
        description: 'Simulated alert for the local load test.',
        instruction: null,
        senderName: 'NWS',
        language: 'en-US',
      },
      summaries: [],
      signature: 'unsigned',
    },
    ALERT_SIGNING.signer,
  );
  const alerts = new InMemoryAlertStore();
  await alerts.save(alert);
  const cachedAlerts = new CachedAlertStore(alerts);
  const verify = createAlertVerifier(ALERT_SIGNING.publicKeys);

  // ----- expected recipients (computed independently of the system under test) -----
  const expectedCount = new Map<string, number>(); // push token -> distinct alert cells holding it
  const devicesInArea = new Set<string>();
  for (const cell of alertCells) {
    const list = index.cells.get(cell);
    if (!list) continue;
    const seen = new Set<string>();
    for (const e of list) {
      if (!e.target || seen.has(e.deviceId)) continue;
      seen.add(e.deviceId);
      expectedCount.set(e.target.token, (expectedCount.get(e.target.token) ?? 0) + 1);
    }
  }
  let devicesInTwoOrMoreCells = 0;
  for (const n of expectedCount.values()) if (n > 1) devicesInTwoOrMoreCells++;
  const devicesWithTarget = expectedCount.size;
  for (const cell of alertCells) {
    for (const e of index.cells.get(cell) ?? []) devicesInArea.add(e.deviceId);
  }
  const devicesInAreaCount = devicesInArea.size;
  devicesInArea.clear();

  // ----- mock services -----
  const apns = await startMockApns({ seed: opts.seed, trackAttempts: false });
  const fcm = await startMockFcm({
    seed: opts.seed,
    ratePerSecond: opts.fcmRate,
    trackAttempts: false,
  });
  const ec = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const rsa = generateKeyPairSync('rsa', { modulusLength: 2048 });
  const apnsKey = ec.privateKey.export({ type: 'pkcs8', format: 'pem' }) as string;
  const fcmKey = rsa.privateKey.export({ type: 'pkcs8', format: 'pem' }) as string;

  const rec = new Recorder(options.collectTokens === true);
  const apnsSenders: ApnsSender[] = [];
  const fcmSenders: FcmSender[] = [];
  const pushers: NativePushSender[] = [];
  for (let i = 0; i < opts.senders; i++) {
    const a = new ApnsSender({
      privateKeyPem: apnsKey,
      keyId: 'SIMKEY0001',
      teamId: 'SIMTEAM001',
      bundleId: 'org.relay.sim',
      authority: apns.url,
      sessions: opts.apnsSessions,
    });
    const f = new FcmSender({
      serviceAccount: { client_email: 'sim@relay-sim.iam', private_key: fcmKey },
      projectId: 'relay-sim',
      tokenUri: `${fcm.url}/token`,
      baseUrl: fcm.url,
    });
    apnsSenders.push(a);
    fcmSenders.push(f);
    pushers.push(new RecordingSender(new RoutingPushSender({ ios: a, android: f, expo: a }), rec));
  }

  // ----- wiring -----
  const logCounts = new Map<string, number>();
  const breakerEvents: Array<Record<string, unknown>> = [];
  const logger: Logger = {
    info: () => {},
    warn: (msg) => void logCounts.set(msg, (logCounts.get(msg) ?? 0) + 1),
    error: (msg, f) => {
      logCounts.set(msg, (logCounts.get(msg) ?? 0) + 1);
      if (msg === 'delivery.permanent_breaker_open' && breakerEvents.length < 20) {
        const { alertId: _a, batchKey: _b, ...rest } = f ?? {};
        breakerEvents.push(rest);
      }
    },
  };
  const idempotency = new MemoryIdempotencyStore();
  const queue = new SimQueue(300);
  let tally = emptyCounts();

  const rawQueue: SendQueue = {
    async enqueue(name, batches, opts) {
      const delay = opts?.delaySeconds ?? 0;
      for (const m of packBatches(batches)) {
        if (delay > 0) queue.pushDelayed(name, serializeMessage(m), delay);
        else queue.push(name, serializeMessage(m));
      }
    },
  };
  // Shard workers wait when the queue is full (stands in for SQS being effectively unbounded
  // while keeping this process's memory bounded). Sender retries never wait.
  const producerQueue: SendQueue = {
    async enqueue(name, batches) {
      for (const m of packBatches(batches)) {
        await queue.waitForSpace();
        queue.push(name, serializeMessage(m));
      }
    },
  };

  const deadDevices = new Set<string>();
  const senderDeps = (push: NativePushSender) => ({
    idempotency,
    alerts: cachedAlerts,
    verifyAlert: (a: Alert) => verify(a),
    isDeliveryPaused: async () => false,
    push,
    sendQueue: rawQueue,
    onDeadTarget: async (deviceId: string) => void deadDevices.add(deviceId),
    clock: { now: () => new Date() },
    logger,
  });
  const depsFor = pushers.map(senderDeps);

  const shardDeps = {
    alerts: cachedAlerts,
    verifyAlert: (a: Alert) => verify(a),
    isDeliveryPaused: async () => false,
    clock: { now: () => new Date() },
    logger,
    cellIndex: index,
    sendQueue: producerQueue,
    makeCheckInToken: (alertId: string, deviceId: string, h3Cell: string, expiresAt: Date) =>
      createCheckInToken({ alertId, deviceId, expiresAt, h3Cell }, TOKEN_KEY),
  };

  let shardsPlanned = 0;
  let plannedBatches = 0;
  let plannedRecipients = 0;
  let skippedNoTarget = 0;
  let sameCellDuplicates = 0;
  let skippedNonPrimary = 0;
  let shardError: unknown;
  let shardsDone: Promise<void> = Promise.resolve();
  let passIsFirst = true;

  const dispatcher: ShardDispatcher = {
    async dispatch(jobs: ShardJob[]): Promise<void> {
      // Returns at once, like the real planner handing jobs to shard Lambdas.
      let next = 0;
      const first = passIsFirst;
      if (first) shardsPlanned = jobs.length;
      const worker = async (): Promise<void> => {
        while (next < jobs.length) {
          const job = jobs[next++];
          if (!job) continue;
          const r = await processShard(job, shardDeps);
          if (first) {
            plannedBatches += r.batches;
            plannedRecipients += r.recipients;
            skippedNoTarget += r.skippedNoTarget;
            sameCellDuplicates += r.duplicates;
            skippedNonPrimary += r.skippedNonPrimary;
          }
        }
      };
      shardsDone = Promise.all(
        Array.from({ length: Math.min(opts.shardConcurrency, jobs.length) }, worker),
      ).then(
        () => {
          queue.closed = true;
          queue.notify();
        },
        (err: unknown) => {
          shardError = err;
          queue.closed = true;
          queue.notify();
        },
      );
    },
  };

  const senderWorker = async (deps: ReturnType<typeof senderDeps>): Promise<void> => {
    for (;;) {
      const body = queue.take();
      if (body === undefined) {
        if (queue.closed && queue.active === 0 && queue.queued === 0) return;
        await queue.changed();
        continue;
      }
      queue.active++;
      try {
        const batches = parseBatches(body);
        if (!batches) throw new Error('unparseable message');
        const results = await Promise.allSettled(batches.map((b) => processBatch(b, deps)));
        let allAcked = true;
        results.forEach((r, i) => {
          if (r.status === 'rejected') {
            allAcked = false;
            return;
          }
          const o = r.value;
          tally.batchesProcessed++;
          if ((batches[i]?.attempt ?? 0) > 0) tally.retryBatchesProcessed++;
          tally.byReason[o.reason as BatchReason] =
            (tally.byReason[o.reason as BatchReason] ?? 0) + 1;
          tally.sent += o.sent;
          tally.permanentFailed += o.permanentFailed;
          tally.retried += o.retried;
          tally.gaveUp += o.gaveUp;
          if (!o.ack) allAcked = false;
        });
        tally.messagesProcessed++;
        if (!allAcked) {
          // Stands in for SQS making the message visible again after its timeout.
          tally.redeliveries++;
          await sleep(200);
          queue.push('urgent', body);
        }
      } finally {
        queue.active--;
        queue.notify();
      }
    }
  };

  const runPass = async (): Promise<{ planMs: number; shards: number; wallMs: number }> => {
    tally = emptyCounts();
    queue.closed = false;
    shardError = undefined;
    const start = performance.now();
    rec.clock.t0 = start;
    const plan = await planShards(
      { alertId: alert.id, sourceId: alert.sourceId, relaySeverity: alert.relaySeverity },
      {
        alerts: cachedAlerts,
        verifyAlert: (a: Alert) => verify(a),
        isDeliveryPaused: async () => false,
        clock: { now: () => new Date() },
        logger,
        shardDispatcher: dispatcher,
      },
    );
    const planMs = performance.now() - start;
    if (plan.refused) throw new Error(`planner refused: ${plan.refused}`);
    const workers: Array<Promise<void>> = [];
    for (const deps of depsFor) {
      for (let i = 0; i < opts.messagesPerSender; i++) workers.push(senderWorker(deps));
    }
    await shardsDone;
    await Promise.all(workers);
    if (shardError) throw shardError;
    return { planMs, shards: plan.shards, wallMs: performance.now() - start };
  };

  // ----- pass 1: the fan-out -----
  log(
    `fan-out: ${alertCells.length} alert cells, ${devicesWithTarget} devices with a target, ${opts.senders} senders`,
  );
  const progress = setInterval(() => {
    log(
      `  t=${Math.round((performance.now() - rec.clock.t0) / 1000)}s ok=${rec.okTotal} queued=${queue.queued} heap=${mb(process.memoryUsage().heapUsed)}MB`,
    );
  }, 5000);
  const p1 = await runPass();
  clearInterval(progress);
  const pass1 = tally;
  const wallMs = p1.wallMs;
  const okAfterPass1 = rec.okTotal;
  log(`pass 1 done in ${Math.round(wallMs)} ms; ok pushes ${okAfterPass1}`);

  // ----- exactly-once check -----
  const delivered = new Map<string, number>();
  for (const [t, n] of apns.counters.delivered) delivered.set(t, n);
  for (const [t, n] of fcm.counters.delivered) delivered.set(t, n);
  const excused = new Set<string>();
  for (const t of apns.counters.unregistered.keys()) excused.add(t);
  for (const t of fcm.counters.unregistered.keys()) excused.add(t);
  const eo: ExactlyOnceResult = checkExactlyOnce(expectedCount.keys(), delivered, { excused });
  // A device in k alert cells legitimately gets k pushes (the shard worker cannot see other
  // shards). Count those apart; anything else delivered twice is a real duplicate.
  let knownTwoCell = 0;
  let unexpectedDuplicates = 0;
  let duplicatesAfterLostResponse = 0;
  let duplicatesUnexplained = 0;
  let deliveredWithoutAck = 0;
  for (const t of delivered.keys()) if (!rec.okTokens.has(t)) deliveredWithoutAck++;
  for (const d of eo.duplicates) {
    const cells = expectedCount.get(d.token) ?? 1;
    if (rec.lostResponseTokens.has(d.token)) {
      unexpectedDuplicates++;
      duplicatesAfterLostResponse++;
    } else if (cells > 1 && d.count <= cells) knownTwoCell++;
    else {
      unexpectedDuplicates++;
      duplicatesUnexplained++;
    }
  }
  // Devices in two cells delivered only once are fine too (checkExactlyOnce counts them as once).
  const exactlyOnceOk =
    eo.missing.length === 0 &&
    knownTwoCell === 0 &&
    unexpectedDuplicates === 0 &&
    eo.unexpected.length === 0;
  // checkExactlyOnce counts the cross-cell deliveries as duplicates; the verdict here
  // excludes them (they are reported separately) and keeps its raw output for the record.
  const eoSummary =
    `exactly-once: ${exactlyOnceOk ? 'PASS' : 'FAIL'} ` +
    `(expected ${eo.expected}, once ${eo.deliveredOnce}, excused ${eo.excused}, missing ${eo.missing.length}, ` +
    `cross-cell ${knownTwoCell}, other duplicates ${unexpectedDuplicates} (after a lost response ${duplicatesAfterLostResponse}, unexplained ${duplicatesUnexplained}), unexpected ${eo.unexpected.length})\n` +
    `raw checkExactlyOnce: ${formatExactlyOnce(eo, 0).split('\n')[0]}`;
  log(eoSummary);
  log(`known two-cell duplicates ${knownTwoCell}; unexpected duplicates ${unexpectedDuplicates}`);

  // ----- pass 2: re-plan the same alert, expect zero new pushes -----
  const before = {
    apnsReq: apns.stats().received,
    fcmReq: fcm.stats().received,
    accepted: apns.stats().ok + fcm.stats().ok,
    attempts: rec.attempts,
  };
  passIsFirst = false;
  const p2 = await runPass();
  const pass2 = tally;
  const replan = {
    planMs: p2.planMs,
    shards: p2.shards,
    batchesProcessed: pass2.batchesProcessed,
    byReason: pass2.byReason,
    newPushRequests: apns.stats().received + fcm.stats().received - before.apnsReq - before.fcmReq,
    newAcceptedPushes: apns.stats().ok + fcm.stats().ok - before.accepted,
    newPushAttemptsInSender: rec.attempts - before.attempts,
  };
  log(
    `re-plan: ${replan.batchesProcessed} batches, new push requests ${replan.newPushRequests}, new accepted ${replan.newAcceptedPushes}`,
  );

  const apnsStats = apns.stats();
  const fcmStats = fcm.stats();
  const windows: WindowStat[] = rec.windows.map((w, i) => ({
    startSec: i * 5,
    ok: w.ok,
    okPerSec: Math.round(w.ok / 5),
    retryable: w.retryable,
    permanent: w.permanent,
  }));
  const lastMs = rec.lastOkMs;
  const firstMs = rec.firstOkMs;
  const activeSeconds = lastMs !== null && firstMs !== null ? (lastMs - firstMs) / 1000 : 0;

  const result: FanoutSimResult = {
    options: opts,
    seedMs: Math.round(seedMs),
    heapAfterSeedMb,
    peakHeapMb: mb(peakHeap),
    peakRssMb: mb(peakRss),
    counts: {
      devices: opts.devices,
      entries: index.entries,
      cellsUsed: index.cells.size,
      alertCells: alertCells.length,
      devicesInArea: devicesInAreaCount,
      devicesWithTarget,
      devicesInTwoOrMoreCells,
      shards: shardsPlanned,
      plannedBatches,
      plannedRecipients,
      skippedNoTarget,
      sameCellDuplicates,
      skippedNonPrimary,
    },
    timing: {
      planMs: Math.round(p1.planMs),
      firstPushMs: firstMs === null ? null : Math.round(firstMs),
      lastPushMs: lastMs === null ? null : Math.round(lastMs),
      wallMs: Math.round(wallMs),
    },
    throughput: {
      meanOkPerSec: activeSeconds > 0 ? Math.round(okAfterPass1 / activeSeconds) : 0,
      peakWindowOkPerSec: Math.max(0, ...windows.map((w) => w.okPerSec)),
      windows,
    },
    pushLatency: rec.latency.summary(),
    pushErrors: Object.fromEntries(rec.errors),
    pass1,
    retryDelays: { delayedMessages: queue.delayedCount, maxDelaySeconds: queue.maxDelaySeconds },
    senderLogEvents: Object.fromEntries(logCounts),
    breakerEvents,
    breakerOpened: (logCounts.get('delivery.permanent_breaker_open') ?? 0) > 0,
    deliveries: {
      totalOkPushes: okAfterPass1,
      apnsAccepted: apnsStats.ok,
      fcmAccepted: fcmStats.ok,
      apnsUnregistered: apns.counters.unregistered.size,
      fcmUnregistered: fcm.counters.unregistered.size,
      apnsRequests: apnsStats.received,
      fcmRequests: fcmStats.received,
      fcmRateLimited: fcmStats.rateLimited,
      injected429or500:
        apnsStats.tooManyRequests +
        apnsStats.serverError +
        fcmStats.tooManyRequests +
        fcmStats.serverError,
    },
    exactlyOnce: {
      ok: exactlyOnceOk,
      expectedDevices: eo.expected,
      deliveredOnce: eo.deliveredOnce,
      excusedPermanent: eo.excused,
      missing: eo.missing.length,
      knownTwoCellDuplicates: knownTwoCell,
      unexpectedDuplicates,
      duplicatesAfterLostResponse,
      duplicatesUnexplained,
      deliveredWithoutAck,
      unexpectedTokens: eo.unexpected.length,
      summary: eoSummary,
    },
    replan,
    permanentFailureDevices: eo.excused,
    deliveredWithoutAck,
    ...(options.collectTokens ? { tokens: { checkInTokens: rec.checkInTokens } } : {}),
  };

  clearInterval(memTimer);
  for (const a of apnsSenders) a.close();
  for (const f of fcmSenders) f.close();
  await apns.close();
  await fcm.close();
  return result;
}
