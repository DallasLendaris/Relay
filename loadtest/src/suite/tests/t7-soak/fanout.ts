/**
 * t7 child: the fan-out driver. It runs the REAL planner (planShards) and shard worker
 * (processShard) over the cell index that lives in the API process (paged over HTTP), and it
 * hosts the three small services the sender processes share, standing in for AWS:
 *   - an SQS-like send queue (visibility timeouts, delays, long polling, urgent before standard)
 *   - the alert store (the senders re-read the alert for every batch, through the real CachedAlertStore)
 *   - the idempotency store (batch claims and leases)
 * The parent tells it which alert event to publish (POST /alert) and polls /status until the
 * fan-out and the queue have drained.
 */
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../../../../backend/src/adapters/memory/alert-store.js';
import { MemoryIdempotencyStore } from '../../../../../backend/src/adapters/memory/idempotency-store.js';
import { createKillSwitch } from '../../../../../backend/src/config/kill-switch.js';
import type { Logger } from '../../../../../backend/src/delivery/types.js';
import { cellsForAlertArea } from '../../../../../backend/src/geo/h3-cells.js';
import {
  type CellIndexPage,
  type SendBatch,
  type SendQueue,
  type SendQueueName,
  type ShardJob,
} from '../../../../../backend/src/scale/contracts.js';
import { CachedAlertReader } from '../../../../../backend/src/scale/fanout/alert-cache.js';
import { packBatches, serializeMessage } from '../../../../../backend/src/scale/fanout/pack.js';
import { planShards } from '../../../../../backend/src/scale/fanout/planner.js';
import type { ShardDispatcher } from '../../../../../backend/src/scale/fanout/queues.js';
import { processShard } from '../../../../../backend/src/scale/fanout/shard-worker.js';
import { createAlertVerifier, signAlertSync } from '../../../../../backend/src/signing/index.js';
import { createCheckInToken } from '../../../../../backend/src/tokens/checkInToken.js';
import { runWorkerMain, sendMessage } from '../../lib/child.js';
import { alertSigningFromPem, TOKEN_KEY } from './keys.js';
import {
  AREAS,
  areaPolygon,
  call,
  deferred,
  quietMetricLines,
  readConfig,
  readPorts,
  sizeOf,
  sleep,
  startSampler,
  startServer,
  type AlertEvent,
} from './common.js';

type Lane = SendQueueName;

interface Msg {
  id: string;
  lane: Lane;
  body: string;
  sentAt: number;
  visibleAt: number;
  inflight: boolean;
  receives: number;
}

/** SQS semantics in one process: FIFO-ish lanes, visibility timeout, delays, long polling. */
export class SoakQueue {
  private readonly msgs = new Map<string, Msg>();
  private readonly ready: Record<Lane, string[]> = { urgent: [], standard: [] };
  private readonly heads: Record<Lane, number> = { urgent: 0, standard: 0 };
  private readonly hidden = new Set<string>();
  private waiters: Array<() => void> = [];
  private spaceWaiters: Array<() => void> = [];
  private nextId = 1;
  enqueued = 0;
  deleted = 0;
  redelivered = 0;
  maxDepth = 0;

  constructor(private readonly maxQueued: number) {}

  get size(): number {
    return this.msgs.size;
  }
  get inflight(): number {
    let n = 0;
    for (const id of this.hidden) if (this.msgs.get(id)?.inflight) n++;
    return n;
  }
  get delayed(): number {
    return this.hidden.size - this.inflight;
  }

  enqueue(lane: Lane, body: string, delaySeconds = 0): void {
    const id = String(this.nextId++);
    const m: Msg = {
      id,
      lane,
      body,
      sentAt: Date.now(),
      visibleAt: Date.now() + delaySeconds * 1000,
      inflight: false,
      receives: 0,
    };
    this.msgs.set(id, m);
    this.enqueued++;
    this.maxDepth = Math.max(this.maxDepth, this.msgs.size);
    if (delaySeconds > 0) this.hidden.add(id);
    else this.makeReady(m);
  }

  private makeReady(m: Msg): void {
    m.inflight = false;
    this.hidden.delete(m.id);
    this.ready[m.lane].push(m.id);
    const w = this.waiters;
    this.waiters = [];
    for (const f of w) f();
  }

  /** Moves messages whose delay or visibility timeout has run out back to the ready lanes. */
  sweep(): void {
    const now = Date.now();
    for (const id of [...this.hidden]) {
      const m = this.msgs.get(id);
      if (!m) {
        this.hidden.delete(id);
        continue;
      }
      if (m.visibleAt <= now) {
        if (m.inflight) this.redelivered++;
        this.makeReady(m);
      }
    }
  }

  private take(lane: Lane): Msg | undefined {
    const list = this.ready[lane];
    while (this.heads[lane] < list.length) {
      const id = list[this.heads[lane]++];
      if (this.heads[lane] > 1024 && this.heads[lane] * 2 > list.length) {
        this.ready[lane] = list.slice(this.heads[lane]);
        this.heads[lane] = 0;
      }
      const m = id === undefined ? undefined : this.msgs.get(id);
      if (m && !m.inflight && !this.hidden.has(m.id)) return m;
    }
    return undefined;
  }

  async receive(
    lane: Lane,
    max: number,
    waitMs: number,
    visibilitySeconds = 30,
  ): Promise<Array<{ id: string; receipt: string; body: string; sentAt: number }>> {
    const out: Array<{ id: string; receipt: string; body: string; sentAt: number }> = [];
    const grab = (): void => {
      while (out.length < max) {
        const m = this.take(lane);
        if (!m) return;
        m.inflight = true;
        m.receives++;
        m.visibleAt = Date.now() + visibilitySeconds * 1000;
        this.hidden.add(m.id);
        out.push({ id: m.id, receipt: `${m.id}:${m.receives}`, body: m.body, sentAt: m.sentAt });
      }
    };
    grab();
    if (out.length === 0 && waitMs > 0) {
      await new Promise<void>((resolve) => {
        const t = setTimeout(resolve, waitMs);
        this.waiters.push(() => {
          clearTimeout(t);
          resolve();
        });
      });
      grab();
    }
    return out;
  }

  delete(receipt: string): void {
    const id = receipt.split(':')[0] ?? '';
    if (this.msgs.delete(id)) {
      this.hidden.delete(id);
      this.deleted++;
      const s = this.spaceWaiters;
      if (this.msgs.size < this.maxQueued && s.length > 0) {
        this.spaceWaiters = [];
        for (const f of s) f();
      }
    }
  }

  changeVisibility(receipt: string, seconds: number): void {
    const id = receipt.split(':')[0] ?? '';
    const m = this.msgs.get(id);
    if (!m || !m.inflight) return;
    m.visibleAt = Date.now() + seconds * 1000;
    if (seconds <= 0) this.makeReady(m);
  }

  /** Producer back-pressure: shard workers wait while the queue is full (SQS itself is unbounded). */
  async waitForSpace(): Promise<void> {
    while (this.msgs.size >= this.maxQueued) {
      await new Promise<void>((r) => this.spaceWaiters.push(r));
    }
  }
}

/** The in-memory idempotency store, plus the TTL purge DynamoDB does for the real one. */
export class PurgingIdempotencyStore extends MemoryIdempotencyStore {
  get size(): number {
    return sizeOf(this, 'entries');
  }
  purge(nowMs: number): number {
    const entries = (this as unknown as { entries: Map<string, { until: number }> }).entries;
    let n = 0;
    for (const [k, e] of entries) {
      if (e.until <= nowMs) {
        entries.delete(k);
        n++;
      }
    }
    return n;
  }
}

/** Claims are kept for the retry window only (the real table keeps them a day, then TTL deletes them). */
const IDEMPOTENCY_KEEP_MS = 30 * 60 * 1000;

runWorkerMain(async () => {
  quietMetricLines();
  const cfg = readConfig();
  const ports = readPorts();
  const apiCtl = ports.apiCtl ?? 0;
  const pem = process.env['T7_ALERT_KEY'] ?? '';
  const { signer, publicKeys } = alertSigningFromPem(pem);
  const verify = createAlertVerifier(publicKeys);

  // ----- logger (counts warnings and errors by name) -----
  const logCounts = new Map<string, number>();
  const logger: Logger = {
    info: () => undefined,
    warn: (msg) => void logCounts.set(msg, (logCounts.get(msg) ?? 0) + 1),
    error: (msg) => void logCounts.set(msg, (logCounts.get(msg) ?? 0) + 1),
  };

  // ----- stores and services -----
  const alertStore = new InMemoryAlertStore();
  const alertReader = new CachedAlertReader(alertStore);
  const queue = new SoakQueue(400);
  const idempotency = new PurgingIdempotencyStore();
  let killSwitchReads = 0;
  const isPaused = createKillSwitch(
    '/relay/soak/killswitch',
    {
      send: async () => {
        killSwitchReads++;
        return { Parameter: { Value: 'running' } };
      },
    } as never,
    Date.now,
  );

  const sweepTimer = setInterval(() => {
    queue.sweep();
    idempotency.purge(Date.now());
  }, 250);
  sweepTimer.unref();

  // ----- cell index over HTTP -----
  const cellIndex = {
    async listForCell(h3Cell: string, cursor?: string, limit = 1000): Promise<CellIndexPage> {
      const q = `/index/list?cell=${h3Cell}&limit=${limit}${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`;
      const r = await call(apiCtl, 'GET', q);
      if (r.status !== 200) throw new Error(`index list ${r.status}`);
      return JSON.parse(r.body) as CellIndexPage;
    },
  };

  // ----- planner and shard workers -----
  const producerQueue: SendQueue = {
    async enqueue(name, batches: SendBatch[]) {
      for (const m of packBatches(batches)) {
        await queue.waitForSpace();
        queue.enqueue(name, serializeMessage(m));
      }
    },
  };
  const deps = {
    alerts: alertReader,
    verifyAlert: (a: Alert) => verify(a),
    isDeliveryPaused: isPaused,
    clock: { now: () => new Date() },
    logger,
  };
  const shardDeps = {
    ...deps,
    cellIndex,
    sendQueue: producerQueue,
    makeCheckInToken: (alertId: string, deviceId: string, h3Cell: string, expiresAt: Date) =>
      createCheckInToken({ alertId, deviceId, expiresAt, h3Cell }, TOKEN_KEY),
  };

  const SHARD_CONCURRENCY = cfg.scale === 'full' ? 16 : 4;
  let shardsRunning = false;
  let shardError: string | null = null;
  const shardTotals = {
    shards: 0,
    batches: 0,
    recipients: 0,
    skippedNoTarget: 0,
    skippedNonPrimary: 0,
  };
  const dispatcher: ShardDispatcher = {
    async dispatch(jobs: ShardJob[]): Promise<void> {
      let next = 0;
      shardsRunning = true;
      const worker = async (): Promise<void> => {
        while (next < jobs.length) {
          const job = jobs[next++];
          if (!job) continue;
          const r = await processShard(job, shardDeps);
          shardTotals.shards++;
          shardTotals.batches += r.batches;
          shardTotals.recipients += r.recipients;
          shardTotals.skippedNoTarget += r.skippedNoTarget;
          shardTotals.skippedNonPrimary += r.skippedNonPrimary;
        }
      };
      void Promise.all(
        Array.from({ length: Math.min(SHARD_CONCURRENCY, jobs.length) }, worker),
      ).then(
        () => {
          shardsRunning = false;
        },
        (err: unknown) => {
          shardError = err instanceof Error ? err.message : String(err);
          shardsRunning = false;
        },
      );
    },
  };

  // ----- alerts -----
  const areaCells = new Map<number, string[]>();
  async function cellsOf(areaIdx: number): Promise<string[]> {
    let c = areaCells.get(areaIdx);
    if (!c) {
      const area = AREAS[areaIdx];
      if (!area) throw new Error(`no area ${areaIdx}`);
      c = await cellsForAlertArea(
        { geometry: areaPolygon(area), ugcCodes: [] },
        {
          getZoneGeometry: () => {
            throw new Error('zone lookup not expected');
          },
        },
      );
      areaCells.set(areaIdx, c);
    }
    return c;
  }

  const published: Array<{
    index: number;
    kind: string;
    refused: string | null;
    planMs: number;
    shards: number;
  }> = [];

  async function publish(ev: AlertEvent): Promise<{ refused: string | null; shards: number }> {
    const cells = await cellsOf(ev.areaIdx);
    const area = AREAS[ev.areaIdx];
    if (!area) throw new Error('area');
    const now = Date.now();
    const cancelled = ev.kind === 'cancel';
    const alert: Alert = signAlertSync(
      {
        id: ev.alertId,
        source: 'nws',
        sourceId: ev.sourceId,
        event: ev.severity === 'watch' ? 'Flood Watch' : 'Flash Flood Warning',
        eventCode: ev.severity === 'watch' ? 'FFA' : 'FFW',
        relaySeverity: ev.severity,
        capSeverity: ev.severity === 'emergency' ? 'Extreme' : 'Severe',
        urgency: 'Immediate',
        certainty: 'Likely',
        status: 'Actual',
        messageType: ev.kind === 'new' ? 'Alert' : cancelled ? 'Cancel' : 'Update',
        state: ev.kind === 'new' ? 'active' : cancelled ? 'cancelled' : 'updated',
        references: [],
        area: {
          description: `${area.name} (soak)`,
          geometry: areaPolygon(area),
          ugcCodes: [],
          sameCodes: [],
          h3Cells: cells,
        },
        sent: new Date(now - 1000).toISOString(),
        effective: new Date(now - 1000).toISOString(),
        onset: null,
        expires: new Date(now + cfg.alertLifeS * 1000).toISOString(),
        ends: null,
        official: {
          headline: `${ev.kind} ${ev.severity} for the ${area.name}`,
          description: 'Simulated alert for the soak test.',
          instruction: null,
          senderName: 'NWS',
          language: 'en-US',
        },
        summaries: [],
        signature: 'unsigned',
      },
      signer,
    );
    await alertStore.save(alert);
    const t0 = Date.now();
    shardError = null;
    const plan = await planShards(
      { alertId: alert.id, sourceId: alert.sourceId, relaySeverity: alert.relaySeverity },
      { ...deps, shardDispatcher: dispatcher },
    );
    published.push({
      index: ev.index,
      kind: ev.kind,
      refused: plan.refused ?? null,
      planMs: Date.now() - t0,
      shards: plan.shards,
    });
    return { refused: plan.refused ?? null, shards: plan.shards };
  }

  // ----- HTTP services -----
  const stop = deferred();
  const sampler = startSampler('fanout', cfg.sampleMs, () => ({
    queueMessages: queue.size,
    queueInflight: queue.inflight,
    queueDelayed: queue.delayed,
    idempotencyEntries: idempotency.size,
    alertReaderCache: sizeOf(alertReader, 'cache'),
    killSwitchReads,
    logKeys: logCounts.size,
  }));

  const server = await startServer(async (r) => {
    const json = (): unknown => JSON.parse(r.body);
    if (r.method === 'POST' && r.path === '/q/receive') {
      const b = json() as { queue: Lane; max: number; wait: number };
      const msgs = await queue.receive(b.queue, b.max, Math.min(b.wait, 1) * 1000);
      return {
        body: msgs.map((m) => ({
          MessageId: m.id,
          ReceiptHandle: m.receipt,
          Body: m.body,
          Attributes: { SentTimestamp: String(m.sentAt) },
        })),
      };
    }
    if (r.method === 'POST' && r.path === '/q/delete') {
      for (const h of (json() as { receipts: string[] }).receipts) queue.delete(h);
      return { body: { ok: true } };
    }
    if (r.method === 'POST' && r.path === '/q/visibility') {
      const b = json() as { receipt: string; seconds: number };
      queue.changeVisibility(b.receipt, b.seconds);
      return { body: { ok: true } };
    }
    if (r.method === 'POST' && r.path === '/q/send') {
      const b = json() as {
        queue: Lane;
        entries: Array<{ body: string; delay?: number }>;
      };
      for (const e of b.entries) queue.enqueue(b.queue, e.body, e.delay ?? 0);
      return { body: { ok: true } };
    }
    if (r.method === 'GET' && r.path.startsWith('/alert/')) {
      const a = await alertStore.get(r.path.slice('/alert/'.length));
      return a ? { body: a } : { status: 404, body: 'none' };
    }
    if (r.method === 'POST' && r.path.startsWith('/idem/')) {
      const op = r.path.slice('/idem/'.length);
      const b = json() as { key: string; until?: number };
      let v: unknown;
      switch (op) {
        case 'claim':
          v = await idempotency.claim(b.key, new Date(b.until ?? 0));
          break;
        case 'markSent':
          await idempotency.markSent(
            b.key,
            new Date(Math.min(b.until ?? 0, Date.now() + IDEMPOTENCY_KEEP_MS)),
          );
          break;
        case 'release':
          await idempotency.release(b.key);
          break;
        case 'extend':
          v = await idempotency.extend(b.key, new Date(b.until ?? 0));
          break;
        case 'peek':
          v = await idempotency.peek(b.key);
          break;
        default:
          return { status: 404, body: 'op' };
      }
      return { body: { v: v ?? null } };
    }
    if (r.method === 'POST' && r.path === '/alert') {
      const ev = json() as AlertEvent;
      const res = await publish(ev);
      return { body: { ...res, planStartMs: Date.now() } };
    }
    if (r.method === 'GET' && r.path === '/status') {
      return {
        body: {
          shardsRunning,
          shardError,
          queued: queue.size,
          inflight: queue.inflight,
          delayed: queue.delayed,
        },
      };
    }
    if (r.method === 'POST' && r.path === '/stop') {
      stop.resolve();
      return { body: { ok: true } };
    }
    return { status: 404, body: 'not found' };
  });

  await sendMessage({ kind: 'ready', port: server.port });
  await stop.promise;
  sampler();
  clearInterval(sweepTimer);
  await sleep(50);
  server.close();
  return {
    published,
    shardTotals,
    queue: {
      enqueued: queue.enqueued,
      deleted: queue.deleted,
      redelivered: queue.redelivered,
      maxDepth: queue.maxDepth,
    },
    killSwitchReads,
    logCounts: Object.fromEntries(logCounts),
  };
});
