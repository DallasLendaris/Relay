/**
 * The work of test 4, as plain functions over an injectable `Backend` (a DynamoDB document client,
 * the table names and a SQL executor factory). A child process (worker.ts) builds a real Backend
 * (DynamoDB Local + node-postgres) and runs one kernel; the vitest smoke test runs the same kernels
 * in-process against a fake document client and PGlite. Every kernel uses the REAL backend code:
 * DynamoCellIndex, processShard, DynamoIdempotencyStore, DynamoSeenStore, DynamoCheckInStore, the
 * check-in aggregator, DynamoCheckInTotals, PostgresPlaceStore/DeviceStore, the outbox, the drainer
 * and syncDeviceCells.
 */
import { randomUUID, createHash } from 'node:crypto';
import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { marshall } from '@aws-sdk/util-dynamodb';
import type { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { DynamoDBRecord, DynamoDBStreamEvent } from 'aws-lambda';
import type { Alert, CheckIn } from '@relay/shared';
import { DynamoCheckInStore } from '../../../../../backend/src/adapters/aws/dynamo-checkin-store.js';
import { DynamoIdempotencyStore } from '../../../../../backend/src/adapters/aws/dynamo-idempotency-store.js';
import { DynamoSeenStore } from '../../../../../backend/src/adapters/aws/dynamo-seen-store.js';
import { PostgresDeviceStore } from '../../../../../backend/src/adapters/postgres/device-store.js';
import { PostgresPlaceStore } from '../../../../../backend/src/adapters/postgres/place-store.js';
import type { SqlExecutor } from '../../../../../backend/src/db/sql.js';
import type { Logger } from '../../../../../backend/src/delivery/types.js';
import type { Device, Place } from '../../../../../backend/src/ports.js';
import { DynamoCellIndex } from '../../../../../backend/src/scale/cell-index/dynamo-cell-index.js';
import { drainOutbox } from '../../../../../backend/src/scale/cell-index/drainer.js';
import { PostgresCellIndexOutbox } from '../../../../../backend/src/scale/cell-index/outbox.js';
import { syncDeviceCells } from '../../../../../backend/src/scale/cell-index/sync-device-cells.js';
import { targetOf } from '../../../../../backend/src/scale/cell-index/util.js';
import { createAggregator } from '../../../../../backend/src/scale/checkins/aggregator.js';
import type {
  CellIndex,
  CellIndexEntry,
  SendQueue,
} from '../../../../../backend/src/scale/contracts.js';
import { processShard } from '../../../../../backend/src/scale/fanout/shard-worker.js';
import {
  createAlertVerifier,
  createMemoryAlertSigning,
  signAlertSync,
} from '../../../../../backend/src/signing/index.js';
import { createCheckInToken } from '../../../../../backend/src/tokens/checkInToken.js';
import { createRng } from '../../../mock-common.js';
import { generateDevices, generateEntries, uuidForDevice } from '../../../seed.js';
import { emptyStats, instrumentDoc, type DocStats } from './instrument.js';
import type { TableNames } from './tables.js';

export type Doc = Pick<DynamoDBDocumentClient, 'send'>;

export interface Backend {
  doc: Doc;
  names: TableNames;
  /** A SQL executor; with `role`, statements run under that Postgres role (SET ROLE). */
  sql(role?: string): SqlExecutor;
  /** Directory for per-task output files. */
  scratchDir: string;
}

export const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

const silent: Logger = { info() {}, warn() {}, error() {} };

/** Runs `fn(i)` for i in [0, n) with at most `limit` in flight. */
export async function pool(
  n: number,
  limit: number,
  fn: (i: number) => Promise<void>,
): Promise<void> {
  let next = 0;
  const worker = async (): Promise<void> => {
    for (let i = next++; i < n; i = next++) await fn(i);
  };
  await Promise.all(Array.from({ length: Math.min(limit, n) }, worker));
}

/** FNV-1a 32 bit, for deterministic "random" decisions that do not depend on call order. */
export function hash32(s: string | number): number {
  const str = String(s);
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h >>> 0;
}

const hex12 = (n: number): string => n.toString(16).padStart(12, '0');

// =====================================================================================
// (a) cell index: bulk write and shard-worker read
// =====================================================================================

export interface AWriteParams {
  worker: number;
  /** Stop (at a device boundary) once this many entries were written. */
  entries: number;
  seed: number;
  /** Entries per CellIndex.put call (the adapter splits it into 25-item BatchWrites). */
  putEntries: number;
  /** put calls in flight per process. */
  putConcurrency: number;
}
export interface AWriteResult {
  entries: number;
  devices: number;
  perCell: Record<string, number>;
  ms: number;
  stats: DocStats;
}

export async function aWrite(b: Backend, p: AWriteParams): Promise<AWriteResult> {
  const stats = emptyStats();
  const index = new DynamoCellIndex(instrumentDoc(b.doc, stats), b.names.cellIndex);
  const perCell: Record<string, number> = {};
  let entries = 0;
  let devices = 0;
  let lastDevice = '';
  let batch: CellIndexEntry[] = [];
  const inflight = new Set<Promise<void>>();
  const t0 = performance.now();
  const flush = async (): Promise<void> => {
    const toWrite = batch;
    batch = [];
    const pr: Promise<void> = index.put(toWrite).finally(() => inflight.delete(pr));
    inflight.add(pr);
    if (inflight.size >= p.putConcurrency) await Promise.race(inflight);
  };
  // Devices have 1 to 3 entries, adjacent in the stream; stop only between devices.
  for (const e of generateEntries({ devices: 10_000_000, seed: p.seed, deviceIds: 'dev' })) {
    if (e.deviceId !== lastDevice) {
      if (entries >= p.entries) break;
      lastDevice = e.deviceId;
      devices++;
    }
    batch.push({ ...e, deviceId: `w${p.worker}-${e.deviceId}` });
    perCell[e.h3Cell] = (perCell[e.h3Cell] ?? 0) + 1;
    entries++;
    if (batch.length >= p.putEntries) await flush();
  }
  if (batch.length > 0) await flush();
  await Promise.all(inflight);
  return { entries, devices, perCell, ms: performance.now() - t0, stats };
}

export interface AReadParams {
  worker: number;
  /** The cells this process reads (a slice of the alert area). */
  cells: string[];
  /** The whole alert area (every cell with entries), for the primary-cell rule. */
  areaCells: string[];
  pageSize: number;
}
export interface AReadResult {
  entries: number;
  recipients: number;
  batches: number;
  skippedNoTarget: number;
  skippedNonPrimary: number;
  duplicates: number;
  perCell: Record<string, number>;
  pageLatenciesMs: number[];
  pages: number;
  ms: number;
}

const TOKEN_KEY = { id: 't4-token', secret: 't4-token-secret' };

/** A signed county-wide alert whose area is exactly `cells`. */
export function makeAlert(cells: string[]): {
  alert: Alert;
  verify: (a: Alert) => boolean;
} {
  const signing = createMemoryAlertSigning('t4-alert');
  const now = Date.now();
  const alert = signAlertSync(
    {
      id: '5f0c2a7e-1b3d-4c8e-9a6f-0d2e4b6a8c10',
      source: 'nws',
      sourceId: 'urn:oid:t4-1',
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
        description: 'Santa Clara County (test 4)',
        geometry: null,
        ugcCodes: [],
        sameCodes: [],
        h3Cells: cells,
      },
      sent: new Date(now - 60_000).toISOString(),
      effective: new Date(now - 60_000).toISOString(),
      onset: null,
      expires: new Date(now + 4 * 3600_000).toISOString(),
      ends: null,
      official: {
        headline: 'Flash Flood Warning for the county',
        description: 'Test 4 alert.',
        instruction: null,
        senderName: 'NWS',
        language: 'en-US',
      },
      summaries: [],
      signature: 'unsigned',
    },
    signing.signer,
  );
  return { alert, verify: createAlertVerifier(signing.publicKeys) };
}

export async function aRead(b: Backend, p: AReadParams): Promise<AReadResult> {
  const stats = emptyStats();
  const real = new DynamoCellIndex(instrumentDoc(b.doc, stats), b.names.cellIndex);
  const pageLatenciesMs: number[] = [];
  const perCell: Record<string, number> = {};
  const timed: Pick<CellIndex, 'listForCell'> = {
    async listForCell(cell, cursor, limit) {
      const t = performance.now();
      const page = await real.listForCell(cell, cursor, limit);
      pageLatenciesMs.push(performance.now() - t);
      perCell[cell] = (perCell[cell] ?? 0) + page.entries.length;
      return page;
    },
  };
  const { alert, verify } = makeAlert(p.areaCells);
  const sendQueue: SendQueue = { enqueue: async () => undefined };
  const t0 = performance.now();
  const r = await processShard(
    {
      alertId: alert.id,
      sourceId: alert.sourceId,
      relaySeverity: alert.relaySeverity,
      shardId: `t4-${p.worker}`,
      cells: p.cells,
    },
    {
      alerts: { get: async () => alert },
      verifyAlert: verify,
      isDeliveryPaused: async () => false,
      clock: { now: () => new Date() },
      logger: silent,
      cellIndex: timed,
      sendQueue,
      makeCheckInToken: (alertId, deviceId, h3Cell, expiresAt) =>
        createCheckInToken({ alertId, deviceId, expiresAt, h3Cell }, TOKEN_KEY),
      pageSize: p.pageSize,
    },
  );
  if (r.refused) throw new Error(`shard refused: ${r.refused}`);
  return {
    entries: r.entries,
    recipients: r.recipients,
    batches: r.batches,
    skippedNoTarget: r.skippedNoTarget,
    skippedNonPrimary: r.skippedNonPrimary,
    duplicates: r.duplicates,
    perCell,
    pageLatenciesMs,
    pages: pageLatenciesMs.length,
    ms: performance.now() - t0,
  };
}

// =====================================================================================
// (b) idempotency contention
// =====================================================================================

/** Which processes work on key k: its primary, plus a second one for `overlapPct` % of the keys. */
export function assignedWorkers(k: number, workers: number, overlapPct: number): number[] {
  const primary = k % workers;
  if (workers < 2 || hash32(`ov${k}`) % 100 >= overlapPct) return [primary];
  const second = (primary + 1 + (hash32(`w2-${k}`) % (workers - 1))) % workers;
  return [primary, second];
}

export interface BParams {
  worker: number;
  workers: number;
  keys: number;
  overlapPct: number;
  /** % of successful claims that "crash" (a third each: abandon, release, die after sending). */
  crashPct: number;
  leaseMs: number;
  concurrency: number;
  seed: number;
  /** Per-key counters are written here: keys bytes of effects, then keys bytes of post-send crashes. */
  outFile: string;
}
export interface BResult {
  assigned: number;
  claimCalls: number;
  claimed: number;
  leasedSeen: number;
  alreadySent: number;
  releases: number;
  abandoned: number;
  postSendCrashes: number;
  markSents: number;
  ms: number;
  seenMarked: number;
  seenOwned: number;
  stats: DocStats;
}

export const idempotencyKey = (k: number): string => `t4:batch:${k}`;
export const seenSourceId = (k: number): string => `t4-src-${k}`;

export async function bClaim(b: Backend, p: BParams): Promise<BResult> {
  const stats = emptyStats();
  const doc = instrumentDoc(b.doc, stats);
  const store = new DynamoIdempotencyStore(doc, b.names.idempotency);
  const rng = createRng(p.seed * 7919 + p.worker);
  const mine: number[] = [];
  for (let k = 0; k < p.keys; k++) {
    if (assignedWorkers(k, p.workers, p.overlapPct).includes(p.worker)) mine.push(k);
  }
  const effects = new Uint8Array(p.keys);
  const postCrash = new Uint8Array(p.keys);
  const third = p.crashPct / 100 / 3;
  const r = {
    claimCalls: 0,
    claimed: 0,
    leasedSeen: 0,
    alreadySent: 0,
    releases: 0,
    abandoned: 0,
    postSendCrashes: 0,
    markSents: 0,
  };
  const tries = new Uint16Array(p.keys);
  const retry: Array<{ k: number; at: number }> = [];
  let head = 0;
  let fresh = 0;
  let active = 0;
  const day = 24 * 3600_000;

  const attempt = async (k: number): Promise<void> => {
    if (++tries[k]! > 400) throw new Error(`key ${k} did not settle after 400 attempts`);
    const key = idempotencyKey(k);
    const res = await store.claim(key, new Date(Date.now() + p.leaseMs));
    r.claimCalls++;
    if (res === 'sent') {
      r.alreadySent++;
      return;
    }
    if (res === 'leased') {
      r.leasedSeen++;
      retry.push({ k, at: Date.now() + p.leaseMs + 50 });
      return;
    }
    r.claimed++;
    const roll = rng();
    if (roll < third) {
      // Crash before sending: nothing sent, the lease just runs out.
      r.abandoned++;
      retry.push({ k, at: Date.now() + p.leaseMs + 50 });
    } else if (roll < 2 * third) {
      // Retryable failure: the lease is given back at once.
      await store.release(key);
      r.releases++;
      retry.push({ k, at: Date.now() });
    } else if (roll < 3 * third) {
      // Crash after the push went out but before markSent: the one case that may send twice.
      effects[k]!++;
      postCrash[k]!++;
      r.postSendCrashes++;
      retry.push({ k, at: Date.now() + p.leaseMs + 50 });
    } else {
      effects[k]!++;
      await store.markSent(key, new Date(Date.now() + day));
      r.markSents++;
    }
  };

  const t0 = performance.now();
  const slot = async (): Promise<void> => {
    for (;;) {
      const next = retry[head];
      let k: number | undefined;
      if (next && next.at <= Date.now()) {
        k = next.k;
        head++;
      } else if (fresh < mine.length) {
        k = mine[fresh++];
      } else if (next) {
        await sleep(Math.max(1, Math.min(250, next.at - Date.now())));
        continue;
      } else if (active > 0) {
        await sleep(10);
        continue;
      } else return;
      if (k === undefined) continue;
      active++;
      try {
        await attempt(k);
      } finally {
        active--;
      }
    }
  };
  await Promise.all(Array.from({ length: p.concurrency }, slot));
  const ms = performance.now() - t0;

  // The ingest dedupe store under the same overlap: mark each assigned source id once or twice.
  const seen = new DynamoSeenStore(doc as DynamoDBDocumentClient, b.names.seen);
  let seenMarked = 0;
  let seenOwned = 0;
  const seenKeys = mine.filter((k) => k % 10 === 0);
  await pool(seenKeys.length, p.concurrency, async (i) => {
    const k = seenKeys[i]!;
    await seen.mark(seenSourceId(k), `t4-${p.worker}`);
    seenMarked++;
    if (await seen.has(seenSourceId(k))) seenOwned++;
  });

  const buf = Buffer.alloc(2 * p.keys);
  buf.set(effects, 0);
  buf.set(postCrash, p.keys);
  writeFileSync(p.outFile, buf);
  return { assigned: mine.length, ...r, ms, seenMarked, seenOwned, stats };
}

// =====================================================================================
// (c) check-ins and the aggregator
// =====================================================================================

export interface CParams {
  worker: number;
  count: number;
  alerts: string[];
  cells: string[];
  dupPct: number;
  seed: number;
  concurrency: number;
  aggBatch: number;
  aggInFlight: number;
  redeliverPct: number;
  eraseDevices: number;
}
export interface CResult {
  attempts: number;
  saved: number;
  rejected: number;
  repeatsAttempted: number;
  /** `${alertId}|${h3Cell}` -> check-ins saved. */
  perCell: Record<string, number>;
  writeMs: number;
  aggMs: number;
  aggInvocations: number;
  aggRetries: number;
  aggFailedGiveUps: number;
  redelivered: number;
  applied: number;
  alreadyCounted: number;
  erased: number;
  erasedStillPresent: number;
  eraseMs: number;
  writeStats: DocStats;
  aggStats: DocStats;
}

export async function cCheckins(b: Backend, p: CParams): Promise<CResult> {
  const rng = createRng(p.seed * 104729 + p.worker);
  const writeStats = emptyStats();
  const aggStats = emptyStats();
  const eraseStats = emptyStats();
  const store = new DynamoCheckInStore(instrumentDoc(b.doc, writeStats), b.names.checkins);
  const retainUntil = new Date(Date.now() + 40 * 24 * 3600_000);

  type Rec = { alertId: string; deviceId: string; h3Cell: string };
  const attempted: Rec[] = [];
  const saved: Rec[] = [];
  const perCell: Record<string, number> = {};
  let rejected = 0;
  let repeats = 0;
  const plan: Array<{ rec: Rec; repeat: boolean }> = [];
  for (let i = 0; i < p.count; i++) {
    if (i > 0 && rng() < p.dupPct / 100) {
      const prev = attempted[Math.floor(rng() * attempted.length)] as Rec;
      plan.push({ rec: prev, repeat: true });
      repeats++;
    } else {
      // Hot cells: squaring the draw puts most check-ins in the first few cells.
      const cell = p.cells[Math.floor(rng() * rng() * p.cells.length)] as string;
      const rec = {
        alertId: p.alerts[i % p.alerts.length] as string,
        deviceId: `c${p.worker}-${i}`,
        h3Cell: cell,
      };
      attempted.push(rec);
      plan.push({ rec, repeat: false });
    }
  }

  const t0 = performance.now();
  await pool(plan.length, p.concurrency, async (i) => {
    const { rec } = plan[i] as { rec: Rec };
    const checkIn = {
      alertId: rec.alertId,
      deviceId: rec.deviceId,
      status: 'ok',
      checkedInAt: new Date().toISOString(),
      token: 't',
      idempotencyKey: randomUUID(),
      h3Cell: rec.h3Cell,
    } as unknown as CheckIn;
    if (await store.save(checkIn, retainUntil)) {
      saved.push(rec);
      const k = `${rec.alertId}|${rec.h3Cell}`;
      perCell[k] = (perCell[k] ?? 0) + 1;
    } else rejected++;
  });
  const writeMs = performance.now() - t0;

  // ----- the aggregator over synthetic stream records (INSERT per saved check-in) -----
  const aggregate = createAggregator({
    doc: instrumentDoc(b.doc, aggStats),
    totalsTable: b.names.checkinTotals,
    sleep: (ms) => sleep(Math.min(ms, 50)),
  });
  const batches: DynamoDBRecord[][] = [];
  let seq = 0;
  for (let i = 0; i < saved.length; i += p.aggBatch) {
    batches.push(
      saved.slice(i, i + p.aggBatch).map((rec): DynamoDBRecord => {
        const n = ++seq;
        return {
          eventID: `t4-${p.worker}-${n}`,
          eventName: 'INSERT',
          dynamodb: {
            SequenceNumber: String(n),
            NewImage: marshall({
              pk: `${rec.alertId}#${rec.deviceId}`,
              alertId: rec.alertId,
              deviceId: rec.deviceId,
              h3Cell: rec.h3Cell,
            }) as never,
          },
        };
      }),
    );
  }
  let aggInvocations = 0;
  let aggRetries = 0;
  let giveUps = 0;
  let redelivered = 0;
  let applied = 0;
  let alreadyCounted = 0;
  // The aggregator logs one JSON line per invocation; capture the counts and keep the console quiet.
  const origLog = console.log;
  console.log = (line?: unknown): void => {
    if (typeof line !== 'string') return;
    try {
      const o = JSON.parse(line) as { msg?: string; applied?: number; alreadyCounted?: number };
      if (o.msg === 'checkin aggregation') {
        applied += o.applied ?? 0;
        alreadyCounted += o.alreadyCounted ?? 0;
      }
    } catch {
      /* not ours */
    }
  };
  const t1 = performance.now();
  try {
    await pool(batches.length, p.aggInFlight, async (i) => {
      let records = batches[i] as DynamoDBRecord[];
      const deliveries = rng() < p.redeliverPct / 100 ? 2 : 1;
      if (deliveries === 2) redelivered++;
      for (let d = 0; d < deliveries; d++) {
        let current = records;
        for (let attempt = 0; current.length > 0; attempt++) {
          aggInvocations++;
          const out = await aggregate({ Records: current } as DynamoDBStreamEvent);
          const failed = new Set(out.batchItemFailures.map((f) => f.itemIdentifier));
          if (failed.size === 0) break;
          if (attempt >= 30) {
            giveUps++;
            break;
          }
          aggRetries++;
          current = current.filter((r) => failed.has(r.dynamodb?.SequenceNumber ?? ''));
          await sleep(20 + Math.floor(rng() * 40));
        }
        records = batches[i] as DynamoDBRecord[]; // a redelivery carries the whole batch again
      }
    });
  } finally {
    console.log = origLog;
  }
  const aggMs = performance.now() - t1;

  // ----- erasure through the byDevice index -----
  const eraseStore = new DynamoCheckInStore(instrumentDoc(b.doc, eraseStats), b.names.checkins);
  const victims = saved.slice(0, p.eraseDevices);
  let erased = 0;
  const t2 = performance.now();
  await pool(victims.length, 8, async (i) => {
    const n = await eraseStore.deleteForDevice((victims[i] as Rec).deviceId);
    erased += n;
  });
  const eraseMs = performance.now() - t2;
  let erasedStillPresent = 0;
  for (const v of victims) {
    if (await eraseStore.get(v.alertId, v.deviceId)) erasedStillPresent++;
  }

  return {
    attempts: plan.length,
    saved: saved.length,
    rejected,
    repeatsAttempted: repeats,
    perCell,
    writeMs,
    aggMs,
    aggInvocations,
    aggRetries,
    aggFailedGiveUps: giveUps,
    redelivered,
    applied,
    alreadyCounted,
    erased,
    erasedStillPresent,
    eraseMs,
    writeStats,
    aggStats,
  };
}

// =====================================================================================
// (d) Postgres: place adds with the outbox, drainer, erasure
// =====================================================================================

export interface DAddParams {
  worker: number;
  /** Stop (at a device boundary) once this many places were added. */
  places: number;
  seed: number;
  /** % of write-throughs that fail (the outbox row is then left for the drainer). */
  failPct: number;
  concurrency: number;
}
export interface DAddResult {
  devices: number;
  places: number;
  injectedFailures: number;
  unexpectedErrors: number;
  addLatenciesMs: number[];
  ms: number;
}

class InjectedFailure extends Error {
  constructor() {
    super('injected cell-index write-through failure');
    this.name = 'InjectedFailure';
  }
}

/** True for the places whose write-through is made to fail (deterministic, `failPct` percent). */
export const failsWriteThrough = (placeId: string, failPct: number): boolean =>
  hash32(`ft${placeId}`) % 10_000 < failPct * 100;

export async function dAdd(b: Backend, p: DAddParams): Promise<DAddResult> {
  const db = b.sql('relay_api');
  const devices = new PostgresDeviceStore(db);
  const places = new PostgresPlaceStore(db);
  const outbox = new PostgresCellIndexOutbox(db);
  const real = new DynamoCellIndex(b.doc, b.names.cellIndexD);

  const list: Array<ReturnType<typeof generateDevices> extends Generator<infer T> ? T : never> = [];
  let placeCount = 0;
  for (const d of generateDevices({ devices: 10_000_000, seed: p.seed, deviceIds: 'uuid' })) {
    if (placeCount >= p.places) break;
    list.push(d);
    placeCount += d.places.length;
  }

  const addLatenciesMs: number[] = [];
  let injected = 0;
  let unexpected = 0;
  let placesAdded = 0;
  const t0 = performance.now();
  await pool(list.length, p.concurrency, async (i) => {
    const d = list[i]!;
    const deviceNo = p.worker * 10_000_000 + i;
    const deviceId = uuidForDevice(deviceNo);
    const device: Device = {
      id: deviceId,
      credentialHash: createHash('sha256').update(deviceId).digest('hex'),
      expoPushToken: null,
      pushPlatform: d.target.platform,
      pushToken: d.target.token,
      pushProject: null,
      language: d.language,
      createdAt: new Date().toISOString(),
    };
    await devices.create(device);
    for (let n = 0; n < d.places.length; n++) {
      const sp = d.places[n]!;
      const place: Place = {
        id: `00000000-0000-4000-9000-${hex12(deviceNo * 4 + n)}`,
        deviceId,
        label: n === 0 ? 'Home' : 'Work',
        h3Cell: sp.h3Cell,
        createdAt: new Date().toISOString(),
      };
      const outboxId = randomUUID();
      const t = performance.now();
      // The place and its outbox row commit together (the real store, one transaction).
      await places.add(place, {
        outbox: { id: outboxId, op: 'put', h3Cell: place.h3Cell, placeId: place.id },
      });
      addLatenciesMs.push(performance.now() - t);
      placesAdded++;
      // Write-through exactly as the API handler does it: syncDeviceCells, then drop the outbox row.
      const failing: Pick<CellIndex, 'put' | 'remove'> = {
        put: async (entries) => {
          if (failsWriteThrough(place.id, p.failPct)) throw new InjectedFailure();
          await real.put(entries);
        },
        remove: (cell, dev, pl) => real.remove(cell, dev, pl),
      };
      try {
        await syncDeviceCells(failing, {
          deviceId,
          added: { placeId: place.id },
          load: async () => {
            const current = await devices.get(deviceId);
            if (!current) return null;
            const ps = await places.listForDevice(deviceId);
            return {
              target: targetOf(current),
              language: current.language,
              places: ps.map((x) => ({ id: x.id, h3Cell: x.h3Cell })),
            };
          },
        });
        try {
          await outbox.remove([outboxId]);
        } catch {
          /* the drainer deletes it */
        }
      } catch (e) {
        if (e instanceof InjectedFailure) injected++;
        else unexpected++;
      }
    }
  });
  return {
    devices: list.length,
    places: placesAdded,
    injectedFailures: injected,
    unexpectedErrors: unexpected,
    addLatenciesMs,
    ms: performance.now() - t0,
  };
}

export interface DDrainParams {
  /** Safety stop. */
  maxRuns: number;
}
export interface DDrainResult {
  pendingBefore: number;
  pendingAfter: number;
  runs: number;
  claimed: number;
  applied: number;
  failed: number;
  maxAttemptsSeen: number;
  ms: number;
}

/** One drainer, back to back (the real one runs once a minute and takes 100 rows per run). */
export async function dDrain(b: Backend, p: DDrainParams): Promise<DDrainResult> {
  const db = b.sql('relay_ops');
  const outbox = new PostgresCellIndexOutbox(db);
  const deps = {
    devices: new PostgresDeviceStore(db),
    places: new PostgresPlaceStore(db),
    cellIndex: new DynamoCellIndex(b.doc, b.names.cellIndexD),
    outbox,
    // Rows must be 30 s old before the drainer touches them; jump the clock instead of waiting.
    now: () => new Date(Date.now() + 120_000),
    log: () => undefined,
    metric: () => undefined,
  };
  const before = await outbox.stats(deps.now());
  let runs = 0;
  let claimed = 0;
  let applied = 0;
  let failed = 0;
  let maxAttemptsSeen = 0;
  let stalled = 0;
  const t0 = performance.now();
  while (runs < p.maxRuns) {
    const stats = await outbox.stats(deps.now());
    if (stats.pending === 0) break;
    const r = await drainOutbox(deps);
    runs++;
    claimed += r.claimed;
    applied += r.applied;
    failed += r.failed;
    maxAttemptsSeen = Math.max(maxAttemptsSeen, r.maxAttempts);
    stalled = r.applied === 0 ? stalled + 1 : 0;
    if (stalled >= 3) break;
  }
  const after = await outbox.stats(deps.now());
  return {
    pendingBefore: before.pending,
    pendingAfter: after.pending,
    runs,
    claimed,
    applied,
    failed,
    maxAttemptsSeen,
    ms: performance.now() - t0,
  };
}

export interface DEraseParams {
  deviceIds: string[];
  alertId: string;
  concurrency: number;
}
export interface DEraseResult {
  erased: number;
  placesRemoved: number;
  checkinRowsDeleted: number;
  latenciesMs: number[];
  ms: number;
}

/** DELETE /devices/me as the handler runs it: index entries, places, device + tombstone, check-ins. */
export async function dErase(b: Backend, p: DEraseParams): Promise<DEraseResult> {
  const db = b.sql('relay_api');
  const devices = new PostgresDeviceStore(db);
  const places = new PostgresPlaceStore(db);
  const index = new DynamoCellIndex(b.doc, b.names.cellIndexD);
  const checkins = new DynamoCheckInStore(b.doc, b.names.checkins);
  // Give every device a check-in first so the byDevice erasure has rows to find.
  await pool(p.deviceIds.length, p.concurrency, async (i) => {
    await checkins.save(
      {
        alertId: p.alertId,
        deviceId: p.deviceIds[i] as string,
        status: 'ok',
        checkedInAt: new Date().toISOString(),
        token: 't',
        idempotencyKey: randomUUID(),
        h3Cell: '872830828ffffff',
      } as unknown as CheckIn,
      new Date(Date.now() + 40 * 24 * 3600_000),
    );
  });
  const latenciesMs: number[] = [];
  let erased = 0;
  let placesRemoved = 0;
  let rows = 0;
  const t0 = performance.now();
  await pool(p.deviceIds.length, p.concurrency, async (i) => {
    const deviceId = p.deviceIds[i] as string;
    const t = performance.now();
    const ps = await places.listForDevice(deviceId);
    for (const place of ps) await index.remove(place.h3Cell, deviceId, place.id);
    const removed = await places.removeAllForDevice(deviceId);
    placesRemoved += removed;
    if (await devices.deleteDevice(deviceId, new Date())) erased++;
    const deletedRows = await checkins.deleteForDevice(deviceId);
    rows += deletedRows;
    latenciesMs.push(performance.now() - t);
  });
  return {
    erased,
    placesRemoved,
    checkinRowsDeleted: rows,
    latenciesMs,
    ms: performance.now() - t0,
  };
}

// =====================================================================================
// task table
// =====================================================================================

export type TaskName =
  'a-write' | 'a-read' | 'b-claim' | 'c-checkins' | 'd-add' | 'd-drain' | 'd-erase';

export const KERNELS: Record<TaskName, (b: Backend, params: never) => Promise<unknown>> = {
  'a-write': aWrite as never,
  'a-read': aRead as never,
  'b-claim': bClaim as never,
  'c-checkins': cCheckins as never,
  'd-add': dAdd as never,
  'd-drain': dDrain as never,
  'd-erase': dErase as never,
};

export function scratchFile(b: Backend, name: string): string {
  return join(b.scratchDir, name);
}
