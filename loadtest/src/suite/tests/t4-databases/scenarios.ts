/**
 * Orchestration of test 4: splits each scenario over worker processes (a TaskRunner), merges the
 * results and checks correctness against the databases directly. Runs unchanged over real
 * containers (ProcessRunner + RealBackend) and, in the vitest smoke test, in-process over a fake
 * document client and PGlite (InProcessRunner).
 */
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { ScanCommand } from '@aws-sdk/lib-dynamodb';
import { DynamoCellIndex } from '../../../../../backend/src/scale/cell-index/dynamo-cell-index.js';
import type { SqlExecutor } from '../../../../../backend/src/db/sql.js';
import { targetOf } from '../../../../../backend/src/scale/cell-index/util.js';
import { DynamoCheckInTotals } from '../../../../../backend/src/scale/checkins/totals.js';
import { spawnWorker, type ChildHandle } from '../../lib/child.js';
import type { RssSampler } from '../../lib/rss.js';
import type { Metric, ScenarioResult } from '../../types.js';
import { mergeStats, percentile, sumRecord } from './instrument.js';
import {
  KERNELS,
  idempotencyKey,
  pool,
  type AReadParams,
  type AReadResult,
  type AWriteParams,
  type AWriteResult,
  type BParams,
  type BResult,
  type Backend,
  type CParams,
  type CResult,
  type DAddParams,
  type DAddResult,
  type DDrainParams,
  type DDrainResult,
  type DEraseParams,
  type DEraseResult,
  type TaskName,
} from './kernels.js';
import {
  grantSnapshot,
  rerunMigration,
  runMigrationsTimed,
  PgExecutor,
  type MigrationTiming,
} from './pg-executor.js';

export const MIGRATIONS_DIR = fileURLToPath(
  new URL('../../../../../backend/migrations', import.meta.url),
);

// ---------- runners ----------

export interface TaskRunner {
  run<R>(task: TaskName, params: object[]): Promise<R[]>;
}

export interface ProcessRunnerOptions {
  dynamoEndpoint: string;
  postgresUrl: string;
  names: Backend['names'];
  scratchDir: string;
  heapMb: number;
  rss: RssSampler;
}

/** One child process per params entry (child.ts); results come back over IPC. */
export class ProcessRunner implements TaskRunner {
  private seq = 0;
  peakChildRssMb = 0;
  constructor(private readonly o: ProcessRunnerOptions) {}

  async run<R>(task: TaskName, params: object[]): Promise<R[]> {
    const file = fileURLToPath(new URL('./worker.ts', import.meta.url));
    const handles: ChildHandle[] = [];
    try {
      const run = ++this.seq;
      params.forEach((p, i) => {
        const pf = join(this.o.scratchDir, `params-${run}-${task}-${i}.json`);
        writeFileSync(pf, JSON.stringify(p));
        const h = spawnWorker(
          file,
          ['--task', task, '--params', pf, '--scratch', this.o.scratchDir],
          {
            heapMb: this.o.heapMb,
            env: {
              RELAY_T4_DYNAMO: this.o.dynamoEndpoint,
              RELAY_T4_PG: this.o.postgresUrl,
              RELAY_T4_NAMES: JSON.stringify(this.o.names),
            },
          },
        );
        this.o.rss.addSource(() => h.rssNow);
        handles.push(h);
      });
      const out = (await Promise.all(handles.map((h) => h.result))) as R[];
      return out;
    } finally {
      for (const h of handles) {
        this.peakChildRssMb = Math.max(this.peakChildRssMb, h.rssPeak);
        h.kill();
      }
    }
  }
}

/** Runs the kernels in this process (smoke test over fakes). Concurrent, like separate processes. */
export class InProcessRunner implements TaskRunner {
  constructor(private readonly backend: Backend) {}
  async run<R>(task: TaskName, params: object[]): Promise<R[]> {
    const kernel = KERNELS[task];
    const out = await Promise.all(
      params.map((p) => kernel(this.backend, JSON.parse(JSON.stringify(p)) as never)),
    );
    return JSON.parse(JSON.stringify(out)) as R[];
  }
}

// ---------- environment and sizes ----------

export interface Sizes {
  a: { entries: number; putEntries: number; putConcurrency: number; pageSize: number };
  b: {
    keys: number;
    overlapPct: number;
    crashPct: number;
    leaseMs: number;
    concurrency: number;
  };
  c: {
    count: number;
    alerts: number;
    cells: number;
    dupPct: number;
    concurrency: number;
    aggBatch: number;
    aggInFlight: number;
    redeliverPct: number;
    eraseDevices: number;
  };
  d: { places: number; failPct: number; concurrency: number; erase: number };
}

export const FULL_SIZES: Sizes = {
  a: { entries: 1_000_000, putEntries: 1000, putConcurrency: 1, pageSize: 1000 },
  b: { keys: 200_000, overlapPct: 20, crashPct: 3, leaseMs: 5000, concurrency: 24 },
  c: {
    count: 500_000,
    alerts: 4,
    cells: 2000,
    dupPct: 2,
    concurrency: 48,
    aggBatch: 100,
    aggInFlight: 4,
    redeliverPct: 5,
    eraseDevices: 25,
  },
  d: { places: 100_000, failPct: 5, concurrency: 8, erase: 1000 },
};

export const SMOKE_SIZES: Sizes = {
  a: { entries: 2000, putEntries: 100, putConcurrency: 2, pageSize: 200 },
  b: { keys: 2000, overlapPct: 20, crashPct: 3, leaseMs: 800, concurrency: 8 },
  c: {
    count: 5000,
    alerts: 2,
    cells: 50,
    dupPct: 2,
    concurrency: 16,
    aggBatch: 100,
    aggInFlight: 4,
    redeliverPct: 5,
    eraseDevices: 10,
  },
  d: { places: 1000, failPct: 5, concurrency: 8, erase: 50 },
};

export interface Env {
  backend: Backend;
  /** Superuser SQL executor (migrations, verification). */
  admin: SqlExecutor;
  runner: TaskRunner;
  workers: number;
  sizes: Sizes;
  log(msg: string): void;
  /** True with a real Postgres: roles exist and SET ROLE is enforced. False on PGlite. */
  realRoles: boolean;
  /** Skip migration 0001 (PostGIS), for PGlite. */
  skipMigration?: (name: string) => boolean;
  /** Connection string of the Postgres database (for the role checks); real runs only. */
  postgresUrl?: string;
}

const sec = (ms: number): number => ms / 1000;
const rate = (n: number, ms: number): number => (ms > 0 ? n / sec(ms) : 0);
const sum = (xs: number[]): number => xs.reduce((a, b) => a + b, 0);
const ok = (name: string, pass: boolean, detail?: string) => ({
  name,
  ok: pass,
  ...(detail !== undefined ? { detail } : {}),
});

function finish(
  name: string,
  t0: number,
  metrics: Metric[],
  checks: ScenarioResult['checks'],
  notes: string[] = [],
): ScenarioResult {
  return {
    name,
    verdict: checks.every((c) => c.ok) ? 'pass' : 'fail',
    durationMs: Date.now() - t0,
    metrics,
    checks,
    notes,
  };
}

async function scan(
  b: Backend,
  table: string,
  onItems: (items: Array<Record<string, unknown>>) => void,
): Promise<void> {
  let start: Record<string, unknown> | undefined;
  do {
    const out = (await b.doc.send(
      new ScanCommand({
        TableName: table,
        Limit: 1000,
        ConsistentRead: true,
        ...(start ? { ExclusiveStartKey: start } : {}),
      }),
    )) as { Items?: Array<Record<string, unknown>>; LastEvaluatedKey?: Record<string, unknown> };
    onItems(out.Items ?? []);
    start = out.LastEvaluatedKey;
  } while (start);
}

async function scanCount(b: Backend, table: string): Promise<number> {
  let n = 0;
  let start: Record<string, unknown> | undefined;
  do {
    const out = (await b.doc.send(
      new ScanCommand({
        TableName: table,
        Select: 'COUNT',
        ConsistentRead: true,
        ...(start ? { ExclusiveStartKey: start } : {}),
      }),
    )) as { Count?: number; LastEvaluatedKey?: Record<string, unknown> };
    n += out.Count ?? 0;
    start = out.LastEvaluatedKey;
  } while (start);
  return n;
}

// ---------- setup: migrations and roles ----------

export async function setupPostgres(env: Env): Promise<ScenarioResult> {
  const t0 = Date.now();
  const metrics: Metric[] = [];
  const checks: ScenarioResult['checks'] = [];
  const notes: string[] = [];
  const timings: MigrationTiming[] = await runMigrationsTimed(env.admin, MIGRATIONS_DIR, {
    ...(env.skipMigration ? { skip: env.skipMigration } : {}),
  });
  for (const t of timings) metrics.push({ name: `migration ${t.name}`, value: t.ms, unit: 'ms' });
  metrics.push({
    name: 'all migrations',
    value: sum(timings.map((t) => t.ms)) / 1000,
    unit: 's',
  });
  const applied = (
    await env.admin.query<{ name: string }>('SELECT name FROM schema_migrations ORDER BY name', [])
  ).map((r) => r.name);
  const wanted = Array.from({ length: 13 }, (_, i) => String(i + 1).padStart(4, '0'));
  const missing = wanted.filter(
    (n) =>
      !applied.some((a) => a.startsWith(`${n}_`)) &&
      !(env.skipMigration && env.skipMigration(`${n}_x`)),
  );
  checks.push(
    ok('migrations 0001..0013 applied in order', missing.length === 0, missing.join(',')),
  );
  if (env.skipMigration) notes.push('Migration 0001 (PostGIS) skipped: this Postgres is PGlite.');

  const before = await grantSnapshot(env.admin);
  const rerunMs = await rerunMigration(env.admin, MIGRATIONS_DIR, '0011_roles.sql');
  const after = await grantSnapshot(env.admin);
  metrics.push({ name: 're-run of 0011_roles.sql (grants)', value: rerunMs, unit: 'ms' });
  checks.push(
    ok(
      '0011 is idempotent (grants identical after a re-run)',
      before.length > 0 && JSON.stringify(before) === JSON.stringify(after),
      `${before.length} grants`,
    ),
  );

  if (env.realRoles && env.postgresUrl) {
    const asRole = async (role: string, sql: string): Promise<string> => {
      const ex = new PgExecutor(env.postgresUrl as string, { role, max: 1 });
      try {
        await ex.query(sql, []);
        return 'allowed';
      } catch (e) {
        return (e as { code?: string }).code ?? 'error';
      } finally {
        await ex.close();
      }
    };
    const who = new PgExecutor(env.postgresUrl, { role: 'relay_api', max: 1 });
    try {
      const r = await who.query<{ u: string }>('SELECT current_user AS u', []);
      checks.push(ok('SET ROLE takes effect (current_user = relay_api)', r[0]?.u === 'relay_api'));
    } finally {
      await who.close();
    }
    checks.push(
      ok(
        'relay_fanout cannot write places (permission denied)',
        (await asRole('relay_fanout', 'INSERT INTO places (id) VALUES (gen_random_uuid())')) ===
          '42501',
      ),
    );
    checks.push(
      ok(
        'relay_api cannot write alerts (permission denied)',
        (await asRole('relay_api', 'INSERT INTO alerts DEFAULT VALUES')) === '42501',
      ),
    );
    checks.push(
      ok(
        'relay_ops can read devices',
        (await asRole('relay_ops', 'SELECT 1 FROM devices LIMIT 1')) === 'allowed',
      ),
    );
  } else {
    notes.push('Role enforcement checks skipped (no real Postgres roles in this run).');
  }
  return finish('Setup: migrations and roles', t0, metrics, checks, notes);
}

// ---------- (a) cell index ----------

export async function scenarioA(env: Env): Promise<ScenarioResult> {
  const t0 = Date.now();
  const s = env.sizes.a;
  const W = env.workers;
  env.log(`(a) writing ~${s.entries} cell-index entries from ${W} processes`);
  const wParams: AWriteParams[] = Array.from({ length: W }, (_, w) => ({
    worker: w,
    entries: Math.ceil(s.entries / W),
    seed: 1000 + w,
    putEntries: s.putEntries,
    putConcurrency: s.putConcurrency,
  }));
  const tw = performance.now();
  const wr = await env.runner.run<AWriteResult>('a-write', wParams);
  const writeWallMs = performance.now() - tw;
  const entries = sum(wr.map((r) => r.entries));
  const devices = sum(wr.map((r) => r.devices));
  const written = new Map<string, number>();
  for (const r of wr)
    for (const [c, n] of Object.entries(r.perCell)) written.set(c, (written.get(c) ?? 0) + n);
  const writeStats = mergeStats(wr.map((r) => r.stats));
  env.log(
    `(a) wrote ${entries} entries in ${sec(writeWallMs).toFixed(1)} s; ${written.size} cells`,
  );

  // Spread the cells over the readers so each gets about the same number of entries.
  const cells = [...written.entries()].sort((x, y) => y[1] - x[1]);
  const load = new Array<number>(W).fill(0);
  const slices: string[][] = Array.from({ length: W }, () => []);
  for (const [cell, n] of cells) {
    let k = 0;
    for (let i = 1; i < W; i++) if ((load[i] as number) < (load[k] as number)) k = i;
    (slices[k] as string[]).push(cell);
    load[k] = (load[k] as number) + n;
  }
  const areaCells = cells.map(([c]) => c);
  const rParams: AReadParams[] = slices.map((c, worker) => ({
    worker,
    cells: c,
    areaCells,
    pageSize: s.pageSize,
  }));
  const tr = performance.now();
  const rr = await env.runner.run<AReadResult>('a-read', rParams);
  const readWallMs = performance.now() - tr;
  const readEntries = sum(rr.map((r) => r.entries));
  const recipients = sum(rr.map((r) => r.recipients));
  const lat = rr.flatMap((r) => r.pageLatenciesMs);
  const readPerCell = new Map<string, number>();
  for (const r of rr)
    for (const [c, n] of Object.entries(r.perCell))
      readPerCell.set(c, (readPerCell.get(c) ?? 0) + n);
  let mismatched = 0;
  for (const [c, n] of written) if (readPerCell.get(c) !== n) mismatched++;
  for (const c of readPerCell.keys()) if (!written.has(c)) mismatched++;
  const skippedNonPrimary = sum(rr.map((r) => r.skippedNonPrimary));
  const dupSameCell = sum(rr.map((r) => r.duplicates));
  const noTarget = sum(rr.map((r) => r.skippedNoTarget));

  const metrics: Metric[] = [
    { name: 'entries written', value: entries, unit: 'entries' },
    { name: 'devices written', value: devices, unit: 'devices' },
    { name: 'cells', value: written.size, unit: 'cells' },
    { name: 'write wall time (DynamoDB Local)', value: sec(writeWallMs), unit: 's' },
    {
      name: 'entries written/s (DynamoDB Local)',
      value: rate(entries, writeWallMs),
      unit: 'entries/s',
    },
    {
      name: 'BatchWriteItem calls',
      value: writeStats.commands['BatchWriteCommand'] ?? 0,
      unit: 'calls',
    },
    { name: 'read wall time (DynamoDB Local)', value: sec(readWallMs), unit: 's' },
    {
      name: 'entries read/s through processShard (DynamoDB Local)',
      value: rate(readEntries, readWallMs),
      unit: 'entries/s',
    },
    { name: 'listForCell page latency p50', value: percentile(lat, 50), unit: 'ms' },
    { name: 'listForCell page latency p99', value: percentile(lat, 99), unit: 'ms' },
    { name: 'listForCell pages', value: lat.length, unit: 'pages' },
    { name: 'recipients planned', value: recipients, unit: 'devices' },
  ];
  const checks = [
    ok('no DynamoDB errors while writing', writeStats.errors === 0, `${writeStats.errors} errors`),
    ok('entries read == entries written', readEntries === entries, `${readEntries} vs ${entries}`),
    ok(
      'per-cell counts identical (written vs read)',
      mismatched === 0,
      `${mismatched} cells differ`,
    ),
    ok(
      'each device is a recipient exactly once (primary-cell rule)',
      recipients === devices,
      `${recipients} vs ${devices}`,
    ),
    ok(
      'entries == recipients + non-primary + same-cell duplicates + no-target',
      entries === recipients + skippedNonPrimary + dupSameCell + noTarget,
      `${entries} vs ${recipients + skippedNonPrimary + dupSameCell + noTarget}`,
    ),
  ];
  return finish('(a) Cell index: bulk write and shard read', t0, metrics, checks, [
    'Page latency is per listForCell call (1,000 entries) with all reader processes, 16 cells each, querying at once.',
  ]);
}

// ---------- (b) idempotency contention ----------

export async function scenarioB(env: Env): Promise<ScenarioResult> {
  const t0 = Date.now();
  const s = env.sizes.b;
  const P = Math.max(2, env.workers);
  env.log(
    `(b) ${s.keys} batch keys over ${P} sender processes, ${s.overlapPct}% overlap, ${s.crashPct}% crashes`,
  );
  const files = Array.from({ length: P }, (_, w) => join(env.backend.scratchDir, `b-${w}.bin`));
  const params: BParams[] = files.map((outFile, worker) => ({
    worker,
    workers: P,
    keys: s.keys,
    overlapPct: s.overlapPct,
    crashPct: s.crashPct,
    leaseMs: s.leaseMs,
    concurrency: s.concurrency,
    seed: 4,
    outFile,
  }));
  const tw = performance.now();
  const res = await env.runner.run<BResult>('b-claim', params);
  const wallMs = performance.now() - tw;
  const stats = mergeStats(res.map((r) => r.stats));
  const total = (f: (r: BResult) => number): number => sum(res.map(f));

  // Final state of the table, key by key.
  const state = new Uint8Array(s.keys); // 0 missing, 1 sent, 2 leased
  let other = 0;
  await scan(env.backend, env.backend.names.idempotency, (items) => {
    for (const it of items) {
      const pk = String(it['pk']);
      const k = Number(pk.slice(idempotencyKey(0).length - 1));
      if (!Number.isInteger(k) || k < 0 || k >= s.keys) {
        other++;
        continue;
      }
      state[k] =
        it['state'] === 'leased' ? 2 : it['state'] === 'sent' || it['state'] === undefined ? 1 : 0;
    }
  });
  let sent = 0;
  let leased = 0;
  let missing = 0;
  for (let k = 0; k < s.keys; k++) {
    if (state[k] === 1) sent++;
    else if (state[k] === 2) leased++;
    else missing++;
  }
  // Effects (simulated pushes) per key against the crashes that explain a repeat.
  const effects = new Uint16Array(s.keys);
  const crashes = new Uint16Array(s.keys);
  for (const f of files) {
    const buf = readFileSync(f);
    for (let k = 0; k < s.keys; k++) {
      effects[k] = (effects[k] as number) + (buf[k] as number);
      crashes[k] = (crashes[k] as number) + (buf[s.keys + k] as number);
    }
  }
  let neverSent = 0;
  let unexplained = 0;
  let duplicateSends = 0;
  for (let k = 0; k < s.keys; k++) {
    const e = effects[k] as number;
    if (e === 0) neverSent++;
    if (e > 1) duplicateSends += e - 1;
    if (e !== (crashes[k] as number) + 1) unexplained++;
  }
  const seenExpected = Math.ceil(s.keys / 10);
  const seenCount = await scanCount(env.backend, env.backend.names.seen);

  const writeCalls =
    (stats.commands['PutCommand'] ?? 0) +
    (stats.commands['UpdateCommand'] ?? 0) +
    (stats.commands['DeleteCommand'] ?? 0);
  const ccf = sumRecord(stats.conditionalFailures);
  const claimCalls = total((r) => r.claimCalls);
  const metrics: Metric[] = [
    { name: 'batch keys', value: s.keys, unit: 'keys' },
    { name: 'sender processes', value: P, unit: 'processes' },
    { name: 'wall time (DynamoDB Local)', value: sec(wallMs), unit: 's' },
    { name: 'claim() calls/s (DynamoDB Local)', value: rate(claimCalls, wallMs), unit: 'claims/s' },
    {
      name: 'all DynamoDB calls/s (DynamoDB Local)',
      value: rate(sumRecord(stats.commands), wallMs),
      unit: 'calls/s',
    },
    { name: 'claim() calls', value: claimCalls, unit: 'calls' },
    { name: 'claimed', value: total((r) => r.claimed), unit: 'claims' },
    { name: 'lost to a live lease', value: total((r) => r.leasedSeen), unit: 'claims' },
    { name: 'found already sent', value: total((r) => r.alreadySent), unit: 'claims' },
    { name: 'conditional-check failures', value: ccf, unit: 'calls' },
    {
      name: 'conditional-check failure rate (of writes)',
      value: writeCalls > 0 ? (100 * ccf) / writeCalls : 0,
      unit: '%',
    },
    {
      name: 'simulated crashes before sending (lease expiry)',
      value: total((r) => r.abandoned),
      unit: 'crashes',
    },
    { name: 'released leases', value: total((r) => r.releases), unit: 'releases' },
    {
      name: 'crashes after sending, before markSent',
      value: total((r) => r.postSendCrashes),
      unit: 'crashes',
    },
    {
      name: 'sends beyond the first (all explained by a crash after sending)',
      value: duplicateSends,
      unit: 'sends',
    },
    {
      name: 'sent keys in the table',
      value: sent,
      unit: 'keys',
      target: { op: '==', value: s.keys },
    },
  ];
  const checks = [
    ok('no DynamoDB errors', stats.errors === 0, `${stats.errors} errors`),
    ok(
      `exactly one 'sent' item per key (${s.keys} keys)`,
      sent === s.keys && missing === 0 && other === 0,
      `sent ${sent}, missing ${missing}, other ${other}`,
    ),
    ok('no lease left over', leased === 0, `${leased} leased`),
    ok('every key was pushed at least once', neverSent === 0, `${neverSent} never pushed`),
    ok(
      'a key is pushed twice only after a crash between push and markSent',
      unexplained === 0,
      `${unexplained} keys with unexplained sends`,
    ),
    ok(
      'seen store: one row per source id under overlap',
      seenCount === seenExpected,
      `${seenCount} vs ${seenExpected}`,
    ),
  ];
  return finish('(b) Idempotency contention', t0, metrics, checks, [
    'Overlap: that share of the keys is given to two processes at once; the loser of a claim retries after the lease would have expired, as an SQS redelivery would.',
    `Crashes: ${s.crashPct}% of won claims, split evenly into abandon (lease expires), release, and die after the push but before markSent (the one case that legitimately sends twice).`,
  ]);
}

// ---------- (c) check-ins ----------

export async function scenarioC(env: Env): Promise<ScenarioResult> {
  const t0 = Date.now();
  const s = env.sizes.c;
  const W = env.workers;
  env.log(`(c) ${s.count} check-ins from ${W} processes, then the aggregator`);
  const alerts = Array.from(
    { length: s.alerts },
    (_, i) => `00000000-0000-4000-a000-${(i + 1).toString(16).padStart(12, '0')}`,
  );
  // Real H3 cells are not needed here: the aggregator and the store treat the cell as a string.
  const cells = Array.from(
    { length: s.cells },
    (_, i) => `87283${(0x10000 + i).toString(16)}ffffff`,
  );
  const params: CParams[] = Array.from({ length: W }, (_, worker) => ({
    worker,
    count: Math.ceil(s.count / W),
    alerts,
    cells,
    dupPct: s.dupPct,
    seed: 9,
    concurrency: s.concurrency,
    aggBatch: s.aggBatch,
    aggInFlight: s.aggInFlight,
    redeliverPct: s.redeliverPct,
    eraseDevices: s.eraseDevices,
  }));
  const tw = performance.now();
  const res = await env.runner.run<CResult>('c-checkins', params);
  const wallMs = performance.now() - tw;
  const tot = (f: (r: CResult) => number): number => sum(res.map(f));
  const saved = tot((r) => r.saved);
  const attempts = tot((r) => r.attempts);
  const repeats = tot((r) => r.repeatsAttempted);
  const erased = tot((r) => r.erased);
  const writeStats = mergeStats(res.map((r) => r.writeStats));
  const aggStats = mergeStats(res.map((r) => r.aggStats));

  const expected = new Map<string, number>();
  for (const r of res)
    for (const [k, n] of Object.entries(r.perCell)) expected.set(k, (expected.get(k) ?? 0) + n);

  // Check-in rows left: saved minus erased (each erased device had exactly one row).
  const rows = await scanCount(env.backend, env.backend.names.checkins);

  // Totals through the real reader (byAlert index), retried briefly in case of index lag.
  const reader = new DynamoCheckInTotals(env.backend.doc, env.backend.names.checkinTotals);
  let mismatched = 0;
  let totalsSum = 0;
  let cellsSeen = 0;
  for (let attempt = 0; attempt < 10; attempt++) {
    mismatched = 0;
    totalsSum = 0;
    cellsSeen = 0;
    const seen = new Set<string>();
    for (const alertId of alerts) {
      let cursor: string | undefined;
      do {
        const page = await reader.listTotals(alertId, cursor);
        for (const t of page.totals) {
          const k = `${t.alertId}|${t.h3Cell}`;
          seen.add(k);
          totalsSum += t.checkedIn;
          if (expected.get(k) !== t.checkedIn) mismatched++;
        }
        cursor = page.cursor;
      } while (cursor);
    }
    for (const k of expected.keys()) if (!seen.has(k)) mismatched++;
    cellsSeen = seen.size;
    if (mismatched === 0) break;
    await new Promise((r) => setTimeout(r, 500));
  }

  const putLat = writeStats.latenciesMs;
  const writeRate = sum(res.map((r) => rate(r.attempts, r.writeMs)));
  const aggRate = sum(res.map((r) => rate(r.saved, r.aggMs)));
  const metrics: Metric[] = [
    { name: 'check-in writes attempted', value: attempts, unit: 'writes' },
    { name: 'check-ins stored', value: saved, unit: 'check-ins' },
    {
      name: 'repeat check-ins rejected by the condition',
      value: tot((r) => r.rejected),
      unit: 'writes',
    },
    {
      name: 'wall time incl. aggregation and erasure (DynamoDB Local)',
      value: sec(wallMs),
      unit: 's',
    },
    {
      name: 'check-in writes/s, conditional put (DynamoDB Local)',
      value: writeRate,
      unit: 'writes/s',
    },
    { name: 'PutItem latency p50 (sampled)', value: percentile(putLat, 50), unit: 'ms' },
    { name: 'PutItem latency p99 (sampled)', value: percentile(putLat, 99), unit: 'ms' },
    { name: 'aggregator records counted/s (DynamoDB Local)', value: aggRate, unit: 'records/s' },
    { name: 'aggregator invocations', value: tot((r) => r.aggInvocations), unit: 'batches' },
    {
      name: 'aggregator transaction cancellations (conflicts and already-counted markers)',
      value: aggStats.transactionCancels,
      unit: 'transactions',
    },
    {
      name: 'aggregator batches retried after a reported failure',
      value: tot((r) => r.aggRetries),
      unit: 'retries',
    },
    {
      name: 'batches delivered twice (stream redelivery)',
      value: tot((r) => r.redelivered),
      unit: 'batches',
    },
    {
      name: 'records skipped as already counted',
      value: tot((r) => r.alreadyCounted),
      unit: 'records',
    },
    { name: 'cells with a total', value: cellsSeen, unit: 'cells' },
    { name: 'devices erased through the byDevice index', value: erased, unit: 'devices' },
    {
      name: 'erasures/s',
      value: sum(res.map((r) => rate(r.erased, r.eraseMs))),
      unit: 'devices/s',
    },
  ];
  const checks = [
    ok('no DynamoDB errors in the writes', writeStats.errors === 0, `${writeStats.errors} errors`),
    ok(
      'exactly one stored check-in per device and alert',
      saved === attempts - repeats,
      `${saved} vs ${attempts - repeats}`,
    ),
    ok(
      'every repeat was rejected',
      tot((r) => r.rejected) === repeats,
      `${tot((r) => r.rejected)} vs ${repeats}`,
    ),
    ok('aggregator gave up on no batch', tot((r) => r.aggFailedGiveUps) === 0),
    ok(
      'aggregator counted every stored check-in exactly once',
      tot((r) => r.applied) === saved,
      `${tot((r) => r.applied)} vs ${saved}`,
    ),
    ok(
      'per-cell totals equal the stored check-ins',
      mismatched === 0 && totalsSum === saved,
      `${mismatched} cells differ, total ${totalsSum} vs ${saved}`,
    ),
    ok(
      'check-in rows left == stored - erased',
      rows === saved - erased,
      `${rows} vs ${saved - erased}`,
    ),
    ok('erased devices have no check-in left', tot((r) => r.erasedStillPresent) === 0),
  ];
  return finish('(c) Check-ins and totals', t0, metrics, checks, [
    'The aggregator is the real handler fed synthetic stream INSERT records (DynamoDB Local streams are not used); 5% of batches are delivered twice to prove the per-record markers.',
  ]);
}

// ---------- (d) Postgres: places, outbox, drainer, erasure ----------

interface IndexVerdict {
  expected: number;
  missing: number;
  wrong: number;
  tableCount: number;
}

/** Compares the whole cell-index table with Postgres (the record), entry by entry. */
export async function verifyCellIndex(env: Env): Promise<IndexVerdict> {
  const rows = await env.admin.query<{
    id: string;
    device_id: string;
    h3_cell: string;
    push_platform: 'ios' | 'android' | 'expo' | null;
    push_token: string | null;
    push_project: string | null;
    language: 'en' | 'es';
  }>(
    `SELECT p.id, p.device_id, p.h3_cell, d.push_platform, d.push_token, d.push_project, d.language
       FROM places p JOIN devices d ON d.id = p.device_id`,
    [],
  );
  const cellsOf = new Map<string, string[]>();
  for (const r of rows) {
    const l = cellsOf.get(r.device_id) ?? [];
    l.push(r.h3_cell);
    cellsOf.set(r.device_id, l);
  }
  const byCell = new Map<string, typeof rows>();
  for (const r of rows) {
    const l = byCell.get(r.h3_cell) ?? [];
    l.push(r);
    byCell.set(r.h3_cell, l);
  }
  const index = new DynamoCellIndex(env.backend.doc, env.backend.names.cellIndexD);
  let found = 0;
  let wrong = 0;
  const cells = [...byCell.keys()];
  await pool(cells.length, 8, async (i) => {
    const cell = cells[i] as string;
    const want = new Map((byCell.get(cell) ?? []).map((r) => [`${r.device_id}#${r.id}`, r]));
    let cursor: string | undefined;
    do {
      const page = await index.listForCell(cell, cursor, 1000);
      for (const e of page.entries) {
        const r = want.get(`${e.deviceId}#${e.placeId}`);
        if (!r) continue; // an extra entry shows up in the table count below
        found++;
        const target = targetOf({
          pushPlatform: r.push_platform,
          pushToken: r.push_token,
          expoPushToken: null,
          pushProject: r.push_project,
        });
        const dc = [...new Set(cellsOf.get(r.device_id))].sort();
        if (
          JSON.stringify(e.target) !== JSON.stringify(target) ||
          e.language !== r.language ||
          JSON.stringify(e.deviceCells) !== JSON.stringify(dc)
        ) {
          wrong++;
        }
      }
      cursor = page.cursor;
    } while (cursor);
  });
  const tableCount = await scanCount(env.backend, env.backend.names.cellIndexD);
  return { expected: rows.length, missing: rows.length - found, wrong, tableCount };
}

const count = async (sql: SqlExecutor, q: string): Promise<number> =>
  Number((await sql.query<{ n: number | string }>(q, []))[0]?.n ?? 0);

export async function scenarioD(env: Env): Promise<ScenarioResult> {
  const t0 = Date.now();
  const s = env.sizes.d;
  const W = env.workers;
  env.log(
    `(d) ${s.places} place adds from ${W} processes (outbox, ${s.failPct}% write-through failures)`,
  );
  const addParams: DAddParams[] = Array.from({ length: W }, (_, worker) => ({
    worker,
    places: Math.ceil(s.places / W),
    seed: 70 + worker,
    failPct: s.failPct,
    concurrency: s.concurrency,
  }));
  const ta = performance.now();
  const add = await env.runner.run<DAddResult>('d-add', addParams);
  const addWallMs = performance.now() - ta;
  const places = sum(add.map((r) => r.places));
  const devices = sum(add.map((r) => r.devices));
  const injected = sum(add.map((r) => r.injectedFailures));
  const unexpected = sum(add.map((r) => r.unexpectedErrors));
  const lat = add.flatMap((r) => r.addLatenciesMs);

  const pgPlaces = await count(env.admin, 'SELECT count(*)::int AS n FROM places');
  const pgDevices = await count(env.admin, 'SELECT count(*)::int AS n FROM devices');
  const pending = await count(env.admin, 'SELECT count(*)::int AS n FROM cell_index_outbox');
  const before = await verifyCellIndex(env);
  env.log(
    `(d) after adds: ${pgPlaces} places, ${pending} outbox rows, ${before.missing} index entries missing`,
  );

  const dr = (
    await env.runner.run<DDrainResult>('d-drain', [{ maxRuns: 100_000 } satisfies DDrainParams])
  )[0] as DDrainResult;
  const afterDrain = await verifyCellIndex(env);
  const pendingAfter = await count(env.admin, 'SELECT count(*)::int AS n FROM cell_index_outbox');

  // Erasure of the first `erase` devices (by id), spread over the processes.
  const ids = (
    await env.admin.query<{ id: string }>(
      `SELECT id FROM devices ORDER BY id LIMIT ${Math.floor(s.erase)}`,
      [],
    )
  ).map((r) => r.id);
  const eraseParams: DEraseParams[] = Array.from({ length: W }, (_, w) => ({
    deviceIds: ids.filter((_, i) => i % W === w),
    alertId: '00000000-0000-4000-a000-0000000000ee',
    concurrency: 8,
  }));
  const te = performance.now();
  const er = await env.runner.run<DEraseResult>('d-erase', eraseParams);
  const eraseWallMs = performance.now() - te;
  const erased = sum(er.map((r) => r.erased));
  const pgDevicesAfter = await count(env.admin, 'SELECT count(*)::int AS n FROM devices');
  const tombstones = await count(env.admin, 'SELECT count(*)::int AS n FROM deleted_devices');
  const afterErase = await verifyCellIndex(env);
  const eraseLat = er.flatMap((r) => r.latenciesMs);
  const rowsDeleted = sum(er.map((r) => r.checkinRowsDeleted));
  const placesRemoved = sum(er.map((r) => r.placesRemoved));

  const metrics: Metric[] = [
    { name: 'devices created', value: devices, unit: 'devices' },
    { name: 'place adds (place + outbox row in one transaction)', value: places, unit: 'places' },
    {
      name: 'place adds/s incl. write-through (real Postgres, DynamoDB Local)',
      value: rate(places, addWallMs),
      unit: 'places/s',
    },
    { name: 'place insert transaction latency p50', value: percentile(lat, 50), unit: 'ms' },
    { name: 'place insert transaction latency p99', value: percentile(lat, 99), unit: 'ms' },
    { name: 'write-throughs failed on purpose', value: injected, unit: 'places' },
    { name: 'outbox rows left for the drainer', value: pending, unit: 'rows' },
    { name: 'index entries missing before the drain', value: before.missing, unit: 'entries' },
    { name: 'drain time (one drainer, back to back)', value: sec(dr.ms), unit: 's' },
    { name: 'drain runs (100 rows each)', value: dr.runs, unit: 'runs' },
    { name: 'drained rows/s', value: rate(dr.applied, dr.ms), unit: 'rows/s' },
    {
      name: 'drain time at the real once-a-minute cadence (100 rows per run)',
      value: Math.ceil(pending / 100),
      unit: 'min',
    },
    { name: 'devices erased', value: erased, unit: 'devices' },
    {
      name: 'erasures/s (index, places, device, check-ins)',
      value: rate(erased, eraseWallMs),
      unit: 'devices/s',
    },
    { name: 'erasure latency p50', value: percentile(eraseLat, 50), unit: 'ms' },
    { name: 'erasure latency p99', value: percentile(eraseLat, 99), unit: 'ms' },
  ];
  const checks = [
    ok('no unexpected write-through errors', unexpected === 0, `${unexpected} errors`),
    ok(
      'every place is in Postgres',
      pgPlaces === places && pgDevices === devices,
      `${pgPlaces}/${places} places, ${pgDevices}/${devices} devices`,
    ),
    ok(
      'outbox rows left == failed write-throughs',
      pending === injected,
      `${pending} vs ${injected}`,
    ),
    ok(
      'before the drain, no more entries are missing than rows wait',
      before.missing <= pending,
      `${before.missing} vs ${pending}`,
    ),
    ok(
      'drainer emptied the outbox without failures',
      pendingAfter === 0 && dr.failed === 0,
      `${pendingAfter} left, ${dr.failed} failed`,
    ),
    ok(
      'after the drain the cell index equals Postgres (none missing, none extra, attributes right)',
      afterDrain.missing === 0 &&
        afterDrain.wrong === 0 &&
        afterDrain.tableCount === afterDrain.expected,
      `missing ${afterDrain.missing}, wrong ${afterDrain.wrong}, table ${afterDrain.tableCount} vs ${afterDrain.expected}`,
    ),
    ok(
      'all devices erased',
      erased === ids.length && pgDevicesAfter === pgDevices - ids.length,
      `${erased}/${ids.length}`,
    ),
    ok(
      'one tombstone per erased device',
      tombstones === ids.length,
      `${tombstones} vs ${ids.length}`,
    ),
    ok(
      'erased devices left no place or index entry',
      afterErase.missing === 0 &&
        afterErase.wrong === 0 &&
        afterErase.tableCount === afterErase.expected &&
        afterErase.expected === pgPlaces - placesRemoved,
      `index ${afterErase.tableCount}, Postgres ${afterErase.expected}`,
    ),
    ok(
      'erased devices left no check-in row',
      rowsDeleted === ids.length,
      `${rowsDeleted} deleted for ${ids.length} devices`,
    ),
  ];
  return finish('(d) Postgres places, outbox, drainer and erasure', t0, metrics, checks, [
    'Add and erase run as the relay_api role, the drainer as relay_ops (migration 0011 grants).',
    'The drainer is run back to back; in AWS it runs once a minute and takes 100 rows per run, so a backlog this size would clear at 100 rows/min (see the row above).',
  ]);
}
