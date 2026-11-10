/**
 * t4: the hot paths of tests 2 and 3 against real storage engines (DynamoDB Local and PostgreSQL
 * with PostGIS in Docker, started by the runner) using the real backend adapters.
 *
 * DynamoDB Local is a single-node SQLite-backed emulator and is far slower than DynamoDB. Its numbers
 * measure correctness under concurrency and a LOWER BOUND, not AWS throughput.
 */
import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { renderTestReport } from '../../lib/report.js';
import { RssSampler } from '../../lib/rss.js';
import type { ScenarioResult, SuiteContext, SuiteTest, TestResult } from '../../types.js';
import { makeRealBackend } from './backend.js';
import { PgExecutor, recreateDatabase, withDatabase } from './pg-executor.js';
import {
  FULL_SIZES,
  ProcessRunner,
  SMOKE_SIZES,
  scenarioA,
  scenarioB,
  scenarioC,
  scenarioD,
  setupPostgres,
  type Env,
} from './scenarios.js';
import { createTables, dropTables, makeDynamoClients, tableNamesFor } from './tables.js';

const INTRO =
  'Repeats the hot paths of tests 2 and 3 against real storage engines instead of in-memory fakes: ' +
  'DynamoDB Local (amazon/dynamodb-local, in memory) through the real DynamoCellIndex, DynamoIdempotencyStore, ' +
  'DynamoSeenStore, DynamoCheckInStore, check-in aggregator and totals reader, and PostgreSQL with PostGIS ' +
  '(postgis/postgis:16-3.4) through the real place/device stores, outbox, drainer and the repo migration runner ' +
  '(node-postgres replaces the RDS Data API executor). **DynamoDB Local is a single-node emulator and is much slower than ' +
  'DynamoDB: its numbers measure correctness under concurrency and a lower bound, not AWS throughput.** ' +
  'Postgres runs with fsync off in a container, so its numbers are not Aurora numbers either. ' +
  'At full scale the DynamoDB Local container holds about 1 million cell-index entries in memory (plan on 2 to 3 GB for Docker).';

/** Runs every scenario in order; a scenario that throws is reported as failed, the rest still run. */
export async function runAllScenarios(env: Env): Promise<ScenarioResult[]> {
  const out: ScenarioResult[] = [];
  const steps: Array<[string, (e: Env) => Promise<ScenarioResult>]> = [
    ['setup', setupPostgres],
    ['a', scenarioA],
    ['b', scenarioB],
    ['c', scenarioC],
    ['d', scenarioD],
  ];
  for (const [id, fn] of steps) {
    const t0 = Date.now();
    try {
      const r = await fn(env);
      env.log(`${id}: ${r.verdict} in ${((Date.now() - t0) / 1000).toFixed(1)} s`);
      out.push(r);
    } catch (e) {
      const msg = e instanceof Error ? (e.stack ?? e.message) : String(e);
      env.log(`${id}: threw ${msg}`);
      out.push({
        name: `scenario ${id}`,
        verdict: 'fail',
        durationMs: Date.now() - t0,
        metrics: [],
        checks: [
          { name: 'scenario completed without an exception', ok: false, detail: msg.slice(0, 400) },
        ],
      });
      // The databases may be half-set-up after a setup failure: the other scenarios would only add noise.
      if (id === 'setup') break;
    }
  }
  return out;
}

function verdictOf(scenarios: ScenarioResult[]): 'pass' | 'fail' {
  return scenarios.length > 0 && scenarios.every((s) => s.verdict === 'pass') ? 'pass' : 'fail';
}

export const t4Databases: SuiteTest = {
  id: 't4',
  title: 'Real databases (DynamoDB Local + PostgreSQL)',
  description:
    'Cell index, idempotency, check-ins and the Postgres place/outbox/erasure paths against DynamoDB Local and PostGIS in Docker, with the real adapters.',
  estimatedMinutes: 35,
  requiresDocker: true,
  async run(ctx: SuiteContext): Promise<TestResult> {
    const startedAt = new Date().toISOString();
    const t0 = Date.now();
    const base = { id: 't4', title: t4Databases.title, startedAt };
    if (!ctx.services) {
      const r: TestResult = {
        ...base,
        verdict: 'skipped',
        durationMs: 0,
        scenarios: [],
        error: 'Docker services (DynamoDB Local, PostGIS) are not available; test skipped.',
      };
      writeFileSync(join(ctx.outDir, 't4.md'), renderTestReport(ctx, r, INTRO));
      return r;
    }
    const full = ctx.scale === 'full';
    const workers = full ? Math.max(2, ctx.workers) : 2;
    const runId = Date.now().toString(36);
    const names = tableNamesFor(runId);
    const scratch = join(ctx.dataDir, `t4-${runId}`);
    mkdirSync(scratch, { recursive: true });
    const database = `relay_t4_${runId}`.replace(/[^a-z0-9_]/g, '');
    const pgUrl = withDatabase(ctx.services.postgresUrl, database);
    const rss = new RssSampler();
    const { raw } = makeDynamoClients(ctx.services.dynamoEndpoint);
    let backend: ReturnType<typeof makeRealBackend> | undefined;
    let scenarios: ScenarioResult[] = [];
    let error: string | undefined;
    let peakChild = 0;
    try {
      ctx.log('t4: creating DynamoDB tables and the Postgres database');
      const ttlNotes = await createTables(raw, names);
      await recreateDatabase(ctx.services.postgresUrl, database);
      backend = makeRealBackend({
        dynamoEndpoint: ctx.services.dynamoEndpoint,
        postgresUrl: pgUrl,
        names,
        scratchDir: scratch,
        poolSize: 4,
      });
      const runner = new ProcessRunner({
        dynamoEndpoint: ctx.services.dynamoEndpoint,
        postgresUrl: pgUrl,
        names,
        scratchDir: scratch,
        heapMb: full ? 1024 : 384,
        rss,
      });
      const env: Env = {
        backend,
        admin: backend.admin,
        runner,
        workers,
        sizes: full ? FULL_SIZES : SMOKE_SIZES,
        log: (m) => ctx.log(m),
        realRoles: true,
        postgresUrl: pgUrl,
      };
      scenarios = await runAllScenarios(env);
      const setup = scenarios[0];
      if (setup && ttlNotes.length > 0) setup.notes = [...(setup.notes ?? []), ...ttlNotes];
      peakChild = runner.peakChildRssMb;
    } catch (e) {
      error = e instanceof Error ? (e.stack ?? e.message) : String(e);
    } finally {
      try {
        await dropTables(raw, names);
      } catch {
        /* best effort */
      }
      raw.destroy();
      if (backend) await backend.close().catch(() => undefined);
      // The Postgres database is dropped with the container; drop it anyway for --keep-services runs.
      try {
        const admin = new PgExecutor(ctx.services.postgresUrl, { max: 1 });
        await admin.query(`DROP DATABASE IF EXISTS ${database} WITH (FORCE)`);
        await admin.close();
      } catch {
        /* best effort */
      }
    }
    const result: TestResult = {
      ...base,
      verdict: error ? 'fail' : verdictOf(scenarios),
      durationMs: Date.now() - t0,
      scenarios,
      peakRssMb: Math.max(rss.stop(), Math.round(peakChild)),
      ...(error ? { error } : {}),
    };
    writeFileSync(join(ctx.outDir, 't4.md'), renderTestReport(ctx, result, INTRO));
    return result;
  },
};
