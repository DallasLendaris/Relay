/**
 * Scalability suite runner.
 *
 *   node --import tsx src/suite/runner.ts [--tests t1,t2] [--scale full|smoke] [--out DIR]
 *        [--data DIR] [--workers N] [--keep-services]
 *
 * Tests come from ./tests/index.ts (`SUITE_TESTS`). They run one after another (they compete for the
 * CPU). A test that throws counts as verdict 'fail'; the runner continues with the next one. After
 * every test `<out>/<id>.json` and `<out>/SUMMARY.md` are (re)written so partial results survive a
 * crash. Docker (loadtest/suite/docker-compose.yml) is started only when a selected test has
 * `requiresDocker`; if Docker is missing or fails, those tests are 'skipped' with the reason.
 * Exit code 1 if any test failed.
 */
import { execFile } from 'node:child_process';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { killAllWorkers } from './lib/child.js';
import { renderTestReport, fmt, machineLine } from './lib/report.js';
import type { MachineInfo, SuiteContext, SuiteTest, TestResult, Verdict } from './types.js';

const here = dirname(fileURLToPath(import.meta.url));
export const LOADTEST_ROOT = resolve(here, '../..');
export const REPO_ROOT = resolve(LOADTEST_ROOT, '..');
export const COMPOSE_FILE = join(LOADTEST_ROOT, 'suite', 'docker-compose.yml');

export interface RunnerOptions {
  /** Test ids to run; empty means all registered tests. */
  tests: string[];
  scale: 'full' | 'smoke';
  outDir: string;
  dataDir: string;
  workers: number;
  keepServices: boolean;
  repoRoot: string;
}

export type Services = NonNullable<SuiteContext['services']>;

export interface ServiceHandle {
  services: Services;
  stop(): Promise<void>;
}

export interface RunnerDeps {
  /** Starts the Docker services; resolves to `{ error }` when they cannot be started. */
  startServices?: (opts: RunnerOptions) => Promise<ServiceHandle | { error: string }>;
  log?: (line: string) => void;
  /** How often to print a progress line while a test runs. */
  progressEveryMs?: number;
}

// ---------- CLI ----------

function timestamp(d = new Date()): string {
  const p = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}${p(d.getMonth() + 1)}${p(d.getDate())}-${p(d.getHours())}${p(d.getMinutes())}`;
}

export function parseCli(argv: string[], repoRoot = REPO_ROOT): RunnerOptions {
  const args: Record<string, string> = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === undefined || !a.startsWith('--')) throw new Error(`unexpected argument: ${a}`);
    const next = argv[i + 1];
    if (next !== undefined && !next.startsWith('--')) {
      args[a.slice(2)] = next;
      i++;
    } else args[a.slice(2)] = 'true';
  }
  const known = new Set(['tests', 'scale', 'out', 'data', 'workers', 'keep-services']);
  for (const k of Object.keys(args)) if (!known.has(k)) throw new Error(`unknown option --${k}`);
  const scale = args['scale'] ?? 'full';
  if (scale !== 'full' && scale !== 'smoke') throw new Error('--scale must be full or smoke');
  const cpus = os.cpus().length;
  const workers = args['workers'] === undefined ? Math.max(1, cpus - 2) : Number(args['workers']);
  if (!Number.isInteger(workers) || workers < 1)
    throw new Error('--workers must be an integer >= 1');
  return {
    tests: (args['tests'] ?? '')
      .split(',')
      .map((s) => s.trim().toLowerCase())
      .filter(Boolean),
    scale,
    outDir: resolve(args['out'] ?? join(repoRoot, 'loadtest', 'results', `suite-${timestamp()}`)),
    dataDir: resolve(args['data'] ?? join(repoRoot, 'loadtest', '.suite-data')),
    workers,
    keepServices: args['keep-services'] !== undefined,
    repoRoot,
  };
}

export function collectMachineInfo(): MachineInfo {
  const cpus = os.cpus();
  return {
    cpuModel: cpus[0]?.model?.trim() ?? 'unknown',
    logicalCpus: cpus.length,
    memoryTotalMb: Math.round(os.totalmem() / 1024 / 1024),
    memoryFreeAtStartMb: Math.round(os.freemem() / 1024 / 1024),
    platform: `${os.platform()} ${os.release()}`,
    node: process.version,
  };
}

// ---------- Docker ----------

function exec(cmd: string, args: string[], env?: NodeJS.ProcessEnv, timeoutMs = 15 * 60_000) {
  return new Promise<{ ok: boolean; out: string }>((res) => {
    execFile(
      cmd,
      args,
      {
        env: { ...process.env, ...env },
        timeout: timeoutMs,
        windowsHide: true,
        maxBuffer: 16 << 20,
      },
      (err, stdout, stderr) => {
        const out = `${stdout}\n${stderr}`.trim();
        res({ ok: !err, out: err ? `${err.message}\n${out}`.trim() : out });
      },
    );
  });
}

export async function startDockerServices(
  opts: RunnerOptions,
  log: (l: string) => void = () => undefined,
): Promise<ServiceHandle | { error: string }> {
  const password = process.env['RELAY_PG_PASSWORD'] ?? 'relay';
  const env = { RELAY_PG_PASSWORD: password };
  const compose = (...a: string[]) => exec('docker', ['compose', '-f', COMPOSE_FILE, ...a], env);
  const probe = await exec('docker', ['version', '--format', '{{.Server.Version}}'], env, 30_000);
  if (!probe.ok) {
    return { error: `docker is not available or not running (${firstLine(probe.out)})` };
  }
  log('starting docker services (dynamodb-local, postgis); first run pulls images');
  const up = await compose('up', '-d', '--wait');
  if (!up.ok) {
    await compose('down', '-v');
    return { error: `docker compose up failed: ${tail(up.out)}` };
  }
  return {
    services: {
      dynamoEndpoint: 'http://127.0.0.1:8000',
      postgresUrl: `postgres://postgres:${encodeURIComponent(password)}@127.0.0.1:55432/postgres`,
    },
    stop: async () => {
      if (opts.keepServices) {
        log('--keep-services: leaving docker services running');
        return;
      }
      log('stopping docker services');
      await compose('down', '-v');
    },
  };
}

const firstLine = (s: string) => s.split('\n')[0]?.slice(0, 200) ?? '';
const tail = (s: string) => s.split('\n').slice(-8).join(' | ').slice(0, 600);

/** Builds packages/shared (the backend imports its dist) when dist is missing. No shell, no npm. */
export async function ensureSharedBuilt(repoRoot = REPO_ROOT): Promise<void> {
  const pkg = join(repoRoot, 'packages', 'shared');
  if (existsSync(join(pkg, 'dist', 'index.js'))) return;
  console.log('[suite] building packages/shared (dist missing)');
  const tsc = join(repoRoot, 'node_modules', 'typescript', 'bin', 'tsc');
  await new Promise<void>((res, rej) => {
    execFile(
      process.execPath,
      [tsc, '-p', 'tsconfig.build.json'],
      { cwd: pkg, windowsHide: true },
      (err, stdout, stderr) =>
        err ? rej(new Error(`building packages/shared failed: ${stdout}${stderr}`)) : res(),
    );
  });
}

// ---------- Planning and summary ----------

function idOrder(id: string): number {
  const n = Number(id.replace(/\D+/g, ''));
  return Number.isFinite(n) ? n : 1e9;
}

export function selectTests(all: SuiteTest[], wanted: string[]): SuiteTest[] {
  const sorted = [...all].sort((a, b) => idOrder(a.id) - idOrder(b.id));
  if (wanted.length === 0) return sorted;
  const missing = wanted.filter((id) => !sorted.some((t) => t.id === id));
  if (missing.length) {
    throw new Error(
      `unknown test id(s): ${missing.join(', ')} (available: ${sorted.map((t) => t.id).join(', ')})`,
    );
  }
  return sorted.filter((t) => wanted.includes(t.id));
}

export function renderPlan(tests: SuiteTest[], scale: 'full' | 'smoke'): string {
  const rows = tests.map(
    (t) =>
      `  ${t.id.padEnd(4)} ${t.title.padEnd(44)} ${String(t.estimatedMinutes).padStart(4)} min${t.requiresDocker ? '  (docker)' : ''}`,
  );
  const total = tests.reduce((n, t) => n + t.estimatedMinutes, 0);
  return [
    `Plan (${scale} scale${scale === 'smoke' ? ', estimates apply to full only' : ''}):`,
    '  id   title                                        est.',
    ...rows,
    `  total estimated: ${total} min (${(total / 60).toFixed(1)} h)`,
  ].join('\n');
}

export function formatDuration(ms: number): string {
  const s = Math.round(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  return h > 0
    ? `${h}h${String(m).padStart(2, '0')}m${String(sec).padStart(2, '0')}s`
    : `${m}m${String(sec).padStart(2, '0')}s`;
}

function headline(r: TestResult): string {
  const out: string[] = [];
  for (const s of r.scenarios) {
    for (const m of s.metrics.slice(0, 2)) {
      if (out.length < 4) {
        out.push(`${m.name} ${fmt(m.value, Number.isInteger(m.value) ? 0 : 1)} ${m.unit}`);
      }
    }
  }
  if (!out.length && r.error)
    out.push(r.verdict === 'skipped' ? r.error : `error: ${firstLine(r.error)}`);
  return out.join('<br>').replace(/\|/g, '/');
}

export interface SummaryInput {
  opts: RunnerOptions;
  machine: MachineInfo;
  startedAt: string;
  plan: SuiteTest[];
  results: TestResult[];
  running?: string;
  finishedAt?: string;
}

export function renderSummary(s: SummaryInput): string {
  const done = new Map(s.results.map((r) => [r.id, r]));
  const failed = s.results.filter((r) => r.verdict === 'fail').length;
  const out = [
    '# Scalability suite summary',
    '',
    `- Started: ${s.startedAt}${s.finishedAt ? `, finished: ${s.finishedAt}` : s.running ? ` (running ${s.running})` : ''}`,
    `- Scale: ${s.opts.scale}, workers: ${s.opts.workers}`,
    `- Machine: ${machineLine(s.machine)}`,
    `- Tests: ${s.results.length}/${s.plan.length} finished, ${failed} failed`,
    '',
    '| Test | Title | Verdict | Duration | Headline | Report |',
    '| --- | --- | --- | --- | --- | --- |',
  ];
  for (const t of s.plan) {
    const r = done.get(t.id);
    if (!r) {
      out.push(`| ${t.id} | ${t.title} | ${s.running === t.id ? 'running' : 'pending'} | | | |`);
      continue;
    }
    out.push(
      `| ${r.id} | ${r.title} | ${r.verdict.toUpperCase()} | ${formatDuration(r.durationMs)} | ${headline(r)} | [${r.id}.md](${r.id}.md), [${r.id}.json](${r.id}.json) |`,
    );
  }
  out.push('');
  return out.join('\n');
}

// ---------- Run ----------

const failResult = (
  t: SuiteTest,
  startedAt: string,
  durationMs: number,
  error: string,
): TestResult => ({
  id: t.id,
  title: t.title,
  verdict: 'fail',
  startedAt,
  durationMs,
  scenarios: [],
  error,
});

export async function runSuite(
  opts: RunnerOptions,
  allTests: SuiteTest[],
  deps: RunnerDeps = {},
): Promise<{ results: TestResult[]; failed: boolean }> {
  const log = deps.log ?? ((l: string) => console.log(l));
  const plan = selectTests(allTests, opts.tests);
  mkdirSync(opts.outDir, { recursive: true });
  mkdirSync(opts.dataDir, { recursive: true });
  const machine = collectMachineInfo();
  const startedAt = new Date().toISOString();
  const t0 = Date.now();
  const results: TestResult[] = [];
  const writeSummary = (running?: string, finishedAt?: string) =>
    writeFileSync(
      join(opts.outDir, 'SUMMARY.md'),
      renderSummary({ opts, machine, startedAt, plan, results, running, finishedAt }),
    );

  log(renderPlan(plan, opts.scale));
  log(`Results: ${opts.outDir}`);
  log(`Data: ${opts.dataDir}`);
  writeSummary(plan[0]?.id);

  let handle: ServiceHandle | { error: string } | null = null;
  if (plan.some((t) => t.requiresDocker)) {
    handle = await (deps.startServices
      ? deps.startServices(opts)
      : startDockerServices(opts, (l) => log(`[suite] ${l}`)));
    if ('error' in handle) log(`[suite] docker unavailable: ${handle.error}`);
  }

  const remainingEstimate = (fromIdx: number) =>
    opts.scale === 'full'
      ? plan.slice(fromIdx).reduce((n, t) => n + t.estimatedMinutes, 0) * 60_000
      : 0;

  try {
    for (let i = 0; i < plan.length; i++) {
      const t = plan[i]!;
      writeSummary(t.id);
      const testStart = Date.now();
      const testStartedAt = new Date(testStart).toISOString();
      log(`\n[suite] (${i + 1}/${plan.length}) ${t.id}: ${t.title}\n[suite] ${t.description}`);
      let result: TestResult;
      if (t.requiresDocker && (!handle || 'error' in handle)) {
        const reason =
          handle && 'error' in handle ? handle.error : 'docker services were not started';
        result = {
          id: t.id,
          title: t.title,
          verdict: 'skipped',
          startedAt: testStartedAt,
          durationMs: 0,
          scenarios: [],
          error: `skipped: ${reason}`,
        };
      } else {
        const ctx: SuiteContext = {
          outDir: opts.outDir,
          dataDir: opts.dataDir,
          machine,
          workers: opts.workers,
          scale: opts.scale,
          repoRoot: opts.repoRoot,
          services: handle && !('error' in handle) ? handle.services : null,
          log: (msg, fields) =>
            log(`[${t.id}] ${msg}${fields ? ` ${JSON.stringify(fields)}` : ''}`),
        };
        const ticker = setInterval(() => {
          const el = Date.now() - testStart;
          const eta = Math.max(0, remainingEstimate(i) - el);
          log(
            `[suite] ${t.id} running ${formatDuration(el)}, suite elapsed ${formatDuration(Date.now() - t0)}${opts.scale === 'full' ? `, ETA ~${formatDuration(eta)}` : ''}`,
          );
        }, deps.progressEveryMs ?? 30_000);
        try {
          result = await t.run(ctx);
        } catch (e) {
          const err = e instanceof Error ? (e.stack ?? e.message) : String(e);
          log(`[suite] ${t.id} threw: ${firstLine(err)}`);
          result = failResult(t, testStartedAt, Date.now() - testStart, err);
        } finally {
          clearInterval(ticker);
          killAllWorkers();
        }
        result = normalise(t, result, testStartedAt, Date.now() - testStart);
      }
      results.push(result);
      writeFileSync(join(opts.outDir, `${t.id}.json`), JSON.stringify(result, null, 2));
      const md = join(opts.outDir, `${t.id}.md`);
      if (!existsSync(md)) {
        // A test normally writes its own report; fall back to a generic one for errors and skips.
        writeFileSync(
          md,
          renderTestReport(
            { outDir: opts.outDir, scale: opts.scale, machine } as SuiteContext,
            result,
            t.description,
          ),
        );
      }
      writeSummary(plan[i + 1]?.id);
      const elapsed = Date.now() - t0;
      log(
        `[suite] ${t.id} ${result.verdict.toUpperCase()} in ${formatDuration(result.durationMs)}; suite elapsed ${formatDuration(elapsed)}${i + 1 < plan.length && opts.scale === 'full' ? `, ETA ~${formatDuration(remainingEstimate(i + 1))}` : ''}`,
      );
    }
  } finally {
    if (handle && !('error' in handle)) await handle.stop().catch(() => undefined);
    writeSummary(undefined, new Date().toISOString());
  }
  const failed = results.some((r) => r.verdict === 'fail');
  log(
    `\n[suite] done in ${formatDuration(Date.now() - t0)}: ${results.filter((r) => r.verdict === 'pass').length} passed, ${results.filter((r) => r.verdict === 'fail').length} failed, ${results.filter((r) => r.verdict === 'skipped').length} skipped`,
  );
  log(`[suite] summary: ${join(opts.outDir, 'SUMMARY.md')}`);
  return { results, failed };
}

/** Guards against a test returning a malformed result: id/title/duration come from the runner. */
function normalise(t: SuiteTest, r: TestResult, startedAt: string, durationMs: number): TestResult {
  const verdict: Verdict = r.verdict;
  return {
    ...r,
    id: t.id,
    title: r.title || t.title,
    verdict,
    startedAt: r.startedAt || startedAt,
    durationMs: r.durationMs || durationMs,
    scenarios: r.scenarios ?? [],
  };
}

// ---------- main ----------

const entry = process.argv[1];
if (entry && import.meta.url === pathToFileURL(resolve(entry)).href) {
  const onSignal = () => {
    killAllWorkers();
    process.exit(130);
  };
  process.on('SIGINT', onSignal);
  process.on('SIGTERM', onSignal);
  try {
    const opts = parseCli(process.argv.slice(2));
    await ensureSharedBuilt(opts.repoRoot);
    const { SUITE_TESTS } = await import('./tests/index.js');
    const { failed } = await runSuite(opts, SUITE_TESTS);
    process.exit(failed ? 1 : 0);
  } catch (e) {
    console.error(e instanceof Error ? e.message : e);
    killAllWorkers();
    process.exit(2);
  }
}
