import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import os from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { parseCli, runSuite, selectTests, type RunnerOptions } from '../src/suite/runner.js';
import type { SuiteContext, SuiteTest, TestResult } from '../src/suite/types.js';

const dirs: string[] = [];
afterEach(() => {
  for (const d of dirs.splice(0)) rmSync(d, { recursive: true, force: true });
});

function opts(over: Partial<RunnerOptions> = {}): RunnerOptions {
  const root = mkdtempSync(join(os.tmpdir(), 'suite-runner-'));
  dirs.push(root);
  return {
    tests: [],
    scale: 'smoke',
    outDir: join(root, 'out'),
    dataDir: join(root, 'data'),
    workers: 1,
    keepServices: false,
    repoRoot: root,
    ...over,
  };
}

function fake(
  id: string,
  run: (ctx: SuiteContext) => Promise<TestResult>,
  extra: Partial<SuiteTest> = {},
): SuiteTest {
  return { id, title: `Test ${id}`, description: 'fake', estimatedMinutes: 1, run, ...extra };
}

const passing = (id: string, order: string[] = []) =>
  fake(id, async () => {
    order.push(id);
    return {
      id,
      title: `Test ${id}`,
      verdict: 'pass',
      startedAt: new Date().toISOString(),
      durationMs: 5,
      scenarios: [
        {
          name: 's',
          verdict: 'pass',
          durationMs: 5,
          metrics: [{ name: 'rate', value: 1234, unit: '/s' }],
          checks: [{ name: 'ok', ok: true }],
        },
      ],
    };
  });

const quiet = { log: () => undefined };

describe('suite runner', () => {
  it('runs tests in id order, writes json/md and a SUMMARY', async () => {
    const order: string[] = [];
    const o = opts();
    const { results, failed } = await runSuite(
      o,
      [passing('t3', order), passing('t1', order), passing('t2', order)],
      quiet,
    );
    expect(order).toEqual(['t1', 't2', 't3']);
    expect(failed).toBe(false);
    expect(results.map((r) => r.verdict)).toEqual(['pass', 'pass', 'pass']);
    for (const id of ['t1', 't2', 't3']) {
      expect(existsSync(join(o.outDir, `${id}.json`))).toBe(true);
      expect(existsSync(join(o.outDir, `${id}.md`))).toBe(true);
    }
    const summary = readFileSync(join(o.outDir, 'SUMMARY.md'), 'utf8');
    expect(summary).toContain('3/3 finished, 0 failed');
    expect(summary).toContain('[t2.md](t2.md)');
    expect(summary).toContain('rate 1,234 /s');
  });

  it('isolates a throwing test and keeps going', async () => {
    const order: string[] = [];
    const o = opts();
    const boom = fake('t2', async () => {
      throw new Error('kaboom');
    });
    const { results, failed } = await runSuite(
      o,
      [passing('t1', order), boom, passing('t3', order)],
      quiet,
    );
    expect(failed).toBe(true);
    expect(order).toEqual(['t1', 't3']);
    expect(results.map((r) => r.verdict)).toEqual(['pass', 'fail', 'pass']);
    expect(results[1]?.error).toContain('kaboom');
    expect(readFileSync(join(o.outDir, 'SUMMARY.md'), 'utf8')).toContain('1 failed');
    expect(readFileSync(join(o.outDir, 't2.md'), 'utf8')).toContain('kaboom');
  });

  it('writes SUMMARY.md after every test (partial results survive a crash)', async () => {
    const o = opts();
    let seenDuringSecond = '';
    const second = fake('t2', async () => {
      seenDuringSecond = readFileSync(join(o.outDir, 'SUMMARY.md'), 'utf8');
      return passing('t2').run({} as SuiteContext);
    });
    await runSuite(o, [passing('t1'), second], quiet);
    expect(seenDuringSecond).toContain('1/2 finished');
    expect(seenDuringSecond).toContain('| t2 | Test t2 | running');
  });

  it('skips docker tests (not fail) when docker cannot start', async () => {
    const o = opts();
    let ran = false;
    const docker = fake(
      't4',
      async () => {
        ran = true;
        throw new Error('must not run');
      },
      { requiresDocker: true },
    );
    const { results, failed } = await runSuite(o, [passing('t1'), docker], {
      ...quiet,
      startServices: async () => ({ error: 'docker is not available' }),
    });
    expect(ran).toBe(false);
    expect(failed).toBe(false);
    expect(results[1]?.verdict).toBe('skipped');
    expect(results[1]?.error).toContain('docker is not available');
  });

  it('does not touch docker when no selected test needs it, and passes services when started', async () => {
    let started = 0;
    let stopped = 0;
    const startServices = async () => {
      started++;
      return {
        services: { dynamoEndpoint: 'http://127.0.0.1:8000', postgresUrl: 'postgres://x' },
        stop: async () => {
          stopped++;
        },
      };
    };
    await runSuite(opts(), [passing('t1')], { ...quiet, startServices });
    expect(started).toBe(0);
    let seen: SuiteContext['services'] = null;
    const docker = fake(
      't4',
      async (ctx) => {
        seen = ctx.services;
        return passing('t4').run(ctx);
      },
      { requiresDocker: true },
    );
    await runSuite(opts(), [docker], { ...quiet, startServices });
    expect(started).toBe(1);
    expect(stopped).toBe(1);
    expect(seen).toEqual({ dynamoEndpoint: 'http://127.0.0.1:8000', postgresUrl: 'postgres://x' });
  });

  it('selects a subset and rejects unknown ids', () => {
    const all = [passing('t2'), passing('t1'), passing('t3')];
    expect(selectTests(all, ['t3', 't1']).map((t) => t.id)).toEqual(['t1', 't3']);
    expect(() => selectTests(all, ['t9'])).toThrow(/unknown test id/);
  });

  it('parses the CLI with defaults', () => {
    const d = parseCli([], '/repo');
    expect(d.scale).toBe('full');
    expect(d.tests).toEqual([]);
    expect(d.workers).toBe(Math.max(1, os.cpus().length - 2));
    expect(d.outDir).toMatch(/suite-\d{8}-\d{4}$/);
    const p = parseCli([
      '--tests',
      't1,T2',
      '--scale',
      'smoke',
      '--workers',
      '3',
      '--keep-services',
    ]);
    expect(p).toMatchObject({
      tests: ['t1', 't2'],
      scale: 'smoke',
      workers: 3,
      keepServices: true,
    });
    expect(() => parseCli(['--scale', 'huge'])).toThrow();
  });
});
