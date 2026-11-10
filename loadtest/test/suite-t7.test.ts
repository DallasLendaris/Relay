import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import os from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { collectMachineInfo, ensureSharedBuilt } from '../src/suite/runner.js';
import { leakReport, leastSquares } from '../src/suite/tests/t7-soak/analysis.js';
import { buildEvents } from '../src/suite/tests/t7-soak/common.js';
import { buildConfig, t7Soak } from '../src/suite/tests/t7-soak/index.js';

const root = mkdtempSync(join(os.tmpdir(), 'suite-t7-'));
afterAll(() => rmSync(root, { recursive: true, force: true }));

describe('suite t7 helpers', () => {
  it('fits a line and flags only real growth', () => {
    const xs = [0, 1, 2, 3, 4];
    const fit = leastSquares(
      xs,
      xs.map((x) => 10 + 2 * x),
    );
    expect(fit.slope).toBeCloseTo(2);
    expect(fit.intercept).toBeCloseTo(10);
    const t0 = 1_000_000;
    const rows = (f: (s: number) => number) =>
      Array.from({ length: 40 }, (_v, i) => ({
        kind: 'sample' as const,
        proc: 'p',
        t: t0 + i * 10_000,
        rssMb: 0,
        heapMb: f(i * 10),
        eldP99Ms: 0,
        eldMaxMs: 0,
        caches: {},
      }));
    const o = { durationS: 400, markS: 100, limitPct: 20, floorMb: 8 };
    expect(
      leakReport(
        'p',
        rows(() => 100),
        t0,
        o,
      ).leak,
    ).toBe(false);
    const grow = leakReport(
      'p',
      rows((s) => 100 + s * 0.5),
      t0,
      o,
    );
    expect(grow.leak).toBe(true);
    expect(grow.counted).toBe(true);
    expect(grow.slopeMbPerHour).toBeCloseTo(0.5 * 3600, 0);
    // The load driver is test code: reported, not counted.
    expect(
      leakReport(
        'load',
        rows((s) => 100 + s * 0.5),
        t0,
        { ...o, uncounted: ['load'] },
      ).counted,
    ).toBe(false);
    // A cache that fills up in the first two thirds and then plateaus is not a leak.
    expect(
      leakReport(
        'p',
        rows((s) => 100 + Math.min(s, 250) * 0.5),
        t0,
        o,
      ).leak,
    ).toBe(false);
    // 50% growth of a tiny heap stays under the absolute floor.
    expect(
      leakReport(
        'p',
        rows((s) => 10 + s * 0.01),
        t0,
        o,
      ).leak,
    ).toBe(false);
  });

  it('scripts new, update, upgrade and cancel events and scales the run', () => {
    expect(buildEvents(5).map((e) => e.kind)).toEqual([
      'new',
      'new',
      'update_same',
      'update_upgrade',
      'cancel',
    ]);
    const full = buildConfig('full', {});
    expect(full.durationS).toBe(3600);
    expect(full.alertIntervalS).toBe(240);
    expect(full.devices).toBe(300_000);
    expect(buildConfig('full', { RELAY_SOAK_MINUTES: '10' }).durationS).toBe(600);
    expect(buildConfig('smoke', {}).durationS).toBe(60);
  });
});

describe('suite t7 (smoke)', () => {
  beforeAll(async () => ensureSharedBuilt(), 120_000);

  it('passes at smoke scale and writes its report and samples', async () => {
    const r = await t7Soak.run({
      outDir: root,
      dataDir: root,
      machine: collectMachineInfo(),
      workers: 1,
      scale: 'smoke',
      repoRoot: root,
      services: null,
      log: (m) => console.log(`[t7] ${m}`),
    });
    expect(r.verdict, JSON.stringify(r.scenarios[0]?.checks.filter((c) => !c.ok))).toBe('pass');
    expect(existsSync(join(root, 't7.md'))).toBe(true);
    expect(readFileSync(join(root, 't7.md'), 'utf8')).toContain('Heap after GC');
    const csv = readFileSync(join(root, 't7-samples.csv'), 'utf8');
    expect(csv.split('\n')[0]).toContain('heap_after_gc_mb');
    expect(csv).toContain(',sender-1,');
    expect(r.peakRssMb).toBeGreaterThan(0);
  }, 120_000);
});
