import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import os from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { t1Baseline } from '../src/suite/tests/t1-baseline.js';
import { collectMachineInfo, ensureSharedBuilt } from '../src/suite/runner.js';

const root = mkdtempSync(join(os.tmpdir(), 'suite-t1-'));
afterAll(() => rmSync(root, { recursive: true, force: true }));

describe('suite t1 (smoke)', () => {
  beforeAll(async () => ensureSharedBuilt(), 120_000);

  it('passes at smoke scale and writes its report', async () => {
    const r = await t1Baseline.run({
      outDir: root,
      dataDir: root,
      machine: collectMachineInfo(),
      workers: 1,
      scale: 'smoke',
      repoRoot: root,
      services: null,
      log: () => undefined,
    });
    expect(r.verdict, JSON.stringify(r.scenarios[0]?.checks.filter((c) => !c.ok))).toBe('pass');
    const names = r.scenarios[0]?.metrics.map((m) => m.name) ?? [];
    expect(names).toContain('time to last push');
    expect(names).toContain('check-ins/s (API)');
    expect(existsSync(join(root, 't1.md'))).toBe(true);
    expect(readFileSync(join(root, 't1.md'), 'utf8')).toContain('missed devices');
    expect(r.peakRssMb).toBeGreaterThan(0);
  }, 120_000);
});
