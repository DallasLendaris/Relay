import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import os from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { collectMachineInfo, ensureSharedBuilt } from '../src/suite/runner.js';
import { minusMultiset, slowdownBoundS, sortPairs, t9 } from '../src/suite/tests/t9-surge/index.js';

const root = mkdtempSync(join(os.tmpdir(), 'suite-t9-'));
afterAll(() => rmSync(root, { recursive: true, force: true }));

describe('suite t9 helpers', () => {
  it('removes one occurrence per element of a sorted multiset', () => {
    const all = BigUint64Array.from([1n, 2n, 2n, 3n, 5n, 5n]);
    expect([...minusMultiset(all, BigUint64Array.from([2n, 5n, 9n]))]).toEqual([1n, 2n, 3n, 5n]);
  });

  it('sorts tokens together with their values', () => {
    const r = sortPairs(BigUint64Array.from([9n, 1n, 5n]), Float64Array.from([90, 10, 50]));
    expect([...r.tokens]).toEqual([1n, 5n, 9n]);
    expect([...r.values]).toEqual([10, 50, 90]);
  });

  it('computes the slowdown bound', () => {
    expect(slowdownBoundS('full', 100)).toBeCloseTo(130);
    expect(slowdownBoundS('smoke', 10)).toBeCloseTo(19);
  });
});

describe('suite t9 (smoke)', () => {
  beforeAll(async () => ensureSharedBuilt(), 120_000);

  it('passes at smoke scale and writes its report', async () => {
    const r = await t9.run({
      outDir: root,
      dataDir: root,
      machine: collectMachineInfo(),
      workers: 1,
      scale: 'smoke',
      repoRoot: root,
      services: null,
      log: (m) => console.log(`[t9] ${m}`),
    });
    expect(r.verdict, JSON.stringify(r.scenarios[0]?.checks.filter((c) => !c.ok))).toBe('pass');
    expect(existsSync(join(root, 't9.md'))).toBe(true);
    expect(readFileSync(join(root, 't9.md'), 'utf8')).toContain('Signup surge');
  }, 120_000);
});
