import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import os from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { collectMachineInfo, ensureSharedBuilt } from '../src/suite/runner.js';
import { t8 } from '../src/suite/tests/t8-concurrent/index.js';

const root = mkdtempSync(join(os.tmpdir(), 'suite-t8-'));
afterAll(() => rmSync(root, { recursive: true, force: true }));

describe('suite t8 (smoke)', () => {
  beforeAll(async () => ensureSharedBuilt(), 120_000);

  it('passes at smoke scale and writes its report', async () => {
    const r = await t8.run({
      outDir: root,
      dataDir: root,
      machine: collectMachineInfo(),
      workers: 1,
      scale: 'smoke',
      repoRoot: root,
      services: null,
      log: (m) => console.log(`[t8] ${m}`),
    });
    expect(r.verdict, JSON.stringify(r.scenarios[0]?.checks.filter((c) => !c.ok))).toBe('pass');
    expect(existsSync(join(root, 't8.md'))).toBe(true);
    expect(readFileSync(join(root, 't8.md'), 'utf8')).toContain('Concurrent alerts');
  }, 120_000);
});
