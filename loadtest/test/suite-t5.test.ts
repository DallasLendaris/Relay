import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import os from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { collectMachineInfo, ensureSharedBuilt } from '../src/suite/runner.js';
import { t5Chaos } from '../src/suite/tests/t5-chaos/index.js';

const root = mkdtempSync(join(os.tmpdir(), 'suite-t5-'));
afterAll(() => rmSync(root, { recursive: true, force: true }));

describe('suite t5 (smoke)', () => {
  beforeAll(async () => ensureSharedBuilt(), 120_000);

  it('passes every fault scenario at smoke scale and writes its report', async () => {
    const r = await t5Chaos.run({
      outDir: root,
      dataDir: root,
      machine: collectMachineInfo(),
      workers: 1,
      scale: 'smoke',
      repoRoot: root,
      services: null,
      log: (m) => console.log(m),
    });
    const failed = r.scenarios.flatMap((s) =>
      s.checks.filter((c) => !c.ok).map((c) => `${s.name}: ${c.name} (${c.detail ?? ''})`),
    );
    expect(r.error).toBeUndefined();
    expect(failed).toEqual([]);
    expect(r.verdict).toBe('pass');
    expect(r.scenarios.map((s) => s.name.slice(0, 2))).toEqual([
      'cl',
      'a:',
      'b:',
      'c:',
      'd:',
      'e:',
      'f:',
    ]);
    expect(existsSync(join(root, 't5.md'))).toBe(true);
    expect(readFileSync(join(root, 't5.md'), 'utf8')).toContain('missed devices');
  }, 120_000);
});
