import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import os from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { collectMachineInfo, ensureSharedBuilt } from '../src/suite/runner.js';
import { t6Ingest } from '../src/suite/tests/t6-ingest/index.js';

const root = mkdtempSync(join(os.tmpdir(), 'suite-t6-'));
afterAll(() => rmSync(root, { recursive: true, force: true }));

describe('suite t6 (smoke)', () => {
  beforeAll(async () => ensureSharedBuilt(), 120_000);

  it('passes the storm, flapping and expiredReferences scenarios and writes its report', async () => {
    const r = await t6Ingest.run({
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
    expect(r.scenarios).toHaveLength(3);
    expect(existsSync(join(root, 't6.md'))).toBe(true);
    expect(readFileSync(join(root, 't6.md'), 'utf8')).toContain('ingest storm');
  }, 120_000);
});
