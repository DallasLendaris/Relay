import { mkdtempSync } from 'node:fs';
import { cpus, freemem, tmpdir, totalmem } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { expect, it } from 'vitest';
import type { SuiteContext } from '../src/suite/types.js';
import { t3 } from '../src/suite/tests/t3-checkins/index.js';

it('t3 check-in burst passes at smoke scale', async () => {
  const dir = mkdtempSync(path.join(tmpdir(), 'suite-t3-'));
  const ctx: SuiteContext = {
    outDir: dir,
    dataDir: dir,
    machine: {
      cpuModel: cpus()[0]?.model ?? 'unknown',
      logicalCpus: cpus().length,
      memoryTotalMb: Math.round(totalmem() / 1048576),
      memoryFreeAtStartMb: Math.round(freemem() / 1048576),
      platform: process.platform,
      node: process.version,
    },
    workers: 2,
    scale: 'smoke',
    repoRoot: fileURLToPath(new URL('../../', import.meta.url)),
    services: null,
    log: () => {},
  };
  const r = await t3.run(ctx);
  const bad = r.scenarios.flatMap((s) =>
    s.checks.filter((c) => !c.ok).map((c) => `${s.name}: ${c.name} (${c.detail ?? ''})`),
  );
  expect(bad).toEqual([]);
  expect(r.error).toBeUndefined();
  expect(r.verdict).toBe('pass');
  expect(r.scenarios.length).toBeGreaterThanOrEqual(3);
}, 120_000);
