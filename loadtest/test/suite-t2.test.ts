import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, describe, expect, it } from 'vitest';
import { t2 } from '../src/suite/tests/t2-fanout/index.js';
import type { SuiteContext } from '../src/suite/types.js';

const root = mkdtempSync(path.join(os.tmpdir(), 'suite-t2-'));

afterAll(() => rmSync(root, { recursive: true, force: true }));

describe('suite t2: multi-process fan-out (smoke)', () => {
  it('delivers exactly once in every scenario', async () => {
    const outDir = path.join(root, 'out');
    const dataDir = path.join(root, 'data');
    mkdirSync(outDir, { recursive: true });
    mkdirSync(dataDir, { recursive: true });
    const ctx: SuiteContext = {
      outDir,
      dataDir,
      machine: {
        cpuModel: os.cpus()[0]?.model ?? 'unknown',
        logicalCpus: os.cpus().length,
        memoryTotalMb: Math.round(os.totalmem() / 1048576),
        memoryFreeAtStartMb: Math.round(os.freemem() / 1048576),
        platform: process.platform,
        node: process.version,
      },
      workers: 2,
      scale: 'smoke',
      repoRoot: path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..'),
      services: null,
      log: (msg, fields) => {
        if (process.env['T2_VERBOSE']) console.log(msg, fields ?? '');
      },
    };
    const r = await t2.run(ctx);
    const failed = r.scenarios.flatMap((s) =>
      s.checks.filter((c) => !c.ok).map((c) => `${s.name}: ${c.name} ${c.detail ?? ''}`),
    );
    expect(r.error).toBeUndefined();
    expect(failed).toEqual([]);
    expect(r.verdict).toBe('pass');
    // a, b, c, d, sweep points 1 and 2, f (dense cell), sweep summary.
    expect(r.scenarios.map((s) => s.name[0])).toEqual(['a', 'b', 'c', 'd', 'e', 'e', 'f', 'e']);
    const metric = (scenario: number, name: string) =>
      r.scenarios[scenario]?.metrics.find((m) => m.name === name)?.value ?? -1;
    expect(metric(0, 'devices in alert area')).toBe(5_000);
    // The statewide-mini alert covers only part of the state: the rest are outside-area controls.
    expect(metric(2, 'devices in alert area')).toBeGreaterThan(0);
    expect(metric(2, 'devices in alert area')).toBeLessThan(20_000);
    expect(metric(0, 'seed time')).toBeGreaterThan(0);
    expect(metric(4, 'seed time')).toBe(0); // the sweep reuses the county seed
    // f: one dense cell sent in parts, SQS bodies within budget, buffering bounded.
    expect(metric(6, 'devices in alert area')).toBe(25_000);
    expect(metric(6, 'dense cells (sent in parts)')).toBeGreaterThanOrEqual(1);
    expect(metric(6, 'largest SQS message body')).toBeLessThanOrEqual(240);
    expect(metric(6, 'peak buffered recipients (largest single shard job)')).toBeGreaterThan(0);
    expect(existsSync(path.join(outDir, 't2.md'))).toBe(true);
    expect(readFileSync(path.join(outDir, 't2.md'), 'utf8')).toContain('Sender scaling');
  }, 120_000);
});
