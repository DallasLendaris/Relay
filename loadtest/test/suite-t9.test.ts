import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { collectMachineInfo, ensureSharedBuilt } from '../src/suite/runner.js';
import {
  LATEJOIN_LOOPS,
  lateJoinLoops,
  mergeLateJoinFinals,
  minusMultiset,
  sizesFor,
  slowdownBoundS,
  sortPairs,
  t9,
} from '../src/suite/tests/t9-surge/index.js';
import type { LateJoinFinal } from '../src/suite/tests/t9-surge/latejoin-worker.js';

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

  it('runs the late-join stand-in in several processes at full scale (one at smoke)', () => {
    // One Node thread for every concurrent invocation was CPU-bound at 50,000 signups a minute
    // (event loop ~100%), which read as queue wait. Full scale defaults to four processes.
    expect(sizesFor('full', {}).lateJoinProcs).toBe(4);
    expect(sizesFor('full', { T9_LJ_PROCS: '2' }).lateJoinProcs).toBe(2);
    expect(sizesFor('full', { T9_LJ_PROCS: '99' }).lateJoinProcs).toBe(LATEJOIN_LOOPS.full);
    expect(sizesFor('full', { T9_LJ_PROCS: '0' }).lateJoinProcs).toBe(4);
    expect(sizesFor('smoke', { T9_LJ_PROCS: '8' }).lateJoinProcs).toBe(1);
  });

  it('shares the consumer loops out over the processes without changing their total', () => {
    expect(lateJoinLoops(16, 4)).toEqual([4, 4, 4, 4]);
    expect(lateJoinLoops(16, 3)).toEqual([6, 5, 5]);
    expect(lateJoinLoops(16, 1)).toEqual([16]);
    expect(lateJoinLoops(4, 9)).toEqual([1, 1, 1, 1]);
    for (const n of [1, 2, 3, 5, 7, 16]) {
      expect(lateJoinLoops(16, n).reduce((a, b) => a + b, 0)).toBe(16);
    }
  });

  it('merges the processes: sums, one deferred-token file, percentiles over all waits', () => {
    const dir = mkdtempSync(join(root, 'merge-'));
    const part = (i: number, tokens: bigint[], waits: number[], busy: number): LateJoinFinal => {
      const t = BigUint64Array.from(tokens);
      const tf = join(dir, `d${i}.bin`);
      writeFileSync(tf, Buffer.from(t.buffer));
      const w = Float64Array.from(waits);
      const wf = join(dir, `w${i}.bin`);
      writeFileSync(wf, Buffer.from(w.buffer));
      return {
        polled: 10 + i,
        processed: 10 + i,
        retried: i,
        deadLettered: 0,
        parked: tokens.length,
        deferredTokensFile: tf,
        waitsFile: wf,
        waitP50: 0,
        waitP99: 0,
        waitMax: Math.max(...waits),
        handleP50: 1,
        handleP99: 2 + i,
        logEvents: { 'latejoin.waiting_for_plan': tokens.length, [`only${i}`]: 1 },
        busyPeak: busy,
        busyMean: busy / 2,
      };
    };
    // Process 0 is idle (waits ~1 ms); process 1 has a backlog (waits 10 s). An average of the
    // two p99s would hide neither; the merged percentiles are over all 100 waits.
    const fast = Array.from({ length: 90 }, () => 1);
    const slow = Array.from({ length: 10 }, () => 10_000);
    const m = mergeLateJoinFinals([part(0, [7n, 3n], fast, 0.4), part(1, [5n], slow, 0.97)], dir);
    expect(m.polled).toBe(21);
    expect(m.processed).toBe(21);
    expect(m.retried).toBe(1);
    expect(m.parked).toBe(3);
    expect(m.logEvents).toEqual({ 'latejoin.waiting_for_plan': 3, only0: 1, only1: 1 });
    const merged = readFileSync(m.deferredTokensFile);
    expect([...new BigUint64Array(merged.buffer, merged.byteOffset, merged.length / 8)]).toEqual([
      7n,
      3n,
      5n,
    ]);
    expect(m.waitP50).toBe(1);
    expect(m.waitP99).toBe(10_000);
    expect(m.waitMax).toBe(10_000);
    expect(m.handleP99).toBe(3);
    expect(m.busyPeak).toBe(0.97);
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
    const md = readFileSync(join(root, 't9.md'), 'utf8');
    expect(md).toContain('Signup surge');
    expect(md).toContain('moved push tokens that waited for the plan');
    expect(md).toContain('late-join stand-in (1 process, loops 4): busiest event loop');
  }, 120_000);
});
