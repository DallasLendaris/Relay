import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import os from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { PGlite } from '@electric-sql/pglite';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { SqlExecutor } from '../../backend/src/db/sql.js';
import { collectMachineInfo, ensureSharedBuilt } from '../src/suite/runner.js';
import { FakeDoc } from '../src/suite/tests/t4-databases/fake-doc.js';
import { runAllScenarios, t4Databases } from '../src/suite/tests/t4-databases/index.js';
import { assignedWorkers } from '../src/suite/tests/t4-databases/kernels.js';
import {
  InProcessRunner,
  SMOKE_SIZES,
  type Env,
  type Sizes,
} from '../src/suite/tests/t4-databases/scenarios.js';
import {
  TABLE_SPECS,
  createTableInput,
  tableNamesFor,
  type TableSpec,
} from '../src/suite/tests/t4-databases/tables.js';

const root = mkdtempSync(join(os.tmpdir(), 'suite-t4-'));
afterAll(() => rmSync(root, { recursive: true, force: true }));

const infraDir = fileURLToPath(new URL('../../infra/lib', import.meta.url));

/** The text of the `{ ... }` block that starts at the first "{" at or after `from`. */
function block(text: string, from: number): string {
  const open = text.indexOf('{', from);
  let depth = 0;
  for (let i = open; i < text.length; i++) {
    if (text[i] === '{') depth++;
    else if (text[i] === '}' && --depth === 0) return text.slice(open, i + 1);
  }
  throw new Error('unbalanced braces');
}

function keyName(src: string, key: 'partitionKey' | 'sortKey'): string | undefined {
  return new RegExp(`${key}: \\{ name: '(\\w+)', type: AttributeType\\.STRING`).exec(src)?.[1];
}

describe('suite t4: table definitions match infra', () => {
  for (const spec of TABLE_SPECS) {
    it(`${spec.key} (${spec.source.constructId})`, () => {
      const file = readFileSync(join(infraDir, spec.source.file), 'utf8');
      const start = file.indexOf(`new Table(this, '${spec.source.constructId}'`);
      expect(start, 'construct not found in the stack').toBeGreaterThan(-1);
      const def = block(file, start);
      expect(keyName(def, 'partitionKey')).toBe(spec.hash.name);
      expect(keyName(def, 'sortKey')).toBe(spec.range?.name);
      const ttl = /timeToLiveAttribute: '(\w+)'/.exec(def)?.[1] ?? '';
      expect(ttl).toBe(spec.ttl);
      // Global secondary indexes are added by statements after the construct.
      const found: TableSpec['gsis'] = [];
      if (spec.source.tableVar) {
        const re = new RegExp(`${spec.source.tableVar}\\.addGlobalSecondaryIndex\\(`, 'g');
        for (let m = re.exec(file); m; m = re.exec(file)) {
          const g = block(file, m.index);
          found.push({
            name: /indexName: '(\w+)'/.exec(g)?.[1] ?? '',
            hash: keyName(g, 'partitionKey') ?? '',
            projection: /ProjectionType\.(\w+)/.exec(g)?.[1] as 'ALL' | 'KEYS_ONLY',
          });
        }
      }
      expect(found).toEqual(spec.gsis);
      // And the CreateTable input carries the same schema.
      const input = createTableInput(spec, 'x');
      expect(input.KeySchema?.map((k) => k.AttributeName)).toEqual(
        spec.range ? [spec.hash.name, spec.range.name] : [spec.hash.name],
      );
      expect(input.GlobalSecondaryIndexes?.map((g) => g.IndexName) ?? []).toEqual(
        spec.gsis.map((g) => g.name),
      );
      for (const k of input.KeySchema ?? []) {
        expect(input.AttributeDefinitions?.some((a) => a.AttributeName === k.AttributeName)).toBe(
          true,
        );
      }
    });
  }

  it('covers every DynamoDB table the stacks create that test 4 uses', () => {
    expect(TABLE_SPECS.map((s) => s.source.constructId)).toEqual(
      expect.arrayContaining([
        'CellIndexTable',
        'IdempotencyTable',
        'CheckinsTable',
        'CheckinTotalsTable',
        'StateTable',
      ]),
    );
  });
});

describe('suite t4: pieces', () => {
  it('gives about 20% of the keys to a second process, never the same one twice', () => {
    let two = 0;
    const n = 20_000;
    for (let k = 0; k < n; k++) {
      const w = assignedWorkers(k, 4, 20);
      expect(w[0]).toBe(k % 4);
      if (w.length === 2) {
        two++;
        expect(w[1]).not.toBe(w[0]);
      }
    }
    expect(two / n).toBeGreaterThan(0.17);
    expect(two / n).toBeLessThan(0.23);
  });

  it('is skipped (and writes a report) when no Docker services are given', async () => {
    const out = join(root, 'skipped');
    mkdirSync(out, { recursive: true });
    const r = await t4Databases.run({
      outDir: out,
      dataDir: out,
      machine: collectMachineInfo(),
      workers: 1,
      scale: 'smoke',
      repoRoot: root,
      services: null,
      log: () => undefined,
    });
    expect(r.verdict).toBe('skipped');
    expect(r.scenarios).toEqual([]);
    expect(t4Databases.requiresDocker).toBe(true);
    expect(existsSync(join(out, 't4.md'))).toBe(true);
    expect(readFileSync(join(out, 't4.md'), 'utf8')).toContain('DynamoDB Local');
  });
});

/** PGlite as a SqlExecutor (the same adapter shape the backend tests use). */
class PgliteExecutor implements SqlExecutor {
  constructor(readonly pg: PGlite) {}
  async query<T>(sql: string, params: unknown[]): Promise<T[]> {
    return (await this.pg.query<T>(sql, params)).rows;
  }
  transaction<T>(fn: (tx: SqlExecutor) => Promise<T>): Promise<T> {
    return this.pg.transaction((t) =>
      fn({
        query: async <R>(sql: string, params: unknown[]) => (await t.query<R>(sql, params)).rows,
        transaction: () => Promise.reject(new Error('nested transactions are not supported')),
      }),
    );
  }
}

const TINY: Sizes = {
  a: { entries: 300, putEntries: 40, putConcurrency: 2, pageSize: 50 },
  b: { keys: 300, overlapPct: 20, crashPct: 9, leaseMs: 250, concurrency: 6 },
  c: { ...SMOKE_SIZES.c, count: 600, cells: 20, eraseDevices: 5, redeliverPct: 100 },
  d: { places: 120, failPct: 10, concurrency: 4, erase: 8 },
};

describe('suite t4 (smoke, in-process: fake DynamoDB client + PGlite)', () => {
  beforeAll(async () => ensureSharedBuilt(), 120_000);

  it('runs all scenarios with the real adapters and every check passes', async () => {
    const scratch = join(root, 'scratch');
    mkdirSync(scratch, { recursive: true });
    const names = tableNamesFor('test');
    const pg = new PgliteExecutor(new PGlite());
    const backend = { doc: new FakeDoc(names), names, sql: () => pg, scratchDir: scratch };
    const env: Env = {
      backend,
      admin: pg,
      runner: new InProcessRunner(backend),
      workers: 2,
      sizes: TINY,
      log: () => undefined,
      realRoles: false,
      skipMigration: (f) => f.startsWith('0001_'),
    };
    const scenarios = await runAllScenarios(env);
    expect(scenarios.map((s) => s.name.slice(0, 3))).toEqual(['Set', '(a)', '(b)', '(c)', '(d)']);
    for (const s of scenarios) {
      expect(
        s.checks.filter((c) => !c.ok),
        `${s.name}: ${JSON.stringify(s.checks.filter((c) => !c.ok))}`,
      ).toEqual([]);
      expect(s.verdict).toBe('pass');
    }
    const metric = (sc: number, name: string) =>
      scenarios[sc]?.metrics.find((m) => m.name.startsWith(name))?.value;
    // The scenarios really exercised the contested paths.
    expect(metric(2, 'crashes after sending')).toBeGreaterThanOrEqual(0);
    expect(metric(2, 'conditional-check failures')).toBeGreaterThan(0);
    expect(metric(4, 'write-throughs failed on purpose')).toBeGreaterThan(0);
    expect(metric(4, 'outbox rows left')).toBe(metric(4, 'write-throughs failed on purpose'));
    expect(metric(3, 'repeat check-ins rejected')).toBeGreaterThan(0);
    // Every aggregator batch was delivered twice: the markers must have absorbed the second copy.
    expect(metric(3, 'records skipped as already counted')).toBeGreaterThan(0);
  }, 120_000);
});
