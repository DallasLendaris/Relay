import { PGlite } from '@electric-sql/pglite';
import { fileURLToPath } from 'node:url';
import { vi } from 'vitest';
import type { Alert } from '@relay/shared';
import { runMigrations, type SqlExecutor } from '../../src/db/sql.js';
import type { Device, Place } from '../../src/ports.js';

// Starting PGlite (WASM Postgres) takes a couple of seconds per database.
vi.setConfig({ testTimeout: 60_000, hookTimeout: 60_000 });

export const MIGRATIONS_DIR = fileURLToPath(new URL('../../migrations', import.meta.url));

/** SqlExecutor on an in-process Postgres (PGlite). */
export class PgliteExecutor implements SqlExecutor {
  constructor(readonly pg: PGlite) {}
  async query<T>(sql: string, params: unknown[]): Promise<T[]> {
    const res = await this.pg.query<T>(sql, params);
    return res.rows;
  }
  /** PGlite's own transaction: commits when fn returns, rolls back when it throws. */
  transaction<T>(fn: (tx: SqlExecutor) => Promise<T>): Promise<T> {
    return this.pg.transaction((t) =>
      fn({
        query: async <R>(sql: string, params: unknown[]) => (await t.query<R>(sql, params)).rows,
        transaction: () => Promise.reject(new Error('nested transactions are not supported')),
      }),
    );
  }
}

/** Fresh database with all migrations except 0001 (PostGIS is not bundled with PGlite). */
export async function freshDb(): Promise<PgliteExecutor> {
  const exec = new PgliteExecutor(new PGlite());
  await runMigrations(exec, MIGRATIONS_DIR, { skip: (f) => f.startsWith('0001_') });
  return exec;
}

export const DEVICE_ID = '11111111-1111-4111-8111-111111111111';

export const device = (over: Partial<Device> = {}): Device => ({
  id: DEVICE_ID,
  credentialHash: 'a'.repeat(64),
  expoPushToken: 'ExponentPushToken[abc]',
  language: 'en',
  createdAt: '2026-01-02T03:04:05.678Z',
  ...over,
});

export const place = (over: Partial<Place> = {}): Place => ({
  id: '22222222-2222-4222-8222-222222222222',
  deviceId: DEVICE_ID,
  label: 'Home',
  shortAddress: '1 Main St, San Jose',
  h3Cell: '872830828ffffff',
  createdAt: '2026-01-02T03:04:06.000Z',
  ...over,
});

export const alert = (over: Partial<Alert> = {}): Alert => ({
  id: '33333333-3333-4333-8333-333333333333',
  source: 'nws',
  sourceId: 'urn:oid:2.49.0.1.840.0.aaa',
  event: 'Flash Flood Warning',
  eventCode: 'FFW',
  relaySeverity: 'warning',
  capSeverity: 'Severe',
  urgency: 'Immediate',
  certainty: 'Likely',
  status: 'Actual',
  messageType: 'Alert',
  state: 'active',
  references: [],
  area: {
    description: 'Santa Clara County',
    geometry: null,
    ugcCodes: ['CAZ513'],
    sameCodes: ['006085'],
    h3Cells: ['872830828ffffff', '872830829ffffff'],
  },
  sent: '2026-01-02T03:00:00.000Z',
  effective: '2026-01-02T03:00:00.000Z',
  onset: null,
  expires: '2026-01-02T09:00:00.000Z',
  ends: null,
  official: {
    headline: 'Flash Flood Warning',
    description: 'Water rising.',
    instruction: null,
    senderName: 'NWS San Francisco',
    language: 'en-US',
  },
  summaries: [],
  signature: null,
  ...over,
});
