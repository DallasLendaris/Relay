import { randomUUID } from 'node:crypto';
import { beforeEach, describe, expect, it } from 'vitest';
import { PostgresDeviceStore } from '../../src/adapters/postgres/device-store.js';
import { PostgresPlaceStore } from '../../src/adapters/postgres/place-store.js';
import {
  MemoryCellIndex,
  PostgresCellIndexOutbox,
  drainOutbox,
} from '../../src/scale/cell-index/index.js';
import type { CellIndex } from '../../src/scale/contracts.js';
import { DEVICE_ID, freshDb, place, device, type PgliteExecutor } from './helpers.js';

const APNS = 'ab12'.repeat(16);

class FlakyIndex extends MemoryCellIndex {
  down = false;
  override async put(entries: Parameters<CellIndex['put']>[0]) {
    if (this.down) throw new Error('dynamo down');
    return super.put(entries);
  }
}

let db: PgliteExecutor;
let outbox: PostgresCellIndexOutbox;
let index: FlakyIndex;

/** Runs 1 h after the rows were written, so they are past the minimum age and any lease. */
const later = () => new Date(Date.now() + 3_600_000);
const deps = (over: Record<string, unknown> = {}) => ({
  devices: new PostgresDeviceStore(db),
  places: new PostgresPlaceStore(db),
  cellIndex: index,
  outbox,
  now: later,
  log: () => {},
  metric: () => {},
  ...over,
});

/** `n` due rows for devices that no longer exist: applying one is a cheap no-op that succeeds. */
const seedGone = (n: number) =>
  db.query(
    `INSERT INTO cell_index_outbox (device_id, op, created_at)
     SELECT gen_random_uuid(), 'update', now() - interval '1 hour' + (g || ' ms')::interval
       FROM generate_series(1, $1::int) g`,
    [n],
  );

const left = () =>
  db.query<{ n: number; max_attempts: number | null }>(
    'SELECT count(*)::int AS n, max(attempts)::int AS max_attempts FROM cell_index_outbox',
    [],
  );

beforeEach(async () => {
  db = await freshDb();
  index = new FlakyIndex();
  outbox = new PostgresCellIndexOutbox(db);
});

describe('outbox drainer: one run drains the whole backlog', () => {
  it('drains 5,000 due rows in a single run, in batches of 500, within the budget', async () => {
    await seedGone(5_000);
    const metrics: Array<Record<string, number>> = [];
    const t0 = Date.now();
    const r = await drainOutbox(
      deps({ metric: (m: string) => void metrics.push(JSON.parse(m) as Record<string, number>) }),
    );
    expect(r).toMatchObject({
      claimed: 5_000,
      applied: 5_000,
      failed: 0,
      batches: 10,
      budgetExhausted: false,
    });
    expect((await left())[0]?.n).toBe(0);
    expect(Date.now() - t0).toBeLessThan(50_000);
    // Metrics are still emitted once per run, after the drain.
    expect(metrics).toHaveLength(1);
    expect(metrics[0]).toMatchObject({
      OutboxMaxAttempts: 0,
      OutboxPending: 0,
      OutboxOldestAgeSeconds: 0,
    });
  }, 120_000);

  it('applies rows with bounded concurrency', async () => {
    await seedGone(40);
    let active = 0;
    let peak = 0;
    const devices = new PostgresDeviceStore(db);
    const slow = Object.create(devices) as PostgresDeviceStore;
    slow.get = async (id: string) => {
      active++;
      peak = Math.max(peak, active);
      await new Promise((r) => setTimeout(r, 2));
      try {
        return await devices.get(id);
      } finally {
        active--;
      }
    };
    const r = await drainOutbox(deps({ devices: slow, concurrency: 8, batchSize: 20 }));
    expect(r.applied).toBe(40);
    expect(peak).toBeGreaterThan(1);
    expect(peak).toBeLessThanOrEqual(8);
  });

  it('stops at the time budget, leaves the rest unclaimed (no attempts burned) and says so', async () => {
    await seedGone(300);
    let t = 0;
    const devices = new PostgresDeviceStore(db);
    const ticking = Object.create(devices) as PostgresDeviceStore;
    ticking.get = async (id: string) => {
      t += 100; // every row "takes" 100 ms
      return devices.get(id);
    };
    const r = await drainOutbox(
      deps({ devices: ticking, clock: () => t, budgetMs: 1_000, batchSize: 50, concurrency: 4 }),
    );
    expect(r.budgetExhausted).toBe(true);
    expect(r.applied).toBeGreaterThan(0);
    expect(r.applied).toBeLessThan(300);
    expect(r.claimed).toBe(r.applied); // rows claimed but not reached were given back
    const rest = (await left())[0];
    expect(rest?.n).toBe(300 - r.applied);
    expect(rest?.max_attempts).toBe(0);
    // The next run (fresh budget) carries on.
    const r2 = await drainOutbox(deps());
    expect(r2.applied).toBe(300 - r.applied);
    expect((await left())[0]?.n).toBe(0);
  }, 120_000);

  it('does not start a batch when the budget is already spent', async () => {
    await seedGone(5);
    const r = await drainOutbox(deps({ budgetMs: 0 }));
    expect(r).toMatchObject({ claimed: 0, applied: 0, batches: 0, budgetExhausted: true });
    expect((await left())[0]).toMatchObject({ n: 5, max_attempts: 0 });
  });
});

describe('outbox drainer: overlapping runs', () => {
  it('two claims at once never take the same row', async () => {
    await seedGone(150);
    const [a, b] = await Promise.all([
      outbox.claimDue(30_000, 100, later()),
      outbox.claimDue(30_000, 100, later()),
    ]);
    const ids = [...a, ...b].map((r) => r.id);
    expect(new Set(ids).size).toBe(ids.length);
    expect(ids).toHaveLength(150);
    expect([...a, ...b].every((r) => r.attempts === 1)).toBe(true);
  });

  it('a live lease hides the rows; once it ends they are claimable again', async () => {
    await seedGone(3);
    const t = later();
    expect(await outbox.claimDue(30_000, 10, t, { leaseMs: 60_000 })).toHaveLength(3);
    expect(await outbox.claimDue(30_000, 10, new Date(t.getTime() + 59_000))).toHaveLength(0);
    const again = await outbox.claimDue(30_000, 10, new Date(t.getTime() + 61_000));
    expect(again.map((r) => r.attempts)).toEqual([2, 2, 2]);
  });

  it('two drainer runs at the same time apply every row exactly once', async () => {
    await seedGone(400);
    const seen = new Map<string, number>();
    const devices = new PostgresDeviceStore(db);
    const counting = Object.create(devices) as PostgresDeviceStore;
    counting.get = async (id: string) => {
      seen.set(id, (seen.get(id) ?? 0) + 1);
      await new Promise((r) => setTimeout(r, 1));
      return devices.get(id);
    };
    const run = () => drainOutbox(deps({ devices: counting, batchSize: 50, concurrency: 4 }));
    const [a, b] = await Promise.all([run(), run()]);
    expect(a.applied + b.applied).toBe(400);
    expect(a.claimed + b.claimed).toBe(400);
    expect(a.applied).toBeGreaterThan(0);
    expect(b.applied).toBeGreaterThan(0);
    expect(Math.max(...seen.values())).toBe(1);
    expect((await left())[0]?.n).toBe(0);
  }, 120_000);
});

describe('outbox drainer: failures', () => {
  it('a failed row is not claimed again in the same run (attempts count once), the rest still drain', async () => {
    const devices = new PostgresDeviceStore(db);
    await devices.create(device());
    await devices.assignPushTarget(DEVICE_ID, { platform: 'ios', token: APNS }, 'en');
    const p = place();
    // The oldest row: the one that fails (a put while the index is down).
    await new PostgresPlaceStore(db).add(p, {
      outbox: { id: randomUUID(), op: 'put', h3Cell: p.h3Cell, placeId: p.id },
    });
    await db.query(`UPDATE cell_index_outbox SET created_at = now() - interval '3 hours'`, []);
    await seedGone(4);
    index.down = true;
    const r = await drainOutbox(deps({ batchSize: 2, concurrency: 1 }));
    expect(r).toMatchObject({ claimed: 5, applied: 4, failed: 1, maxAttempts: 1 });
    const rest = await db.query<{ attempts: number; claimed_until: string | null }>(
      'SELECT attempts, claimed_until FROM cell_index_outbox',
      [],
    );
    // Counted once, and due again at the next run (the lease is released for failed rows).
    expect(rest).toEqual([{ attempts: 1, claimed_until: null }]);

    index.down = false;
    const r2 = await drainOutbox(deps());
    expect(r2).toMatchObject({ claimed: 1, applied: 1, failed: 0 });
    expect((await left())[0]?.n).toBe(0);
  });

  it('stops early when a whole batch fails (index down) instead of burning through the backlog', async () => {
    const devices = new PostgresDeviceStore(db);
    await devices.create(device());
    await devices.assignPushTarget(DEVICE_ID, { platform: 'ios', token: APNS }, 'en');
    const p = place();
    await new PostgresPlaceStore(db).add(p, {
      outbox: { id: randomUUID(), op: 'put', h3Cell: p.h3Cell, placeId: p.id },
    });
    index.down = true;
    const r = await drainOutbox(deps({ batchSize: 1 }));
    expect(r).toMatchObject({ claimed: 1, failed: 1, batches: 1 });
  });
});
