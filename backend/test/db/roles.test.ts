import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { afterEach, beforeAll, describe, expect, it } from 'vitest';
import { PostgresAlertStore } from '../../src/adapters/postgres/alert-store.js';
import { PostgresDeviceStore } from '../../src/adapters/postgres/device-store.js';
import { PostgresPlaceStore } from '../../src/adapters/postgres/place-store.js';
import { splitStatements } from '../../src/db/sql.js';
import {
  DEVICE_ID,
  MIGRATIONS_DIR,
  alert,
  device,
  freshDb,
  place,
  type PgliteExecutor,
} from './helpers.js';

/**
 * Least-privilege database roles (migration 0011, audit PRIV-6). Each role runs the REAL store
 * code its function runs (SET ROLE on PGlite), so a statement an adapter needs that the role was
 * not granted fails here, and so does a grant that is wider than intended.
 */
const NOW = new Date('2026-01-02T05:00:00.000Z');
const ALERT = alert();
const ALL_ROLES = [
  'relay_api',
  'relay_ingest',
  'relay_ingest_inline',
  'relay_fanout',
  'relay_sender',
  'relay_ops',
  'relay_latejoin',
];

let db: PgliteExecutor;

/** Runs fn as the given role. */
async function as<T>(role: string, fn: () => Promise<T>): Promise<T> {
  await db.pg.exec(`SET ROLE ${role}`);
  try {
    return await fn();
  } finally {
    await db.pg.exec('RESET ROLE');
  }
}

/** True when running sql as role is refused by a privilege check (and not by anything else). */
async function denied(role: string, sql: string): Promise<boolean> {
  try {
    await as(role, () => db.pg.exec(sql));
    return false;
  } catch (err) {
    expect(String(err)).toMatch(/permission denied/);
    return true;
  }
}

const rerunMigration = async () => {
  const sql = await readFile(`${MIGRATIONS_DIR}/0011_roles.sql`, 'utf8');
  for (const statement of splitStatements(sql)) await db.query(statement, []);
};

beforeAll(async () => {
  db = await freshDb();
  // Seeded as the admin: one device, one place, one alert.
  await new PostgresDeviceStore(db).create(device());
  await new PostgresPlaceStore(db).add(place());
  await new PostgresAlertStore(db).save(ALERT);
});

afterEach(async () => {
  await db.pg.exec('RESET ROLE');
});

describe('0011_roles', () => {
  it('creates the seven roles NOLOGIN, so nothing can connect before db-roles sets a password', async () => {
    const rows = await db.query<{ rolname: string; rolcanlogin: boolean }>(
      "SELECT rolname, rolcanlogin FROM pg_roles WHERE rolname LIKE 'relay\\_%' ORDER BY rolname",
      [],
    );
    expect(rows.map((r) => r.rolname)).toEqual([...ALL_ROLES].sort());
    expect(rows.every((r) => !r.rolcanlogin)).toBe(true);
  });

  it('is safe to run again and leaves the grants as they were', async () => {
    await rerunMigration();
    await rerunMigration();
    expect(await denied('relay_ingest', 'SELECT 1 FROM places')).toBe(true);
    await as('relay_ingest_inline', () => db.pg.exec('SELECT 1 FROM places'));
  });

  it('gives no role the right to create objects or to see the migrations table', async () => {
    for (const role of ALL_ROLES) {
      expect(await denied(role, 'CREATE TABLE rogue (a int)')).toBe(true);
      expect(await denied(role, 'SELECT * FROM schema_migrations')).toBe(true);
    }
  });
});

describe('relay_api: devices and places read/write, alerts read', () => {
  it('runs the device, place and alert-read statements of the API', async () => {
    await as('relay_api', async () => {
      const devices = new PostgresDeviceStore(db);
      const places = new PostgresPlaceStore(db);
      const other = '99999999-9999-4999-8999-999999999999';
      const d = device({ id: other, credentialHash: 'c'.repeat(64), expoPushToken: null });
      await devices.create(d);
      await devices.update({ ...d, language: 'es' });
      await devices.assignPushTarget(other, { platform: 'ios', token: 'apns-token' }, 'es');
      expect(await devices.get(other)).toMatchObject({ pushToken: 'apns-token' });
      const p = place({ id: '55555555-5555-4555-8555-555555555555', deviceId: other });
      await places.add(p);
      expect(await places.listForDevice(other)).toHaveLength(1);
      expect(await places.remove(other, p.id)).toBe(true);
      const alerts = new PostgresAlertStore(db);
      expect(await alerts.get(ALERT.id)).not.toBeNull();
      expect(await alerts.listActiveForCells(ALERT.area.h3Cells, NOW)).not.toHaveLength(0);
    });
  });

  it('runs the erasure, move-rule and outbox statements (DELETE /devices/me, push-target move)', async () => {
    const a = '88888888-8888-4888-8888-888888888881';
    const b = '88888888-8888-4888-8888-888888888882';
    await as('relay_api', async () => {
      const devices = new PostgresDeviceStore(db);
      const places = new PostgresPlaceStore(db);
      await devices.create(device({ id: a, credentialHash: 'd'.repeat(64), expoPushToken: null }));
      await devices.create(device({ id: b, credentialHash: 'e'.repeat(64), expoPushToken: null }));
      const target = { platform: 'ios', token: 'moving-token' } as const;
      await devices.assignPushTarget(a, target, 'en');
      // Moving the token to b clears it on a, records the move and prunes (push_token_moves).
      const moved = await devices.assignPushTarget(b, target, 'en', { outbox: true });
      expect(moved.displaced).toEqual([a]);
      expect(await devices.pushTargetMeta(b)).not.toBeNull();
      const p = place({ id: '55555555-5555-4555-8555-555555555556', deviceId: b });
      const entry = (op: 'put' | 'remove') => ({
        outbox: { id: randomUUID(), op, h3Cell: p.h3Cell, placeId: p.id },
      });
      await places.add(p, entry('put'));
      await places.remove(b, p.id, entry('remove'));
      await places.add({ ...p, id: '55555555-5555-4555-8555-555555555557' });
      expect(await places.removeAllForDevice(b)).toBe(1);
      expect(await devices.deleteDevice(b, NOW)).toBe(true);
      expect(await devices.listDeletedDevices()).toContain(b);
    });
  });

  it('cannot write alerts', async () => {
    expect(await denied('relay_api', "UPDATE alerts SET state = 'cancelled'")).toBe(true);
    expect(await denied('relay_api', 'DELETE FROM alert_cells')).toBe(true);
    await expect(as('relay_api', () => new PostgresAlertStore(db).save(ALERT))).rejects.toThrow(
      /permission denied/,
    );
  });
});

describe('relay_ingest: alerts only', () => {
  it('runs the ingest store statements: save (upsert, cells), get, findBySourceId', async () => {
    await as('relay_ingest', async () => {
      const store = new PostgresAlertStore(db);
      await store.save({ ...ALERT, event: 'Updated event' });
      await store.save(
        alert({ id: '44444444-4444-4444-8444-444444444444', sourceId: 'urn:other' }),
      );
      expect((await store.get(ALERT.id))?.event).toBe('Updated event');
      expect(await store.findBySourceId('urn:other')).not.toBeNull();
      expect(await store.listActiveForCells(ALERT.area.h3Cells, NOW)).not.toHaveLength(0);
    });
  });

  it('is denied places and devices, and cannot delete alerts', async () => {
    expect(await denied('relay_ingest', 'SELECT * FROM places')).toBe(true);
    expect(await denied('relay_ingest', 'SELECT * FROM devices')).toBe(true);
    expect(await denied('relay_ingest', 'SELECT count(*) FROM places')).toBe(true);
    expect(await denied('relay_ingest', 'DELETE FROM alerts')).toBe(true);
    expect(await denied('relay_ingest', 'DELETE FROM alert_source_ids')).toBe(true);
  });
});

describe('relay_ingest_inline: ingest plus read of devices and places (dev inline planning)', () => {
  it('plans (reads places and devices) and saves alerts', async () => {
    await as('relay_ingest_inline', async () => {
      const matches = await new PostgresPlaceStore(db).listMatchesForCells([place().h3Cell]);
      expect(matches.length).toBeGreaterThan(0);
      expect(await new PostgresDeviceStore(db).get(DEVICE_ID)).not.toBeNull();
      await new PostgresAlertStore(db).save(ALERT);
    });
  });

  it('cannot write devices or places', async () => {
    expect(await denied('relay_ingest_inline', "UPDATE devices SET language = 'es'")).toBe(true);
    expect(await denied('relay_ingest_inline', 'DELETE FROM places')).toBe(true);
  });
});

describe('relay_fanout: alerts read', () => {
  it('reads alerts the way the planner and shard workers do', async () => {
    await as('relay_fanout', async () => {
      const store = new PostgresAlertStore(db);
      expect(await store.get(ALERT.id)).not.toBeNull();
      expect(await store.findBySourceId(ALERT.sourceId)).not.toBeNull();
      expect(await store.listActiveForCells(ALERT.area.h3Cells, NOW)).not.toHaveLength(0);
    });
  });

  it('cannot write alerts or read devices and places', async () => {
    expect(await denied('relay_fanout', "UPDATE alerts SET state = 'cancelled'")).toBe(true);
    expect(await denied('relay_fanout', 'DELETE FROM alerts')).toBe(true);
    await expect(as('relay_fanout', () => new PostgresAlertStore(db).save(ALERT))).rejects.toThrow(
      /permission denied/,
    );
    expect(await denied('relay_fanout', 'SELECT * FROM devices')).toBe(true);
    expect(await denied('relay_fanout', 'SELECT * FROM places')).toBe(true);
  });
});

describe('relay_sender: alerts read, push-token clearing, places read', () => {
  it('runs the sender statements: read alert, dead-target cleanup (get, clear, list places)', async () => {
    await as('relay_sender', async () => {
      expect(await new PostgresAlertStore(db).get(ALERT.id)).not.toBeNull();
      const devices = new PostgresDeviceStore(db);
      expect(await devices.get(DEVICE_ID)).not.toBeNull();
      expect(await new PostgresPlaceStore(db).listForDevice(DEVICE_ID)).toHaveLength(1);
      await devices.clearPushToken('ExponentPushToken[abc]');
    });
    expect(await new PostgresDeviceStore(db).get(DEVICE_ID)).toMatchObject({
      expoPushToken: null,
      pushToken: null,
    });
  });

  it('may clear the push target columns and nothing else on devices', async () => {
    expect(await denied('relay_sender', "UPDATE devices SET language = 'es'")).toBe(true);
    expect(await denied('relay_sender', "UPDATE devices SET credential_hash = 'x'")).toBe(true);
    expect(await denied('relay_sender', 'DELETE FROM devices')).toBe(true);
    expect(await denied('relay_sender', 'INSERT INTO devices SELECT * FROM devices')).toBe(true);
  });

  it('cannot write places or alerts', async () => {
    expect(await denied('relay_sender', "UPDATE places SET label = 'x'")).toBe(true);
    expect(await denied('relay_sender', 'DELETE FROM places')).toBe(true);
    expect(await denied('relay_sender', "UPDATE alerts SET state = 'cancelled'")).toBe(true);
  });
});

describe('relay_ops: devices and places read (repair, drainer, backfill)', () => {
  it('reads what the index repair reads', async () => {
    await as('relay_ops', async () => {
      expect(await new PostgresDeviceStore(db).get(DEVICE_ID)).not.toBeNull();
      const places = new PostgresPlaceStore(db);
      expect(await places.listForDevice(DEVICE_ID)).toHaveLength(1);
      expect(await places.listMatchesForCells([place().h3Cell])).not.toHaveLength(0);
    });
  });

  it('cannot write devices or places, nor read alerts', async () => {
    expect(await denied('relay_ops', "UPDATE devices SET language = 'es'")).toBe(true);
    expect(await denied('relay_ops', 'DELETE FROM places')).toBe(true);
    expect(await denied('relay_ops', 'SELECT * FROM alerts')).toBe(true);
    expect(await denied('relay_ops', 'SELECT * FROM alert_cells')).toBe(true);
  });
});

describe('relay_latejoin: read-only devices, places and alerts', () => {
  it('runs the late-join statements: device, push-target timestamps, places, active alerts', async () => {
    await as('relay_latejoin', async () => {
      const devices = new PostgresDeviceStore(db);
      expect(await devices.get(DEVICE_ID)).not.toBeNull();
      expect(await devices.pushTargetMeta(DEVICE_ID)).not.toBeNull();
      expect(await new PostgresPlaceStore(db).listForDevice(DEVICE_ID)).toHaveLength(1);
      const alerts = new PostgresAlertStore(db);
      expect(await alerts.listActiveForCells(ALERT.area.h3Cells, NOW)).not.toHaveLength(0);
      expect(await alerts.get(ALERT.id)).not.toBeNull();
    });
  });

  it('writes nothing and reads nothing else', async () => {
    expect(await denied('relay_latejoin', "UPDATE devices SET language = 'es'")).toBe(true);
    expect(await denied('relay_latejoin', 'UPDATE devices SET push_token = NULL')).toBe(true);
    expect(await denied('relay_latejoin', 'DELETE FROM places')).toBe(true);
    expect(await denied('relay_latejoin', "UPDATE alerts SET state = 'cancelled'")).toBe(true);
    expect(await denied('relay_latejoin', 'SELECT * FROM push_token_moves')).toBe(true);
    expect(await denied('relay_latejoin', 'SELECT * FROM deleted_devices')).toBe(true);
    expect(await denied('relay_latejoin', 'SELECT * FROM cell_index_outbox')).toBe(true);
  });
});

describe('tables added by other migrations (granted by name when present)', () => {
  it('grants tables from later migrations once a re-run finds them', async () => {
    // The tombstones and the push-token move log arrive in 0008 and 0010 (the outbox in 0009).
    // Create them the way those migrations do where missing, then re-run the grants.
    await db.query(
      'CREATE TABLE IF NOT EXISTS deleted_devices (device_id uuid PRIMARY KEY, deleted_at timestamptz NOT NULL)',
      [],
    );
    await db.query(
      'CREATE TABLE IF NOT EXISTS push_token_moves (token_hash text NOT NULL, device_id uuid NOT NULL, moved_at timestamptz NOT NULL)',
      [],
    );
    // Some of these already exist once 0008-0010 are merged; IF NOT EXISTS keeps the test valid
    // either way. What matters is that a re-run grants them.
    await rerunMigration();

    await as('relay_api', async () => {
      await db.pg.exec(`INSERT INTO cell_index_outbox (device_id, op) VALUES ('${DEVICE_ID}', 'put')`);
      await db.pg.exec(`INSERT INTO deleted_devices VALUES ('${DEVICE_ID}', now())`);
      await db.pg.exec(`INSERT INTO push_token_moves VALUES ('h', '${DEVICE_ID}', now())`);
      await db.pg.exec("DELETE FROM push_token_moves WHERE token_hash = 'h'");
    });
    await as('relay_ops', async () => {
      await db.pg.exec('SELECT * FROM deleted_devices');
      await db.pg.exec('UPDATE cell_index_outbox SET created_at = now()');
      await db.pg.exec('DELETE FROM cell_index_outbox');
    });
    expect(
      await denied('relay_ops', `INSERT INTO deleted_devices VALUES ('${DEVICE_ID}', now())`),
    ).toBe(true);
    expect(await denied('relay_ops', 'SELECT * FROM push_token_moves')).toBe(true);
    for (const role of ['relay_ingest', 'relay_ingest_inline', 'relay_fanout', 'relay_sender']) {
      expect(await denied(role, 'SELECT * FROM cell_index_outbox')).toBe(true);
      expect(await denied(role, 'SELECT * FROM deleted_devices')).toBe(true);
    }
  });
});

describe('splitStatements', () => {
  it('keeps a dollar-quoted DO block whole and still splits around it', () => {
    const parts = splitStatements(
      "SELECT 1;\n-- note\nDO $$ BEGIN PERFORM 1; PERFORM 2; END $$;\nSELECT $1, 'a'; SELECT $tag$ x; y $tag$",
    );
    expect(parts).toEqual([
      'SELECT 1',
      'DO $$ BEGIN PERFORM 1; PERFORM 2; END $$',
      "SELECT $1, 'a'",
      'SELECT $tag$ x; y $tag$',
    ]);
  });
});
