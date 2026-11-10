/**
 * t7 child: the API process. It runs the REAL API handler (backend/src/api createApi) behind
 * node:http with in-memory stores, and owns the cell index (the seeded static devices plus whatever
 * the API writes through). Two ports:
 *   public  the API (`ApiEvent` in, `ApiResult` out), hit by the load driver
 *   control /index/list (the fan-out process pages the cell index here), /stats, /stop
 *
 * The check-in store is an in-memory stand-in for DynamoDB that honours the row TTL the API
 * writes (alert end + retention), so memory is flat once the first alerts have expired, exactly
 * like the real table. Per-(alert, cell) totals are kept forever (they are tiny).
 */
import { randomUUID } from 'node:crypto';
import type { CheckIn } from '@relay/shared';
import { InMemoryAlertStore } from '../../../../../backend/src/adapters/memory/alert-store.js';
import { MemoryDeviceStore } from '../../../../../backend/src/adapters/memory/device-store.js';
import { MemoryPlaceStore } from '../../../../../backend/src/adapters/memory/place-store.js';
import {
  createApi,
  type ApiDeps,
  type CheckInEraser,
  type CheckInLookup,
} from '../../../../../backend/src/api/index.js';
import type { CheckInStore } from '../../../../../backend/src/ports.js';
import type { CellIndexEntry } from '../../../../../backend/src/scale/contracts.js';
import { MemoryCellIndex } from '../../../../../backend/src/scale/cell-index/memory-cell-index.js';
import type { StoredCheckIn } from '../../../../../backend/src/scale/checkins/types.js';
import { generateEntries } from '../../../seed.js';
import { runWorkerMain, sendMessage } from '../../lib/child.js';
import { DEVICE_CRED_KEY, TOKEN_KEY } from './keys.js';
import { deferred, readConfig, sizeOf, sleep, startSampler, startServer } from './common.js';

interface Row {
  h3Cell: string;
  checkedInAt: string;
  idempotencyKey: string;
}

/** TTL-honouring in-memory check-in rows, grouped per alert so expiry drops a whole group at once. */
export class SoakCheckInStore implements CheckInStore, CheckInLookup, CheckInEraser {
  private readonly groups = new Map<string, { until: number; rows: Map<string, Row> }>();
  /** `alertId#cell` -> count; never purged. */
  readonly totalsByCell = new Map<string, number>();
  readonly totalsByAlert = new Map<string, number>();
  rows = 0;
  purgedRows = 0;

  async save(c: CheckIn, retainUntil?: Date): Promise<boolean> {
    let g = this.groups.get(c.alertId);
    if (!g) {
      g = { until: retainUntil?.getTime() ?? Number.MAX_SAFE_INTEGER, rows: new Map() };
      this.groups.set(c.alertId, g);
    } else if (retainUntil && retainUntil.getTime() > g.until) {
      g.until = retainUntil.getTime();
    }
    if (g.rows.has(c.deviceId)) return false;
    g.rows.set(c.deviceId, {
      h3Cell: c.h3Cell,
      checkedInAt: c.checkedInAt,
      idempotencyKey: c.idempotencyKey,
    });
    this.rows++;
    const k = `${c.alertId}#${c.h3Cell}`;
    this.totalsByCell.set(k, (this.totalsByCell.get(k) ?? 0) + 1);
    this.totalsByAlert.set(c.alertId, (this.totalsByAlert.get(c.alertId) ?? 0) + 1);
    return true;
  }

  async get(alertId: string, deviceId: string): Promise<StoredCheckIn | null> {
    const row = this.groups.get(alertId)?.rows.get(deviceId);
    return row ? { alertId, deviceId, ...row } : null;
  }

  async deleteForDevice(deviceId: string): Promise<number> {
    let n = 0;
    for (const g of this.groups.values()) {
      if (g.rows.delete(deviceId)) n++;
    }
    this.rows -= n;
    return n;
  }

  /** The table's TTL deleter: drops every alert group whose retention has passed. */
  purge(now: number): void {
    for (const [id, g] of this.groups) {
      if (g.until < now) {
        this.rows -= g.rows.size;
        this.purgedRows += g.rows.size;
        this.groups.delete(id);
      }
    }
  }

  get groupCount(): number {
    return this.groups.size;
  }
}

const UUID_RE = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;

runWorkerMain(async () => {
  const cfg = readConfig();
  const clock = { now: () => new Date() };
  const devices = new MemoryDeviceStore(clock);
  const places = new MemoryPlaceStore();
  const checkins = new SoakCheckInStore();
  const index = new MemoryCellIndex();
  const trap = new Proxy({}, { get: () => () => Promise.reject(new Error('not used')) }) as never;

  // ----- seed the static devices into the cell index -----
  const t0 = Date.now();
  let batch: CellIndexEntry[] = [];
  let entries = 0;
  for (const e of generateEntries({ devices: cfg.devices, seed: cfg.seed, deviceIds: 'uuid' })) {
    batch.push(e as CellIndexEntry);
    entries++;
    if (batch.length >= 2000) {
      await index.put(batch);
      batch = [];
      await new Promise((r) => setImmediate(r));
    }
  }
  if (batch.length) await index.put(batch);
  console.error(
    `[t7-api] seeded ${cfg.devices} devices, ${entries} entries in ${Date.now() - t0} ms`,
  );

  const handler = createApi({
    devices,
    places,
    alerts: new InMemoryAlertStore(),
    geocoder: trap,
    cellIndex: index,
    checkins,
    checkInRetentionDays: cfg.retentionDays,
    tokenKeys: new Map([[TOKEN_KEY.id, TOKEN_KEY.secret]]),
    deviceCredentialKeys: new Map([[DEVICE_CRED_KEY.id, DEVICE_CRED_KEY.secret]]),
    deviceCredentialKey: DEVICE_CRED_KEY,
    clock,
    newId: randomUUID,
  } as ApiDeps);

  // ----- request accounting -----
  const byRoute = new Map<string, number>();
  let requests = 0;
  let serverErrors = 0;
  const pub = await startServer(async (r) => {
    requests++;
    const res = await handler({
      rawPath: r.path,
      headers: Object.fromEntries(
        Object.entries(r.headers).map(([k, v]) => [k, Array.isArray(v) ? v.join(',') : v]),
      ),
      body: r.body === '' ? null : r.body,
      requestContext: { http: { method: r.method } },
    });
    const key = `${r.method} ${r.path.replace(UUID_RE, '{id}')} ${res.statusCode}`;
    byRoute.set(key, (byRoute.get(key) ?? 0) + 1);
    if (res.statusCode >= 500) serverErrors++;
    return { status: res.statusCode, headers: res.headers, body: res.body };
  });

  const purgeTimer = setInterval(() => checkins.purge(Date.now()), 5000);
  purgeTimer.unref();

  const indexEntries = (): number => {
    const cells = (index as unknown as { cells: Map<string, Map<string, unknown>> }).cells;
    let n = 0;
    for (const m of cells.values()) n += m.size;
    return n;
  };

  const stop = deferred();
  const sampler = startSampler('api', cfg.sampleMs, () => ({
    checkinRows: checkins.rows,
    checkinAlertGroups: checkins.groupCount,
    checkinTotalsKeys: checkins.totalsByCell.size,
    tombstones: sizeOf(devices, 'tombstones'),
    devices: sizeOf(devices, 'devices'),
    deviceMeta: sizeOf(devices, 'meta'),
    placeRows: sizeOf(places, 'places'),
    cellIndexEntries: indexEntries(),
  }));

  const ctl = await startServer(async (r) => {
    if (r.method === 'GET' && r.path === '/index/list') {
      const cell = r.query.get('cell') ?? '';
      const cursor = r.query.get('cursor') ?? undefined;
      const limit = Number(r.query.get('limit') ?? 1000);
      return { body: await index.listForCell(cell, cursor, limit) };
    }
    if (r.method === 'GET' && r.path === '/stats') {
      return {
        body: {
          requests,
          serverErrors,
          byRoute: Object.fromEntries(byRoute),
          totalsByAlert: Object.fromEntries(checkins.totalsByAlert),
          rows: checkins.rows,
          purgedRows: checkins.purgedRows,
          tombstones: sizeOf(devices, 'tombstones'),
        },
      };
    }
    if (r.method === 'POST' && r.path === '/stop') {
      stop.resolve();
      return { body: { ok: true } };
    }
    return { status: 404, body: 'not found' };
  });

  await sendMessage({ kind: 'ready', pub: pub.port, ctl: ctl.port, entries });
  await stop.promise;
  sampler();
  clearInterval(purgeTimer);
  await sleep(50);
  pub.close();
  ctl.close();
  return {
    requests,
    serverErrors,
    byRoute: Object.fromEntries(byRoute),
    totalsByAlert: Object.fromEntries(checkins.totalsByAlert),
  };
});
