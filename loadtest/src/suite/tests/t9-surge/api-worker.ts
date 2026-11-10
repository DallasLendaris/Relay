/**
 * t9 child: the API process. The REAL API handler (backend/src/api createApi) behind node:http, with
 * the indexed in-memory stores, the fake geocoder, and the late-join queue the API writes intents to.
 * Two ports:
 *   public   the API, hit by the load driver
 *   control  (runCtlWorker) overlay: a cell's index entries for the shard workers; lj_poll and
 *            lj_context for the late-join process; stats
 *
 * Control: init, overlay, lj_poll, lj_context, stats, stop.
 */
import { randomUUID } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../../../../backend/src/adapters/memory/alert-store.js';
import { createApi, type ApiDeps } from '../../../../../backend/src/api/index.js';
import type { CheckInStore } from '../../../../../backend/src/ports.js';
import {
  type LateJoinIntent,
  type LateJoinQueue,
} from '../../../../../backend/src/scale/latejoin/intent.js';
import { HashList } from '../t2-fanout/verify.js';
import { startServer } from '../t7-soak/common.js';
import { DEVICE_CRED_KEY, TOKEN_KEY } from '../t7-soak/keys.js';
import { runCtlWorker } from '../t8-concurrent/ctl.js';
import { FakeGeocoder } from './geocoder.js';
import { IndexedDeviceStore, IndexedPlaceStore, OverlayCellIndex } from './stores.js';

/** FIFO of intents with their enqueue time; the late-join process polls it (SQS stand-in). */
class IntentQueue implements LateJoinQueue {
  private items: Array<{ intent: LateJoinIntent; at: number }> = [];
  private head = 0;
  enqueued = 0;
  polled = 0;
  byReason: Record<string, number> = {};

  async enqueue(intent: LateJoinIntent): Promise<void> {
    this.items.push({ intent, at: Date.now() });
    this.enqueued++;
    this.byReason[intent.reason] = (this.byReason[intent.reason] ?? 0) + 1;
  }

  poll(max: number): Array<{ intent: LateJoinIntent; at: number }> {
    const out = this.items.slice(this.head, this.head + max);
    this.head += out.length;
    this.polled += out.length;
    if (this.head > 10_000 && this.head * 2 > this.items.length) {
      this.items = this.items.slice(this.head);
      this.head = 0;
    }
    return out;
  }

  get waiting(): number {
    return this.items.length - this.head;
  }
}

const UUID_RE = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;

class NoCheckIns implements CheckInStore {
  async save(): Promise<boolean> {
    return true;
  }
  async get(): Promise<null> {
    return null;
  }
  async deleteForDevice(): Promise<number> {
    return 0;
  }
}

runCtlWorker(async () => {
  const clock = { now: () => new Date() };
  const devices = new IndexedDeviceStore(clock);
  const places = new IndexedPlaceStore();
  const overlay = new OverlayCellIndex();
  const queue = new IntentQueue();
  const byRoute = new Map<string, number>();
  let requests = 0;
  let serverErrors = 0;
  let pub: { port: number; close(): void } | undefined;
  let geocoder: FakeGeocoder | undefined;

  return {
    handlers: {
      init: async (p: { alertsFile: string; fcmProjects: string[] }) => {
        const alerts = new InMemoryAlertStore();
        const list = JSON.parse(readFileSync(p.alertsFile, 'utf8')) as Alert[];
        for (const a of list) await alerts.save(a);
        const area = new Set<string>(list.flatMap((a) => a.area.h3Cells));
        geocoder = new FakeGeocoder(area);
        const deps: ApiDeps = {
          ...(p.fcmProjects.length > 0 ? { fcmProjectIds: p.fcmProjects } : {}),
          devices,
          places,
          alerts,
          geocoder,
          checkins: new NoCheckIns() as unknown as ApiDeps['checkins'],
          tokenKeys: new Map([[TOKEN_KEY.id, TOKEN_KEY.secret]]),
          deviceCredentialKeys: new Map([[DEVICE_CRED_KEY.id, DEVICE_CRED_KEY.secret]]),
          deviceCredentialKey: DEVICE_CRED_KEY,
          cellIndex: overlay,
          lateJoin: { queue },
          clock,
          newId: randomUUID,
        };
        const handler = createApi(deps);
        pub = await startServer(async (r) => {
          requests++;
          const res = await handler({
            rawPath: r.path,
            headers: Object.fromEntries(
              Object.entries(r.headers).map(([k, v]) => [k, Array.isArray(v) ? v.join(',') : v]),
            ),
            body: r.body === '' ? null : r.body,
            requestContext: { http: { method: r.method } },
          } as Parameters<typeof handler>[0]);
          const key = `${r.method} ${r.path.replace(UUID_RE, '{id}')} ${res.statusCode}`;
          byRoute.set(key, (byRoute.get(key) ?? 0) + 1);
          if (res.statusCode >= 500) serverErrors++;
          return { status: res.statusCode, headers: res.headers, body: res.body };
        });
        return { pub: pub.port };
      },
      overlay: (p: { cell: string }) => ({ entries: overlay.entriesForCell(p.cell) }),
      lj_poll: (p: { max: number }) => ({ items: queue.poll(p.max), waiting: queue.waiting }),
      lj_context: async (p: { deviceId: string }) => {
        const device = await devices.get(p.deviceId);
        const meta = await devices.pushTargetMeta(p.deviceId);
        return {
          device,
          places: device ? await places.listForDevice(p.deviceId) : [],
          meta: meta
            ? { updatedAt: meta.updatedAt.toISOString(), changedAt: meta.changedAt.toISOString() }
            : null,
        };
      },
      /**
       * Heavier: scans the overlay. Read once at the end. Writes the token hash and cell number of
       * every entry with a push target to files, for the planned-versus-late classification.
       */
      final: (p: { outDir: string }) => {
        const list = new HashList();
        const ids: number[] = [];
        const cellNo = new Map<string, number>();
        for (const t of overlay.targets()) {
          list.add(t.token);
          let n = cellNo.get(t.h3Cell);
          if (n === undefined) cellNo.set(t.h3Cell, (n = cellNo.size));
          ids.push(n);
        }
        const tokensFile = join(p.outDir, 'api-target-tokens.bin');
        const cellsFile = join(p.outDir, 'api-target-cells.bin');
        const words = list.toArray();
        writeFileSync(tokensFile, Buffer.from(words.buffer, words.byteOffset, words.byteLength));
        const u = Uint32Array.from(ids);
        writeFileSync(cellsFile, Buffer.from(u.buffer, u.byteOffset, u.byteLength));
        return {
          overlayWithTarget: list.n,
          cells: [...cellNo.keys()],
          files: { tokens: tokensFile, cells: cellsFile },
        };
      },
      stats: () => ({
        requests,
        serverErrors,
        byRoute: Object.fromEntries(byRoute),
        devices: devices.size,
        devicesHolding: devices.holding,
        places: places.size,
        overlayEntries: overlay.size,
        intents: {
          enqueued: queue.enqueued,
          polled: queue.polled,
          waiting: queue.waiting,
          byReason: queue.byReason,
        },
        geocoder: { calls: geocoder?.calls ?? 0, outside: geocoder?.outside ?? 0 },
      }),
    },
    onStop: () => pub?.close(),
  };
});
