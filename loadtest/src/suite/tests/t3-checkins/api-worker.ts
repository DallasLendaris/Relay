/**
 * t3 API process: the REAL check-in API handler (createApi) behind a minimal node:http server.
 * The handler is called with an API Gateway v2 event; its logic is untouched. The check-in store
 * is the real MemoryCheckInStore (conditional, first write wins) plus a change stream: each first
 * save is batched (1,000 or 25 ms) and sent over TCP to an aggregator process.
 *
 * Idempotency across processes: the load generators route a device to process (deviceNumber % K),
 * so one process owns every request of a device (a sticky load balancer, like partitioning the
 * table by alertId#deviceId). No shared store, no IPC on the request path.
 */
import { randomUUID } from 'node:crypto';
import http from 'node:http';
import net from 'node:net';
import { monitorEventLoopDelay } from 'node:perf_hooks';
import type { CheckIn } from '@relay/shared';
import { MemoryCheckInStore } from '../../../../../backend/src/adapters/memory/checkin-store.js';
import { createApi, type ApiDeps } from '../../../../../backend/src/api/index.js';
import { classOf, deviceIdOf, DEVICE_CRED_KEY, TOKEN_KEY, nowMs, type ApiDone } from './common.js';

interface Init {
  type: 'init';
  n: number;
  aggPort: number;
}

class StreamingStore extends MemoryCheckInStore {
  accepted = 0;
  duplicateSaves = 0;
  lastAcceptedMs = 0;
  readonly perCell = new Map<string, number>();
  onAccepted: (deviceId: string, cell: string) => void = () => {};
  override async save(c: CheckIn, retainUntil?: Date): Promise<boolean> {
    const first = await super.save(c, retainUntil);
    if (!first) {
      this.duplicateSaves++;
      return false;
    }
    this.accepted++;
    this.lastAcceptedMs = nowMs();
    this.perCell.set(c.h3Cell, (this.perCell.get(c.h3Cell) ?? 0) + 1);
    this.onAccepted(c.deviceId, c.h3Cell);
    return true;
  }
}

const forbidden = (what: string): never => {
  throw new Error(`the stateless check-in route must not touch ${what}`);
};
const trap = (what: string) => new Proxy({}, { get: () => () => forbidden(what) }) as never;

function start(init: Init): void {
  const store = new StreamingStore();
  const tombstones: string[] = [];
  for (let i = 0; i < init.n; i++) if (classOf(i) === 'tombstoned') tombstones.push(deviceIdOf(i));
  const devices = new Proxy(
    { listDeletedDevices: async (): Promise<string[]> => tombstones },
    {
      get: (t, k) =>
        k === 'listDeletedDevices' ? t.listDeletedDevices : () => forbidden('the device store'),
    },
  ) as never;
  const handler = createApi({
    devices,
    places: trap('the place store'),
    alerts: trap('the alert store'),
    geocoder: trap('the geocoder'),
    cellIndex: trap('the cell index'),
    checkins: store,
    tokenKeys: new Map([[TOKEN_KEY.id, TOKEN_KEY.secret]]),
    deviceCredentialKeys: new Map([[DEVICE_CRED_KEY.id, DEVICE_CRED_KEY.secret]]),
    clock: { now: () => new Date() },
    newId: randomUUID,
  } as ApiDeps);

  // ----- change stream to the aggregator -----
  const sock = net.connect(init.aggPort, '127.0.0.1');
  sock.setNoDelay(true);
  let lines: string[] = [];
  const flush = (): void => {
    if (lines.length === 0) return;
    sock.write(lines.join('') + '-\n');
    lines = [];
  };
  store.onAccepted = (deviceId, cell) => {
    lines.push(`${deviceId} ${cell}\n`);
    if (lines.length >= 1000) flush();
  };
  const timer = setInterval(flush, 25);

  // ----- HTTP server -----
  let requests = 0;
  const server = http.createServer((req, res) => {
    requests++;
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => {
      handler({
        rawPath: req.url ?? '/',
        headers: req.headers as Record<string, string>,
        body: Buffer.concat(chunks).toString('utf8'),
        requestContext: { http: { method: req.method ?? 'GET' } },
      })
        .then((r) => {
          res.writeHead(r.statusCode, {
            ...r.headers,
            'content-length': Buffer.byteLength(r.body),
          });
          res.end(r.body);
        })
        .catch(() => {
          res.writeHead(500);
          res.end();
        });
    });
  });
  server.keepAliveTimeout = 120_000;
  server.headersTimeout = 125_000;
  const loop = monitorEventLoopDelay({ resolution: 5 });
  loop.enable();
  server.listen(0, '127.0.0.1', () => {
    const addr = server.address();
    process.send?.({ type: 'ready', port: typeof addr === 'object' && addr ? addr.port : 0 });
  });

  process.on('message', (m: { type: string }) => {
    if (m.type !== 'finish') return;
    clearInterval(timer);
    flush();
    sock.write('END\n', () => {
      const done: ApiDone = {
        type: 'done',
        accepted: store.accepted,
        duplicateSaves: store.duplicateSaves,
        requests,
        perCell: [...store.perCell],
        lastAcceptedMs: store.lastAcceptedMs,
        loopDelayP99Ms: loop.percentile(99) / 1e6,
        maxRssKb: process.resourceUsage().maxRSS,
      };
      process.send?.(done);
    });
  });
}

process.on('disconnect', () => process.exit(0));
process.on('message', (m: Init) => {
  if (m.type === 'init') start(m);
});
process.send?.({ type: 'loaded' });
