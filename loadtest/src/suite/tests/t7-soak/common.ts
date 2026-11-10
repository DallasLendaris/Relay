/**
 * Shared pieces of the t7 soak test: the config handed to every child process, a tiny HTTP
 * client and server (the processes talk over 127.0.0.1 HTTP, which works the same on Windows),
 * the per-process sampler (RSS, heap after GC, event-loop delay p99, cache sizes), the alert
 * areas and the order-independent token hashes used by the exactly-once check.
 */
import http from 'node:http';
import { monitorEventLoopDelay } from 'node:perf_hooks';
import { sendMessage } from '../../lib/child.js';
import { COUNTY_BOUNDS } from '../../../seed.js';

// ---------- Config ----------

export interface T7Config {
  scale: 'full' | 'smoke';
  minutes: number;
  durationS: number;
  /** Static seed devices (never changed by the API load). */
  devices: number;
  seed: number;
  /** Seconds between alert events. */
  alertIntervalS: number;
  /** Alert lifetime (`expires`) in seconds; also the check-in token lifetime. */
  alertLifeS: number;
  /** Check-in row retention after the alert ends, in days (a fraction: the soak keeps rows for minutes). */
  retentionDays: number;
  rates: { checkins: number; placeOps: number; tokenOps: number; erasures: number };
  sampleMs: number;
  /** Managed devices: erasure pool, flappers (anchor in an alert area), quiet (token rotation). */
  eCount: number;
  fCount: number;
  qCount: number;
  fcmRate: number;
}

export function readConfig(): T7Config {
  return JSON.parse(process.env['T7_CONFIG'] ?? '{}') as T7Config;
}

export interface T7Ports {
  apnsUrl?: string;
  fcmUrl?: string;
  apiPublic?: number;
  apiCtl?: number;
  load?: number;
  fanout?: number;
}

export function readPorts(): T7Ports {
  return JSON.parse(process.env['T7_PORTS'] ?? '{}') as T7Ports;
}

export const sleep = (ms: number): Promise<void> =>
  ms <= 0 ? Promise.resolve() : new Promise((r) => setTimeout(r, ms));

// ---------- HTTP ----------

const agent = new http.Agent({ keepAlive: true, maxSockets: 256 });

export interface HttpReply {
  status: number;
  body: string;
}

export function call(
  port: number,
  method: string,
  path: string,
  body?: string,
  headers: Record<string, string> = {},
  timeoutMs = 120_000,
): Promise<HttpReply> {
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: '127.0.0.1',
        port,
        method,
        path,
        agent,
        headers: {
          ...headers,
          ...(body !== undefined
            ? { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) }
            : {}),
        },
        timeout: timeoutMs,
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (c: Buffer) => chunks.push(c));
        res.on('end', () =>
          resolve({ status: res.statusCode ?? 0, body: Buffer.concat(chunks).toString('utf8') }),
        );
        res.on('error', reject);
      },
    );
    req.on('timeout', () => req.destroy(new Error(`timeout ${method} ${path}`)));
    req.on('error', reject);
    if (body !== undefined) req.write(body);
    req.end();
  });
}

export async function callJson<T>(
  port: number,
  method: string,
  path: string,
  body?: unknown,
  timeoutMs?: number,
): Promise<T> {
  const r = await call(
    port,
    method,
    path,
    body === undefined ? undefined : JSON.stringify(body),
    {},
    timeoutMs,
  );
  if (r.status < 200 || r.status >= 300) {
    throw new Error(`${method} ${path} -> ${r.status} ${r.body.slice(0, 200)}`);
  }
  return (r.body ? JSON.parse(r.body) : null) as T;
}

export interface Req {
  method: string;
  path: string;
  query: URLSearchParams;
  body: string;
  headers: http.IncomingHttpHeaders;
}
export interface Reply {
  status?: number;
  headers?: Record<string, string>;
  /** A string is sent as is; anything else as JSON. */
  body?: unknown;
}

export interface Server {
  port: number;
  close(): void;
}

export async function startServer(handler: (r: Req) => Promise<Reply> | Reply): Promise<Server> {
  const server = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => {
      const url = new URL(req.url ?? '/', 'http://127.0.0.1');
      Promise.resolve(
        handler({
          method: req.method ?? 'GET',
          path: url.pathname,
          query: url.searchParams,
          body: Buffer.concat(chunks).toString('utf8'),
          headers: req.headers,
        }),
      ).then(
        (r) => {
          const text =
            r.body === undefined
              ? ''
              : typeof r.body === 'string'
                ? r.body
                : JSON.stringify(r.body);
          res.writeHead(r.status ?? 200, {
            ...(r.body !== undefined && typeof r.body !== 'string'
              ? { 'content-type': 'application/json' }
              : {}),
            ...r.headers,
          });
          res.end(text);
        },
        (e: unknown) => {
          res.writeHead(500, { 'content-type': 'text/plain' });
          res.end(e instanceof Error ? `${e.name}: ${e.message}` : String(e));
        },
      );
    });
  });
  server.keepAliveTimeout = 120_000;
  server.headersTimeout = 125_000;
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const addr = server.address();
  const port = typeof addr === 'object' && addr ? addr.port : 0;
  return { port, close: () => server.close() };
}

/** A promise another part of the child resolves, used for "stop when the parent says so". */
export function deferred(): { promise: Promise<void>; resolve(): void } {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

// ---------- Sampler ----------

export interface Sample {
  kind: 'sample';
  proc: string;
  /** Epoch ms. */
  t: number;
  rssMb: number;
  /** heapUsed right after a full GC. */
  heapMb: number;
  eldP99Ms: number;
  eldMaxMs: number;
  caches: Record<string, number>;
}

const mb = (n: number): number => Math.round((n / 1024 / 1024) * 100) / 100;

/** Samples this process every `everyMs` and sends each sample to the parent. Returns a stop function. */
export function startSampler(
  proc: string,
  everyMs: number,
  extra: () => Record<string, number>,
): () => void {
  const h = monitorEventLoopDelay({ resolution: 10 });
  h.enable();
  let stopped = false;
  const gc = (globalThis as { gc?: () => void }).gc;
  void (async () => {
    while (!stopped) {
      await sleep(everyMs);
      if (stopped) break;
      // Read the delay histogram first: the forced GC below is itself a pause, not workload.
      const p99 = h.count > 0 ? h.percentile(99) / 1e6 : 0;
      const max = h.count > 0 ? h.max / 1e6 : 0;
      gc?.();
      const m = process.memoryUsage();
      let caches: Record<string, number> = {};
      try {
        caches = extra();
      } catch {
        /* a cache probe never breaks the sampler */
      }
      const s: Sample = {
        kind: 'sample',
        proc,
        t: Date.now(),
        rssMb: mb(m.rss),
        heapMb: mb(m.heapUsed),
        eldP99Ms: Math.round(p99 * 10) / 10,
        eldMaxMs: Math.round(max * 10) / 10,
        caches,
      };
      void sendMessage(s);
      await sleep(40); // let the post-GC stall land in the histogram, then discard it
      h.reset();
    }
    h.disable();
  })();
  return () => {
    stopped = true;
  };
}

/** Size of a Map/Set field of an object, read by name (for private cache fields the classes do not expose). */
export function sizeOf(obj: unknown, field: string): number {
  const v = (obj as Record<string, unknown> | null | undefined)?.[field];
  if (v instanceof Map || v instanceof Set) return v.size;
  if (Array.isArray(v)) return v.length;
  return 0;
}

// ---------- Areas ----------

export interface AreaDef {
  name: string;
  south: number;
  north: number;
  west: number;
  east: number;
}

const { south: S0, north: N0, west: W0, east: E0 } = COUNTY_BOUNDS;
const MID_LAT = (S0 + N0) / 2;
const MID_LNG = (W0 + E0) / 2;

/** Rotating alert areas: the whole county and its four halves. */
export const AREAS: AreaDef[] = [
  { name: 'county', south: S0, north: N0, west: W0, east: E0 },
  { name: 'north half', south: MID_LAT, north: N0, west: W0, east: E0 },
  { name: 'south half', south: S0, north: MID_LAT, west: W0, east: E0 },
  { name: 'east half', south: S0, north: N0, west: MID_LNG, east: E0 },
  { name: 'west half', south: S0, north: N0, west: W0, east: MID_LNG },
];

export function areaPolygon(a: AreaDef) {
  return {
    type: 'Polygon' as const,
    coordinates: [
      [
        [a.west, a.south],
        [a.east, a.south],
        [a.east, a.north],
        [a.west, a.north],
        [a.west, a.south],
      ],
    ],
  };
}

/** Where the "quiet" places go: far from every alert area (Los Angeles), so churn there never changes who an alert reaches. */
export const QUIET_BOX = { south: 33.9, north: 34.15, west: -118.45, east: -118.1 };

// ---------- Events ----------

export type EventKind = 'new' | 'update_same' | 'update_upgrade' | 'cancel';

export interface AlertEvent {
  index: number;
  kind: EventKind;
  /** Stable alert id of the logical alert this event is about. */
  alertId: string;
  /** Version id (changes with every event on the same alert). */
  sourceId: string;
  severity: 'watch' | 'warning' | 'emergency';
  areaIdx: number;
  /** True when the event makes the system push (a new alert or a severity upgrade). */
  pushes: boolean;
}

/** Deterministic UUID for logical alert n. */
export const alertUuid = (n: number): string =>
  `7a1e0000-0000-4000-8000-${(0x500000 + n).toString(16).padStart(12, '0')}`;

/**
 * The event script: a repeating cycle of five over two logical alerts, rotating areas.
 *   0: new watch alert A        1: new warning alert B
 *   2: A updated, same severity (no repeat push: same batch keys)
 *   3: B upgraded to emergency (new batch keys: everyone is pushed again)
 *   4: A cancelled (nothing sent)
 */
export function buildEvents(count: number): AlertEvent[] {
  const out: AlertEvent[] = [];
  for (let i = 0; i < count; i++) {
    const cycle = Math.floor(i / 5);
    const step = i % 5;
    const a = cycle * 2;
    const b = cycle * 2 + 1;
    const areaA = a % AREAS.length;
    const areaB = b % AREAS.length;
    const base = { index: i, sourceId: `urn:oid:soak-${i}` };
    if (step === 0) {
      out.push({
        ...base,
        kind: 'new',
        alertId: alertUuid(a),
        severity: 'watch',
        areaIdx: areaA,
        pushes: true,
      });
    } else if (step === 1) {
      out.push({
        ...base,
        kind: 'new',
        alertId: alertUuid(b),
        severity: 'warning',
        areaIdx: areaB,
        pushes: true,
      });
    } else if (step === 2) {
      out.push({
        ...base,
        kind: 'update_same',
        alertId: alertUuid(a),
        severity: 'watch',
        areaIdx: areaA,
        pushes: false,
      });
    } else if (step === 3) {
      out.push({
        ...base,
        kind: 'update_upgrade',
        alertId: alertUuid(b),
        severity: 'emergency',
        areaIdx: areaB,
        pushes: true,
      });
    } else {
      out.push({
        ...base,
        kind: 'cancel',
        alertId: alertUuid(a),
        severity: 'watch',
        areaIdx: areaA,
        pushes: false,
      });
    }
  }
  return out;
}

// ---------- Token hashes (order-independent set fingerprints) ----------

export function hash1(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

export function hash2(s: string): number {
  let h = 0x9e3779b9;
  for (let i = 0; i < s.length; i++) {
    h = Math.imul(h ^ s.charCodeAt(i), 0x5bd1e995);
    h ^= h >>> 15;
  }
  return (h ^ (h >>> 13)) >>> 0;
}

export interface Fingerprint {
  count: number;
  h1: number;
  h2: number;
}

export const emptyFingerprint = (): Fingerprint => ({ count: 0, h1: 0, h2: 0 });

export function addToFingerprint(f: Fingerprint, token: string): void {
  f.count++;
  f.h1 += hash1(token);
  f.h2 += hash2(token);
}

/** Cheap deterministic coin flip from a string (for sampling tokens by value). */
export const fraction = (s: string): number => hash1(s) / 4294967296;

export function claimsOf(token: string): { a: string; d: string; c: string; exp: number } {
  const body = token.slice(0, token.indexOf('.'));
  return JSON.parse(Buffer.from(body, 'base64url').toString('utf8')) as {
    a: string;
    d: string;
    c: string;
    exp: number;
  };
}

/** Drops the EMF metric lines the real kill switch prints on every read (they would flood the console). */
export function quietMetricLines(): void {
  const log = console.log.bind(console);
  console.log = (...args: unknown[]): void => {
    if (typeof args[0] === 'string' && args[0].startsWith('{"_aws"')) return;
    log(...args);
  };
}
