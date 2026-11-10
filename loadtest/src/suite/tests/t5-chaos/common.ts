/**
 * Shared pieces of the t5 (failure under load) processes: the virtual clock, deterministic token
 * hashing for dead-token faults, the fault rule format, and a tiny JSON-over-HTTP client and server
 * (127.0.0.1 only) the processes use to talk to each other.
 *
 * VIRTUAL TIME. The real sender, kill switch and idempotency code use fixed time constants (45 s
 * lease, 30 s kill-switch cache, 60 s queue visibility, 2 s x 2^n retry backoff). To run the same
 * code in a smoke run that must finish in seconds, every process shares one clock that runs `scale`
 * times faster than the wall clock from a common epoch (full runs use scale 1). Only durations that
 * the real code derives from its clock are compressed; HTTP latencies are real.
 */
import http from 'node:http';
import type { AddressInfo } from 'node:net';

export interface VClock {
  /** Wall-clock ms at which virtual time and real time coincide. */
  epochMs: number;
  /** Virtual seconds per real second (1 = real time). */
  scale: number;
}

/** Virtual "now" in epoch ms. */
export const vnow = (c: VClock): number => c.epochMs + (Date.now() - c.epochMs) * c.scale;

/** A Clock for the backend code (`{ now(): Date }`). */
export const vclock = (c: VClock): { now: () => Date } => ({ now: () => new Date(vnow(c)) });

/** Real milliseconds a virtual duration takes. */
/** Sub-chunks of one batch sent at once (the sender's default; set explicitly in the sender stand-in). */
export const CHUNK_CONCURRENCY = 8;

export const realMs = (c: VClock, virtualMs: number): number => virtualMs / c.scale;

// ---------- Deterministic token hash ----------

/** FNV-1a 32 bit of the text, mapped to [0, 1). */
export function unitHash(text: string, salt = ''): number {
  let h = 0x811c9dc5;
  const s = salt + text;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h / 4294967296;
}

// ---------- Faults ----------

export type FaultKind = 'throttle' | 'unavailable' | 'unregistered' | 'badtoken';

/**
 * One fault window for the mock push servers. `fromS`/`toS` are virtual seconds since the mock's
 * first request. A request matches when its probability test passes: with `byToken` the test is a
 * deterministic hash of the token (the same tokens fail every time), otherwise a fresh random draw.
 */
export interface FaultRule {
  fromS: number;
  toS: number;
  p: number;
  byToken?: boolean;
  kind: FaultKind;
  /** Retry-After header, virtual seconds (throttle and unavailable). */
  retryAfterS?: number;
  /** Only this platform's mock applies the rule; default both. */
  platform?: 'apns' | 'fcm';
}

/** Salt of the dead-token hash, shared by the mock (who answers 410) and the broker (who checks clears). */
export const DEAD_SALT = 'dead:';
export const isDeadToken = (token: string, rate: number): boolean =>
  unitHash(token, DEAD_SALT) < rate;

/** A claim-throttling window for the idempotency store (scenario f), virtual seconds since the first claim. */
export interface ThrottleSpec {
  fromS: number;
  toS: number;
  p: number;
}

// ---------- HTTP ----------

const agent = new http.Agent({ keepAlive: true, maxSockets: 64 });

export class HttpError extends Error {
  constructor(
    readonly status: number,
    readonly text: string,
    path: string,
  ) {
    super(`HTTP ${status} ${path}: ${text.slice(0, 200)}`);
  }
}

/** JSON call to another process. Bodies that are strings are sent as they are (text/plain). */
export function call<T = unknown>(
  base: string,
  method: 'GET' | 'POST',
  path: string,
  body?: unknown,
  timeoutMs = 120_000,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const payload =
      body === undefined ? undefined : typeof body === 'string' ? body : JSON.stringify(body);
    const req = http.request(
      `${base}${path}`,
      {
        method,
        agent,
        timeout: timeoutMs,
        headers:
          payload === undefined
            ? {}
            : {
                'content-type': typeof body === 'string' ? 'text/plain' : 'application/json',
                'content-length': Buffer.byteLength(payload),
              },
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (c: Buffer) => chunks.push(c));
        res.on('end', () => {
          const text = Buffer.concat(chunks).toString('utf8');
          if ((res.statusCode ?? 500) >= 400) {
            reject(new HttpError(res.statusCode ?? 500, text, path));
            return;
          }
          try {
            resolve((text === '' ? {} : JSON.parse(text)) as T);
          } catch (e) {
            reject(e instanceof Error ? e : new Error(String(e)));
          }
        });
        res.on('error', reject);
      },
    );
    req.on('timeout', () => req.destroy(new Error(`timeout calling ${path}`)));
    req.on('error', reject);
    if (payload !== undefined) req.write(payload);
    req.end();
  });
}

export type Route = (
  method: string,
  path: string,
  query: URLSearchParams,
  body: string,
) => Promise<unknown> | unknown;

/** A small JSON server on 127.0.0.1 (port 0). Throw HttpError from a route to answer with its status. */
export async function startServer(route: Route): Promise<{ port: number; close(): void }> {
  const server = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => {
      const url = new URL(req.url ?? '/', 'http://127.0.0.1');
      void (async () => {
        try {
          const out = await route(
            req.method ?? 'GET',
            url.pathname,
            url.searchParams,
            Buffer.concat(chunks).toString('utf8'),
          );
          const text = JSON.stringify(out ?? {});
          res.writeHead(200, { 'content-type': 'application/json' });
          res.end(text);
        } catch (e) {
          const status = e instanceof HttpError ? e.status : 500;
          res.writeHead(status, { 'content-type': 'text/plain' });
          res.end(e instanceof Error ? e.message : String(e));
        }
      })();
    });
  });
  server.keepAliveTimeout = 60_000;
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  return {
    port: (server.address() as AddressInfo).port,
    close: () => {
      server.closeAllConnections();
      server.close();
    },
  };
}

export const sleep = (ms: number): Promise<void> =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

/** The parsed body of a POST (JSON). */
export const json = <T>(body: string): T => JSON.parse(body) as T;
