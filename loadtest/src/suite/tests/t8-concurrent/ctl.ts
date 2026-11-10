/**
 * Control channel for the t8/t9 processes: every child serves `POST /<name>` (JSON in, JSON out) on
 * 127.0.0.1 and announces the base URL to its parent with a `{ control }` message. The parent calls
 * it with `proc.req(name, payload)`. Built on suite/lib/child.ts (spawn, RSS reports, cleanup) and the
 * small HTTP helpers t5 uses. Bulk data (token hashes) goes through files, never through here.
 *
 * Also wraps the t2 processes (broker, mocks, seeders), which speak t2's own IPC, in the same
 * `Proc` shape so the harness can watch and stop all of them alike.
 */
import {
  runWorkerMain,
  spawnWorker,
  type ChildHandle,
  type SpawnOptions,
} from '../../lib/child.js';
import { sendMessage } from '../../lib/child.js';
import http from 'node:http';
import { HttpError, startServer } from '../t5-chaos/common.js';
import type { Worker as T2Worker } from '../t2-fanout/proc.js';

export const sleep = (ms: number): Promise<void> =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

// ---------- harness HTTP client ----------

/**
 * Every harness-to-harness call goes through here: one keep-alive agent per process with a small
 * socket cap (few long-lived loopback connections, never a connection per request), at most
 * MAX_CONCURRENT calls in flight (the rest wait), and a retry with backoff for errors that mean the
 * OS refused a connection before anything was sent (ENOBUFS, EADDRINUSE, EAGAIN, EMFILE, connection
 * refused). A reset after the request went out is retried only for read-only routes. Only harness
 * calls use this; the code under test never does.
 */
const ctlAgent = new http.Agent({
  keepAlive: true,
  maxSockets: 16,
  maxFreeSockets: 16,
  scheduling: 'lifo',
});
const MAX_CONCURRENT = 32;
const SAFE_TO_REPEAT = new Set(['/status', '/info', '/stats', '/final', '/overlay', '/lj_context']);
const CONNECT_ERRORS = new Set(['ENOBUFS', 'EADDRINUSE', 'EAGAIN', 'EMFILE', 'ECONNREFUSED']);

export interface ConnStats {
  /** Calls retried after a connection error. */
  retried: number;
  /** Calls that still failed with a connection error after the retries. */
  failed: number;
  byCode: Record<string, number>;
  /** Highest number of calls in flight at once. */
  peakInFlight: number;
}

export const connStats: ConnStats = { retried: 0, failed: 0, byCode: {}, peakInFlight: 0 };

let inFlight = 0;
const waiting: Array<() => void> = [];
async function acquire(): Promise<void> {
  if (inFlight >= MAX_CONCURRENT) await new Promise<void>((resolve) => waiting.push(resolve));
  inFlight++;
  connStats.peakInFlight = Math.max(connStats.peakInFlight, inFlight);
}
function release(): void {
  inFlight--;
  waiting.shift()?.();
}

function once<T>(
  base: string,
  path: string,
  payload: string | undefined,
  timeoutMs: number,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const req = http.request(
      `${base}${path}`,
      {
        method: 'POST',
        agent: ctlAgent,
        timeout: timeoutMs,
        headers:
          payload === undefined
            ? {}
            : { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) },
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

/** POST `body` as JSON to `${base}${path}` (path starts with a slash). */
export async function ctlCall<T = unknown>(
  base: string,
  path: string,
  body: unknown = {},
  timeoutMs = 120_000,
): Promise<T> {
  const payload = JSON.stringify(body);
  await acquire();
  try {
    for (let attempt = 0; ; attempt++) {
      try {
        return await once<T>(base, path, payload, timeoutMs);
      } catch (e) {
        const code = (e as NodeJS.ErrnoException).code ?? '';
        const retryable =
          CONNECT_ERRORS.has(code) || (code === 'ECONNRESET' && SAFE_TO_REPEAT.has(path));
        if (!retryable) throw e;
        connStats.byCode[code] = (connStats.byCode[code] ?? 0) + 1;
        if (attempt >= 8) {
          connStats.failed++;
          throw e;
        }
        connStats.retried++;
        await sleep(Math.min(2_000, 50 * 2 ** attempt) + Math.random() * 50);
      }
    }
  } finally {
    release();
  }
}

// ---------- child side ----------

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Handlers = Record<string, (payload: any) => unknown>;

/**
 * Child main: `setup` builds the handlers (and may return extra fields for the ready message); the
 * process serves them until the parent calls `stop`, then exits 0.
 */
export function runCtlWorker(
  setup: () => Promise<{
    handlers: Handlers;
    extra?: Record<string, unknown>;
    onStop?: () => Promise<void> | void;
  }>,
): void {
  runWorkerMain(async () => {
    let done!: () => void;
    const stopped = new Promise<void>((resolve) => {
      done = resolve;
    });
    const { handlers, extra, onStop } = await setup();
    const server = await startServer(async (method, path, _query, body) => {
      const name = path.slice(1);
      if (method === 'POST' && name === 'stop') {
        setTimeout(done, 20);
        return { ok: true };
      }
      const h = handlers[name];
      if (method !== 'POST' || !h) throw new HttpError(404, 'unknown route', path);
      return h(body === '' ? undefined : JSON.parse(body));
    });
    await sendMessage({ control: `http://127.0.0.1:${server.port}`, ...extra });
    await stopped;
    await onStop?.();
    server.close();
    return {};
  });
}

// ---------- parent side ----------

/** A process the harness watches: its memory, whether it is alive, and how to stop it. */
export interface Proc {
  role: string;
  readonly alive: boolean;
  readonly rssNow: number;
  readonly rssPeak: number;
  kill(): void;
}

export interface CtlProc extends Proc {
  base: string;
  /** Whatever the child put in its ready message besides `control`. */
  ready: Record<string, unknown>;
  handle: ChildHandle;
  req<T = unknown>(name: string, payload?: unknown, timeoutMs?: number): Promise<T>;
  /** Asks the child to stop, waits up to 5 s, then kills it. */
  stop(): Promise<void>;
}

export async function spawnCtl(
  file: string,
  role: string,
  opts: SpawnOptions,
  args: string[] = [],
): Promise<CtlProc> {
  const handle = spawnWorker(file, args, opts);
  let alive = true;
  void handle.exited.then(() => {
    alive = false;
  });
  const ready = await new Promise<Record<string, unknown>>((resolve, reject) => {
    handle.onMessage((m) => {
      if (m && typeof m === 'object' && 'control' in m) resolve(m as Record<string, unknown>);
    });
    void handle.exited.then((code) =>
      reject(new Error(`${role} exited before it was ready (code ${String(code)})`)),
    );
    handle.result.catch(reject);
  });
  const base = String(ready['control']);
  const proc: CtlProc = {
    role,
    base,
    ready,
    handle,
    get alive() {
      return alive;
    },
    get rssNow() {
      return handle.rssNow;
    },
    get rssPeak() {
      return handle.rssPeak;
    },
    kill: () => handle.kill(),
    req: <T>(name: string, payload: unknown = {}, timeoutMs = 600_000) => {
      if (!alive) return Promise.reject(new Error(`${role} is not running`));
      return ctlCall<T>(base, `/${name}`, payload, timeoutMs);
    },
    stop: async () => {
      if (!alive) return;
      try {
        await ctlCall(base, '/stop', {}, 5_000);
      } catch {
        /* going down anyway */
      }
      await Promise.race([handle.exited, sleep(5_000)]);
      handle.kill();
    },
  };
  return proc;
}

/** A t2 worker (broker, mock, seeder) seen as a `Proc`. */
export function t2Proc(w: T2Worker): Proc {
  return {
    role: w.role,
    get alive() {
      return w.alive;
    },
    get rssNow() {
      return w.lastRssMb;
    },
    get rssPeak() {
      return w.peakRssMb;
    },
    kill: () => {
      try {
        w.proc.kill();
      } catch {
        /* already gone */
      }
    },
  };
}
