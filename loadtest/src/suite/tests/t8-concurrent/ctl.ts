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
import { HttpError, call, startServer } from '../t5-chaos/common.js';
import type { Worker as T2Worker } from '../t2-fanout/proc.js';

export const sleep = (ms: number): Promise<void> =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

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
      return call<T>(base, 'POST', `/${name}`, payload, timeoutMs);
    },
    stop: async () => {
      if (!alive) return;
      try {
        await call(base, 'POST', '/stop', {}, 5_000);
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
