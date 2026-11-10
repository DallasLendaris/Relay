/**
 * Worker processes with a tiny IPC protocol (works on Windows: no signals, no shell).
 *
 * PARENT
 *   const w = spawnWorker(fileURLToPath(new URL('./my-worker.ts', import.meta.url)),
 *                         ['--n', '5'], { heapMb: 512 });
 *   const value = await w.result;   // what the child passed to sendResult(); rejects if the child
 *                                   // reported an error or exited without a result
 *   w.rssPeak;                      // peak RSS of the child, MB (from its own reports)
 *   w.rssNow;                       // latest reported RSS, MB (feed to RssSampler.addSource)
 *   w.kill();                       // terminate now
 *   w.onMessage(fn)                 // optional extra messages the child sent with sendMessage()
 *
 * CHILD (the file you spawn is run with `node --max-old-space-size=<heapMb> --import tsx file args`)
 *   runWorkerMain(async () => { ...; return resultObject; });
 * which starts RSS reporting every second, sends the returned object as the result (JSON-serialisable)
 * and exits 0, or sends the error and exits 1. Lower level: `reportRss()`, `startRssReporting(ms)`,
 * `sendResult(obj)`, `sendError(err)`, `sendMessage(obj)`.
 *
 * Every spawned child is tracked and killed when the parent exits, on SIGINT/SIGTERM, or on an
 * uncaught error, so no orphans remain. stdout/stderr of children are inherited (they appear in the
 * parent's console and transcript). `process.argv.slice(2)` in the child is exactly `args`.
 */
import { spawn, type ChildProcess } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

type ToParent =
  | { __suite: 'rss'; mb: number }
  | { __suite: 'result'; value: unknown }
  | { __suite: 'error'; message: string; stack?: string }
  | { __suite: 'message'; value: unknown };

/** The loadtest package directory (cwd for children, so `--import tsx` resolves). */
export const LOADTEST_DIR = resolve(dirname(fileURLToPath(import.meta.url)), '../../..');

export interface SpawnOptions {
  /** V8 old-space limit in MB (`--max-old-space-size`). Required: always explicit. */
  heapMb: number;
  /** Extra environment variables for the child. */
  env?: Record<string, string>;
  /** Extra node flags, e.g. ['--expose-gc']. */
  nodeArgs?: string[];
  /** Working directory; default LOADTEST_DIR. */
  cwd?: string;
}

export interface ChildHandle {
  readonly pid: number | undefined;
  readonly result: Promise<unknown>;
  readonly rssPeak: number;
  readonly rssNow: number;
  /** Resolves with the exit code when the process has exited. */
  readonly exited: Promise<number | null>;
  kill(): void;
  onMessage(fn: (value: unknown) => void): void;
}

const live = new Set<ChildProcess>();
let hooked = false;

function killAll(): void {
  for (const c of live) {
    try {
      c.kill();
    } catch {
      /* already gone */
    }
  }
  live.clear();
}

function hook(): void {
  if (hooked) return;
  hooked = true;
  process.on('exit', killAll);
  for (const sig of ['SIGINT', 'SIGTERM', 'SIGBREAK'] as const) {
    process.on(sig, () => {
      killAll();
      process.exit(130);
    });
  }
  process.on('uncaughtException', (e) => {
    killAll();
    console.error(e);
    process.exit(1);
  });
}

/** Kills every worker spawned by this process (the runner calls it on shutdown). */
export function killAllWorkers(): void {
  killAll();
}

export function spawnWorker(file: string, args: string[], opts: SpawnOptions): ChildHandle {
  hook();
  const child = spawn(
    process.execPath,
    [
      `--max-old-space-size=${opts.heapMb}`,
      ...(opts.nodeArgs ?? []),
      '--import',
      'tsx',
      file,
      ...args,
    ],
    {
      cwd: opts.cwd ?? LOADTEST_DIR,
      env: { ...process.env, ...opts.env },
      stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
      windowsHide: true,
    },
  );
  live.add(child);

  let rssPeak = 0;
  let rssNow = 0;
  let settled = false;
  const listeners: Array<(v: unknown) => void> = [];
  let resolveResult!: (v: unknown) => void;
  let rejectResult!: (e: Error) => void;
  const result = new Promise<unknown>((res, rej) => {
    resolveResult = res;
    rejectResult = rej;
  });
  // Avoid unhandled-rejection noise when the caller kills the child and never awaits `result`.
  result.catch(() => undefined);

  child.on('message', (raw) => {
    const m = raw as ToParent;
    if (!m || typeof m !== 'object' || !('__suite' in m)) return;
    if (m.__suite === 'rss') {
      rssNow = m.mb;
      if (m.mb > rssPeak) rssPeak = m.mb;
    } else if (m.__suite === 'result' && !settled) {
      settled = true;
      resolveResult(m.value);
    } else if (m.__suite === 'error' && !settled) {
      settled = true;
      rejectResult(new Error(`worker failed: ${m.message}${m.stack ? `\n${m.stack}` : ''}`));
    } else if (m.__suite === 'message') {
      for (const l of listeners) l(m.value);
    }
  });
  const exited = new Promise<number | null>((res) => {
    child.on('exit', (code, signal) => {
      live.delete(child);
      rssNow = 0;
      // Give an in-flight result message a tick to land before declaring "no result".
      setImmediate(() => {
        if (!settled) {
          settled = true;
          rejectResult(
            new Error(`worker exited without a result (code ${code}, signal ${signal ?? 'none'})`),
          );
        }
        res(code);
      });
    });
  });
  child.on('error', (e) => {
    live.delete(child);
    if (!settled) {
      settled = true;
      rejectResult(e);
    }
  });

  return {
    get pid() {
      return child.pid;
    },
    result,
    get rssPeak() {
      return rssPeak;
    },
    get rssNow() {
      return rssNow;
    },
    exited,
    kill: () => {
      try {
        child.kill();
      } catch {
        /* already gone */
      }
    },
    onMessage: (fn) => {
      listeners.push(fn);
    },
  };
}

// ---------- child side ----------

function send(m: ToParent): Promise<void> {
  return new Promise((res) => {
    if (!process.send) {
      res();
      return;
    }
    process.send(m, undefined, undefined, () => res());
  });
}

export function reportRss(): void {
  void send({ __suite: 'rss', mb: process.memoryUsage().rss / 1024 / 1024 });
}

export function startRssReporting(intervalMs = 1000): NodeJS.Timeout {
  reportRss();
  const t = setInterval(reportRss, intervalMs);
  t.unref();
  return t;
}

export function sendMessage(value: unknown): Promise<void> {
  return send({ __suite: 'message', value });
}

export function sendResult(value: unknown): Promise<void> {
  reportRss();
  return send({ __suite: 'result', value });
}

export function sendError(e: unknown): Promise<void> {
  const err = e instanceof Error ? e : new Error(String(e));
  return send({ __suite: 'error', message: err.message, stack: err.stack });
}

/** Standard child main: RSS reporting, result or error to the parent, then exit. */
export function runWorkerMain(fn: () => Promise<unknown>): void {
  startRssReporting();
  fn().then(
    async (value) => {
      await sendResult(value);
      process.exit(0);
    },
    async (e) => {
      await sendError(e);
      process.exit(1);
    },
  );
}
