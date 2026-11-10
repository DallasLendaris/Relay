/**
 * Minimal child-process helper for test 2 (to be unified with suite/lib/child.ts later).
 *
 * Parent side: `spawnWorker(file, opts)` starts `node --max-old-space-size=N --import tsx file`
 * with an IPC channel (advanced serialization, so Buffers and typed arrays pass through), waits for
 * the child's 'ready', and gives request/response calls plus RSS and CPU tracking. Every child is
 * killed when the parent exits or gets SIGINT/SIGTERM. Windows-safe: no shell, no signals sent to
 * process groups.
 *
 * Child side: `serve(handlers)` answers requests, reports memory and CPU every 500 ms, and exits
 * when the parent goes away (IPC disconnect), so a crashed parent never leaves orphans.
 */
import { spawn, type ChildProcess } from 'node:child_process';
import { createRequire } from 'node:module';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const require = createRequire(import.meta.url);
const TSX_LOADER = pathToFileURL(require.resolve('tsx')).href;

const live = new Set<ChildProcess>();
let hooked = false;

function killAll(): void {
  for (const c of live) {
    try {
      c.kill();
    } catch {
      // already gone
    }
  }
}

function hookExit(): void {
  if (hooked) return;
  hooked = true;
  process.on('exit', killAll);
  for (const sig of ['SIGINT', 'SIGTERM'] as const) {
    process.once(sig, () => {
      killAll();
      // Only exit ourselves when nobody else (the suite runner) handles the signal.
      if (process.listenerCount(sig) === 0) process.exit(130);
    });
  }
}

interface Pending {
  resolve(v: unknown): void;
  reject(e: Error): void;
}

type ChildMsg =
  | { kind: 'ready' }
  | { kind: 'res'; id: number; ok: true; result: unknown }
  | { kind: 'res'; id: number; ok: false; error: string }
  | { kind: 'mem'; rssMb: number; cpuUs: number };

export interface WorkerOptions {
  /** Role name for logs and per-role memory figures. */
  role: string;
  /** --max-old-space-size for the child, MB. */
  heapMb: number;
  env?: Record<string, string>;
  /** Defaults to the loadtest workspace (where tsx resolves). */
  cwd?: string;
}

export class Worker {
  private seq = 0;
  private readonly pending = new Map<number, Pending>();
  peakRssMb = 0;
  lastRssMb = 0;
  cpuUs = 0;
  private cpuAtMark = 0;
  private exitCode: number | null | undefined;
  readonly exited: Promise<void>;

  constructor(
    readonly role: string,
    readonly proc: ChildProcess,
  ) {
    this.exited = new Promise((resolve) => {
      proc.once('exit', (code) => {
        this.exitCode = code;
        live.delete(proc);
        const err = new Error(`${role} exited (code ${String(code)})`);
        for (const p of this.pending.values()) p.reject(err);
        this.pending.clear();
        resolve();
      });
    });
    proc.on('message', (raw) => {
      const m = raw as ChildMsg;
      if (m.kind === 'mem') {
        this.lastRssMb = m.rssMb;
        this.peakRssMb = Math.max(this.peakRssMb, m.rssMb);
        this.cpuUs = m.cpuUs;
      } else if (m.kind === 'res') {
        const p = this.pending.get(m.id);
        if (!p) return;
        this.pending.delete(m.id);
        if (m.ok) p.resolve(m.result);
        else p.reject(new Error(`${role}: ${m.error}`));
      }
    });
  }

  get alive(): boolean {
    return this.exitCode === undefined;
  }

  request<T = unknown>(type: string, payload?: unknown): Promise<T> {
    if (!this.alive) return Promise.reject(new Error(`${this.role} is not running`));
    const id = ++this.seq;
    return new Promise<T>((resolve, reject) => {
      this.pending.set(id, { resolve: resolve as (v: unknown) => void, reject });
      this.proc.send({ kind: 'req', id, type, payload }, (err) => {
        if (err) {
          this.pending.delete(id);
          reject(err);
        }
      });
    });
  }

  /** Starts a new measurement window for peak RSS and CPU. */
  mark(): void {
    this.peakRssMb = this.lastRssMb;
    this.cpuAtMark = this.cpuUs;
  }

  /** CPU seconds used since mark(). */
  cpuSecondsSinceMark(): number {
    return (this.cpuUs - this.cpuAtMark) / 1e6;
  }

  async stop(): Promise<void> {
    if (!this.alive) return;
    try {
      this.proc.disconnect();
    } catch {
      // channel already closed
    }
    const t = setTimeout(() => this.proc.kill(), 5000);
    await this.exited;
    clearTimeout(t);
  }
}

export async function spawnWorker(file: string, opts: WorkerOptions): Promise<Worker> {
  hookExit();
  const cwd = opts.cwd ?? path.resolve(path.dirname(file), '..', '..', '..', '..');
  const proc = spawn(
    process.execPath,
    [`--max-old-space-size=${Math.round(opts.heapMb)}`, '--import', TSX_LOADER, file],
    {
      cwd,
      env: { ...process.env, ...opts.env, T2_ROLE: opts.role },
      stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
      serialization: 'advanced',
      windowsHide: true,
    },
  );
  live.add(proc);
  const w = new Worker(opts.role, proc);
  await new Promise<void>((resolve, reject) => {
    const onMsg = (raw: unknown) => {
      if ((raw as ChildMsg).kind === 'ready') {
        proc.off('message', onMsg);
        resolve();
      }
    };
    proc.on('message', onMsg);
    void w.exited.then(() => reject(new Error(`${opts.role} exited before it was ready`)));
  });
  return w;
}

// ---------- child side ----------

export type Handlers = Record<string, (payload: never) => unknown>;

export function serve(handlers: Handlers): void {
  const send = (m: ChildMsg) => {
    if (process.connected) process.send?.(m);
  };
  process.on('disconnect', () => process.exit(0));
  process.on('message', (raw) => {
    const m = raw as { kind: string; id: number; type: string; payload: never };
    if (m.kind !== 'req') return;
    const h = handlers[m.type];
    void (async () => {
      try {
        if (!h) throw new Error(`unknown request ${m.type}`);
        const result = await h(m.payload);
        send({ kind: 'res', id: m.id, ok: true, result });
      } catch (err) {
        const error = err instanceof Error ? `${err.name}: ${err.message}` : String(err);
        send({ kind: 'res', id: m.id, ok: false, error });
      }
    })();
  });
  const report = () => {
    const c = process.cpuUsage();
    send({
      kind: 'mem',
      rssMb: Math.round(process.memoryUsage.rss() / 1048576),
      cpuUs: c.user + c.system,
    });
  };
  setInterval(report, 500).unref();
  report();
  send({ kind: 'ready' });
}
