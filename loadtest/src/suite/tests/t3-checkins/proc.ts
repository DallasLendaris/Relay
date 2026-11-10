/** Minimal child-process helper for t3: fork a TypeScript worker with tsx, talk over IPC, clean up. */
import { fork, type ChildProcess } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const LOADTEST_DIR = fileURLToPath(new URL('../../../../', import.meta.url));
const live = new Set<ChildProcess>();
let hooked = false;

function killAll(): void {
  for (const p of live) {
    try {
      p.kill('SIGKILL');
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
  for (const sig of ['SIGINT', 'SIGTERM'] as const) {
    process.once(sig, () => {
      killAll();
      process.kill(process.pid, sig);
    });
  }
}

export interface Child {
  name: string;
  send(msg: object): void;
  /** Resolves with the next message of this type; rejects on timeout or if the child dies. */
  waitFor<T extends { type: string }>(type: string, timeoutMs: number): Promise<T>;
  kill(): void;
}

export function spawnChild(file: string, args: string[], heapMb: number, name: string): Child {
  hook();
  const proc = fork(file, args, {
    cwd: LOADTEST_DIR,
    execArgv: ['--import', 'tsx', `--max-old-space-size=${heapMb}`],
    stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
  });
  live.add(proc);
  let stderr = '';
  proc.stderr?.on('data', (c: Buffer) => {
    stderr = (stderr + c.toString()).slice(-2000);
  });
  const inbox: Array<{ type: string }> = [];
  const waiters: Array<{ type: string; ok: (m: never) => void; fail: (e: Error) => void }> = [];
  let dead: Error | undefined;
  proc.on('message', (m) => {
    const msg = m as { type: string };
    const i = waiters.findIndex((w) => w.type === msg.type);
    if (i >= 0) waiters.splice(i, 1)[0]?.ok(msg as never);
    else inbox.push(msg);
  });
  proc.on('exit', (code, sig) => {
    live.delete(proc);
    dead = new Error(`${name} exited (code ${code}, signal ${sig}): ${stderr.trim()}`);
    for (const w of waiters.splice(0)) w.fail(dead);
  });
  proc.on('error', (e) => {
    dead = e;
    for (const w of waiters.splice(0)) w.fail(e);
  });
  return {
    name,
    send: (msg) => {
      if (proc.connected) proc.send(msg);
    },
    waitFor: <T extends { type: string }>(type: string, timeoutMs: number) =>
      new Promise<T>((resolve, reject) => {
        const i = inbox.findIndex((m) => m.type === type);
        if (i >= 0) return resolve(inbox.splice(i, 1)[0] as T);
        if (dead) return reject(dead);
        const t = setTimeout(() => {
          reject(new Error(`${name}: timeout waiting for "${type}" ${stderr.trim()}`));
        }, timeoutMs);
        waiters.push({
          type,
          ok: (m) => {
            clearTimeout(t);
            resolve(m as T);
          },
          fail: (e) => {
            clearTimeout(t);
            reject(e);
          },
        });
      }),
    kill: () => {
      try {
        proc.kill('SIGKILL');
      } catch {
        /* already gone */
      }
      live.delete(proc);
    },
  };
}
