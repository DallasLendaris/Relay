import type { Logger } from '../../delivery/types.js';
import type { PushTarget } from '../contracts.js';
import { emfLine, type MetricWriter } from './metrics.js';

export interface DeadTargetItem {
  deviceId: string;
  target: PushTarget;
}

export interface DeadTargetQueueOptions {
  /** Clears one dead target (device store plus cell index). Errors are logged, never thrown. */
  process(deviceId: string, target: PushTarget): Promise<void>;
  logger: Logger;
  writeMetric?: MetricWriter;
  /** Parallel cleanups. Default 4. */
  concurrency?: number;
  /** Items held before new ones are dropped. Default 10,000. */
  maxQueued?: number;
  /** How often DeadTargetDropped is emitted while drops happen. Default 10 s. */
  metricsIntervalMs?: number;
}

/**
 * Dead-target cleanup runs here, off the send path. A batch is marked sent first and only then
 * pushes its dead targets with enqueue(), which is synchronous and cheap, so cleanup can never
 * delay sending or hold a batch past its lease. A background pump drains the queue with a
 * bounded number of workers. When the queue is full the target is dropped and counted
 * (DeadTargetDropped); the device keeps its dead token until a later alert finds it again.
 */
export class DeadTargetQueue {
  private readonly items: DeadTargetItem[] = [];
  private head = 0;
  private active = 0;
  private dropped = 0;
  private idleWaiters: Array<() => void> = [];
  private readonly concurrency: number;
  private readonly maxQueued: number;
  private readonly write: MetricWriter;
  private readonly timer: NodeJS.Timeout;

  constructor(private readonly opts: DeadTargetQueueOptions) {
    this.concurrency = opts.concurrency ?? 4;
    this.maxQueued = opts.maxQueued ?? 10_000;
    this.write = opts.writeMetric ?? ((l) => console.log(l));
    this.timer = setInterval(() => this.flushMetrics(), opts.metricsIntervalMs ?? 10_000);
    this.timer.unref();
  }

  get queued(): number {
    return this.items.length - this.head;
  }
  get inProgress(): number {
    return this.active;
  }

  /** Queues a cleanup. Returns false (and counts a drop) when the queue is full. Never throws. */
  enqueue(deviceId: string, target: PushTarget): boolean {
    if (this.queued >= this.maxQueued) {
      this.dropped++;
      return false;
    }
    this.items.push({ deviceId, target });
    // Start on a later tick so the caller's own work (markSent, logging) goes first.
    queueMicrotask(() => this.pump());
    return true;
  }

  private pump(): void {
    while (this.active < this.concurrency && this.head < this.items.length) {
      const item = this.items[this.head++];
      if (!item) break;
      if (this.head > 1024 && this.head * 2 > this.items.length) {
        this.items.splice(0, this.head);
        this.head = 0;
      }
      this.active++;
      this.opts
        .process(item.deviceId, item.target)
        .catch((err: unknown) => {
          this.opts.logger.error('sender.clear_dead_target_failed', {
            deviceId: item.deviceId,
            error: err instanceof Error ? err.name : 'Error',
          });
        })
        .finally(() => {
          this.active--;
          this.pump();
          if (this.active === 0 && this.queued === 0) {
            const w = this.idleWaiters;
            this.idleWaiters = [];
            for (const f of w) f();
          }
        });
    }
  }

  /** Emits DeadTargetDropped if anything was dropped since the last call. */
  flushMetrics(): void {
    if (this.dropped === 0) return;
    const n = this.dropped;
    this.dropped = 0;
    this.write(emfLine('DeadTargetDropped', n));
    this.opts.logger.warn('sender.dead_target_dropped', { count: n, maxQueued: this.maxQueued });
  }

  /**
   * Graceful shutdown: waits until the queue is empty and idle, or `timeoutMs` passes. Returns how
   * many targets were left uncleaned. Emits the drop metric either way.
   */
  async flush(timeoutMs: number): Promise<number> {
    this.pump();
    if (this.active > 0 || this.queued > 0) {
      let t: NodeJS.Timeout | undefined;
      await Promise.race([
        new Promise<void>((resolve) => this.idleWaiters.push(resolve)),
        new Promise<void>((resolve) => {
          t = setTimeout(resolve, Math.max(0, timeoutMs));
        }),
      ]);
      clearTimeout(t);
    }
    clearInterval(this.timer);
    this.flushMetrics();
    const left = this.queued + this.active;
    this.opts.logger.info('sender.dead_target_flush', { remaining: left });
    return left;
  }
}
