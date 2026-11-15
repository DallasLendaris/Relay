/**
 * Long-running SQS consumer for the sender service (runs on Fargate, not Lambda).
 *
 * - Strict priority: the urgent queue is polled first; the standard queue only when urgent
 *   returned nothing. (While standard has a backlog, urgent is polled with wait 0 so an empty
 *   urgent queue does not cost a second per receive; otherwise it is a 1 s long poll.)
 * - Up to `concurrency` messages in flight; each message holds up to 10 batches, which run
 *   concurrently.
 * - Successful messages are deleted with DeleteMessageBatch. Failures are left alone and
 *   become visible again when the queue's visibility timeout runs out.
 * - A message still running after `extendAfterMs` gets its visibility extended (repeatedly),
 *   so a slow batch is not redelivered while its lease is live.
 * - Pause: the kill switch is checked before every receive (cached 5 s). While paused nothing is
 *   received at all (so no message's receive count grows toward the DLQ); the loop sleeps 5 s.
 *   A batch already in flight that finds the pause releases its claim and its message gets a
 *   60 s visibility timeout instead of bouncing back quickly.
 * - Lease: the visibility heartbeat also extends the idempotency lease of every active batch in
 *   the message. If an extension is refused the lease is lost; the batch stops sending.
 * - stop() (SIGTERM): stop receiving and let in-flight batches finish for `drainTimeoutMs`
 *   (25 s). Batches still running then are told to abort: they stop starting chunks, persist
 *   nothing, release their claims, and their messages are made visible again. Recipients in
 *   chunks that were already sent may get one duplicate push when the batch is redone.
 * - Metrics are EMF lines on stdout in namespace Relay/Sender, one per queue per flush.
 */
import {
  ChangeMessageVisibilityCommand,
  DeleteMessageBatchCommand,
  ReceiveMessageCommand,
  type Message,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import type { Logger } from '../../delivery/types.js';
import type { SendBatch, SendQueueName } from '../contracts.js';
import { METRIC_NAMESPACE } from './metrics.js';
import { newBatchControl, type BatchControl, type BatchOutcome } from './sender.js';

export { METRIC_NAMESPACE };
const DELETE_BATCH = 10;
const MAX_LAG_SAMPLES = 100; // EMF allows up to 100 values per metric

export interface ConsumerOptions {
  sqs: Pick<SQSClient, 'send'>;
  urgentQueueUrl: string;
  standardQueueUrl: string;
  /** Processes one batch. Throwing counts as a failed batch (the message stays on the queue). */
  handle(batch: SendBatch, control: BatchControl): Promise<BatchOutcome>;
  /**
   * Extends the idempotency lease of an active batch. Resolves false when the lease is lost.
   * Called from the visibility heartbeat for every active batch of a message.
   */
  extendLease?(batch: SendBatch): Promise<boolean>;
  /** Kill switch (true = paused). Checked before each receive, cached for `pauseCacheMs`. */
  isPaused?(): Promise<boolean>;
  /** Sleep used while paused. Default: setTimeout, cut short by stop(). */
  sleep?(ms: number): Promise<void>;
  /** How long a pause check stays cached. Default 5 s. */
  pauseCacheMs?: number;
  /** Sleep between checks while paused. Default 5 s. */
  pausePollMs?: number;
  /** Visibility timeout for a message whose batch found the pause mid-way. Default 60 s. */
  pauseVisibilitySeconds?: number;
  /** After the drain deadline, how long aborted batches get to wind down. Default 2 s. */
  abortGraceMs?: number;
  logger: Logger;
  /** Max messages in flight. Default 50. */
  concurrency?: number;
  /** Extend visibility for messages running longer than this. Default 20 s. */
  extendAfterMs?: number;
  /** New visibility timeout set on each extension. Default 60 s. */
  extendToSeconds?: number;
  /** Long-poll wait for the urgent queue when nothing is backlogged. Default 1 s. */
  waitSeconds?: number;
  /** After stop(), wait at most this long for in-flight work. Default 25 s (Fargate stops at 30 s). */
  drainTimeoutMs?: number;
  /** How often metrics are flushed. Default 10 s. */
  metricsIntervalMs?: number;
  /** Where EMF lines go. Default console.log. */
  writeMetric?: (line: string) => void;
  now?: () => number;
}

interface Pending {
  queueUrl: string;
  message: Message;
}

interface ActiveMessage {
  queueUrl: string;
  message: Message;
  batches: SendBatch[];
  controls: BatchControl[];
  done: boolean;
}

interface Counters {
  Sent: number;
  PermanentFailed: number;
  Retried: number;
  GaveUp: number;
  BatchesProcessed: number;
  lag: number[];
}

const emptyCounters = (): Counters => ({
  Sent: 0,
  PermanentFailed: 0,
  Retried: 0,
  GaveUp: 0,
  BatchesProcessed: 0,
  lag: [],
});

/** Parses a message body: a JSON array of SendBatch (a single object is tolerated). Null if malformed. */
export function parseBatches(body: string | undefined): SendBatch[] | null {
  try {
    const raw: unknown = JSON.parse(body ?? '');
    const list = Array.isArray(raw) ? raw : [raw];
    if (list.length === 0) return null;
    for (const b of list as Array<Partial<SendBatch> | null>) {
      if (
        !b ||
        typeof b.batchKey !== 'string' ||
        typeof b.alertId !== 'string' ||
        typeof b.attempt !== 'number' ||
        !Array.isArray(b.recipients)
      ) {
        return null;
      }
    }
    return list as SendBatch[];
  } catch {
    return null;
  }
}

export class SenderConsumer {
  private stopping = false;
  private readonly inflight = new Set<Promise<void>>();
  private readonly pendingDeletes: Pending[] = [];
  private readonly counters: Record<SendQueueName, Counters> = {
    urgent: emptyCounters(),
    standard: emptyCounters(),
  };
  private readonly active = new Set<ActiveMessage>();
  private pauseCache: { at: number; paused: boolean } | undefined;
  private wake: (() => void) | undefined;
  private sleepWake: (() => void) | undefined;
  private readonly concurrency: number;
  private readonly extendAfterMs: number;
  private readonly extendToSeconds: number;
  private readonly waitSeconds: number;
  private readonly drainTimeoutMs: number;
  private readonly pauseCacheMs: number;
  private readonly pausePollMs: number;
  private readonly pauseVisibilitySeconds: number;
  private readonly abortGraceMs: number;
  private readonly now: () => number;
  private readonly write: (line: string) => void;

  constructor(private readonly opts: ConsumerOptions) {
    this.concurrency = opts.concurrency ?? 50;
    this.extendAfterMs = opts.extendAfterMs ?? 20_000;
    this.extendToSeconds = opts.extendToSeconds ?? 60;
    this.waitSeconds = opts.waitSeconds ?? 1;
    this.drainTimeoutMs = opts.drainTimeoutMs ?? 25_000;
    this.now = opts.now ?? Date.now;
    this.pauseCacheMs = opts.pauseCacheMs ?? 5000;
    this.pausePollMs = opts.pausePollMs ?? 5000;
    this.pauseVisibilitySeconds = opts.pauseVisibilitySeconds ?? 60;
    this.abortGraceMs = opts.abortGraceMs ?? 2000;
    this.write = opts.writeMetric ?? ((line) => console.log(line));
  }

  /** Stop receiving and let run() finish in-flight work. Safe to call more than once. */
  stop(): void {
    this.stopping = true;
    this.wake?.();
    this.sleepWake?.();
  }

  get inFlightCount(): number {
    return this.inflight.size;
  }

  async run(): Promise<void> {
    const metricsTimer = setInterval(
      () => this.flushMetrics(),
      this.opts.metricsIntervalMs ?? 10_000,
    );
    metricsTimer.unref();
    let standardBacklog = false;
    try {
      while (!this.stopping) {
        await this.flushDeletes();
        if (await this.paused()) {
          await this.sleep(this.pausePollMs);
          continue;
        }
        const free = this.concurrency - this.inflight.size;
        if (free <= 0) {
          await this.waitForSlot();
          continue;
        }
        const max = Math.min(10, free);
        let queue: SendQueueName = 'urgent';
        let messages = await this.receive(
          this.opts.urgentQueueUrl,
          max,
          standardBacklog ? 0 : this.waitSeconds,
        );
        if (messages.length === 0 && !this.stopping) {
          queue = 'standard';
          messages = await this.receive(this.opts.standardQueueUrl, max, 0);
          standardBacklog = messages.length > 0;
        }
        // Messages already received are always processed, even if stop() was called meanwhile.
        for (const m of messages) this.start(queue, m);
      }
    } finally {
      await this.drain();
      clearInterval(metricsTimer);
      await this.flushDeletes();
      this.flushMetrics();
    }
  }

  /** Kill switch, cached. A failed read counts as paused: never receive when we cannot tell. */
  private async paused(): Promise<boolean> {
    const check = this.opts.isPaused;
    if (!check) return false;
    const t = this.now();
    if (this.pauseCache && t - this.pauseCache.at < this.pauseCacheMs) {
      return this.pauseCache.paused;
    }
    let paused: boolean;
    try {
      paused = await check();
    } catch (err) {
      this.opts.logger.warn('sender.pause_check_failed', { error: errName(err) });
      paused = true;
    }
    if (paused && !this.pauseCache?.paused) this.opts.logger.warn('sender.receiving_paused', {});
    if (!paused && this.pauseCache?.paused) this.opts.logger.info('sender.receiving_resumed', {});
    this.pauseCache = { at: t, paused };
    return paused;
  }

  private sleep(ms: number): Promise<void> {
    if (this.opts.sleep) return this.opts.sleep(ms);
    return new Promise<void>((resolve) => {
      const t = setTimeout(() => done(), ms);
      const done = (): void => {
        clearTimeout(t);
        this.sleepWake = undefined;
        resolve();
      };
      this.sleepWake = done;
    });
  }

  private async receive(queueUrl: string, max: number, wait: number): Promise<Message[]> {
    try {
      const out = await this.opts.sqs.send(
        new ReceiveMessageCommand({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: max,
          WaitTimeSeconds: wait,
          MessageSystemAttributeNames: ['SentTimestamp'],
        }),
      );
      return out.Messages ?? [];
    } catch (err) {
      this.opts.logger.error('sender.receive_failed', { error: errName(err) });
      await new Promise((r) => setTimeout(r, 1000)); // don't spin on a broken connection
      return [];
    }
  }

  private waitForSlot(): Promise<void> {
    return new Promise<void>((resolve) => {
      this.wake = () => {
        this.wake = undefined;
        resolve();
      };
      // Resolve as soon as any in-flight message finishes.
      void Promise.race(this.inflight).then(() => this.wake?.());
    });
  }

  private start(queue: SendQueueName, message: Message): void {
    const queueUrl = queue === 'urgent' ? this.opts.urgentQueueUrl : this.opts.standardQueueUrl;
    const task = this.handleMessage(queue, queueUrl, message)
      .catch((err: unknown) => {
        this.opts.logger.error('sender.message_failed', { error: errName(err) });
      })
      .finally(() => {
        this.inflight.delete(task);
        this.wake?.();
      });
    this.inflight.add(task);
  }

  private async handleMessage(
    queue: SendQueueName,
    queueUrl: string,
    message: Message,
  ): Promise<void> {
    const batches = parseBatches(message.Body);
    if (!batches) {
      // Poison message: leave it; it will land in the DLQ after maxReceiveCount.
      this.opts.logger.error('sender.unparseable_message', { messageId: message.MessageId });
      return;
    }
    const entry: ActiveMessage = {
      queueUrl,
      message,
      batches,
      controls: batches.map(() => newBatchControl()),
      done: false,
    };
    this.active.add(entry);
    // Heartbeat while this message is still running: keep the SQS message invisible and keep
    // the idempotency lease of every active batch alive.
    const timer = setInterval(() => {
      void this.setVisibility(queueUrl, message, this.extendToSeconds);
      this.extendLeases(entry);
    }, this.extendAfterMs);

    try {
      const results = await Promise.allSettled(
        batches.map((b, i) => this.opts.handle(b, entry.controls[i] ?? newBatchControl())),
      );
      const c = this.counters[queue];
      let allAcked = true;
      let sawPause = false;
      for (const r of results) {
        if (r.status === 'rejected') {
          allAcked = false;
          this.opts.logger.error('sender.batch_failed', { error: errName(r.reason) });
          continue;
        }
        c.BatchesProcessed++;
        c.Sent += r.value.sent;
        c.PermanentFailed += r.value.permanentFailed;
        c.Retried += r.value.retried;
        c.GaveUp += r.value.gaveUp;
        if (!r.value.ack) allAcked = false;
        if (r.value.reason === 'paused') sawPause = true;
      }
      if (sawPause && !allAcked) {
        // Found the kill switch on: park the message instead of letting it bounce to the DLQ.
        await this.setVisibility(queueUrl, message, this.pauseVisibilitySeconds);
      }
      const sentAt = Number(message.Attributes?.['SentTimestamp']);
      if (Number.isFinite(sentAt) && c.lag.length < MAX_LAG_SAMPLES) {
        c.lag.push(Math.max(0, (this.now() - sentAt) / 1000));
      }
      if (allAcked) this.pendingDeletes.push({ queueUrl, message });
    } finally {
      clearInterval(timer);
      entry.done = true;
      this.active.delete(entry);
    }
  }

  private setVisibility(queueUrl: string, message: Message, seconds: number): Promise<void> {
    if (!message.ReceiptHandle) return Promise.resolve();
    return this.opts.sqs
      .send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: queueUrl,
          ReceiptHandle: message.ReceiptHandle,
          VisibilityTimeout: seconds,
        }),
      )
      .then(
        () => undefined,
        (err: unknown) => {
          this.opts.logger.warn('sender.extend_failed', { error: errName(err) });
        },
      );
  }

  /** Extends the claim lease of each active batch; a refused extension means the lease is lost. */
  private extendLeases(entry: ActiveMessage): void {
    const extend = this.opts.extendLease;
    if (!extend) return;
    entry.batches.forEach((batch, i) => {
      const control = entry.controls[i];
      if (!control?.active || control.leaseLost) return;
      extend(batch).then(
        (ok) => {
          if (!ok && control.active) {
            control.leaseLost = true;
            this.opts.logger.warn('sender.lease_extend_refused', { batchKey: batch.batchKey });
          }
        },
        (err: unknown) => {
          // A failed call says nothing about ownership; the next heartbeat tries again.
          this.opts.logger.warn('sender.lease_extend_failed', {
            batchKey: batch.batchKey,
            error: errName(err),
          });
        },
      );
    });
  }

  private async flushDeletes(): Promise<void> {
    while (this.pendingDeletes.length > 0) {
      const queueUrl = this.pendingDeletes[0]?.queueUrl;
      const group: Pending[] = [];
      for (let i = 0; i < this.pendingDeletes.length && group.length < DELETE_BATCH;) {
        const p = this.pendingDeletes[i];
        if (p && p.queueUrl === queueUrl) {
          group.push(p);
          this.pendingDeletes.splice(i, 1);
        } else i++;
      }
      try {
        const out = await this.opts.sqs.send(
          new DeleteMessageBatchCommand({
            QueueUrl: queueUrl,
            Entries: group.map((p, i) => ({
              Id: String(i),
              ReceiptHandle: p.message.ReceiptHandle,
            })),
          }),
        );
        if (out.Failed?.length) {
          // The message returns after its visibility timeout; the claim says 'sent', so it is acked fast.
          this.opts.logger.warn('sender.delete_partial_failure', { failed: out.Failed.length });
        }
      } catch (err) {
        this.opts.logger.warn('sender.delete_failed', { count: group.length, error: errName(err) });
      }
    }
  }

  private async drain(): Promise<void> {
    if (this.inflight.size === 0) return;
    const settle = (ms: number): Promise<'done' | 'timeout'> => {
      let timer: NodeJS.Timeout | undefined;
      const deadline = new Promise<'timeout'>((resolve) => {
        timer = setTimeout(() => resolve('timeout'), ms);
      });
      const done = Promise.all([...this.inflight]).then(() => 'done' as const);
      return Promise.race([done, deadline]).finally(() => clearTimeout(timer));
    };
    if ((await settle(this.drainTimeoutMs)) === 'done') return;

    // Deadline: tell unfinished batches to stop starting chunks, persist nothing and release.
    const unfinished = [...this.active];
    this.opts.logger.warn('sender.drain_timeout', { abandoned: unfinished.length });
    for (const e of unfinished) for (const c of e.controls) c.abort = true;
    await settle(this.abortGraceMs);
    for (const e of unfinished) {
      if (e.done) continue;
      // Still stuck in a network call: release the claims ourselves.
      await Promise.all(e.controls.map((c) => (c.active ? c.release?.() : undefined)));
    }
    // Leave the messages visible so another consumer takes them right away.
    await Promise.all(
      unfinished
        .filter((e) => !this.pendingDeletes.some((p) => p.message === e.message))
        .map((e) => this.setVisibility(e.queueUrl, e.message, 0)),
    );
  }

  /** Writes one EMF line per queue that has activity, then resets the counters. */
  flushMetrics(): void {
    for (const queue of ['urgent', 'standard'] as const) {
      const c = this.counters[queue];
      if (c.BatchesProcessed === 0 && c.lag.length === 0) continue;
      this.counters[queue] = emptyCounters();
      const metrics = [
        ['Sent', 'Count'],
        ['PermanentFailed', 'Count'],
        ['Retried', 'Count'],
        ['GaveUp', 'Count'],
        ['BatchesProcessed', 'Count'],
        ['LagSeconds', 'Seconds'],
      ] as const;
      this.write(
        JSON.stringify({
          _aws: {
            Timestamp: this.now(),
            CloudWatchMetrics: [
              {
                Namespace: METRIC_NAMESPACE,
                Dimensions: [['Queue']],
                Metrics: metrics.map(([Name, Unit]) => ({ Name, Unit })),
              },
            ],
          },
          Queue: queue,
          Sent: c.Sent,
          PermanentFailed: c.PermanentFailed,
          Retried: c.Retried,
          GaveUp: c.GaveUp,
          BatchesProcessed: c.BatchesProcessed,
          LagSeconds: c.lag.length > 0 ? c.lag : [0],
        }),
      );
    }
  }
}

const errName = (err: unknown): string => (err instanceof Error ? err.name : 'Error');
