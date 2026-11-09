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
 * - stop() (SIGTERM): stop receiving, finish in-flight work, flush deletes and metrics, return.
 *   Work that does not finish within `drainTimeoutMs` is abandoned; SQS redelivers it and the
 *   idempotency lease expires on its own.
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
import type { BatchOutcome } from './sender.js';

export const METRIC_NAMESPACE = 'Relay/Sender';
const DELETE_BATCH = 10;
const MAX_LAG_SAMPLES = 100; // EMF allows up to 100 values per metric

export interface ConsumerOptions {
  sqs: Pick<SQSClient, 'send'>;
  urgentQueueUrl: string;
  standardQueueUrl: string;
  /** Processes one batch. Throwing counts as a failed batch (the message stays on the queue). */
  handle(batch: SendBatch): Promise<BatchOutcome>;
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
  private wake: (() => void) | undefined;
  private readonly concurrency: number;
  private readonly extendAfterMs: number;
  private readonly extendToSeconds: number;
  private readonly waitSeconds: number;
  private readonly drainTimeoutMs: number;
  private readonly now: () => number;
  private readonly write: (line: string) => void;

  constructor(private readonly opts: ConsumerOptions) {
    this.concurrency = opts.concurrency ?? 50;
    this.extendAfterMs = opts.extendAfterMs ?? 20_000;
    this.extendToSeconds = opts.extendToSeconds ?? 60;
    this.waitSeconds = opts.waitSeconds ?? 1;
    this.drainTimeoutMs = opts.drainTimeoutMs ?? 25_000;
    this.now = opts.now ?? Date.now;
    this.write = opts.writeMetric ?? ((line) => console.log(line));
  }

  /** Stop receiving and let run() finish in-flight work. Safe to call more than once. */
  stop(): void {
    this.stopping = true;
    this.wake?.();
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
    // Visibility heartbeat while this message is still running.
    const timer = setInterval(() => {
      if (!message.ReceiptHandle) return;
      this.opts.sqs
        .send(
          new ChangeMessageVisibilityCommand({
            QueueUrl: queueUrl,
            ReceiptHandle: message.ReceiptHandle,
            VisibilityTimeout: this.extendToSeconds,
          }),
        )
        .catch((err: unknown) => {
          this.opts.logger.warn('sender.extend_failed', { error: errName(err) });
        });
    }, this.extendAfterMs);

    try {
      const results = await Promise.allSettled(batches.map((b) => this.opts.handle(b)));
      const c = this.counters[queue];
      let allAcked = true;
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
      }
      const sentAt = Number(message.Attributes?.['SentTimestamp']);
      if (Number.isFinite(sentAt) && c.lag.length < MAX_LAG_SAMPLES) {
        c.lag.push(Math.max(0, (this.now() - sentAt) / 1000));
      }
      if (allAcked) this.pendingDeletes.push({ queueUrl, message });
    } finally {
      clearInterval(timer);
    }
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
    let timer: NodeJS.Timeout | undefined;
    const deadline = new Promise<'timeout'>((resolve) => {
      timer = setTimeout(() => resolve('timeout'), this.drainTimeoutMs);
    });
    const done = Promise.all([...this.inflight]).then(() => 'done' as const);
    const result = await Promise.race([done, deadline]);
    clearTimeout(timer);
    if (result === 'timeout') {
      this.opts.logger.warn('sender.drain_timeout', { abandoned: this.inflight.size });
    }
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
