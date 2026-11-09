import { InvokeCommand, type LambdaClient } from '@aws-sdk/client-lambda';
import {
  SendMessageBatchCommand,
  type SendMessageBatchRequestEntry,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import type { AlertReady, SendBatch, SendQueue, SendQueueName, ShardJob } from '../contracts.js';
import { packBatches, serializeMessage } from './pack.js';

/** Publishes "this alert is ready" for the planner (used by ingest). */
export interface PlanQueue {
  publish(message: AlertReady): Promise<void>;
}

/**
 * How the planner hands ShardJobs to shard workers. Two implementations:
 *  - LambdaShardDispatcher (default): one async Lambda invoke per job. Async invokes scale at
 *    Lambda's burst rate, while an SQS event source adds only about 300 concurrent invocations a
 *    minute, too slow for a 60 s fan-out.
 *  - SqsShardDispatcher: one SQS message per job, for a queue-triggered shard worker.
 * Rejects if any job could not be handed over, so the planner's own message is retried
 * (re-dispatching a job is safe: batch keys are deterministic).
 */
export interface ShardDispatcher {
  dispatch(jobs: ShardJob[]): Promise<void>;
}

const ENTRIES_PER_REQUEST = 10; // SQS maximum
/** One SendMessageBatch request may carry at most 256 KB in total (older accounts); stay under. */
const REQUEST_BYTES = 250 * 1024;
const MAX_ATTEMPTS = 4;

/** SQS allows 0 to 900 seconds of per-message delay. */
export const MAX_SQS_DELAY_SECONDS = 900;
export const clampDelay = (s: number | undefined): number | undefined =>
  s === undefined ? undefined : Math.max(0, Math.min(MAX_SQS_DELAY_SECONDS, Math.ceil(s)));

type SqsLike = Pick<SQSClient, 'send'>;
type Sleep = (ms: number) => Promise<void>;
const defaultSleep: Sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Sends message bodies with SendMessageBatch: at most 10 per request and under the request size
 * cap. Entries SQS reports as failed are retried on their own (with a short backoff); if any
 * are still failing after MAX_ATTEMPTS, throws so the caller's message is retried.
 */
export async function sendBodies(
  sqs: SqsLike,
  queueUrl: string,
  bodies: string[],
  sleep: Sleep = defaultSleep,
  /** Per-message delay (SQS caps it at 900 s). Omitted: no delay. */
  delaySeconds?: number,
): Promise<void> {
  const requests: string[][] = [];
  let current: string[] = [];
  let bytes = 0;
  for (const body of bodies) {
    const size = Buffer.byteLength(body, 'utf8');
    if (
      current.length > 0 &&
      (current.length >= ENTRIES_PER_REQUEST || bytes + size > REQUEST_BYTES)
    ) {
      requests.push(current);
      current = [];
      bytes = 0;
    }
    current.push(body);
    bytes += size;
  }
  if (current.length > 0) requests.push(current);

  for (const request of requests) {
    let pending: SendMessageBatchRequestEntry[] = request.map((body, i) => ({
      Id: String(i),
      MessageBody: body,
      ...(delaySeconds ? { DelaySeconds: delaySeconds } : {}),
    }));
    for (let attempt = 1; pending.length > 0; attempt++) {
      const res = await sqs.send(
        new SendMessageBatchCommand({ QueueUrl: queueUrl, Entries: pending }),
      );
      const failed = res.Failed ?? [];
      if (failed.length === 0) break;
      const failedIds = new Set(failed.map((f) => f.Id));
      pending = pending.filter((e) => failedIds.has(e.Id));
      const permanent = failed.some((f) => f.SenderFault === true);
      if (permanent || attempt >= MAX_ATTEMPTS) {
        throw new Error(
          `SQS rejected ${pending.length} message(s) after ${attempt} attempt(s): ${failed
            .map((f) => f.Code ?? 'unknown')
            .join(',')}`,
        );
      }
      await sleep(50 * 2 ** attempt);
    }
  }
}

export class SqsShardDispatcher implements ShardDispatcher {
  constructor(
    private readonly sqs: SqsLike,
    private readonly queueUrl: string,
    private readonly sleep: Sleep = defaultSleep,
  ) {}
  async dispatch(jobs: ShardJob[]): Promise<void> {
    await sendBodies(
      this.sqs,
      this.queueUrl,
      jobs.map((j) => JSON.stringify(j)),
      this.sleep,
    );
  }
}

/** Invokes the shard function asynchronously (InvocationType 'Event'), `concurrency` at a time. */
export class LambdaShardDispatcher implements ShardDispatcher {
  constructor(
    private readonly lambda: Pick<LambdaClient, 'send'>,
    private readonly functionName: string,
    private readonly concurrency = 50,
  ) {}

  async dispatch(jobs: ShardJob[]): Promise<void> {
    let next = 0;
    let failure: unknown;
    const run = async (): Promise<void> => {
      while (next < jobs.length && failure === undefined) {
        const job = jobs[next++];
        if (!job) continue;
        try {
          const res = await this.lambda.send(
            new InvokeCommand({
              FunctionName: this.functionName,
              InvocationType: 'Event',
              Payload: Buffer.from(JSON.stringify(job), 'utf8'),
            }),
          );
          // An accepted async invoke returns 202.
          if (res.StatusCode !== 202 || res.FunctionError) {
            throw new Error(`async invoke of shard ${job.shardId} returned ${res.StatusCode}`);
          }
        } catch (err) {
          failure ??= err;
        }
      }
    };
    await Promise.all(Array.from({ length: Math.min(this.concurrency, jobs.length) }, run));
    if (failure !== undefined) throw failure;
  }
}

export class SqsSendQueue implements SendQueue {
  constructor(
    private readonly sqs: SqsLike,
    private readonly urls: Record<SendQueueName, string>,
    private readonly sleep: Sleep = defaultSleep,
  ) {}
  async enqueue(
    queue: SendQueueName,
    batches: SendBatch[],
    opts?: { delaySeconds?: number },
  ): Promise<void> {
    const bodies = packBatches(batches).map(serializeMessage);
    await sendBodies(
      this.sqs,
      this.urls[queue],
      bodies,
      this.sleep,
      clampDelay(opts?.delaySeconds),
    );
  }
}

export class SqsPlanQueue implements PlanQueue {
  constructor(
    private readonly sqs: SqsLike,
    private readonly queueUrl: string,
    private readonly sleep: Sleep = defaultSleep,
  ) {}
  async publish(message: AlertReady): Promise<void> {
    await sendBodies(this.sqs, this.queueUrl, [JSON.stringify(message)], this.sleep);
  }
}

// ---------- In-memory versions (tests and local runs) ----------

export class MemoryPlanQueue implements PlanQueue {
  readonly messages: AlertReady[] = [];
  async publish(message: AlertReady): Promise<void> {
    this.messages.push(structuredClone(message));
  }
}

export class MemoryShardDispatcher implements ShardDispatcher {
  readonly jobs: ShardJob[] = [];
  async dispatch(jobs: ShardJob[]): Promise<void> {
    for (const j of jobs) this.jobs.push(structuredClone(j));
  }
}

/** Records the packed messages exactly as SqsSendQueue would send them. */
export class MemorySendQueue implements SendQueue {
  readonly messages: Record<SendQueueName, string[]> = { urgent: [], standard: [] };
  async enqueue(
    queue: SendQueueName,
    batches: SendBatch[],
    _opts?: { delaySeconds?: number },
  ): Promise<void> {
    for (const m of packBatches(batches)) this.messages[queue].push(serializeMessage(m));
  }
  /** Every batch sent to a queue, parsed back from the messages. */
  batches(queue: SendQueueName): SendBatch[] {
    return this.messages[queue].flatMap((m) => JSON.parse(m) as SendBatch[]);
  }
}
