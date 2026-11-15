/**
 * The "paused" parking queue (`relay-<stage>-shard-paused`).
 *
 * A planner or shard worker that finds the kill switch paused does NOT throw (an async Lambda
 * invoke that throws is retried twice and then lands in the DLQ, which needs a manual redrive).
 * It parks its message here instead, with a 60 s delivery delay, and succeeds. The paused-queue
 * consumer re-checks the switch: still paused means park again (same `parkedAt`), running means
 * hand the job back to the shard function (or the plan queue), and after 24 hours of pause the
 * message is left to fail into the DLQ, which has an alarm.
 */
import { SendMessageCommand, type SQSClient } from '@aws-sdk/client-sqs';
import type { AlertReady } from '../contracts.js';
import type { ResumableShardJob } from './messages.js';

/** Delivery delay of a parked message (and of every re-park). */
export const PARK_DELAY_SECONDS = 60;
/** A job parked this long is given up on (DLQ + alarm). */
export const PARK_MAX_AGE_MS = 24 * 60 * 60 * 1000;

export type ParkedMessage =
  | { kind: 'shard'; job: ResumableShardJob; parkedAt: string }
  | { kind: 'plan'; ready: AlertReady; parkedAt: string };

export interface PausedQueue {
  /** Parks a message for PARK_DELAY_SECONDS. Rejects when SQS does (the caller then throws). */
  park(message: ParkedMessage): Promise<void>;
}

export class SqsPausedQueue implements PausedQueue {
  constructor(
    private readonly sqs: Pick<SQSClient, 'send'>,
    private readonly queueUrl: string,
  ) {}
  async park(message: ParkedMessage): Promise<void> {
    await this.sqs.send(
      new SendMessageCommand({
        QueueUrl: this.queueUrl,
        MessageBody: JSON.stringify(message),
        DelaySeconds: PARK_DELAY_SECONDS,
      }),
    );
  }
}

export class MemoryPausedQueue implements PausedQueue {
  readonly messages: ParkedMessage[] = [];
  async park(message: ParkedMessage): Promise<void> {
    this.messages.push(structuredClone(message));
  }
}

export function parseParked(body: string): ParkedMessage | null {
  let raw: unknown;
  try {
    raw = JSON.parse(body);
  } catch {
    return null;
  }
  const o = raw as Partial<ParkedMessage> | null;
  if (!o || typeof o.parkedAt !== 'string' || Number.isNaN(Date.parse(o.parkedAt))) return null;
  if (o.kind === 'shard' && o.job && typeof o.job === 'object') return o as ParkedMessage;
  if (o.kind === 'plan' && o.ready && typeof o.ready === 'object') return o as ParkedMessage;
  return null;
}
