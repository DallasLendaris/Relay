import { SendMessageBatchCommand, type SQSClient } from '@aws-sdk/client-sqs';
import {
  MAX_BATCHES_PER_MESSAGE,
  type SendBatch,
  type SendQueue,
  type SendQueueName,
} from '../contracts.js';
import { clampDelay } from '../fanout/queues.js';

/**
 * SendQueue on SQS, used by the sender to re-queue failed recipients. Batches are packed as a
 * JSON array per message (the same shape the fan-out writes). Entries SQS rejects are retried
 * once, then this throws so the caller fails the message.
 */
export class SqsSendQueue implements SendQueue {
  constructor(
    private readonly sqs: Pick<SQSClient, 'send'>,
    private readonly urls: Record<SendQueueName, string>,
  ) {}

  async enqueue(
    queue: SendQueueName,
    batches: SendBatch[],
    opts?: { delaySeconds?: number },
  ): Promise<void> {
    const delay = clampDelay(opts?.delaySeconds);
    const bodies: string[] = [];
    for (let i = 0; i < batches.length; i += MAX_BATCHES_PER_MESSAGE) {
      bodies.push(JSON.stringify(batches.slice(i, i + MAX_BATCHES_PER_MESSAGE)));
    }
    for (let i = 0; i < bodies.length; i += 10) {
      let entries = bodies.slice(i, i + 10).map((b, n) => ({
        Id: String(n),
        MessageBody: b,
        ...(delay ? { DelaySeconds: delay } : {}),
      }));
      for (let attempt = 0; attempt < 2 && entries.length > 0; attempt++) {
        const res = await this.sqs.send(
          new SendMessageBatchCommand({ QueueUrl: this.urls[queue], Entries: entries }),
        );
        const failed = new Set((res.Failed ?? []).map((f) => f.Id));
        entries = entries.filter((e) => failed.has(e.Id));
      }
      if (entries.length > 0) {
        throw new Error(`SQS rejected ${entries.length} send message(s) after retry`);
      }
    }
  }
}
