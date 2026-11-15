import { SendMessageBatchCommand, type SQSClient } from '@aws-sdk/client-sqs';
import type { SendBatch, SendQueue, SendQueueName } from '../contracts.js';
import { assertMessageFits, packBatches, serializeMessage } from '../fanout/pack.js';
import { clampDelay } from '../fanout/queues.js';

/** At most 10 entries per SendMessageBatch, and under the request size cap (256 KB on older accounts). */
const ENTRIES_PER_REQUEST = 10;
const REQUEST_BYTES = 250 * 1024;

/**
 * SendQueue on SQS, used by the sender to re-queue failed recipients. Batches are packed as a
 * JSON array per message (the same shape and the same packBatches the fan-out uses), so a message
 * never exceeds the SQS size limit by count alone: packing is by measured bytes, and a batch too
 * big for one message is split. Entries SQS rejects are retried once, then this throws so the
 * caller fails the message.
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
    const bodies = packBatches(batches).map(serializeMessage);
    const requests: string[][] = [];
    let current: string[] = [];
    let bytes = 0;
    for (const body of bodies) {
      assertMessageFits(body);
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
      let entries = request.map((b, n) => ({
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
