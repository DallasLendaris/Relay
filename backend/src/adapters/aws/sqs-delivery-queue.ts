import {
  SendMessageBatchCommand,
  type SendMessageBatchRequestEntry,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import type { DeliveryJob, DeliveryQueue } from '../../ports.js';

const BATCH_LIMIT = 10; // SQS maximum per SendMessageBatch

/** DeliveryQueue on SQS. One message per job; failed batch entries are retried once. */
export class SqsDeliveryQueue implements DeliveryQueue {
  constructor(
    private readonly sqs: Pick<SQSClient, 'send'>,
    private readonly queueUrl: string,
  ) {}

  async enqueue(jobs: DeliveryJob[]): Promise<void> {
    for (let i = 0; i < jobs.length; i += BATCH_LIMIT) {
      const entries = jobs.slice(i, i + BATCH_LIMIT).map((job, n) => ({
        Id: String(n),
        MessageBody: JSON.stringify(job),
      }));
      const failed = await this.sendBatch(entries);
      if (failed.length === 0) continue;
      const stillFailed = await this.sendBatch(failed); // one retry
      if (stillFailed.length > 0) {
        throw new Error(`SQS rejected ${stillFailed.length} delivery job(s) after retry`);
      }
    }
  }

  /** Returns the entries SQS reported as failed. */
  private async sendBatch(
    entries: SendMessageBatchRequestEntry[],
  ): Promise<SendMessageBatchRequestEntry[]> {
    const res = await this.sqs.send(
      new SendMessageBatchCommand({ QueueUrl: this.queueUrl, Entries: entries }),
    );
    const failedIds = new Set((res.Failed ?? []).map((f) => f.Id));
    return entries.filter((e) => failedIds.has(e.Id));
  }
}
