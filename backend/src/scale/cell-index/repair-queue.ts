import { SendMessageCommand, type SQSClient } from '@aws-sdk/client-sqs';

/**
 * A cell-index write that failed in the API and must be redone. The message names WHAT to look
 * at, never what to write: the repair Lambda re-reads Aurora (the record) and applies the truth,
 * so a stale or repeated message can never resurrect a deleted place or an old push target.
 *
 *   put     a place was added: make sure its entry exists with the device's current target
 *   remove  a place was deleted: make sure its entry is gone (unless Aurora still has the place)
 *   update  the device's push target or language changed. `h3Cell` and `placeId` are '' to mean
 *           "all of this device's places" (the API cannot always list them after a failure)
 */
export interface IndexRepairMessage {
  op: 'put' | 'remove' | 'update';
  h3Cell: string;
  deviceId: string;
  placeId: string;
}

export interface IndexRepairQueue {
  enqueue(messages: IndexRepairMessage[]): Promise<void>;
}

/** SQS-backed repair queue (INDEX_REPAIR_QUEUE_URL). One message per repair. */
export class SqsIndexRepairQueue implements IndexRepairQueue {
  constructor(
    private readonly sqs: Pick<SQSClient, 'send'>,
    private readonly queueUrl: string,
  ) {}

  async enqueue(messages: IndexRepairMessage[]): Promise<void> {
    for (const m of messages) {
      await this.sqs.send(
        new SendMessageCommand({ QueueUrl: this.queueUrl, MessageBody: JSON.stringify(m) }),
      );
    }
  }
}

/** In-memory repair queue for tests and local runs. */
export class MemoryIndexRepairQueue implements IndexRepairQueue {
  readonly messages: IndexRepairMessage[] = [];
  async enqueue(messages: IndexRepairMessage[]): Promise<void> {
    this.messages.push(...messages.map((m) => ({ ...m })));
  }
}

/** Parses a queue message body, or null when it is not a valid repair message. */
export function parseRepairMessage(body: string): IndexRepairMessage | null {
  let raw: unknown;
  try {
    raw = JSON.parse(body);
  } catch {
    return null;
  }
  if (typeof raw !== 'object' || raw === null) return null;
  const m = raw as Record<string, unknown>;
  const { op, h3Cell, deviceId, placeId } = m;
  if (op !== 'put' && op !== 'remove' && op !== 'update') return null;
  if (typeof h3Cell !== 'string' || typeof deviceId !== 'string' || typeof placeId !== 'string') {
    return null;
  }
  if (deviceId === '') return null;
  return { op, h3Cell, deviceId, placeId };
}
