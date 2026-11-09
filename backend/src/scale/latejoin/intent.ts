import { SendMessageCommand, type SQSClient } from '@aws-sdk/client-sqs';

/**
 * "This device may have missed an active alert." The API sends this when a place is added or a
 * push target changes. It is a hint only: the late-join Lambda reads `deviceId`, re-reads the
 * device, its places and the active alerts from Aurora, and ignores everything else here. An
 * intent that names the wrong place, or a made-up one, therefore cannot choose a recipient,
 * an alert or a cell. See latejoin.ts.
 */
export interface LateJoinIntent {
  deviceId: string;
  placeId?: string;
  reason: 'place_added' | 'push_target';
}

/** The API's only way to cause a late-join send. Only the API has permission to send to it. */
export interface LateJoinQueue {
  enqueue(intent: LateJoinIntent): Promise<void>;
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** Only the device id is trusted to be present; everything else is advisory. */
export function isLateJoinIntent(v: unknown): v is LateJoinIntent {
  const o = v as Partial<LateJoinIntent> | null;
  return (
    typeof o === 'object' && o !== null && typeof o.deviceId === 'string' && UUID.test(o.deviceId)
  );
}

export class SqsLateJoinQueue implements LateJoinQueue {
  constructor(
    private readonly sqs: Pick<SQSClient, 'send'>,
    private readonly queueUrl: string,
  ) {}
  async enqueue(intent: LateJoinIntent): Promise<void> {
    await this.sqs.send(
      new SendMessageCommand({ QueueUrl: this.queueUrl, MessageBody: JSON.stringify(intent) }),
    );
  }
}

/** For tests and local runs. */
export class MemoryLateJoinQueue implements LateJoinQueue {
  readonly intents: LateJoinIntent[] = [];
  async enqueue(intent: LateJoinIntent): Promise<void> {
    this.intents.push(intent);
  }
}
