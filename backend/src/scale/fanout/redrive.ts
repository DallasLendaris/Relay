/**
 * Replays shard jobs from the shard DLQ. Invoke by hand (see infra/README.md runbook):
 *   aws lambda invoke --function-name <redrive fn> --payload '{"max":1000}' out.json
 * The DLQ holds Lambda async-failure records ({requestPayload: ShardJob, ...}) or bare ShardJobs.
 * Each is re-invoked asynchronously on the shard function and only then deleted from the DLQ, so
 * a crash here loses nothing. Replays are safe: batch keys are deterministic and the sender
 * claims them.
 */
import { InvokeCommand, LambdaClient } from '@aws-sdk/client-lambda';
import {
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SQSClient,
  type Message,
} from '@aws-sdk/client-sqs';
import type { ShardJob } from '../contracts.js';
import { isShardJob } from './messages.js';

/** Pulls the ShardJob out of a DLQ message body, or null if it is neither shape. */
export function shardJobFromDlqBody(body: string): ShardJob | null {
  try {
    const parsed: unknown = JSON.parse(body);
    const inner = (parsed as { requestPayload?: unknown } | null)?.requestPayload;
    if (isShardJob(inner)) return inner;
    return isShardJob(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

export interface RedriveDeps {
  sqs: Pick<SQSClient, 'send'>;
  lambda: Pick<LambdaClient, 'send'>;
  dlqUrl: string;
  shardFunctionName: string;
}

export async function redrive(
  deps: RedriveDeps,
  max = 1_000,
): Promise<{ replayed: number; skipped: number }> {
  let replayed = 0;
  let skipped = 0;
  while (replayed + skipped < max) {
    const out = await deps.sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: deps.dlqUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 1,
      }),
    );
    const messages: Message[] = out.Messages ?? [];
    if (messages.length === 0) break;
    for (const m of messages) {
      const job = shardJobFromDlqBody(m.Body ?? '');
      if (!job) {
        skipped++; // left in the DLQ for a person to look at
        continue;
      }
      await deps.lambda.send(
        new InvokeCommand({
          FunctionName: deps.shardFunctionName,
          InvocationType: 'Event',
          Payload: Buffer.from(JSON.stringify(job), 'utf8'),
        }),
      );
      await deps.sqs.send(
        new DeleteMessageCommand({ QueueUrl: deps.dlqUrl, ReceiptHandle: m.ReceiptHandle }),
      );
      replayed++;
    }
  }
  return { replayed, skipped };
}

export const redriveHandler = async (event: {
  max?: number;
}): Promise<{ replayed: number; skipped: number }> => {
  const { SHARD_DLQ_URL, SHARD_FUNCTION_NAME } = process.env;
  if (!SHARD_DLQ_URL || !SHARD_FUNCTION_NAME) {
    throw new Error('SHARD_DLQ_URL and SHARD_FUNCTION_NAME must be set');
  }
  return redrive(
    {
      sqs: new SQSClient({}),
      lambda: new LambdaClient({}),
      dlqUrl: SHARD_DLQ_URL,
      shardFunctionName: SHARD_FUNCTION_NAME,
    },
    event.max,
  );
};
