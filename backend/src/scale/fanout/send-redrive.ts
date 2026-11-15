/**
 * Moves send batches from the urgent / standard DLQs back to their source queues. Invoke by hand
 * (runbook in infra/README.md):
 *   aws lambda invoke --function-name <send redrive fn> --payload '{"queue":"urgent"}' out.json
 * It uses SQS's own message-move task (a DLQ redrive to the original queue), so nothing is lost
 * if this function dies: the task runs inside SQS. Replays are safe: the sender claims each
 * batch key, so a batch that was in fact sent is skipped.
 */
import { SQSClient, StartMessageMoveTaskCommand } from '@aws-sdk/client-sqs';
import type { SendQueueName } from '../contracts.js';

export interface SendRedriveDeps {
  sqs: Pick<SQSClient, 'send'>;
  dlqArns: Record<SendQueueName, string>;
}

export async function redriveSendDlqs(
  deps: SendRedriveDeps,
  which: SendQueueName[],
): Promise<Record<string, string>> {
  const started: Record<string, string> = {};
  for (const queue of which) {
    const out = await deps.sqs.send(
      new StartMessageMoveTaskCommand({ SourceArn: deps.dlqArns[queue] }),
    );
    started[queue] = out.TaskHandle ?? '';
  }
  return started;
}

export const sendRedriveHandler = async (event: {
  queue?: SendQueueName | 'both';
}): Promise<Record<string, string>> => {
  const { URGENT_DLQ_ARN, STANDARD_DLQ_ARN } = process.env;
  if (!URGENT_DLQ_ARN || !STANDARD_DLQ_ARN) {
    throw new Error('URGENT_DLQ_ARN and STANDARD_DLQ_ARN must be set');
  }
  const which: SendQueueName[] =
    event.queue === 'urgent' || event.queue === 'standard' ? [event.queue] : ['urgent', 'standard'];
  return redriveSendDlqs(
    {
      sqs: new SQSClient({}),
      dlqArns: { urgent: URGENT_DLQ_ARN, standard: STANDARD_DLQ_ARN },
    },
    which,
  );
};
