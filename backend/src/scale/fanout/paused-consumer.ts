/**
 * Paused-queue consumer Lambda (SQS trigger, ReportBatchItemFailures). See paused-queue.ts.
 *
 * Per record: re-check the kill switch (cached; fails closed). Paused: park again with the
 * original `parkedAt`. Running: dispatch the shard job through the ShardDispatcher (or publish
 * the AlertReady to the plan queue). A message parked for over 24 hours is reported as a failure
 * on purpose: the queue's redrive policy moves it to the DLQ, whose alarm fires. Anything that
 * throws (SQS or Lambda error) is reported too and retried by SQS.
 */
import { LambdaClient } from '@aws-sdk/client-lambda';
import { SQSClient } from '@aws-sdk/client-sqs';
import type { SQSBatchResponse, SQSEvent } from 'aws-lambda';
import { createKillSwitch } from '../../config/kill-switch.js';
import type { Logger } from '../../delivery/types.js';
import { PARK_MAX_AGE_MS, SqsPausedQueue, parseParked, type PausedQueue } from './paused-queue.js';
import {
  LambdaShardDispatcher,
  SqsPlanQueue,
  type PlanQueue,
  type ShardDispatcher,
} from './queues.js';

export interface PausedConsumerDeps {
  isDeliveryPaused(): Promise<boolean>;
  paused: PausedQueue;
  shards: ShardDispatcher;
  plans: PlanQueue;
  now(): Date;
  logger: Logger;
  maxAgeMs?: number;
}

const defaultLogger: Logger = {
  info: (msg, f) => console.log(JSON.stringify({ level: 'info', msg, ...f })),
  warn: (msg, f) => console.warn(JSON.stringify({ level: 'warn', msg, ...f })),
  error: (msg, f) => console.error(JSON.stringify({ level: 'error', msg, ...f })),
};

export function createPausedConsumer(deps: PausedConsumerDeps) {
  const maxAge = deps.maxAgeMs ?? PARK_MAX_AGE_MS;
  return async (event: SQSEvent): Promise<SQSBatchResponse> => {
    const failures: SQSBatchResponse['batchItemFailures'] = [];
    for (const record of event.Records) {
      const msg = parseParked(record.body);
      if (!msg) {
        // Cannot be fixed by retrying: let it reach the DLQ where the alarm sees it.
        deps.logger.error('fanout.paused_message_invalid', { messageId: record.messageId });
        failures.push({ itemIdentifier: record.messageId });
        continue;
      }
      try {
        const ageMs = deps.now().getTime() - Date.parse(msg.parkedAt);
        if (ageMs > maxAge) {
          deps.logger.error('fanout.paused_gave_up', {
            kind: msg.kind,
            alertId: msg.kind === 'shard' ? msg.job.alertId : msg.ready.alertId,
            ...(msg.kind === 'shard' ? { shardId: msg.job.shardId } : {}),
            parkedHours: Math.round(ageMs / 3_600_000),
          });
          failures.push({ itemIdentifier: record.messageId });
          continue;
        }
        if (await deps.isDeliveryPaused()) {
          await deps.paused.park(msg);
          continue;
        }
        if (msg.kind === 'shard') await deps.shards.dispatch([msg.job]);
        else await deps.plans.publish(msg.ready);
        deps.logger.info('fanout.unparked', {
          kind: msg.kind,
          alertId: msg.kind === 'shard' ? msg.job.alertId : msg.ready.alertId,
        });
      } catch (err) {
        deps.logger.error('fanout.unpark_failed', {
          messageId: record.messageId,
          error: err instanceof Error ? err.name : 'UnknownError',
        });
        failures.push({ itemIdentifier: record.messageId });
      }
    }
    return { batchItemFailures: failures };
  };
}

let real: ReturnType<typeof createPausedConsumer> | undefined;

/** Lambda entry. Env: KILL_SWITCH_PARAM, PAUSED_QUEUE_URL, PLAN_QUEUE_URL, SHARD_FUNCTION_NAME. */
export const handler = async (event: SQSEvent): Promise<SQSBatchResponse> => {
  if (!real) {
    const env = process.env;
    const names = [
      'KILL_SWITCH_PARAM',
      'PAUSED_QUEUE_URL',
      'PLAN_QUEUE_URL',
      'SHARD_FUNCTION_NAME',
    ] as const;
    const missing = names.filter((n) => !env[n]);
    const [param, pausedUrl, planUrl, shardFn] = names.map((n) => env[n]);
    if (!param || !pausedUrl || !planUrl || !shardFn) {
      throw new Error(`Missing environment variables: ${missing.join(', ')}`);
    }
    const sqs = new SQSClient({});
    real = createPausedConsumer({
      isDeliveryPaused: createKillSwitch(param),
      paused: new SqsPausedQueue(sqs, pausedUrl),
      shards: new LambdaShardDispatcher(new LambdaClient({}), shardFn, 10),
      plans: new SqsPlanQueue(sqs, planUrl),
      now: () => new Date(),
      logger: defaultLogger,
    });
  }
  return real(event);
};
