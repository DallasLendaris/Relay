import type { AlertReady, ShardJob } from '../contracts.js';

/**
 * A ShardJob that may resume partway: `startAt` is the index into `cells` of the first cell still
 * to do (default 0). contracts.ts owns ShardJob and has no startAt yet (reported to the
 * orchestrator); the extra field is optional, so a plain ShardJob is a valid ResumableShardJob.
 */
export type ResumableShardJob = ShardJob & { startAt?: number };

const isString = (v: unknown): v is string => typeof v === 'string' && v !== '';

export function isAlertReady(v: unknown): v is AlertReady {
  const o = v as Partial<AlertReady> | null;
  return !!o && isString(o.alertId) && isString(o.sourceId) && isString(o.relaySeverity);
}

export function isShardJob(v: unknown): v is ResumableShardJob {
  const o = v as Partial<ResumableShardJob> | null;
  return (
    !!o &&
    isString(o.alertId) &&
    isString(o.sourceId) &&
    isString(o.relaySeverity) &&
    isString(o.shardId) &&
    Array.isArray(o.cells) &&
    (o.startAt === undefined || (Number.isInteger(o.startAt) && o.startAt >= 0))
  );
}
