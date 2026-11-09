import type { AlertReady, ShardJob } from '../contracts.js';

const isString = (v: unknown): v is string => typeof v === 'string' && v !== '';

export function isAlertReady(v: unknown): v is AlertReady {
  const o = v as Partial<AlertReady> | null;
  return !!o && isString(o.alertId) && isString(o.sourceId) && isString(o.relaySeverity);
}

export function isShardJob(v: unknown): v is ShardJob {
  const o = v as Partial<ShardJob> | null;
  return (
    !!o &&
    isString(o.alertId) &&
    isString(o.sourceId) &&
    isString(o.relaySeverity) &&
    isString(o.shardId) &&
    Array.isArray(o.cells)
  );
}
