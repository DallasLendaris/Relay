import { DescribeServicesCommand, UpdateServiceCommand, type ECSClient } from '@aws-sdk/client-ecs';
import type { Logger } from '../../delivery/types.js';

/** Raises the sender fleet ahead of the send queues filling. */
export interface FleetScaler {
  preScale(shards: number): Promise<{ current: number; target: number; updated: boolean }>;
}

export interface FleetScalerConfig {
  cluster: string;
  service: string;
  maxTasks: number;
  /** Expected recipients per ShardJob (an estimate). */
  scalePerShard: number;
  /** What one sender task pushes per minute. */
  recipientsPerTaskPerMin: number;
}

/**
 * Target desiredCount for `shards` ShardJobs: ceil(shards x recipients-per-shard / per-task
 * rate), clamped to [current, maxTasks]. Never below `current` (this only scales out; scale-in
 * stays with the idle alarm), even when `current` is already above `maxTasks`.
 */
export function targetTasks(
  shards: number,
  current: number,
  c: Pick<FleetScalerConfig, 'maxTasks' | 'scalePerShard' | 'recipientsPerTaskPerMin'>,
): number {
  const estimate = Math.ceil((shards * c.scalePerShard) / c.recipientsPerTaskPerMin);
  const bounded = Number.isFinite(estimate) ? Math.min(c.maxTasks, estimate) : 0;
  return Math.max(current, bounded);
}

export class EcsFleetScaler implements FleetScaler {
  constructor(
    private readonly ecs: Pick<ECSClient, 'send'>,
    private readonly config: FleetScalerConfig,
    private readonly logger: Logger,
  ) {}

  async preScale(shards: number) {
    const { cluster, service } = this.config;
    const described = await this.ecs.send(
      new DescribeServicesCommand({ cluster, services: [service] }),
    );
    const current = described.services?.[0]?.desiredCount;
    if (current === undefined) throw new Error('sender service not found');
    const target = targetTasks(shards, current, this.config);
    if (target <= current) {
      this.logger.info('fanout.prescale', { shards, current, target, updated: false });
      return { current, target, updated: false };
    }
    await this.ecs.send(new UpdateServiceCommand({ cluster, service, desiredCount: target }));
    this.logger.info('fanout.prescale', { shards, current, target, updated: true });
    return { current, target, updated: true };
  }
}

const positive = (v: string | undefined): number | undefined => {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : undefined;
};

/** Env names for the pre-scale; optional at runtime (no scaler is built when any is missing). */
export const SCALER_ENV = [
  'SENDER_CLUSTER_ARN',
  'SENDER_SERVICE_NAME',
  'SENDER_MAX_TASKS',
  'SENDER_SCALE_PER_SHARD',
  'SENDER_RECIPIENTS_PER_TASK_PER_MIN',
] as const;

/** Builds the scaler from env, or undefined when it is off (SENDER_MAX_TASKS 0 or unset). */
export function fleetScalerFromEnv(
  env: Record<string, string | undefined>,
  ecs: Pick<ECSClient, 'send'>,
  logger: Logger,
): EcsFleetScaler | undefined {
  const maxTasks = positive(env.SENDER_MAX_TASKS);
  const scalePerShard = positive(env.SENDER_SCALE_PER_SHARD);
  const perTask = positive(env.SENDER_RECIPIENTS_PER_TASK_PER_MIN);
  const { SENDER_CLUSTER_ARN: cluster, SENDER_SERVICE_NAME: service } = env;
  if (!maxTasks || !scalePerShard || !perTask || !cluster || !service) return undefined;
  return new EcsFleetScaler(
    ecs,
    { cluster, service, maxTasks, scalePerShard, recipientsPerTaskPerMin: perTask },
    logger,
  );
}
