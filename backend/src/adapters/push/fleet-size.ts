import { DescribeServicesCommand, type ECSClient } from '@aws-sdk/client-ecs';
import { DEFAULT_FCM_PROJECT_FLEET_RATE } from './fcm-projects.js';

/**
 * How many sender tasks are running right now. The FCM per-project quota is shared by the whole
 * fleet, and the fleet scales (10 to 100 tasks) during an urgent alert, so each task's share of
 * the budget is budget / fleet size.
 */
export interface FleetSizeSource {
  /** Current running task count, at least 1. Never throws. */
  size(): number;
  /** Re-reads the count from the source. Never rejects. */
  refresh(): Promise<void>;
}

/** A constant fleet size (local dev, tests, or an explicit override). */
export class FixedFleetSize implements FleetSizeSource {
  constructor(private readonly n: number = 1) {}
  size(): number {
    return Math.max(1, Math.floor(this.n));
  }
  async refresh(): Promise<void> {}
}

export interface EcsFleetSizeOptions {
  cluster: string;
  service: string;
  /** Base cache time. Default 15 s. */
  ttlMs?: number;
  /** Up to this fraction of the ttl is added at random, so tasks do not poll in lockstep. */
  jitter?: number;
  /** Used before the first successful read. Default 1. */
  initial?: number;
  onError?: (err: unknown) => void;
  now?: () => number;
  random?: () => number;
}

/**
 * Reads runningCount from ECS DescribeServices, cached for about 15 s with jitter. On an error
 * (throttling, permissions, an outage) the last known value stays in force: guessing would either
 * overshoot the quota or stall sends. Never below 1.
 */
export class EcsFleetSize implements FleetSizeSource {
  private current: number;
  private nextAt = 0;
  private inFlight: Promise<void> | undefined;
  private readonly now: () => number;
  private readonly random: () => number;

  constructor(
    private readonly ecs: Pick<ECSClient, 'send'>,
    private readonly opts: EcsFleetSizeOptions,
  ) {
    this.current = Math.max(1, Math.floor(opts.initial ?? 1));
    this.now = opts.now ?? Date.now;
    this.random = opts.random ?? Math.random;
  }

  size(): number {
    return this.current;
  }

  /** Refreshes when the cache has expired; concurrent callers share one request. */
  refresh(): Promise<void> {
    if (this.now() < this.nextAt) return Promise.resolve();
    this.inFlight ??= this.fetch().finally(() => {
      this.inFlight = undefined;
    });
    return this.inFlight;
  }

  private async fetch(): Promise<void> {
    const ttl = this.opts.ttlMs ?? 15_000;
    this.nextAt = this.now() + ttl * (1 + (this.opts.jitter ?? 0.2) * this.random());
    try {
      const out = await this.ecs.send(
        new DescribeServicesCommand({
          cluster: this.opts.cluster,
          services: [this.opts.service],
        }),
      );
      const running = out.services?.[0]?.runningCount;
      if (typeof running === 'number' && Number.isFinite(running)) {
        this.current = Math.max(1, Math.floor(running));
      } else {
        this.opts.onError?.(new Error('sender service not found'));
      }
    } catch (err) {
      this.opts.onError?.(err);
    }
  }
}

/**
 * The fleet size source for this process: ECS when ECS_CLUSTER and ECS_SERVICE are set, otherwise
 * a fixed 1 (local dev).
 */
export function fleetSizeFromEnv(
  env: Record<string, string | undefined>,
  ecs: () => Pick<ECSClient, 'send'>,
  onError?: (err: unknown) => void,
): FleetSizeSource {
  const cluster = env['ECS_CLUSTER'];
  const service = env['ECS_SERVICE'];
  if (!cluster || !service) return new FixedFleetSize(1);
  return new EcsFleetSize(ecs(), { cluster, service, ...(onError ? { onError } : {}) });
}

/** Per-task rate: the fleet budget split over the running tasks. */
export function perTaskRate(fleetBudgetPerSec: number, source: FleetSizeSource): number {
  return fleetBudgetPerSec / Math.max(1, source.size());
}

function positiveNumber(name: string, raw: string): number {
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) throw new Error(`${name} must be a positive number`);
  return n;
}

/**
 * The per-task FCM rate from the environment. An explicit FCM_PROJECT_RATE_PER_SEC (a per-task
 * override) wins and is constant; otherwise the rate is FCM_PROJECT_FLEET_RATE_PER_SEC (default
 * 9,000) divided by the running sender task count.
 */
export function fcmRateFromEnv(
  env: Record<string, string | undefined>,
  source: FleetSizeSource,
): { provider: () => number; mode: 'per-task' | 'fleet' } {
  const override = env['FCM_PROJECT_RATE_PER_SEC'];
  if (override !== undefined && override !== '') {
    const rate = positiveNumber('FCM_PROJECT_RATE_PER_SEC', override);
    return { provider: () => rate, mode: 'per-task' };
  }
  const raw = env['FCM_PROJECT_FLEET_RATE_PER_SEC'];
  const budget =
    raw !== undefined && raw !== ''
      ? positiveNumber('FCM_PROJECT_FLEET_RATE_PER_SEC', raw)
      : DEFAULT_FCM_PROJECT_FLEET_RATE;
  return { provider: () => perTaskRate(budget, source), mode: 'fleet' };
}
