/**
 * Account-level capacity assumptions and the Lambda reserved-concurrency budget (SEC-C-2 / RED-4).
 *
 * Lambda concurrency is one pool per account and Region. A function without reserved concurrency
 * draws from the unreserved part of that pool, so an API flood could use all of it and starve the
 * planner, the shard workers, ingest and the check-in aggregator. Reserving concurrency for those
 * and capping the API means the API can never take more than its own share.
 *
 * The budget, for prod (dev functions are unreserved except the ones that always were):
 *
 *   apiCap + planner + shard + aggregator + ingest + delivery + repair <= quota - 100
 *
 * AWS also refuses a reservation that leaves under 100 unreserved, and the late-join, paused
 * consumer, drainer and redrive functions share that 100. docs/scale-quotas.md has the table.
 */

/**
 * Lambda concurrent executions per Region assumed today: the AWS default (docs/scale-quotas.md).
 * Raise the real quota first, then pass `-c lambdaConcurrencyQuota=15000` so the API cap grows.
 */
export const DEFAULT_LAMBDA_CONCURRENCY_QUOTA = 1_000;
/** AWS requires at least this much unreserved concurrency in the account. */
export const LAMBDA_MIN_UNRESERVED = 100;

/** HTTP API throttle quota per account and Region (docs/scale-quotas.md). The stage never exceeds it. */
export const DEFAULT_APIGW_RPS_QUOTA = 10_000;
/** The HTTP API burst bucket is fixed by the service at 5,000. */
export const APIGW_BURST = 5_000;

// ---- Reserved concurrency of the functions that must never be starved ----
/** One AlertReady at a time per planner invoke; a handful covers back-to-back alerts. */
export const PLANNER_RESERVED_CONCURRENCY = 10;
/** The shard worker pool floor: the 60 s fan-out of one county needs hundreds of workers at once. */
export const DEFAULT_SHARD_CONCURRENCY = 500;
/** Ingest polls with one overlapping-free poller. */
export const INGEST_RESERVED_CONCURRENCY = 1;
/** The Expo delivery worker (also caps parallel sends for Expo's rate limits). */
export const DELIVERY_RESERVED_CONCURRENCY = 10;
/** The check-in stream aggregator: parallelizationFactor 10 over a handful of stream shards. */
export const DEFAULT_AGGREGATOR_CONCURRENCY = 50;
/** The cell-index repair consumer (SQS, small batches). */
export const DEFAULT_REPAIR_CONCURRENCY = 5;

export interface ConcurrencyPlan {
  quota: number;
  api: number;
  aggregator: number;
  repair: number;
  planner: number;
  shard: number;
  ingest: number;
  delivery: number;
  /** Everything reserved, API included. */
  reservedTotal: number;
}

/**
 * Works out the prod reservation and checks the inequality. Unset values get their defaults; the
 * API gets whatever the quota leaves unless `apiReservedConcurrency` is given. Throws when the
 * reservations do not fit, so a bad context value fails at synth time instead of at deploy.
 */
export function planConcurrency(
  o: {
    lambdaConcurrencyQuota?: number;
    apiReservedConcurrency?: number;
    aggregatorReservedConcurrency?: number;
    repairReservedConcurrency?: number;
    plannerReservedConcurrency?: number;
    shardReservedConcurrency?: number;
  } = {},
): ConcurrencyPlan {
  const quota = o.lambdaConcurrencyQuota ?? DEFAULT_LAMBDA_CONCURRENCY_QUOTA;
  const aggregator = o.aggregatorReservedConcurrency ?? DEFAULT_AGGREGATOR_CONCURRENCY;
  const repair = o.repairReservedConcurrency ?? DEFAULT_REPAIR_CONCURRENCY;
  const planner = o.plannerReservedConcurrency ?? PLANNER_RESERVED_CONCURRENCY;
  const shard = o.shardReservedConcurrency ?? DEFAULT_SHARD_CONCURRENCY;
  const ingest = INGEST_RESERVED_CONCURRENCY;
  const delivery = DELIVERY_RESERVED_CONCURRENCY;
  const others = aggregator + repair + planner + shard + ingest + delivery;
  const api = o.apiReservedConcurrency ?? quota - LAMBDA_MIN_UNRESERVED - others;
  const reservedTotal = api + others;
  if (api < 1 || reservedTotal > quota - LAMBDA_MIN_UNRESERVED) {
    throw new Error(
      `Lambda reserved concurrency does not fit the account quota: API ${api} + others ${others} ` +
        `= ${reservedTotal}, but quota ${quota} minus the ${LAMBDA_MIN_UNRESERVED} AWS keeps unreserved ` +
        `leaves ${quota - LAMBDA_MIN_UNRESERVED}. Raise -c lambdaConcurrencyQuota (after raising the ` +
        `real quota, see docs/scale-quotas.md) or lower a reservation.`,
    );
  }
  return {
    quota,
    api,
    aggregator,
    repair,
    planner,
    shard,
    ingest,
    delivery,
    reservedTotal,
  };
}
