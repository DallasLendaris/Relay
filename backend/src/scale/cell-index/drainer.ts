import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { SQSClient } from '@aws-sdk/client-sqs';
import { RDSDataClient } from '@aws-sdk/client-rds-data';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { PostgresDeviceStore } from '../../adapters/postgres/device-store.js';
import { PostgresPlaceStore } from '../../adapters/postgres/place-store.js';
import { DataApiExecutor } from '../../db/sql.js';
import type { LateJoinQueue } from '../latejoin/intent.js';
import { SqsLateJoinQueue } from '../latejoin/intent.js';
import { DynamoCellIndex } from './dynamo-cell-index.js';
import {
  OUTBOX_LEASE_MS,
  PostgresCellIndexOutbox,
  type CellIndexOutbox,
  type OutboxRow,
} from './outbox.js';
import { applyRepair, type RepairDeps } from './repair.js';
import { targetOf } from './util.js';

/** Rows younger than this are left to the API's own write-through. */
export const OUTBOX_MIN_AGE_MS = 30_000;
/** Attempts at which the alarm fires. */
export const OUTBOX_ALARM_ATTEMPTS = 10;
/** Rows claimed per loop iteration. */
export const OUTBOX_BATCH = 500;
/** Rows applied at the same time within a batch. */
export const OUTBOX_CONCURRENCY = 8;
/** Time kept back at the end of the Lambda invocation (stats, metric, logging). */
export const OUTBOX_RESERVE_MS = 10_000;
/** Budget when the caller gives none (a 60 s Lambda minus the reserve). */
export const OUTBOX_DEFAULT_BUDGET_MS = 50_000;
export const OUTBOX_METRIC_NAMESPACE = 'Relay/CellIndex';

export interface DrainerDeps extends RepairDeps {
  outbox: CellIndexOutbox;
  /**
   * Where to ask for a late-join check. When the drainer puts a device in the index (or gives it a
   * working push target), the planned fan-out may have run without it, so the device is sent
   * the alerts already active over its places. Absent in tests that do not care.
   */
  lateJoin?: LateJoinQueue;
  now?: () => Date;
  /** Milliseconds from any fixed origin; measures the time budget (tests inject a fake). */
  clock?: () => number;
  /** How long the run may keep starting work (default OUTBOX_DEFAULT_BUDGET_MS). */
  budgetMs?: number;
  batchSize?: number;
  concurrency?: number;
  log?: (line: Record<string, unknown>) => void;
  metric?: (line: string) => void;
}

export interface DrainResult {
  claimed: number;
  applied: number;
  failed: number;
  maxAttempts: number;
  /** Claim batches taken in this run. */
  batches: number;
  /** True when the run stopped because the time budget was used up (rows may be left). */
  budgetExhausted: boolean;
}

const emf = (values: Record<string, number>, now: number): string =>
  JSON.stringify({
    _aws: {
      Timestamp: now,
      CloudWatchMetrics: [
        {
          Namespace: OUTBOX_METRIC_NAMESPACE,
          Dimensions: [[]],
          Metrics: Object.keys(values).map((Name) => ({
            Name,
            Unit: Name === 'OutboxOldestAgeSeconds' ? 'Seconds' : 'Count',
          })),
        },
      ],
    },
    ...values,
  });

/**
 * Drains the cell-index outbox: claims rows older than 30 s (incrementing `attempts`), applies
 * each from Aurora's truth with the same logic as the repair Lambda (applyRepair), and deletes
 * the row on success. A failure leaves the row for the next run. Emits `OutboxMaxAttempts`,
 * `OutboxPending` and `OutboxOldestAgeSeconds` every run (zeros when empty, so the alarm on
 * attempts >= 10 sees data), and never throws for a single row.
 *
 * One run loops claim -> apply (batches of OUTBOX_BATCH, OUTBOX_CONCURRENCY rows at a time, rows
 * are independent per device) until no due row is left or the time budget is used up, so a large
 * backlog drains in one scheduled run instead of 100 rows a minute. A row that fails is not
 * claimed again in the same run (its attempts count once per run); a run whose whole batch failed
 * stops (the index is probably down). Rows claimed but not reached when the budget ends are given
 * back.
 */
export async function drainOutbox(deps: DrainerDeps): Promise<DrainResult> {
  const now = deps.now ?? (() => new Date());
  const log = deps.log ?? ((l) => console.log(JSON.stringify(l)));
  const metric = deps.metric ?? ((l) => console.log(l));
  const clock = deps.clock ?? (() => performance.now());
  const budgetMs = deps.budgetMs ?? OUTBOX_DEFAULT_BUDGET_MS;
  const batchSize = deps.batchSize ?? OUTBOX_BATCH;
  const concurrency = Math.max(1, deps.concurrency ?? OUTBOX_CONCURRENCY);
  const started = clock();
  const outOfTime = (): boolean => clock() - started >= budgetMs;

  let claimed = 0;
  let applied = 0;
  let failed = 0;
  let batches = 0;
  let budgetExhausted = false;
  /** Rows that failed in this run: not claimed again until the next run. */
  const failedIds = new Set<string>();
  /** Devices already asked about in this run (one intent per device is enough). */
  const asked = new Map<string, Promise<void>>();

  const applyRow = async (row: OutboxRow): Promise<void> => {
    await applyRepair(deps, {
      op: row.op,
      h3Cell: row.h3Cell,
      deviceId: row.deviceId,
      placeId: row.placeId,
    });
    const lateJoin = deps.lateJoin;
    if (row.op === 'remove' || !lateJoin) return;
    let ask = asked.get(row.deviceId);
    if (!ask) {
      // Re-read: an erased device is gone (the tombstone and the row delete are one statement),
      // and applyRepair has already removed the entry the row named, so there is nothing to
      // send to. Otherwise a device with a working target was indexed late: ask for a late join
      // that does not trust "its place predates the alert". If this throws the row stays and
      // the next run redoes both steps (both are idempotent). Rows of one device that run at the
      // same time share the one promise.
      ask = (async () => {
        const device = await deps.devices.get(row.deviceId);
        if (!device) log({ level: 'info', msg: 'cellindex.outbox_device_erased', op: row.op });
        else if (targetOf(device)) {
          await lateJoin.enqueue({ deviceId: row.deviceId, reason: 'outbox' });
        }
      })();
      asked.set(row.deviceId, ask);
      ask.catch(() => asked.delete(row.deviceId));
    }
    await ask;
  };

  while (!outOfTime()) {
    const rows = (
      await deps.outbox.claimDue(OUTBOX_MIN_AGE_MS, batchSize, now(), {
        leaseMs: OUTBOX_LEASE_MS,
        excludeIds: [...failedIds],
      })
    ).filter((r) => !failedIds.has(r.id)); // an outbox that ignores excludeIds must not loop
    if (rows.length === 0) break;
    batches++;
    claimed += rows.length;
    const done: string[] = [];
    const untouched: OutboxRow[] = [];
    let next = 0;
    let batchApplied = 0;
    const worker = async (): Promise<void> => {
      for (;;) {
        const row = rows[next++];
        if (!row) return;
        if (outOfTime()) {
          untouched.push(row);
          continue;
        }
        try {
          await applyRow(row);
          done.push(row.id);
          batchApplied++;
        } catch (err) {
          failed++;
          failedIds.add(row.id);
          // Class name only: never ids of a person's place or token.
          log({
            level: row.attempts >= OUTBOX_ALARM_ATTEMPTS ? 'error' : 'warn',
            msg: 'cellindex.outbox_apply_failed',
            op: row.op,
            attempts: row.attempts,
            error: err instanceof Error ? err.name : 'UnknownError',
          });
        }
      }
    };
    await Promise.all(Array.from({ length: Math.min(concurrency, rows.length) }, worker));
    applied += batchApplied;
    await deps.outbox.remove(done);
    if (untouched.length > 0) {
      budgetExhausted = true;
      claimed -= untouched.length;
      await deps.outbox.release?.(untouched.map((r) => r.id));
      break;
    }
    if (batchApplied === 0) break; // the whole batch failed: do not hammer a downed index
  }
  if (!budgetExhausted && outOfTime()) budgetExhausted = true;
  // Failed rows keep their counted attempt but are due again at the next run, not after the lease.
  await deps.outbox.release?.([...failedIds], false);

  const stats = await deps.outbox.stats(now());
  const maxAttempts = stats.maxAttempts;
  metric(
    emf(
      {
        OutboxMaxAttempts: maxAttempts,
        OutboxPending: stats.pending,
        OutboxOldestAgeSeconds: stats.oldestAgeSeconds,
      },
      now().getTime(),
    ),
  );
  log({
    level: failed > 0 ? 'warn' : 'info',
    msg: 'cellindex.outbox_drain',
    claimed,
    applied,
    failed,
    batches,
    budgetExhausted,
    pending: stats.pending,
    maxAttempts,
  });
  return { claimed, applied, failed, maxAttempts, batches, budgetExhausted };
}

let real: DrainerDeps | undefined;

/**
 * Scheduled Lambda entry (every minute on prod, 5 on dev). Each run drains until nothing is due
 * or the invocation has 10 s left. Env: DB_CLUSTER_ARN, DB_SECRET_ARN, DB_NAME,
 * CELL_INDEX_TABLE, LATEJOIN_QUEUE_URL.
 */
export const handler = async (
  _event?: unknown,
  context?: { getRemainingTimeInMillis?: () => number },
): Promise<DrainResult> => {
  if (!real) {
    const env = process.env;
    const names = [
      'DB_CLUSTER_ARN',
      'DB_SECRET_ARN',
      'DB_NAME',
      'CELL_INDEX_TABLE',
      'LATEJOIN_QUEUE_URL',
    ] as const;
    const [cluster, secret, dbName, table, lateJoinUrl] = names.map((n) => env[n]);
    if (!cluster || !secret || !dbName || !table || !lateJoinUrl) {
      throw new Error(`Missing environment variables: ${names.filter((n) => !env[n]).join(', ')}`);
    }
    const db = new DataApiExecutor(new RDSDataClient({}), cluster, secret, dbName);
    real = {
      devices: new PostgresDeviceStore(db),
      places: new PostgresPlaceStore(db),
      cellIndex: new DynamoCellIndex(DynamoDBDocumentClient.from(new DynamoDBClient({})), table),
      outbox: new PostgresCellIndexOutbox(db),
      lateJoin: new SqsLateJoinQueue(new SQSClient({}), lateJoinUrl),
    };
  }
  const remaining = context?.getRemainingTimeInMillis?.();
  const budgetMs =
    remaining === undefined ? OUTBOX_DEFAULT_BUDGET_MS : Math.max(0, remaining - OUTBOX_RESERVE_MS);
  return drainOutbox({ ...real, budgetMs });
};
