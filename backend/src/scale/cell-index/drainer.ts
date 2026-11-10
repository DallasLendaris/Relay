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
import { PostgresCellIndexOutbox, type CellIndexOutbox } from './outbox.js';
import { applyRepair, type RepairDeps } from './repair.js';
import { targetOf } from './util.js';

/** Rows younger than this are left to the API's own write-through. */
export const OUTBOX_MIN_AGE_MS = 30_000;
/** Attempts at which the alarm fires. */
export const OUTBOX_ALARM_ATTEMPTS = 10;
const BATCH = 100;
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
  log?: (line: Record<string, unknown>) => void;
  metric?: (line: string) => void;
}

export interface DrainResult {
  claimed: number;
  applied: number;
  failed: number;
  maxAttempts: number;
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
 */
export async function drainOutbox(deps: DrainerDeps): Promise<DrainResult> {
  const now = deps.now ?? (() => new Date());
  const log = deps.log ?? ((l) => console.log(JSON.stringify(l)));
  const metric = deps.metric ?? ((l) => console.log(l));
  const rows = await deps.outbox.claimDue(OUTBOX_MIN_AGE_MS, BATCH, now());
  let applied = 0;
  let failed = 0;
  const done: string[] = [];
  /** Devices already asked about in this run (one intent per device is enough). */
  const asked = new Set<string>();
  for (const row of rows) {
    try {
      await applyRepair(deps, {
        op: row.op,
        h3Cell: row.h3Cell,
        deviceId: row.deviceId,
        placeId: row.placeId,
      });
      if (row.op !== 'remove' && deps.lateJoin && !asked.has(row.deviceId)) {
        // Re-read: an erased device is gone (the tombstone and the row delete are one statement),
        // and applyRepair has already removed the entry the row named, so there is nothing to
        // send to. Otherwise a device with a working target was indexed late: ask for a late join
        // that does not trust "its place predates the alert". If this throws the row stays and
        // the next run redoes both steps (both are idempotent).
        const device = await deps.devices.get(row.deviceId);
        if (!device) log({ level: 'info', msg: 'cellindex.outbox_device_erased', op: row.op });
        else if (targetOf(device)) {
          await deps.lateJoin.enqueue({ deviceId: row.deviceId, reason: 'outbox' });
          asked.add(row.deviceId);
        }
      }
      done.push(row.id);
      applied++;
    } catch (err) {
      failed++;
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
  await deps.outbox.remove(done);
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
    claimed: rows.length,
    applied,
    failed,
    pending: stats.pending,
    maxAttempts,
  });
  return { claimed: rows.length, applied, failed, maxAttempts };
}

let real: DrainerDeps | undefined;

/**
 * Scheduled Lambda entry (every minute). Env: DB_CLUSTER_ARN, DB_SECRET_ARN, DB_NAME,
 * CELL_INDEX_TABLE, LATEJOIN_QUEUE_URL.
 */
export const handler = async (): Promise<DrainResult> => {
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
  return drainOutbox(real);
};
