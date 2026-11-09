import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { RDSDataClient } from '@aws-sdk/client-rds-data';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { SQSBatchResponse, SQSEvent } from 'aws-lambda';
import { PostgresDeviceStore } from '../../adapters/postgres/device-store.js';
import { PostgresPlaceStore } from '../../adapters/postgres/place-store.js';
import { DataApiExecutor } from '../../db/sql.js';
import type { DeviceStore, PlaceStore } from '../../ports.js';
import type { CellIndex } from '../contracts.js';
import { DynamoCellIndex } from './dynamo-cell-index.js';
import { parseRepairMessage, type IndexRepairMessage } from './repair-queue.js';
import { syncDeviceCells } from './sync-device-cells.js';
import { targetOf } from './util.js';

/**
 * Cell-index repair Lambda (SQS trigger, ReportBatchItemFailures). The API writes the cell index
 * through after saving to Aurora; when that write fails it enqueues a message here instead of
 * losing the entry (a missing entry is a missed alert).
 *
 * Each message is applied from the TRUTH in Aurora, not from the message: the place and the
 * device are re-read and the index is made to match. That makes every repair idempotent and
 * order-independent: a duplicate, a late message, or a put that arrives after the place was
 * deleted all converge on the same state.
 *   - place still in Aurora  -> upsert it, and every other entry of the device, with the current
 *                               target, language and deviceCells
 *   - place gone from Aurora -> delete its entry (the message names the cell), after rewriting
 *                               the remaining entries without that cell
 *   - update, placeId ''     -> upsert every place of the device
 * Messages that cannot be parsed are dropped (retrying cannot fix them); anything that throws is
 * reported in batchItemFailures so SQS retries only that message, and after the queue's
 * maxReceiveCount it lands in the DLQ, which has an alarm.
 */
export interface RepairDeps {
  devices: Pick<DeviceStore, 'get'>;
  places: Pick<PlaceStore, 'listForDevice'>;
  cellIndex: CellIndex;
}

/**
 * Applies one repair message. Throws when a store call fails (the caller reports it).
 * Re-reads the device and its places and rewrites ALL of the device's entries through
 * syncDeviceCells (target, language and deviceCells from the truth), so the primary-cell lists
 * are repaired together with the entry the message names. When the message names a place that
 * Aurora no longer has, its entry is deleted last.
 */
export async function applyRepair(deps: RepairDeps, msg: IndexRepairMessage): Promise<void> {
  const named = msg.placeId !== '' && msg.h3Cell !== '';
  await syncDeviceCells(deps.cellIndex, {
    deviceId: msg.deviceId,
    load: async () => {
      const device = await deps.devices.get(msg.deviceId);
      if (!device) return null;
      const places = await deps.places.listForDevice(msg.deviceId);
      return {
        target: targetOf(device),
        language: device.language,
        places: places.map((p) => ({ id: p.id, h3Cell: p.h3Cell })),
      };
    },
    ...(msg.placeId !== '' ? { added: { placeId: msg.placeId } } : {}),
    ...(named ? { removed: { h3Cell: msg.h3Cell, placeId: msg.placeId } } : {}),
  });
}

export function createRepairHandler(
  deps: RepairDeps,
): (event: SQSEvent) => Promise<SQSBatchResponse> {
  return async (event) => {
    const failures: string[] = [];
    let applied = 0;
    let dropped = 0;
    for (const record of event.Records) {
      const msg = parseRepairMessage(record.body);
      if (!msg) {
        dropped++;
        continue;
      }
      try {
        await applyRepair(deps, msg);
        applied++;
      } catch (err) {
        failures.push(record.messageId);
        // Never log ids that identify a person's place or token: class name only.
        console.warn(
          JSON.stringify({
            level: 'warn',
            msg: 'cellindex.repair_failed',
            op: msg.op,
            error: err instanceof Error ? err.name : 'UnknownError',
          }),
        );
      }
    }
    console.log(
      JSON.stringify({
        level: failures.length > 0 ? 'warn' : 'info',
        msg: 'cellindex.repair',
        records: event.Records.length,
        applied,
        dropped,
        failed: failures.length,
      }),
    );
    return { batchItemFailures: failures.map((itemIdentifier) => ({ itemIdentifier })) };
  };
}

let real: ReturnType<typeof createRepairHandler> | undefined;

/** Lambda entry point. Env: DB_CLUSTER_ARN, DB_SECRET_ARN, DB_NAME, CELL_INDEX_TABLE. */
export const handler = async (event: SQSEvent): Promise<SQSBatchResponse> => {
  if (!real) {
    const env = process.env;
    const names = ['DB_CLUSTER_ARN', 'DB_SECRET_ARN', 'DB_NAME', 'CELL_INDEX_TABLE'] as const;
    const [cluster, secret, dbName, table] = names.map((n) => env[n]);
    if (!cluster || !secret || !dbName || !table) {
      throw new Error(`Missing environment variables: ${names.filter((n) => !env[n]).join(', ')}`);
    }
    const db = new DataApiExecutor(new RDSDataClient({}), cluster, secret, dbName);
    real = createRepairHandler({
      devices: new PostgresDeviceStore(db),
      places: new PostgresPlaceStore(db),
      cellIndex: new DynamoCellIndex(DynamoDBDocumentClient.from(new DynamoDBClient({})), table),
    });
  }
  return real(event);
};
