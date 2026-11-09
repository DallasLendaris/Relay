import { DynamoDBClient, TransactionCanceledException } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, TransactWriteCommand } from '@aws-sdk/lib-dynamodb';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import type { AttributeValue } from '@aws-sdk/client-dynamodb';
import type { DynamoDBBatchResponse, DynamoDBRecord, DynamoDBStreamEvent } from 'aws-lambda';

/**
 * Check-in totals aggregator: consumes the check-ins table's stream (NEW_IMAGE) and keeps one
 * counter per `${alertId}#${h3Cell}` in the totals table (pk "pk"; attributes alertId, h3Cell,
 * checkedIn). A million check-ins become a few thousand counter updates instead of a million
 * writes to one hot row, because each batch is counted in memory first.
 *
 * Wire it with ReportBatchItemFailures. Only INSERT records count: a check-in is written once
 * (conditional put), so MODIFY and REMOVE (TTL cleanup) never change a total.
 *
 * Exactly-once counting. Lambda re-delivers a batch (or the tail of one, after a partial failure
 * report) and a counter ADD is not idempotent, so a retry would double-count. Choice: PER-RECORD
 * MARKERS, written in the same DynamoDB transaction as the counter update:
 *   Update  totals[`${alertId}#${cell}`]   ADD checkedIn :n
 *   Put     totals[`rec#${eventID}`]       attribute_not_exists(pk)     (one per record, TTL'd)
 * The transaction is atomic: either the count and all its markers land, or neither does. A retry
 * finds the markers, the transaction cancels, and the cancellation reasons say exactly which
 * records were already counted; we drop those and re-apply the rest. Counts stay exact even when
 * a retry starts mid-batch with different batch boundaries.
 * Rejected alternative: one "last sequence number per shard" marker. A stream event carries no
 * shard id, and after a partial failure Lambda retries from the first failed record, so a
 * watermark would either skip uncounted records or admit counted ones. Markers cost one extra
 * item write per check-in, but only inside this off-path consumer (about 1M small writes over the
 * burst, trivial for on-demand DynamoDB) and never delay a phone.
 *
 * Marker items have no alertId attribute, so they stay out of the sparse `byAlert` index. They
 * carry a `ttl` (epoch seconds) attribute: enable DynamoDB TTL on it. Streams retain records for
 * 24 hours, so a marker only has to outlive that; we keep 3 days.
 */

type Doc = Pick<DynamoDBDocumentClient, 'send'>;

export interface AggregatorDeps {
  doc: Doc;
  totalsTable: string;
  now?: () => Date;
  /** Waits between retries of a transaction conflict; injectable for tests. */
  sleep?: (ms: number) => Promise<void>;
}

/** Records per transaction: 1 counter update + up to this many markers, under the 100-item limit. */
export const RECORDS_PER_TRANSACTION = 50;
const CONFLICT_ATTEMPTS = 4;
const CONCURRENCY = 8;
const MARKER_TTL_SECONDS = 3 * 24 * 3600;

interface Counted {
  record: DynamoDBRecord;
  sequenceNumber: string;
  eventId: string;
}

/** Builds a Streams handler over the given client. Exported for tests; production uses `handler`. */
export function createAggregator(
  deps: AggregatorDeps,
): (event: DynamoDBStreamEvent) => Promise<DynamoDBBatchResponse> {
  const now = deps.now ?? (() => new Date());
  const sleep = deps.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));

  async function applyChunk(
    alertId: string,
    h3Cell: string,
    chunk: Counted[],
  ): Promise<{ applied: number; skipped: number } | 'failed'> {
    let pending = chunk;
    let skipped = 0;
    for (let attempt = 0; pending.length > 0;) {
      const ttl = Math.floor(now().getTime() / 1000) + MARKER_TTL_SECONDS;
      try {
        await deps.doc.send(
          new TransactWriteCommand({
            TransactItems: [
              {
                Update: {
                  TableName: deps.totalsTable,
                  Key: { pk: `${alertId}#${h3Cell}` },
                  UpdateExpression: 'ADD checkedIn :n SET alertId = :a, h3Cell = :c',
                  ExpressionAttributeValues: { ':n': pending.length, ':a': alertId, ':c': h3Cell },
                },
              },
              ...pending.map((p) => ({
                Put: {
                  TableName: deps.totalsTable,
                  Item: { pk: `rec#${p.eventId}`, ttl },
                  ConditionExpression: 'attribute_not_exists(pk)',
                },
              })),
            ],
          }),
        );
        return { applied: pending.length, skipped };
      } catch (err) {
        if (!(err instanceof TransactionCanceledException)) return 'failed';
        const reasons = err.CancellationReasons ?? [];
        // reasons[0] is the counter update; reasons[i + 1] is pending[i]'s marker.
        const seen = new Set(
          pending.flatMap((p, i) => (reasons[i + 1]?.Code === 'ConditionalCheckFailed' ? [p] : [])),
        );
        if (seen.size > 0) {
          // Already counted by an earlier delivery: drop them and apply the rest.
          skipped += seen.size;
          pending = pending.filter((p) => !seen.has(p));
          continue;
        }
        if (
          reasons.some((r) => r.Code === 'TransactionConflict') &&
          ++attempt < CONFLICT_ATTEMPTS
        ) {
          await sleep(20 * 2 ** attempt + Math.floor(Math.random() * 20));
          continue;
        }
        return 'failed';
      }
    }
    return { applied: 0, skipped };
  }

  return async (event) => {
    const failures = new Set<string>();
    const groups = new Map<string, { alertId: string; h3Cell: string; items: Counted[] }>();
    let ignored = 0;
    let malformed = 0;

    for (const record of event.Records) {
      if (record.eventName !== 'INSERT') {
        ignored++;
        continue;
      }
      const image = record.dynamodb?.NewImage;
      const sequenceNumber = record.dynamodb?.SequenceNumber;
      const eventId = record.eventID;
      if (!image || !sequenceNumber || !eventId) {
        malformed++;
        continue;
      }
      const item = unmarshall(image as Record<string, AttributeValue>);
      const alertId: unknown = item['alertId'];
      const h3Cell: unknown = item['h3Cell'];
      if (typeof alertId !== 'string' || typeof h3Cell !== 'string' || !alertId || !h3Cell) {
        malformed++; // Retrying cannot fix it, so it is not reported as a failure.
        continue;
      }
      const key = `${alertId}#${h3Cell}`;
      const group = groups.get(key) ?? { alertId, h3Cell, items: [] };
      group.items.push({ record, sequenceNumber, eventId });
      groups.set(key, group);
    }

    const chunks: Array<{ alertId: string; h3Cell: string; items: Counted[] }> = [];
    for (const g of groups.values()) {
      for (let i = 0; i < g.items.length; i += RECORDS_PER_TRANSACTION) {
        chunks.push({ ...g, items: g.items.slice(i, i + RECORDS_PER_TRANSACTION) });
      }
    }

    let applied = 0;
    let skipped = 0;
    let next = 0;
    const worker = async (): Promise<void> => {
      for (let c = next++; c < chunks.length; c = next++) {
        const chunk = chunks[c];
        if (!chunk) continue;
        const out = await applyChunk(chunk.alertId, chunk.h3Cell, chunk.items);
        if (out === 'failed') chunk.items.forEach((i) => failures.add(i.sequenceNumber));
        else {
          applied += out.applied;
          skipped += out.skipped;
        }
      }
    };
    await Promise.all(Array.from({ length: Math.min(CONCURRENCY, chunks.length) }, worker));

    console.log(
      JSON.stringify({
        level: failures.size > 0 ? 'warn' : 'info',
        msg: 'checkin aggregation',
        records: event.Records.length,
        keys: groups.size,
        applied,
        alreadyCounted: skipped,
        ignored,
        malformed,
        failed: failures.size,
      }),
    );
    // Lambda retries from the lowest failed sequence number; markers make the rest exact.
    return { batchItemFailures: [...failures].map((itemIdentifier) => ({ itemIdentifier })) };
  };
}

let real: ReturnType<typeof createAggregator> | undefined;

/** Lambda entry point. Env: CHECKIN_TOTALS_TABLE. The client is created on first use. */
export const handler = async (event: DynamoDBStreamEvent): Promise<DynamoDBBatchResponse> => {
  if (!real) {
    const totalsTable = process.env['CHECKIN_TOTALS_TABLE'];
    if (!totalsTable) throw new Error('Missing environment variable: CHECKIN_TOTALS_TABLE');
    real = createAggregator({
      doc: DynamoDBDocumentClient.from(new DynamoDBClient({})),
      totalsTable,
    });
  }
  return real(event);
};
