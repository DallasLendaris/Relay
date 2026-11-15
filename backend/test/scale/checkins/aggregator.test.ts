import { TransactionCanceledException } from '@aws-sdk/client-dynamodb';
import { TransactWriteCommand } from '@aws-sdk/lib-dynamodb';
import type { DynamoDBStreamEvent } from 'aws-lambda';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  createAggregator,
  RECORDS_PER_TRANSACTION,
} from '../../../src/scale/checkins/aggregator.js';

const ALERT_A = '11111111-1111-4111-8111-111111111111';
const ALERT_B = '22222222-2222-4222-8222-222222222222';

type Item = Record<string, unknown>;

/**
 * A small in-memory DynamoDB that implements what the aggregator uses, faithfully enough to
 * catch double counting: transactions are atomic, a failed put condition cancels the whole
 * transaction and reports per-item reasons in order, and ADD adds.
 */
class FakeTable {
  readonly items = new Map<string, Item>();
  readonly transactionSizes: number[] = [];
  /** Throw this for any transaction touching a counter key that matches. */
  failCounterKey: ((pk: string) => Error | undefined) | undefined;

  async send(cmd: unknown): Promise<object> {
    if (!(cmd instanceof TransactWriteCommand)) throw new Error('unexpected command');
    const tx = cmd.input.TransactItems ?? [];
    this.transactionSizes.push(tx.length);
    const counter = tx[0]?.Update;
    const counterPk = String((counter?.Key as Item | undefined)?.['pk']);
    const injected = this.failCounterKey?.(counterPk);
    if (injected) throw injected;

    const reasons = tx.map((t) => {
      if (t.Put && !this.items.has(String((t.Put.Item as Item)['pk']))) return { Code: 'None' };
      if (t.Put) return { Code: 'ConditionalCheckFailed' };
      return { Code: 'None' };
    });
    if (reasons.some((r) => r.Code !== 'None')) {
      throw new TransactionCanceledException({
        message: 'cancelled',
        $metadata: {},
        CancellationReasons: reasons,
      });
    }
    for (const t of tx) {
      if (t.Put) this.items.set(String((t.Put.Item as Item)['pk']), { ...(t.Put.Item as Item) });
      if (t.Update) {
        const pk = String((t.Update.Key as Item)['pk']);
        const values = t.Update.ExpressionAttributeValues as Item;
        const row = this.items.get(pk) ?? { pk };
        row['checkedIn'] = Number(row['checkedIn'] ?? 0) + Number(values[':n']);
        row['alertId'] = values[':a'];
        row['h3Cell'] = values[':c'];
        this.items.set(pk, row);
      }
    }
    return {};
  }

  /** Counter rows only (markers have no alertId, like the real sparse index). */
  totals(): Record<string, number> {
    const out: Record<string, number> = {};
    for (const [pk, row] of this.items) {
      if (row['alertId'] !== undefined) out[pk] = Number(row['checkedIn']);
    }
    return out;
  }
  markers(): number {
    return [...this.items.keys()].filter((k) => k.startsWith('rec#')).length;
  }
}

let seq = 0;
const rec = (
  alertId: string,
  h3Cell: string,
  over: { eventName?: 'INSERT' | 'MODIFY' | 'REMOVE'; seq?: number; image?: object } = {},
) => {
  const n = over.seq ?? ++seq;
  return {
    eventID: `evt-${n}`,
    eventName: over.eventName ?? 'INSERT',
    eventSource: 'aws:dynamodb',
    dynamodb: {
      SequenceNumber: String(1000 + n),
      NewImage: over.image ?? {
        pk: { S: `${alertId}#device-${n}` },
        alertId: { S: alertId },
        deviceId: { S: `device-${n}` },
        h3Cell: { S: h3Cell },
        checkedInAt: { S: '2026-11-09T12:00:00.000Z' },
        idempotencyKey: { S: 'k' },
      },
    },
  };
};
const batch = (...records: ReturnType<typeof rec>[]): DynamoDBStreamEvent =>
  ({ Records: records }) as unknown as DynamoDBStreamEvent;

let table: FakeTable;
let run: ReturnType<typeof createAggregator>;

beforeEach(() => {
  seq = 0;
  table = new FakeTable();
  run = createAggregator({
    doc: table as never,
    totalsTable: 'totals',
    now: () => new Date('2026-11-09T12:00:00Z'),
    sleep: async () => {},
  });
  vi.spyOn(console, 'log').mockImplementation(() => {});
});

describe('check-in aggregator', () => {
  it('counts INSERTs per alertId#cell over a batch with mixed alerts and cells', async () => {
    const out = await run(
      batch(
        rec(ALERT_A, 'c1'),
        rec(ALERT_A, 'c1'),
        rec(ALERT_A, 'c2'),
        rec(ALERT_B, 'c1'), // same cell, different alert: a separate counter
        rec(ALERT_A, 'c1'),
        rec(ALERT_B, 'c1'),
      ),
    );
    expect(out).toEqual({ batchItemFailures: [] });
    expect(table.totals()).toEqual({
      [`${ALERT_A}#c1`]: 3,
      [`${ALERT_A}#c2`]: 1,
      [`${ALERT_B}#c1`]: 2,
    });
    expect(table.items.get(`${ALERT_A}#c1`)).toMatchObject({ alertId: ALERT_A, h3Cell: 'c1' });
  });

  it('does ONE counter update per key, not one per record', async () => {
    await run(batch(...Array.from({ length: 30 }, () => rec(ALERT_A, 'c1'))));
    expect(table.transactionSizes).toEqual([1 + 30]); // one counter update plus 30 markers
  });

  it('ignores MODIFY and REMOVE records', async () => {
    const out = await run(
      batch(
        rec(ALERT_A, 'c1', { eventName: 'MODIFY' }),
        rec(ALERT_A, 'c1', { eventName: 'REMOVE' }),
        rec(ALERT_A, 'c1'),
      ),
    );
    expect(out.batchItemFailures).toEqual([]);
    expect(table.totals()).toEqual({ [`${ALERT_A}#c1`]: 1 });
    // A batch with only ignorable records writes nothing at all.
    table = new FakeTable();
    run = createAggregator({ doc: table as never, totalsTable: 'totals' });
    await run(batch(rec(ALERT_A, 'c1', { eventName: 'REMOVE' })));
    expect(table.transactionSizes).toEqual([]);
  });

  it('does not double count when the same batch is delivered again', async () => {
    const event = batch(rec(ALERT_A, 'c1'), rec(ALERT_A, 'c1'), rec(ALERT_A, 'c2'));
    await run(event);
    await run(event);
    await run(event);
    expect(table.totals()).toEqual({ [`${ALERT_A}#c1`]: 2, [`${ALERT_A}#c2`]: 1 });
    expect(table.markers()).toBe(3);
  });

  it('reports only the failed keys on partial failure, and a retry counts each record exactly once', async () => {
    const r1 = rec(ALERT_A, 'c1');
    const r2 = rec(ALERT_A, 'c2');
    const r3 = rec(ALERT_A, 'c1');
    const r4 = rec(ALERT_B, 'c3');
    // The c2 counter is throttled.
    table.failCounterKey = (pk) =>
      pk === `${ALERT_A}#c2` ? new Error('ProvisionedThroughputExceededException') : undefined;
    const out = await run(batch(r1, r2, r3, r4));
    expect(out.batchItemFailures).toEqual([{ itemIdentifier: r2.dynamodb.SequenceNumber }]);
    expect(table.totals()).toEqual({ [`${ALERT_A}#c1`]: 2, [`${ALERT_B}#c3`]: 1 });

    // Lambda retries from the first failed record: it re-sends r2, and also r3 and r4 which
    // were already counted, plus a record that arrived meanwhile.
    table.failCounterKey = undefined;
    const r5 = rec(ALERT_A, 'c1');
    const retry = await run(batch(r2, r3, r4, r5));
    expect(retry.batchItemFailures).toEqual([]);
    expect(table.totals()).toEqual({
      [`${ALERT_A}#c1`]: 3, // r1, r3, r5: r3 was not counted twice
      [`${ALERT_A}#c2`]: 1,
      [`${ALERT_B}#c3`]: 1, // r4 was not counted twice
    });
  });

  it('a transaction that already counted part of a chunk is split, and only the new records are added', async () => {
    const old = [rec(ALERT_A, 'c1'), rec(ALERT_A, 'c1')];
    await run(batch(...old));
    const fresh = [rec(ALERT_A, 'c1'), rec(ALERT_A, 'c1'), rec(ALERT_A, 'c1')];
    const out = await run(batch(...old, ...fresh));
    expect(out.batchItemFailures).toEqual([]);
    expect(table.totals()).toEqual({ [`${ALERT_A}#c1`]: 5 });
  });

  it('keeps every transaction under the 100-item limit for a key with many records', async () => {
    const n = RECORDS_PER_TRANSACTION * 2 + 20;
    await run(batch(...Array.from({ length: n }, () => rec(ALERT_A, 'hot'))));
    expect(table.totals()).toEqual({ [`${ALERT_A}#hot`]: n });
    expect(Math.max(...table.transactionSizes)).toBeLessThanOrEqual(100);
    expect(table.transactionSizes).toHaveLength(3);
  });

  it('fails every record of a failing chunk but leaves other chunks applied', async () => {
    const n = RECORDS_PER_TRANSACTION + 5;
    const records = Array.from({ length: n }, () => rec(ALERT_A, 'c1'));
    let calls = 0;
    table.failCounterKey = () => (++calls === 2 ? new Error('boom') : undefined);
    const out = await run(batch(...records));
    expect(out.batchItemFailures).toHaveLength(5);
    expect(table.totals()).toEqual({ [`${ALERT_A}#c1`]: RECORDS_PER_TRANSACTION });
  });

  it('retries a transaction conflict and succeeds, or reports failure if it never clears', async () => {
    let conflicts = 2;
    const conflict = () =>
      new TransactionCanceledException({
        message: 'conflict',
        $metadata: {},
        CancellationReasons: [{ Code: 'TransactionConflict' }, { Code: 'None' }],
      });
    table.failCounterKey = () => (conflicts-- > 0 ? conflict() : undefined);
    const ok = await run(batch(rec(ALERT_A, 'c1')));
    expect(ok.batchItemFailures).toEqual([]);
    expect(table.totals()).toEqual({ [`${ALERT_A}#c1`]: 1 });

    const stuck = rec(ALERT_A, 'c2');
    table.failCounterKey = () => conflict();
    const bad = await run(batch(stuck));
    expect(bad.batchItemFailures).toEqual([{ itemIdentifier: stuck.dynamodb.SequenceNumber }]);
    expect(table.totals()[`${ALERT_A}#c2`]).toBeUndefined();
  });

  it('skips malformed INSERT records without failing the batch (a retry could not fix them)', async () => {
    const out = await run(
      batch(
        rec(ALERT_A, 'c1', { image: { alertId: { S: ALERT_A } } }), // no h3Cell
        rec(ALERT_A, 'c1', { image: { alertId: { N: '5' }, h3Cell: { S: 'c1' } } }),
        rec(ALERT_A, 'c1'),
      ),
    );
    expect(out.batchItemFailures).toEqual([]);
    expect(table.totals()).toEqual({ [`${ALERT_A}#c1`]: 1 });
  });

  it('writes markers with a TTL so they expire after the stream retention window', async () => {
    await run(batch(rec(ALERT_A, 'c1')));
    const marker = [...table.items.entries()].find(([k]) => k.startsWith('rec#'))?.[1];
    expect(marker?.['ttl']).toBe(Math.floor(Date.UTC(2026, 10, 9, 12) / 1000) + 3 * 24 * 3600);
    expect(marker?.['alertId']).toBeUndefined(); // stays out of the byAlert index
  });
});
