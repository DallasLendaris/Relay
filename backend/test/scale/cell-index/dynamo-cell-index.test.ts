import { ConditionalCheckFailedException, DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  BatchWriteCommand,
  DeleteCommand,
  DynamoDBDocumentClient,
  QueryCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { beforeEach, describe, expect, it } from 'vitest';
import { DynamoCellIndex } from '../../../src/scale/cell-index/index.js';
import { CELL, cellIndexContract, entry, listAll } from './contract.js';

type Item = Record<string, unknown>;
type PutReq = { PutRequest: { Item: Item } };
const TABLE = 'cell-index';

const doc = DynamoDBDocumentClient.from(new DynamoDBClient({ region: 'us-west-2' }));
const ddb = mockClient(doc);

/** A small fake DynamoDB table behind the mock: sorted by (h3Cell, sk), real Limit paging. */
class FakeTable {
  items = new Map<string, Item>();
  batchSizes: number[] = [];
  /** Number of upcoming batch writes that leave half of their items unprocessed. */
  throttleBatches = 0;
  /** Leave every item unprocessed, forever. */
  rejectAll = false;
  updatesInFlight = 0;
  maxUpdatesInFlight = 0;

  private key = (h: unknown, sk: unknown) => `${String(h)}\u0000${String(sk)}`;

  install(): void {
    ddb.reset();
    ddb.on(BatchWriteCommand).callsFake((input) => {
      const reqs = input.RequestItems[TABLE] as PutReq[];
      this.batchSizes.push(reqs.length);
      if (reqs.length > 25) throw new Error('ValidationException: more than 25 requests');
      const keys = new Set(
        reqs.map((r) => this.key(r.PutRequest.Item['h3Cell'], r.PutRequest.Item['sk'])),
      );
      if (keys.size !== reqs.length) throw new Error('ValidationException: duplicate keys');
      if (this.rejectAll) return Promise.resolve({ UnprocessedItems: { [TABLE]: reqs } });
      let apply = reqs;
      let left: PutReq[] = [];
      if (this.throttleBatches > 0 && reqs.length > 1) {
        this.throttleBatches--;
        const half = Math.ceil(reqs.length / 2);
        apply = reqs.slice(0, half);
        left = reqs.slice(half);
      }
      for (const r of apply) {
        const it = r.PutRequest.Item;
        this.items.set(this.key(it['h3Cell'], it['sk']), structuredClone(it));
      }
      return Promise.resolve({ UnprocessedItems: left.length ? { [TABLE]: left } : {} });
    });
    ddb.on(DeleteCommand).callsFake((input) => {
      this.items.delete(this.key(input.Key.h3Cell, input.Key.sk));
      return Promise.resolve({});
    });
    ddb.on(UpdateCommand).callsFake(async (input) => {
      this.updatesInFlight++;
      this.maxUpdatesInFlight = Math.max(this.maxUpdatesInFlight, this.updatesInFlight);
      await new Promise((r) => setTimeout(r, 1));
      this.updatesInFlight--;
      const item = this.items.get(this.key(input.Key.h3Cell, input.Key.sk));
      if (!item) throw new ConditionalCheckFailedException({ message: 'x', $metadata: {} });
      const v = input.ExpressionAttributeValues;
      item['target'] = v[':target'];
      item['language'] = v[':language'];
      item['updatedAt'] = v[':at'];
      return {};
    });
    ddb.on(QueryCommand).callsFake((input) => {
      const cell = input.ExpressionAttributeValues[':cell'] as string;
      const start = input.ExclusiveStartKey as { h3Cell: string; sk: string } | undefined;
      const rows = [...this.items.values()]
        .filter((i) => i['h3Cell'] === cell)
        .sort((a, b) => String(a['sk']).localeCompare(String(b['sk'])))
        .filter((i) => !start || String(i['sk']) > start.sk);
      const page = rows.slice(0, input.Limit);
      const last = page[page.length - 1];
      // Like DynamoDB: a key is returned whenever the Limit was reached, even on the last item.
      const more = page.length === input.Limit && last;
      return Promise.resolve({
        Items: structuredClone(page),
        ...(more ? { LastEvaluatedKey: { h3Cell: cell, sk: last['sk'] } } : {}),
      });
    });
  }
}

let table: FakeTable;
const make = () => new DynamoCellIndex(doc, TABLE, { sleep: async () => {} });

beforeEach(() => {
  table = new FakeTable();
  table.install();
});

cellIndexContract('DynamoCellIndex (aws-sdk-client-mock fake table)', make);

describe('DynamoCellIndex specifics', () => {
  it('stores the documented item shape', async () => {
    await make().put([entry(1)]);
    const item = [...table.items.values()][0];
    expect(item).toMatchObject({
      h3Cell: CELL,
      sk: `${entry(1).deviceId}#${entry(1).placeId}`,
      deviceId: entry(1).deviceId,
      placeId: entry(1).placeId,
      target: entry(1).target,
      language: 'en',
    });
    expect(typeof item?.['updatedAt']).toBe('string');
  });

  it('writes in batches of at most 25', async () => {
    await make().put(Array.from({ length: 60 }, (_, i) => entry(i + 1)));
    expect([...table.batchSizes].sort((a, b) => a - b)).toEqual([10, 25, 25]);
    expect(table.items.size).toBe(60);
  });

  it('retries UnprocessedItems with backoff until everything is written', async () => {
    const sleeps: number[] = [];
    const index = new DynamoCellIndex(doc, TABLE, { sleep: async (ms) => void sleeps.push(ms) });
    table.throttleBatches = 3;
    await index.put(Array.from({ length: 25 }, (_, i) => entry(i + 1)));
    expect(table.items.size).toBe(25);
    expect(sleeps.length).toBeGreaterThanOrEqual(3);
    expect(table.batchSizes.length).toBeGreaterThan(1);
  });

  it('gives up with an error when items stay unprocessed', async () => {
    const index = new DynamoCellIndex(doc, TABLE, { sleep: async () => {} });
    table.rejectAll = true;
    await expect(index.put(Array.from({ length: 4 }, (_, i) => entry(i + 1)))).rejects.toThrow(
      /unprocessed/,
    );
  });

  it('runs updateDevice with at most 25 updates in flight', async () => {
    const dev = entry(1).deviceId;
    const rows = Array.from({ length: 100 }, (_, i) => entry(i + 1, { deviceId: dev }));
    const index = make();
    await index.put(rows);
    await index.updateDevice(
      dev,
      rows.map((c) => ({ h3Cell: c.h3Cell, placeId: c.placeId })),
      { target: null, language: 'es' },
    );
    expect(table.maxUpdatesInFlight).toBeGreaterThan(1);
    expect(table.maxUpdatesInFlight).toBeLessThanOrEqual(25);
    expect((await listAll(index, CELL)).entries.every((e) => e.language === 'es')).toBe(true);
  });

  it('propagates a non-conditional failure from updateDevice', async () => {
    const index = make();
    await index.put([entry(1)]);
    ddb.on(UpdateCommand).rejects(new Error('boom'));
    await expect(
      index.updateDevice(entry(1).deviceId, [{ h3Cell: CELL, placeId: entry(1).placeId }], {
        target: null,
        language: 'en',
      }),
    ).rejects.toThrow('boom');
  });

  it('queries the partition by h3Cell with the page limit', async () => {
    await make().listForCell(CELL, undefined, 7);
    const call = ddb.commandCalls(QueryCommand)[0]?.args[0].input;
    expect(call?.Limit).toBe(7);
    expect(call?.ExpressionAttributeValues).toEqual({ ':cell': CELL });
  });
});
