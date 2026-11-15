import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  BatchWriteCommand,
  DynamoDBDocumentClient,
  PutCommand,
  QueryCommand,
} from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { beforeEach, describe, expect, it } from 'vitest';
import type { CheckIn } from '@relay/shared';
import { DynamoCheckInStore } from '../../src/adapters/aws/dynamo-checkin-store.js';
import { MemoryCheckInStore } from '../../src/adapters/memory/checkin-store.js';
import { checkInTtlSeconds } from '../../src/scale/checkins/types.js';

const DAY = 24 * 3600;
const DEVICE = '22222222-2222-4222-8222-222222222222';
const checkIn = (over: Partial<CheckIn> = {}): CheckIn => ({
  alertId: '11111111-1111-4111-8111-111111111111',
  deviceId: DEVICE,
  status: 'ok',
  checkedInAt: '2026-11-09T12:00:00.000Z',
  token: 'tok',
  idempotencyKey: 'key-1',
  h3Cell: '872830828ffffff',
  ...over,
});

describe('checkInTtlSeconds', () => {
  it('is the alert end plus the retention period, in epoch seconds', () => {
    const end = new Date('2026-11-09T18:00:00Z');
    expect(checkInTtlSeconds(end)).toBe(end.getTime() / 1000 + 30 * DAY);
    expect(checkInTtlSeconds(end, 7)).toBe(end.getTime() / 1000 + 7 * DAY);
  });
});

describe('DynamoCheckInStore retention and erasure', () => {
  const ddb = mockClient(DynamoDBDocumentClient);
  const store = (retentionDays?: number) =>
    new DynamoCheckInStore(
      DynamoDBDocumentClient.from(new DynamoDBClient({ region: 'us-west-2' })),
      'checkins',
      retentionDays,
    );
  beforeEach(() => ddb.reset());

  it('writes ttl = retainUntil (the alert end + retention) on the item', async () => {
    ddb.on(PutCommand).resolves({});
    const until = new Date(checkInTtlSeconds(new Date('2026-11-09T18:00:00Z')) * 1000);
    await store().save(checkIn(), until);
    const item = ddb.commandCalls(PutCommand)[0]?.args[0].input.Item;
    expect(item?.['ttl']).toBe(Date.parse('2026-11-09T18:00:00Z') / 1000 + 30 * DAY);
  });

  it('without retainUntil still writes a ttl (never a row that lives forever)', async () => {
    ddb.on(PutCommand).resolves({});
    await store(14).save(checkIn());
    const item = ddb.commandCalls(PutCommand)[0]?.args[0].input.Item;
    expect(item?.['ttl']).toBe(Date.parse('2026-11-09T12:00:00Z') / 1000 + 14 * DAY);
  });

  it('deleteForDevice queries the byDevice index (no scan) and batch-deletes in groups of 25', async () => {
    const keys = Array.from({ length: 60 }, (_, i) => `alert-${i}#${DEVICE}`);
    ddb
      .on(QueryCommand)
      .resolvesOnce({
        Items: keys.slice(0, 40).map((pk) => ({ pk })),
        LastEvaluatedKey: { pk: keys[39], deviceId: DEVICE },
      })
      .resolvesOnce({ Items: keys.slice(40).map((pk) => ({ pk })) });
    ddb.on(BatchWriteCommand).resolves({});

    expect(await store().deleteForDevice(DEVICE)).toBe(60);

    const queries = ddb.commandCalls(QueryCommand).map((c) => c.args[0].input);
    expect(queries).toHaveLength(2);
    expect(queries[0]).toMatchObject({
      TableName: 'checkins',
      IndexName: 'byDevice',
      KeyConditionExpression: 'deviceId = :d',
      ExpressionAttributeValues: { ':d': DEVICE },
    });
    expect(queries[1]?.ExclusiveStartKey).toEqual({ pk: keys[39], deviceId: DEVICE });
    const batches = ddb.commandCalls(BatchWriteCommand).map((c) => c.args[0].input);
    // 40 keys -> 25 + 15, then 20 keys -> 20.
    expect(batches.map((b) => b.RequestItems?.['checkins']?.length)).toEqual([25, 15, 20]);
    const deleted = batches.flatMap((b) =>
      (b.RequestItems?.['checkins'] ?? []).map((r) => r.DeleteRequest?.Key?.['pk']),
    );
    expect(deleted).toEqual(keys);
  });

  it('retries UnprocessedItems and fails loudly if they never clear', async () => {
    ddb.on(QueryCommand).resolves({ Items: [{ pk: 'a#d' }, { pk: 'b#d' }] });
    ddb
      .on(BatchWriteCommand)
      .resolvesOnce({ UnprocessedItems: { checkins: [{ DeleteRequest: { Key: { pk: 'b#d' } } }] } })
      .resolves({});
    expect(await store().deleteForDevice(DEVICE)).toBe(2);
    const sent = ddb.commandCalls(BatchWriteCommand).map((c) => c.args[0].input.RequestItems);
    expect(sent[1]?.['checkins']).toEqual([{ DeleteRequest: { Key: { pk: 'b#d' } } }]);

    ddb.reset();
    ddb.on(QueryCommand).resolves({ Items: [{ pk: 'a#d' }] });
    ddb.on(BatchWriteCommand).resolves({
      UnprocessedItems: { checkins: [{ DeleteRequest: { Key: { pk: 'a#d' } } }] },
    });
    await expect(store().deleteForDevice(DEVICE)).rejects.toThrow('unprocessed');
  });

  it('a device with no check-ins deletes nothing', async () => {
    ddb.on(QueryCommand).resolves({ Items: [] });
    expect(await store().deleteForDevice(DEVICE)).toBe(0);
    expect(ddb.commandCalls(BatchWriteCommand)).toHaveLength(0);
  });
});

describe('MemoryCheckInStore retention and erasure', () => {
  it('records the ttl and deletes only the named device, leaving the totals as the aggregator would', async () => {
    const s = new MemoryCheckInStore();
    const until = new Date('2026-12-09T18:00:00Z');
    await s.save(checkIn(), until);
    await s.save(checkIn({ alertId: '33333333-3333-4333-8333-333333333333' }), until);
    await s.save(checkIn({ deviceId: '44444444-4444-4444-8444-444444444444' }), until);
    expect(s.ttlOf(checkIn().alertId, DEVICE)).toBe(until.getTime() / 1000);

    expect(await s.deleteForDevice(DEVICE)).toBe(2);
    expect(await s.get(checkIn().alertId, DEVICE)).toBeNull();
    expect(s.ttlOf(checkIn().alertId, DEVICE)).toBeUndefined();
    expect(await s.get(checkIn().alertId, '44444444-4444-4444-8444-444444444444')).not.toBeNull();
    expect(await s.deleteForDevice(DEVICE)).toBe(0); // idempotent
  });
});
