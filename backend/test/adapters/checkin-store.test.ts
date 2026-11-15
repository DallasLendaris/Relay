import { ConditionalCheckFailedException, DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
  TransactWriteCommand,
} from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { beforeEach, describe, expect, it } from 'vitest';
import type { CheckIn } from '@relay/shared';
import { DynamoCheckInStore } from '../../src/adapters/aws/dynamo-checkin-store.js';
import { MemoryCheckInStore } from '../../src/adapters/memory/checkin-store.js';
import { DynamoCheckInTotals } from '../../src/scale/checkins/totals.js';

const checkIn = (over: Partial<CheckIn> = {}): CheckIn => ({
  alertId: '11111111-1111-4111-8111-111111111111',
  deviceId: '22222222-2222-4222-8222-222222222222',
  status: 'ok',
  checkedInAt: '2026-11-09T12:00:00.000Z',
  token: 'tok',
  idempotencyKey: 'key-1',
  h3Cell: '872830828ffffff',
  ...over,
});

describe('MemoryCheckInStore', () => {
  it('saves once per alert and device, counts per cell, and returns the original on get', async () => {
    const store = new MemoryCheckInStore();
    const first = checkIn();
    expect(await store.save(first)).toBe(true);
    // Same device, new idempotency key: not saved, not counted.
    const again = checkIn({ idempotencyKey: 'key-2', checkedInAt: '2026-11-09T13:00:00.000Z' });
    expect(await store.save(again)).toBe(false);
    expect(await store.get(first.alertId, first.deviceId)).toEqual({
      alertId: first.alertId,
      deviceId: first.deviceId,
      h3Cell: first.h3Cell,
      checkedInAt: first.checkedInAt,
      idempotencyKey: 'key-1',
    });
    expect(await store.get(first.alertId, 'other-device')).toBeNull();
    expect(store.total(first.alertId, first.h3Cell)).toBe(1);
    // The same device on another alert is a different check-in.
    expect(await store.save(checkIn({ alertId: '33333333-3333-4333-8333-333333333333' }))).toBe(
      true,
    );
  });

  it('counts two devices separately, and one cannot block another by reusing a key', async () => {
    const store = new MemoryCheckInStore();
    const b = checkIn({ deviceId: '44444444-4444-4444-8444-444444444444' });
    expect(await store.save(b)).toBe(true);
    expect(await store.save(checkIn())).toBe(true); // same idempotencyKey, other device
    expect(store.total(b.alertId, b.h3Cell)).toBe(2);
  });
});

describe('DynamoCheckInStore', () => {
  const ddb = mockClient(DynamoDBDocumentClient);
  const store = () =>
    new DynamoCheckInStore(
      DynamoDBDocumentClient.from(new DynamoDBClient({ region: 'us-west-2' })),
      'checkins',
    );
  beforeEach(() => ddb.reset());

  const conditionFailed = () =>
    new ConditionalCheckFailedException({
      message: 'The conditional request failed',
      $metadata: {},
    });

  it('saves with ONE conditional put keyed alertId#deviceId: no transaction, no counter', async () => {
    ddb.on(PutCommand).resolves({});
    const c = checkIn();
    expect(await store().save(c)).toBe(true);
    expect(ddb.commandCalls(TransactWriteCommand)).toHaveLength(0);
    const calls = ddb.commandCalls(PutCommand);
    expect(calls).toHaveLength(1);
    expect(ddb.calls()).toHaveLength(1);
    expect(calls[0]?.args[0].input).toEqual({
      TableName: 'checkins',
      Item: {
        pk: `${c.alertId}#${c.deviceId}`,
        alertId: c.alertId,
        deviceId: c.deviceId,
        h3Cell: c.h3Cell,
        checkedInAt: c.checkedInAt,
        idempotencyKey: c.idempotencyKey,
        // No retainUntil passed: the default retention after the check-in itself.
        ttl: Date.parse(c.checkedInAt) / 1000 + 30 * 24 * 3600,
      },
      ConditionExpression: 'attribute_not_exists(pk)',
    });
  });

  it('never stores the token (it is a signed secret-bearing value the table does not need)', async () => {
    ddb.on(PutCommand).resolves({});
    await store().save(checkIn({ token: 'secret-token' }));
    expect(JSON.stringify(ddb.commandCalls(PutCommand)[0]?.args[0].input)).not.toContain(
      'secret-token',
    );
  });

  it('returns false when the put condition failed (a repeat)', async () => {
    ddb.on(PutCommand).rejects(conditionFailed());
    expect(await store().save(checkIn())).toBe(false);
  });

  it('rethrows every other error, so the caller can retry', async () => {
    ddb.on(PutCommand).rejects(new Error('ProvisionedThroughputExceeded'));
    await expect(store().save(checkIn())).rejects.toThrow('ProvisionedThroughputExceeded');
  });

  it('gets a check-in by alert and device with a consistent read, or null', async () => {
    const c = checkIn();
    ddb.on(GetCommand).resolves({
      Item: {
        pk: `${c.alertId}#${c.deviceId}`,
        alertId: c.alertId,
        deviceId: c.deviceId,
        h3Cell: c.h3Cell,
        checkedInAt: c.checkedInAt,
        idempotencyKey: c.idempotencyKey,
      },
    });
    expect(await store().get(c.alertId, c.deviceId)).toEqual({
      alertId: c.alertId,
      deviceId: c.deviceId,
      h3Cell: c.h3Cell,
      checkedInAt: c.checkedInAt,
      idempotencyKey: c.idempotencyKey,
    });
    expect(ddb.commandCalls(GetCommand)[0]?.args[0].input).toMatchObject({
      TableName: 'checkins',
      Key: { pk: `${c.alertId}#${c.deviceId}` },
      ConsistentRead: true,
    });
    ddb.on(GetCommand).resolves({});
    expect(await store().get(c.alertId, 'nope')).toBeNull();
  });
});

describe('DynamoCheckInTotals.listTotals', () => {
  const ddb = mockClient(DynamoDBDocumentClient);
  const totals = () =>
    new DynamoCheckInTotals(
      DynamoDBDocumentClient.from(new DynamoDBClient({ region: 'us-west-2' })),
      'totals',
    );
  beforeEach(() => ddb.reset());

  it('queries the byAlert index and pages with an opaque cursor', async () => {
    const lastKey = { pk: 'A#cell-2', alertId: 'A' };
    ddb
      .on(QueryCommand)
      .resolvesOnce({
        Items: [
          { pk: 'A#cell-1', alertId: 'A', h3Cell: 'cell-1', checkedIn: 7 },
          { pk: 'A#cell-2', alertId: 'A', h3Cell: 'cell-2', checkedIn: 3 },
        ],
        LastEvaluatedKey: lastKey,
      })
      .resolvesOnce({ Items: [{ pk: 'A#cell-3', alertId: 'A', h3Cell: 'cell-3', checkedIn: 1 }] });

    const page1 = await totals().listTotals('A', undefined, 2);
    expect(page1.totals).toEqual([
      { alertId: 'A', h3Cell: 'cell-1', checkedIn: 7 },
      { alertId: 'A', h3Cell: 'cell-2', checkedIn: 3 },
    ]);
    expect(page1.cursor).toBeTypeOf('string');
    const page2 = await totals().listTotals('A', page1.cursor, 2);
    expect(page2.totals).toEqual([{ alertId: 'A', h3Cell: 'cell-3', checkedIn: 1 }]);
    expect(page2.cursor).toBeUndefined();

    const [first, second] = ddb.commandCalls(QueryCommand).map((c) => c.args[0].input);
    expect(first).toMatchObject({
      TableName: 'totals',
      IndexName: 'byAlert',
      KeyConditionExpression: 'alertId = :a',
      ExpressionAttributeValues: { ':a': 'A' },
      Limit: 2,
    });
    expect(first?.ExclusiveStartKey).toBeUndefined();
    expect(second?.ExclusiveStartKey).toEqual(lastKey);
  });

  it('rejects a garbage cursor instead of querying', async () => {
    await expect(totals().listTotals('A', '%%%not-a-cursor')).rejects.toThrow(
      'Invalid totals cursor',
    );
    expect(ddb.calls()).toHaveLength(0);
  });
});
