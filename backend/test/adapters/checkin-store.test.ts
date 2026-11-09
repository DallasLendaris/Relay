import { DynamoDBClient, TransactionCanceledException } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, TransactWriteCommand } from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { beforeEach, describe, expect, it } from 'vitest';
import type { CheckIn } from '@relay/shared';
import { DynamoCheckInStore } from '../../src/adapters/aws/dynamo-checkin-store.js';
import { MemoryCheckInStore } from '../../src/adapters/memory/checkin-store.js';

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
  it('saves once per alert and key, counts per cell, and returns the original on get', async () => {
    const store = new MemoryCheckInStore();
    const first = checkIn();
    expect(await store.save(first)).toBe(true);
    expect(await store.save(checkIn({ checkedInAt: '2026-11-09T13:00:00.000Z' }))).toBe(false);
    expect(await store.get(first.alertId, 'key-1')).toEqual(first);
    expect(await store.get(first.alertId, 'other')).toBeNull();
    expect(store.total(first.alertId, first.h3Cell)).toBe(1);
    // The same key on another alert is a different check-in.
    expect(await store.save(checkIn({ alertId: '33333333-3333-4333-8333-333333333333' }))).toBe(
      true,
    );
  });
});

describe('DynamoCheckInStore', () => {
  const ddb = mockClient(DynamoDBDocumentClient);
  const store = () =>
    new DynamoCheckInStore(
      DynamoDBDocumentClient.from(new DynamoDBClient({ region: 'us-west-2' })),
      'checkins',
      'totals',
    );
  beforeEach(() => ddb.reset());

  const cancelled = (...codes: string[]) =>
    new TransactionCanceledException({
      message: 'Transaction cancelled',
      $metadata: {},
      CancellationReasons: codes.map((Code) => ({ Code })),
    });

  it('saves with one transaction: conditional put plus a total increment', async () => {
    ddb.on(TransactWriteCommand).resolves({});
    const c = checkIn();
    expect(await store().save(c)).toBe(true);
    const calls = ddb.commandCalls(TransactWriteCommand);
    expect(calls).toHaveLength(1);
    const items = calls[0]?.args[0].input.TransactItems;
    expect(items).toHaveLength(2);
    expect(items?.[0]?.Put).toMatchObject({
      TableName: 'checkins',
      Item: c,
      ConditionExpression: 'attribute_not_exists(alertId)',
    });
    expect(items?.[1]?.Update).toMatchObject({
      TableName: 'totals',
      Key: { alertId: c.alertId, h3Cell: c.h3Cell },
      UpdateExpression: 'ADD checkedIn :one',
      ExpressionAttributeValues: { ':one': 1 },
    });
  });

  it('returns false when the put condition failed (repeat key)', async () => {
    ddb.on(TransactWriteCommand).rejects(cancelled('ConditionalCheckFailed', 'None'));
    expect(await store().save(checkIn())).toBe(false);
  });

  it('rethrows cancellations that are not a failed put condition', async () => {
    ddb.on(TransactWriteCommand).rejects(cancelled('None', 'ConditionalCheckFailed'));
    await expect(store().save(checkIn())).rejects.toBeInstanceOf(TransactionCanceledException);
    ddb.on(TransactWriteCommand).rejects(cancelled('TransactionConflict', 'None'));
    await expect(store().save(checkIn())).rejects.toBeInstanceOf(TransactionCanceledException);
    ddb
      .on(TransactWriteCommand)
      .rejects(new TransactionCanceledException({ message: 'x', $metadata: {} }));
    await expect(store().save(checkIn())).rejects.toBeInstanceOf(TransactionCanceledException);
    ddb.on(TransactWriteCommand).rejects(new Error('boom'));
    await expect(store().save(checkIn())).rejects.toThrow('boom');
  });

  it('gets a check-in by alert and key, or null', async () => {
    const c = checkIn();
    ddb.on(GetCommand).resolves({ Item: { ...c } });
    expect(await store().get(c.alertId, 'key-1')).toEqual(c);
    expect(ddb.commandCalls(GetCommand)[0]?.args[0].input).toMatchObject({
      TableName: 'checkins',
      Key: { alertId: c.alertId, idempotencyKey: 'key-1' },
      ConsistentRead: true,
    });
    ddb.on(GetCommand).resolves({});
    expect(await store().get(c.alertId, 'nope')).toBeNull();
  });
});
