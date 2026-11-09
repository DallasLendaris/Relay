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
  it('saves once per alert and device, counts per cell, and returns the original on get', async () => {
    const store = new MemoryCheckInStore();
    const first = checkIn();
    expect(await store.save(first)).toBe(true);
    // Same device, new idempotency key: not saved, not counted.
    const again = checkIn({ idempotencyKey: 'key-2', checkedInAt: '2026-11-09T13:00:00.000Z' });
    expect(await store.save(again)).toBe(false);
    expect(await store.get(first.alertId, first.deviceId)).toEqual(first);
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

  it('returns false when the put condition failed (device already checked in)', async () => {
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

  it('gets a check-in by alert and device, or null', async () => {
    const c = checkIn();
    ddb.on(GetCommand).resolves({ Item: { ...c } });
    expect(await store().get(c.alertId, c.deviceId)).toEqual(c);
    expect(ddb.commandCalls(GetCommand)[0]?.args[0].input).toMatchObject({
      TableName: 'checkins',
      Key: { alertId: c.alertId, deviceId: c.deviceId },
      ConsistentRead: true,
    });
    ddb.on(GetCommand).resolves({});
    expect(await store().get(c.alertId, 'nope')).toBeNull();
  });
});
