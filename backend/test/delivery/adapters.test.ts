import { describe, expect, it } from 'vitest';
import { mockClient } from 'aws-sdk-client-mock';
import { SQSClient, SendMessageBatchCommand } from '@aws-sdk/client-sqs';
import { ConditionalCheckFailedException, DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DeleteCommand, DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';
import { SqsDeliveryQueue } from '../../src/adapters/aws/sqs-delivery-queue.js';
import { DynamoIdempotencyStore } from '../../src/adapters/aws/dynamo-idempotency-store.js';
import { ExpoPushSender } from '../../src/adapters/expo/expo-push-sender.js';
import { MemoryIdempotencyStore } from '../../src/adapters/memory/idempotency-store.js';
import type { DeliveryJob, PushMessage } from '../../src/ports.js';

const job = (n: number): DeliveryJob => ({
  alertId: 'a',
  deviceId: `d${n}`,
  expoPushToken: `t${n}`,
  language: 'en',
  checkInToken: 'c',
  expiresAt: '2026-11-10T14:00:00Z',
  groupingKey: 'a',
});

describe('SqsDeliveryQueue', () => {
  it('sends batches of at most 10', async () => {
    const sqs = new SQSClient({ region: 'us-west-2' });
    const mock = mockClient(sqs);
    mock.on(SendMessageBatchCommand).resolves({});
    await new SqsDeliveryQueue(sqs, 'url').enqueue(Array.from({ length: 23 }, (_, i) => job(i)));
    const sizes = mock
      .commandCalls(SendMessageBatchCommand)
      .map((c) => c.args[0].input.Entries?.length);
    expect(sizes).toEqual([10, 10, 3]);
  });

  it('retries failed entries once, then succeeds', async () => {
    const sqs = new SQSClient({ region: 'us-west-2' });
    const mock = mockClient(sqs);
    mock
      .on(SendMessageBatchCommand)
      .resolvesOnce({ Failed: [{ Id: '1', SenderFault: false, Code: 'x' }] })
      .resolvesOnce({});
    await new SqsDeliveryQueue(sqs, 'url').enqueue([job(0), job(1), job(2)]);
    const calls = mock.commandCalls(SendMessageBatchCommand);
    expect(calls).toHaveLength(2);
    expect(calls[1]?.args[0].input.Entries).toHaveLength(1);
    expect(calls[1]?.args[0].input.Entries?.[0]?.MessageBody).toContain('"d1"');
  });

  it('throws when entries still fail after the retry', async () => {
    const sqs = new SQSClient({ region: 'us-west-2' });
    const mock = mockClient(sqs);
    mock
      .on(SendMessageBatchCommand)
      .resolves({ Failed: [{ Id: '0', SenderFault: false, Code: 'x' }] });
    await expect(new SqsDeliveryQueue(sqs, 'url').enqueue([job(0)])).rejects.toThrow(/after retry/);
    expect(mock.commandCalls(SendMessageBatchCommand)).toHaveLength(2);
  });
});

describe('DynamoIdempotencyStore', () => {
  const doc = () => DynamoDBDocumentClient.from(new DynamoDBClient({ region: 'us-west-2' }));

  it('puts with attribute_not_exists(pk) and a TTL', async () => {
    const d = doc();
    const mock = mockClient(d);
    mock.on(PutCommand).resolves({});
    const ok = await new DynamoIdempotencyStore(d, 'tbl').claim(
      'k',
      new Date('2026-11-10T14:00:00Z'),
    );
    expect(ok).toBe(true);
    expect(mock.commandCalls(PutCommand)[0]?.args[0].input).toMatchObject({
      TableName: 'tbl',
      Item: { pk: 'k', ttl: Date.parse('2026-11-10T14:00:00Z') / 1000 },
      ConditionExpression: 'attribute_not_exists(pk)',
    });
  });

  it('returns false when the key already exists, and rethrows other errors', async () => {
    const d = doc();
    const mock = mockClient(d);
    const store = new DynamoIdempotencyStore(d, 'tbl');
    mock
      .on(PutCommand)
      .rejects(new ConditionalCheckFailedException({ message: 'x', $metadata: {} }));
    expect(await store.claim('k', new Date())).toBe(false);
    mock.on(PutCommand).rejects(new Error('boom'));
    await expect(store.claim('k', new Date())).rejects.toThrow('boom');
  });
});

describe('DynamoIdempotencyStore.release', () => {
  it('deletes the key', async () => {
    const d = DynamoDBDocumentClient.from(new DynamoDBClient({ region: 'us-west-2' }));
    const mock = mockClient(d);
    mock.on(DeleteCommand).resolves({});
    await new DynamoIdempotencyStore(d, 'tbl').release('k');
    expect(mock.commandCalls(DeleteCommand)[0]?.args[0].input).toEqual({
      TableName: 'tbl',
      Key: { pk: 'k' },
    });
  });
});

describe('MemoryIdempotencyStore', () => {
  it('claims once, and again after expiry', async () => {
    let now = new Date('2026-11-10T10:00:00Z');
    const s = new MemoryIdempotencyStore({ now: () => now });
    const exp = new Date('2026-11-10T11:00:00Z');
    expect(await s.claim('k', exp)).toBe(true);
    expect(await s.claim('k', exp)).toBe(false);
    now = new Date('2026-11-10T12:00:00Z');
    expect(await s.claim('k', exp)).toBe(true);
  });
});

describe('ExpoPushSender', () => {
  const msg = (n: number): PushMessage => ({
    to: `t${n}`,
    title: 'T',
    body: 'B',
    data: {},
    categoryId: 'alert-checkin',
  });
  const ok = (n: number) =>
    new Response(JSON.stringify({ data: Array.from({ length: n }, () => ({ status: 'ok' })) }));

  it('chunks by 100 and sets priority, sound and channel', async () => {
    const bodies: Array<Array<Record<string, unknown>>> = [];
    const fetchFn = (async (url: string, init: RequestInit) => {
      expect(url).toBe('https://exp.host/--/api/v2/push/send');
      const b = JSON.parse(String(init.body)) as Array<Record<string, unknown>>;
      bodies.push(b);
      return ok(b.length);
    }) as unknown as typeof fetch;
    const res = await new ExpoPushSender({ fetch: fetchFn }).send(
      Array.from({ length: 250 }, (_, i) => msg(i)),
    );
    expect(bodies.map((b) => b.length)).toEqual([100, 100, 50]);
    expect(bodies[0]?.[0]).toMatchObject({
      priority: 'high',
      sound: 'default',
      channelId: 'alerts',
      categoryId: 'alert-checkin',
    });
    expect(res).toHaveLength(250);
    expect(res.every((r) => r.ok)).toBe(true);
  });

  it('maps ticket errors per message', async () => {
    const fetchFn = (async () =>
      new Response(
        JSON.stringify({
          data: [
            { status: 'ok' },
            { status: 'error', message: 'gone', details: { error: 'DeviceNotRegistered' } },
            { status: 'error', message: 'slow down', details: { error: 'MessageRateExceeded' } },
            { status: 'error', message: 'plain' },
          ],
        }),
      )) as unknown as typeof fetch;
    const res = await new ExpoPushSender({ fetch: fetchFn }).send([msg(0), msg(1), msg(2), msg(3)]);
    expect(res).toEqual([
      { ok: true },
      { ok: false, error: 'DeviceNotRegistered' },
      { ok: false, error: 'MessageRateExceeded' },
      { ok: false, error: 'plain' },
    ]);
  });

  it('retries 429 and 5xx with backoff, then succeeds', async () => {
    const statuses = [503, 429];
    const sleeps: number[] = [];
    const fetchFn = (async () => {
      const s = statuses.shift();
      return s ? new Response('', { status: s }) : ok(1);
    }) as unknown as typeof fetch;
    const sender = new ExpoPushSender({
      fetch: fetchFn,
      sleep: async (ms) => void sleeps.push(ms),
      backoffMs: 100,
    });
    expect(await sender.send([msg(0)])).toEqual([{ ok: true }]);
    expect(sleeps).toEqual([100, 200]);
  });

  it('gives up after 3 retries and fails the chunk', async () => {
    let calls = 0;
    const fetchFn = (async () => {
      calls++;
      return new Response('', { status: 500 });
    }) as unknown as typeof fetch;
    const res = await new ExpoPushSender({ fetch: fetchFn, sleep: async () => {} }).send([
      msg(0),
      msg(1),
    ]);
    expect(calls).toBe(4);
    expect(res).toEqual([
      { ok: false, error: 'HttpError500' },
      { ok: false, error: 'HttpError500' },
    ]);
  });

  it('treats bad JSON or a wrong shape as a retryable failure, not a throw', async () => {
    for (const body of ['<html>oops</html>', '{"data":"nope"}', 'null']) {
      const fetchFn = (async () => new Response(body)) as unknown as typeof fetch;
      const res = await new ExpoPushSender({ fetch: fetchFn }).send([msg(0), msg(1)]);
      expect(res).toEqual([
        { ok: false, error: 'BadResponse' },
        { ok: false, error: 'BadResponse' },
      ]);
    }
  });

  it('sends a bearer token only when an access token is given', async () => {
    const seen: Array<string | undefined> = [];
    const fetchFn = (async (_u: string, init: RequestInit) => {
      seen.push((init.headers as Record<string, string>)['authorization']);
      return ok(1);
    }) as unknown as typeof fetch;
    await new ExpoPushSender({ fetch: fetchFn, accessToken: 'tok' }).send([msg(0)]);
    await new ExpoPushSender({ fetch: fetchFn }).send([msg(0)]);
    expect(seen).toEqual(['Bearer tok', undefined]);
  });

  it('returns per-message results when a later chunk fails, keeping earlier ones', async () => {
    let calls = 0;
    const fetchFn = (async () => {
      calls++;
      return calls === 1 ? ok(100) : new Response('', { status: 400 });
    }) as unknown as typeof fetch;
    const res = await new ExpoPushSender({ fetch: fetchFn }).send(
      Array.from({ length: 150 }, (_, i) => msg(i)),
    );
    expect(res).toHaveLength(150);
    expect(res.slice(0, 100).every((r) => r.ok)).toBe(true);
    expect(res.slice(100).every((r) => !r.ok)).toBe(true);
  });

  it('does not retry a 400', async () => {
    let calls = 0;
    const fetchFn = (async () => {
      calls++;
      return new Response('', { status: 400 });
    }) as unknown as typeof fetch;
    await new ExpoPushSender({ fetch: fetchFn }).send([msg(0)]);
    expect(calls).toBe(1);
  });
});
