import { ConditionalCheckFailedException, DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { describe, expect, it } from 'vitest';
import {
  DynamoIdempotencyStore,
  IdempotencyThrottledError,
} from '../../src/adapters/aws/dynamo-idempotency-store.js';
import { processBatch } from '../../src/scale/sender/sender.js';
import { makeBatch, makeDeps, recordingLogger, seedAlert } from '../scale/sender/fakes.js';

const NOW = new Date('2026-11-10T10:00:00Z');
const throttle = (name: string) => Object.assign(new Error(name), { name });

function store(opts: { random?: () => number } = {}) {
  const d = DynamoDBDocumentClient.from(new DynamoDBClient({ region: 'us-west-2' }));
  const mock = mockClient(d);
  const sleeps: number[] = [];
  const s = new DynamoIdempotencyStore(
    d,
    'tbl',
    { now: () => NOW },
    {
      sleep: async (ms) => void sleeps.push(ms),
      random: opts.random ?? (() => 0.999),
    },
  );
  return { s, mock, sleeps };
}

describe('DynamoIdempotencyStore.claim throttling (PERF-4b)', () => {
  it.each([
    'ProvisionedThroughputExceededException',
    'ThrottlingException',
    'RequestLimitExceeded',
  ])('retries %s with jittered, growing backoff and then claims', async (name) => {
    const { s, mock, sleeps } = store();
    mock.on(PutCommand).rejectsOnce(throttle(name)).rejectsOnce(throttle(name)).resolves({});
    expect(await s.claim('k', NOW)).toBe('claimed');
    expect(mock.commandCalls(PutCommand)).toHaveLength(3);
    expect(sleeps).toHaveLength(2);
    // Full jitter: each delay is below its doubling ceiling (50 ms, then 100 ms).
    expect(sleeps[0]).toBeLessThan(50);
    expect(sleeps[1]).toBeLessThan(100);
    expect(sleeps[1]).toBeGreaterThan(sleeps[0] ?? 0);
  });

  it('uses the random source for jitter (0 means no wait)', async () => {
    const { s, mock, sleeps } = store({ random: () => 0 });
    mock.on(PutCommand).rejectsOnce(throttle('ThrottlingException')).resolves({});
    await s.claim('k', NOW);
    expect(sleeps).toEqual([0]);
  });

  it('gives up inside a bounded budget with a retryable error, never a raw throttle', async () => {
    const { s, mock, sleeps } = store();
    mock.on(PutCommand).rejects(throttle('ProvisionedThroughputExceededException'));
    const err = await s.claim('k', NOW).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(IdempotencyThrottledError);
    expect((err as IdempotencyThrottledError).retryable).toBe(true);
    expect(mock.commandCalls(PutCommand)).toHaveLength(5); // maxAttempts
    expect(sleeps.reduce((a, b) => a + b, 0)).toBeLessThanOrEqual(2_500);
  });

  it('retries a throttled read after losing the race, too', async () => {
    const { s, mock } = store();
    mock
      .on(PutCommand)
      .rejects(new ConditionalCheckFailedException({ message: 'x', $metadata: {} }));
    mock
      .on(GetCommand)
      .rejectsOnce(throttle('RequestLimitExceeded'))
      .resolves({ Item: { pk: 'k', state: 'sent' } });
    expect(await s.claim('k', NOW)).toBe('sent');
  });

  it('does not retry other errors', async () => {
    const { s, mock, sleeps } = store();
    mock.on(PutCommand).rejects(new Error('boom'));
    await expect(s.claim('k', NOW)).rejects.toThrow('boom');
    expect(mock.commandCalls(PutCommand)).toHaveLength(1);
    expect(sleeps).toEqual([]);
  });
});

describe('processBatch when the claim is throttled out', () => {
  it('returns a retryable outcome for that batch instead of throwing', async () => {
    const { logger, lines } = recordingLogger();
    const t = makeDeps({ logger });
    await seedAlert(t.alerts);
    const { s, mock } = store();
    mock.on(PutCommand).rejects(throttle('ThrottlingException'));
    t.deps.idempotency = s;
    const out = await processBatch(makeBatch(), t.deps);
    expect(out).toMatchObject({ ack: false, reason: 'claim_throttled', sent: 0 });
    expect(t.push.calls).toHaveLength(0);
    expect(JSON.stringify(lines)).toContain('sender.claim_throttled');
  });

  it('still throws for a non-retryable claim failure', async () => {
    const t = makeDeps();
    await seedAlert(t.alerts);
    t.deps.idempotency = {
      ...t.idempotency,
      claim: async () => {
        throw new Error('boom');
      },
    } as typeof t.deps.idempotency;
    await expect(processBatch(makeBatch(), t.deps)).rejects.toThrow('boom');
  });
});
