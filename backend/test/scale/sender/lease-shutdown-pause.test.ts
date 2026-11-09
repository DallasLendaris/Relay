import { ConditionalCheckFailedException, DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { SendMessageBatchCommand } from '@aws-sdk/client-sqs';
import { DynamoDBDocumentClient, PutCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { describe, expect, it } from 'vitest';
import { DynamoIdempotencyStore } from '../../../src/adapters/aws/dynamo-idempotency-store.js';
import { MemoryIdempotencyStore } from '../../../src/adapters/memory/idempotency-store.js';
import { SqsSendQueue as FanoutSqsSendQueue } from '../../../src/scale/fanout/queues.js';
import { SENDER_CHUNK_SIZE, type SendBatch } from '../../../src/scale/contracts.js';
import { SenderConsumer } from '../../../src/scale/sender/consumer.js';
import {
  SENDER_LEASE_MS,
  processBatch,
  type BatchControl,
  type BatchOutcome,
} from '../../../src/scale/sender/sender.js';
import { SqsSendQueue } from '../../../src/scale/sender/sqs-send-queue.js';
import {
  FakePush,
  FakeSqs,
  makeBatch,
  makeDeps,
  recordingLogger,
  seedAlert,
  waitFor,
} from './fakes.js';

const URGENT = 'urgent-q';
const STANDARD = 'standard-q';
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('IdempotencyStore.extend', () => {
  const NOW = new Date('2026-11-10T10:00:00Z');

  it('memory: extends our own live claim so it survives past the original lease', async () => {
    let now = NOW.getTime();
    const store = new MemoryIdempotencyStore({ now: () => new Date(now) });
    expect(await store.claim('k', new Date(now + 45_000))).toBe('claimed');
    now += 40_000;
    expect(await store.extend('k', new Date(now + 45_000))).toBe(true);
    now += 40_000; // past the ORIGINAL lease, inside the extended one
    expect(await store.claim('k', new Date(now + 45_000))).toBe('leased');
    now += 10_000;
    expect(await store.claim('k', new Date(now + 45_000))).toBe('claimed'); // now it expired
  });

  it('memory: false when never claimed, released, or already sent', async () => {
    const store = new MemoryIdempotencyStore({ now: () => NOW });
    const until = new Date(NOW.getTime() + 1000);
    expect(await store.extend('nope', until)).toBe(false);
    await store.claim('r', until);
    await store.release('r');
    expect(await store.extend('r', until)).toBe(false);
    await store.claim('s', until);
    await store.markSent('s', new Date(NOW.getTime() + 86_400_000));
    expect(await store.extend('s', until)).toBe(false);
  });

  it('dynamo: conditional on state=leased AND our attemptId; false when the condition fails', async () => {
    const d = DynamoDBDocumentClient.from(new DynamoDBClient({ region: 'us-west-2' }));
    const mock = mockClient(d);
    mock.on(PutCommand).resolves({});
    const store = new DynamoIdempotencyStore(d, 'tbl', { now: () => NOW });
    expect(await store.extend('k', NOW)).toBe(false); // not claimed by this instance: no call
    expect(mock.commandCalls(UpdateCommand)).toHaveLength(0);

    await store.claim('k', NOW);
    const attemptId = mock.commandCalls(PutCommand)[0]?.args[0].input.Item?.['attemptId'];
    mock.on(UpdateCommand).resolves({});
    const until = new Date(NOW.getTime() + 45_000);
    expect(await store.extend('k', until)).toBe(true);
    expect(mock.commandCalls(UpdateCommand)[0]?.args[0].input).toMatchObject({
      TableName: 'tbl',
      Key: { pk: 'k' },
      ConditionExpression: '#state = :leased AND attemptId = :attempt',
      ExpressionAttributeValues: {
        ':leased': 'leased',
        ':attempt': attemptId,
        ':until': until.getTime(),
      },
    });

    mock
      .on(UpdateCommand)
      .rejects(new ConditionalCheckFailedException({ message: 'x', $metadata: {} }));
    expect(await store.extend('k', until)).toBe(false); // someone else owns it now
    mock.on(UpdateCommand).rejects(new Error('throttled'));
    await expect(store.extend('k', until)).rejects.toThrow('throttled');
  });
});

describe('SQS delay on retry enqueue', () => {
  const sqsMock = () => {
    const calls: Array<Array<{ DelaySeconds?: number }>> = [];
    return {
      calls,
      sqs: {
        send: async (c: unknown) => {
          calls.push((c as SendMessageBatchCommand).input.Entries ?? []);
          return {};
        },
      },
    };
  };
  const urls = { urgent: 'u', standard: 's' };

  it.each([
    ['sender queue', (s: ReturnType<typeof sqsMock>['sqs']) => new SqsSendQueue(s as never, urls)],
    [
      'fan-out queue',
      (s: ReturnType<typeof sqsMock>['sqs']) => new FanoutSqsSendQueue(s as never, urls),
    ],
  ])('%s: sets DelaySeconds, caps it at 900, omits it when not asked', async (_n, make) => {
    const m = sqsMock();
    const q = make(m.sqs);
    await q.enqueue('urgent', [makeBatch()], { delaySeconds: 37 });
    await q.enqueue('urgent', [makeBatch()], { delaySeconds: 5000 });
    await q.enqueue('urgent', [makeBatch()]);
    expect(m.calls[0]?.[0]?.DelaySeconds).toBe(37);
    expect(m.calls[1]?.[0]?.DelaySeconds).toBe(900);
    expect(m.calls[2]?.[0]).not.toHaveProperty('DelaySeconds');
  });
});

// ---------- consumer: leases, shutdown, pause ----------

const msgOf = (key: string, n = 1): SendBatch[] =>
  Array.from({ length: n }, (_, i) => makeBatch({ batchKey: `${key}:${i}` }, 1));
const ok = (over: Partial<BatchOutcome> = {}): BatchOutcome => ({
  ack: true,
  reason: 'sent',
  sent: 1,
  permanentFailed: 0,
  retried: 0,
  gaveUp: 0,
  ...over,
});

function makeConsumer(
  sqs: FakeSqs,
  handle: (b: SendBatch, c: BatchControl) => Promise<BatchOutcome>,
  o: Record<string, unknown> = {},
) {
  const { lines, logger } = recordingLogger();
  const consumer = new SenderConsumer({
    sqs: sqs as never,
    urgentQueueUrl: URGENT,
    standardQueueUrl: STANDARD,
    handle,
    logger,
    writeMetric: () => {},
    ...o,
  });
  return { consumer, lines };
}

describe('lease renewal', () => {
  it('the visibility heartbeat also extends the lease of every active batch in the message', async () => {
    const sqs = new FakeSqs(10_000);
    sqs.add(URGENT, msgOf('m', 3));
    const extended = new Map<string, number>();
    let release!: () => void;
    const hold = new Promise<void>((r) => (release = r));
    let started = 0;
    const { consumer } = makeConsumer(
      sqs,
      async (_b, control) => {
        control.active = true; // what processBatch does once it holds the claim
        started++;
        await hold;
        control.active = false;
        return ok();
      },
      {
        extendAfterMs: 15,
        extendLease: async (b: SendBatch) => {
          extended.set(b.batchKey, (extended.get(b.batchKey) ?? 0) + 1);
          return true;
        },
      },
    );
    const run = consumer.run();
    await waitFor(() => started === 3);
    await waitFor(() => [...extended.values()].every((n) => n >= 2) && extended.size === 3);
    expect(sqs.extensions.length).toBeGreaterThanOrEqual(2); // SQS visibility extended too
    release();
    await waitFor(() => sqs.size(URGENT) === 0);
    consumer.stop();
    await run;
  });

  it('does not touch batches that are not active (finished, or not claimed yet)', async () => {
    const sqs = new FakeSqs(10_000);
    sqs.add(URGENT, msgOf('m', 1));
    let calls = 0;
    let release!: () => void;
    const hold = new Promise<void>((r) => (release = r));
    const { consumer } = makeConsumer(
      sqs,
      async () => {
        await hold; // never marks itself active
        return ok();
      },
      {
        extendAfterMs: 10,
        extendLease: async () => (calls++, true),
      },
    );
    const run = consumer.run();
    await sleep(60);
    expect(calls).toBe(0);
    release();
    consumer.stop();
    await run;
  });

  it('when extend reports the lease lost, processBatch stops sending the remaining chunks and logs delivery.lease_lost', async () => {
    const { lines, logger } = recordingLogger();
    const t = makeDeps({ logger, chunkConcurrency: 1 });
    await seedAlert(t.alerts);
    // Real store: the lease "is taken" by releasing it from outside while chunk 1 is in flight.
    const push = new FakePush(() => ({ ok: true }), 80);
    t.deps.push = push;
    const batch = makeBatch({}, SENDER_CHUNK_SIZE * 3);
    const sqs = new FakeSqs(10_000);
    sqs.add(URGENT, [batch]);
    let outcome: BatchOutcome | undefined;
    const { consumer } = makeConsumer(
      sqs,
      async (b, control) => {
        outcome = await processBatch(b, t.deps, control);
        return outcome;
      },
      {
        extendAfterMs: 20,
        extendLease: (b: SendBatch) =>
          t.idempotency.extend(b.batchKey, new Date(Date.now() + SENDER_LEASE_MS)),
      },
    );
    const run = consumer.run();
    await waitFor(() => push.calls.length === 1);
    await t.idempotency.release(batch.batchKey); // another consumer takes over
    await waitFor(() => outcome !== undefined);
    consumer.stop();
    await run;

    expect(outcome).toMatchObject({ ack: false, reason: 'lease_lost' });
    expect(push.calls).toHaveLength(1); // chunks 2 and 3 never started
    expect(lines.find((l) => l.msg === 'delivery.lease_lost')?.level).toBe('error');
    expect(t.sendQueue.enqueued).toHaveLength(0); // nothing persisted
    expect(sqs.size(URGENT)).toBe(1); // message not deleted
  });

  it('a live lease keeps being extended while a slow batch finishes, then marks sent', async () => {
    const t = makeDeps({ chunkConcurrency: 1 });
    await seedAlert(t.alerts);
    const push = new FakePush(() => ({ ok: true }), 40);
    t.deps.push = push;
    const batch = makeBatch({}, SENDER_CHUNK_SIZE * 2);
    const sqs = new FakeSqs(10_000);
    sqs.add(URGENT, [batch]);
    let extendCalls = 0;
    const { consumer } = makeConsumer(sqs, (b, c) => processBatch(b, t.deps, c), {
      extendAfterMs: 15,
      extendLease: (b: SendBatch) => (
        extendCalls++,
        t.idempotency.extend(b.batchKey, new Date(Date.now() + SENDER_LEASE_MS))
      ),
    });
    const run = consumer.run();
    await waitFor(() => sqs.size(URGENT) === 0);
    consumer.stop();
    await run;
    expect(extendCalls).toBeGreaterThanOrEqual(1);
    expect(push.calls).toHaveLength(2);
    expect(await t.idempotency.claim(batch.batchKey, new Date(Date.now() + 1000))).toBe('sent');
  });
});

describe('graceful shutdown (SIGTERM)', () => {
  it('batches that finish inside the drain window are completed and deleted', async () => {
    const sqs = new FakeSqs(10_000);
    sqs.add(URGENT, msgOf('m', 2));
    let started = 0;
    const { consumer } = makeConsumer(
      sqs,
      async () => {
        started++;
        await sleep(40);
        return ok();
      },
      { drainTimeoutMs: 2000 },
    );
    const run = consumer.run();
    await waitFor(() => started === 2);
    consumer.stop();
    await run;
    expect(sqs.size(URGENT)).toBe(0);
  });

  it('at the deadline unfinished batches stop starting chunks, persist nothing, release their claims and leave the message visible', async () => {
    const t = makeDeps({ chunkConcurrency: 1 });
    await seedAlert(t.alerts);
    const push = new FakePush(() => ({ ok: true }), 120); // each chunk takes 120 ms
    t.deps.push = push;
    const batch = makeBatch({}, SENDER_CHUNK_SIZE * 3);
    const sqs = new FakeSqs(10_000); // would stay invisible for 10 s unless made visible
    sqs.add(URGENT, [batch]);
    const { consumer } = makeConsumer(sqs, (b, c) => processBatch(b, t.deps, c), {
      drainTimeoutMs: 40,
      abortGraceMs: 1000,
    });
    const run = consumer.run();
    await waitFor(() => push.calls.length === 1);
    consumer.stop();
    await run;

    expect(push.calls).toHaveLength(1); // chunks 2 and 3 were never started
    expect(t.sendQueue.enqueued).toHaveLength(0); // no retries persisted
    // Claim released (not marked sent): the next consumer can claim at once.
    expect(await t.idempotency.claim(batch.batchKey, new Date(Date.now() + 1000))).toBe('claimed');
    expect(sqs.size(URGENT)).toBe(1);
    expect(sqs.visible(URGENT)).toBe(1); // visibility set to 0: visible right away
  });

  it('a batch stuck in a network call has its claim released by the consumer after the grace period', async () => {
    const t = makeDeps();
    await seedAlert(t.alerts);
    t.deps.push = { send: () => new Promise(() => {}) }; // never answers
    const batch = makeBatch();
    const sqs = new FakeSqs(10_000);
    sqs.add(URGENT, [batch]);
    const { consumer } = makeConsumer(sqs, (b, c) => processBatch(b, t.deps, c), {
      drainTimeoutMs: 30,
      abortGraceMs: 30,
    });
    const run = consumer.run();
    await sleep(30);
    consumer.stop();
    await run;
    expect(await t.idempotency.claim(batch.batchKey, new Date(Date.now() + 1000))).toBe('claimed');
    expect(sqs.visible(URGENT)).toBe(1);
  });

  it('stops receiving at once', async () => {
    const sqs = new FakeSqs(10_000);
    const { consumer } = makeConsumer(sqs, async () => ok());
    const run = consumer.run();
    await waitFor(() => sqs.receives.length > 0);
    consumer.stop();
    await run;
    const after = sqs.receives.length;
    await sleep(30);
    expect(sqs.receives.length).toBe(after);
  });
});

describe('pause handling', () => {
  it('receives nothing while paused, and an in-flight batch that finds the pause is parked, not bounced to the DLQ (10 minutes of fake time)', async () => {
    let t = 1_000_000;
    const startT = t;
    const sqs = new FakeSqs(30_000, () => t);
    sqs.maxReceiveCount = 5;
    const rec = recordingLogger();
    const d = makeDeps({ logger: rec.logger });
    await seedAlert(d.alerts);
    const batch = makeBatch({}, 3);
    sqs.add(URGENT, [batch], t);

    let consumerPaused = false; // what the consumer's pre-receive check sees
    let batchPaused = true; // what the batch sees mid-way: the switch is already on
    let receivesWhenPauseBegan = -1;
    let receivesAtResume = -1;
    let sleeps = 0;
    d.deps.isDeliveryPaused = async () => batchPaused;
    const { consumer } = makeConsumer(sqs, (b, c) => processBatch(b, d.deps, c), {
      now: () => t,
      isPaused: async () => consumerPaused,
      sleep: async (ms: number) => {
        sleeps++;
        t += ms;
        if (receivesWhenPauseBegan < 0) receivesWhenPauseBegan = sqs.receives.length;
        if (t - startT >= 10 * 60_000 && receivesAtResume < 0) {
          receivesAtResume = sqs.receives.length;
          consumerPaused = false;
          batchPaused = false; // operator resumes after 10 minutes
        }
        await sleep(0);
      },
    });
    const run = consumer.run();

    // 1. Received once while the consumer still saw "running"; the batch found the pause.
    await waitFor(() => d.push.calls.length === 0 && sqs.extensions.some((e) => e.seconds === 60));
    expect(sqs.extensions.at(-1)?.seconds).toBe(60); // parked for 60 s, not re-received at 30 s
    expect(rec.lines.some((l) => l.msg === 'sender.paused')).toBe(true);
    expect(await d.idempotency.claim(batch.batchKey, new Date(t + 1))).toBe('claimed'); // claim released
    await d.idempotency.release(batch.batchKey);

    // 2. The consumer now sees the pause (cache expired): from here on no receives at all.
    consumerPaused = true;
    t += 6000;
    await waitFor(() => sleeps > 0);
    const receivesAtPause = receivesWhenPauseBegan;
    await waitFor(() => receivesAtResume >= 0);
    expect(receivesAtResume).toBe(receivesAtPause); // not one receive in 10 minutes
    expect(sleeps).toBeGreaterThan(100); // 5 s sleeps across 10 minutes
    expect(sqs.dead).toHaveLength(0); // no DLQ growth

    // 3. Resumed: the batch is received again (receive #2 of 5) and goes out.
    await waitFor(() => sqs.size(URGENT) === 0);
    expect(d.push.calls.flat()).toHaveLength(3);
    consumer.stop();
    await run;
    expect(sqs.dead).toHaveLength(0);
  });

  it('checks the kill switch before each receive but caches the answer for 5 s', async () => {
    let t = 5_000_000;
    const sqs = new FakeSqs(10_000, () => t);
    let checks = 0;
    const { consumer } = makeConsumer(sqs, async () => ok(), {
      now: () => t,
      isPaused: async () => (checks++, false),
    });
    const run = consumer.run();
    await waitFor(() => sqs.receives.length >= 5);
    expect(checks).toBe(1); // same fake instant: one real check
    t += 5001;
    const before = checks;
    await waitFor(() => checks > before);
    consumer.stop();
    await run;
    expect(checks).toBe(before + 1);
  });

  it('an unreadable kill switch is treated as paused: nothing is received', async () => {
    const sqs = new FakeSqs(10_000);
    sqs.add(URGENT, msgOf('m'));
    let sleeps = 0;
    const { consumer, lines } = makeConsumer(sqs, async () => ok(), {
      isPaused: async () => {
        throw new Error('ssm down');
      },
      pauseCacheMs: 0,
      sleep: async () => {
        sleeps++;
        await sleep(1);
      },
    });
    const run = consumer.run();
    await waitFor(() => sleeps >= 3);
    consumer.stop();
    await run;
    expect(sqs.receives).toHaveLength(0);
    expect(lines.some((l) => l.msg === 'sender.pause_check_failed')).toBe(true);
  });
});
