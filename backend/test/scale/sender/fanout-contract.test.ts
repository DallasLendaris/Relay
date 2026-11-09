import { describe, expect, it } from 'vitest';
import type { SendBatch } from '../../../src/scale/contracts.js';
import { bucketFor } from '../../../src/scale/fanout/hash.js';
import { MAX_MESSAGE_BYTES } from '../../../src/scale/fanout/pack.js';
import { processShard } from '../../../src/scale/fanout/shard-worker.js';
import { SenderConsumer, parseBatches } from '../../../src/scale/sender/consumer.js';
import { baseBatchKey, processBatch } from '../../../src/scale/sender/sender.js';
import { countyCells, entry, setup, signed } from '../fanout/helpers.js';
import { FakePush, FakeSqs, makeDeps, silent, waitFor } from './fakes.js';

const CELL = countyCells()[0] as string;
const URGENT = 'urgent-q';
const STANDARD = 'standard-q';

/** Runs the real consumer over a fake SQS until the urgent queue is empty. */
async function consume(sqs: FakeSqs, deps: ReturnType<typeof makeDeps>['deps']) {
  const consumer = new SenderConsumer({
    sqs: sqs as never,
    urgentQueueUrl: URGENT,
    standardQueueUrl: STANDARD,
    handle: (b) => processBatch(b, deps),
    logger: silent,
    writeMetric: () => {},
    concurrency: 4,
  });
  const run = consumer.run();
  await waitFor(() => sqs.size(URGENT) === 0 && consumer.inFlightCount === 0);
  consumer.stop();
  await run;
}

describe('fan-out message body -> sender consumer', () => {
  it('parseBatches reads exactly what the shard worker wrote (a JSON array of SendBatch)', async () => {
    const alert = signed();
    const { deps, cellIndex, sendQueue } = setup(alert);
    for (let i = 0; i < 300; i++) cellIndex.add(entry(CELL, `dev-${i}`));
    await processShard(
      {
        alertId: alert.id,
        sourceId: alert.sourceId,
        relaySeverity: alert.relaySeverity,
        shardId: 's:0',
        cells: [CELL],
      },
      deps,
    );
    const bodies = sendQueue.messages.urgent;
    expect(bodies.length).toBeGreaterThan(0);
    expect(bodies.some((b) => (JSON.parse(b) as unknown[]).length > 1)).toBe(true);
    const parsed = bodies.flatMap((b) => parseBatches(b) ?? []);
    expect(parseBatches(bodies[0])).not.toBeNull();
    // Identical to what the fan-out holds, field for field.
    expect(parsed).toEqual(sendQueue.batches('urgent'));
    expect(parsed.reduce((n, b) => n + b.recipients.length, 0)).toBe(300);
  });

  it('feeding shard-produced messages through the consumer pushes every recipient once', async () => {
    const alert = signed();
    const shard = setup(alert);
    for (let i = 0; i < 300; i++) shard.cellIndex.add(entry(CELL, `dev-${i}`));
    await processShard(
      {
        alertId: alert.id,
        sourceId: alert.sourceId,
        relaySeverity: alert.relaySeverity,
        shardId: 's:0',
        cells: [CELL],
      },
      shard.deps,
    );
    const sender = makeDeps();
    await sender.alerts.save(alert);
    const sqs = new FakeSqs(60_000);
    for (const body of shard.sendQueue.messages.urgent) sqs.add(URGENT, body);

    await consume(sqs, sender.deps);

    // The fan-out's token carries the device id, so it identifies each push.
    const pushed = sender.push.calls.flat().map((m) => m.data['checkInToken']);
    expect(pushed).toHaveLength(300);
    expect(new Set(pushed).size).toBe(300);
    // The check-in token the fan-out made is what reaches the phone.
    const [first] = shard.sendQueue.batches('urgent');
    const sentTokens = new Set(sender.push.calls.flat().map((m) => m.data['checkInToken']));
    expect(sentTokens.has(first?.recipients[0]?.checkInToken)).toBe(true);
  });
});

describe('oversize bucket split keys (`:s<path>`)', () => {
  /** 1,000 devices that all hash to bucket 0, with 500-char tokens, so one batch exceeds a message. */
  async function oversize() {
    const alert = signed();
    const ids: string[] = [];
    for (let i = 0; ids.length < 1_000; i++) if (bucketFor(`big-${i}`) === 0) ids.push(`big-${i}`);
    const { deps, cellIndex, sendQueue, lines } = setup(alert, {
      makeCheckInToken: (a, d, c) => `${a}.${d}.${c}.`.padEnd(500, 'x'),
    });
    for (const id of ids) cellIndex.add(entry(CELL, id));
    await processShard(
      {
        alertId: alert.id,
        sourceId: alert.sourceId,
        relaySeverity: alert.relaySeverity,
        shardId: 's:0',
        cells: [CELL],
      },
      deps,
    );
    return { alert, ids, sendQueue, lines };
  }

  it('the shard worker really produces split keys, each message under the SQS limit', async () => {
    const { alert, ids, sendQueue, lines } = await oversize();
    const batches = sendQueue.batches('urgent');
    expect(lines.some((l) => l.msg === 'splitting oversize batch')).toBe(true);
    expect(batches.length).toBeGreaterThanOrEqual(4);
    const base = `${alert.id}:${alert.relaySeverity}:${CELL}:0`;
    for (const b of batches) expect(b.batchKey).toMatch(new RegExp(`^${base}:s[01]+$`));
    expect(new Set(batches.map((b) => b.batchKey)).size).toBe(batches.length);
    expect(batches.reduce((n, b) => n + b.recipients.length, 0)).toBe(ids.length);
    for (const m of sendQueue.messages.urgent) {
      expect(Buffer.byteLength(m)).toBeLessThanOrEqual(MAX_MESSAGE_BYTES);
    }
  });

  it('the sender accepts them: each split batch is its own claim, sent once, and a replay sends nothing', async () => {
    const { alert, ids, sendQueue } = await oversize();
    const sender = makeDeps();
    await sender.alerts.save(alert);
    const run = async () => {
      const sqs = new FakeSqs(60_000);
      for (const body of sendQueue.messages.urgent) sqs.add(URGENT, body);
      await consume(sqs, sender.deps);
      return sqs;
    };
    await run();
    const tokens = sender.push.calls.flat().map((m) => m.data['checkInToken']);
    expect(tokens).toHaveLength(ids.length);
    expect(new Set(tokens).size).toBe(ids.length);

    const before = sender.push.calls.length;
    const sqs = await run(); // the same messages again (an SQS redelivery or a re-plan)
    expect(sender.push.calls.length).toBe(before);
    expect(sqs.size(URGENT)).toBe(0); // all acked as duplicates
  });

  it('a retry of a split batch is `<key>:r1`, and baseBatchKey strips only the retry suffix', async () => {
    const { alert, sendQueue } = await oversize();
    const [one] = sendQueue.batches('urgent') as [SendBatch];
    expect(baseBatchKey(`${one.batchKey}:r2`)).toBe(one.batchKey);
    expect(baseBatchKey(one.batchKey)).toBe(one.batchKey);

    const failing = new FakePush((m) =>
      m.data['checkInToken'] === one.recipients[0]?.checkInToken
        ? { ok: false, kind: 'retryable', error: 'Unavailable' }
        : { ok: true },
    );
    const sender = makeDeps({ push: failing });
    await sender.alerts.save(alert);
    const out = await processBatch(one, sender.deps);
    expect(out).toMatchObject({ ack: true, sent: one.recipients.length - 1, retried: 1 });
    const retry = sender.sendQueue.enqueued[0]?.batches[0];
    expect(retry?.batchKey).toBe(`${one.batchKey}:r1`);
    expect(retry?.attempt).toBe(1);
    expect(retry?.recipients).toHaveLength(1);
  });
});
