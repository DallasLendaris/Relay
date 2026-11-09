import { describe, expect, it } from 'vitest';
import { SenderConsumer, parseBatches } from '../../../src/scale/sender/consumer.js';
import type { BatchOutcome } from '../../../src/scale/sender/sender.js';
import { FakeSqs, makeBatch, silent, waitFor } from './fakes.js';

const URGENT = 'urgent-q';
const STANDARD = 'standard-q';
const ok = (over: Partial<BatchOutcome> = {}): BatchOutcome => ({
  ack: true,
  reason: 'sent',
  sent: 1,
  permanentFailed: 0,
  retried: 0,
  gaveUp: 0,
  ...over,
});
const msg = (key: string, n = 1) =>
  Array.from({ length: n }, (_, i) => makeBatch({ batchKey: `${key}:${i}` }, 1));

function make(
  sqs: FakeSqs,
  handle: (b: ReturnType<typeof makeBatch>) => Promise<BatchOutcome>,
  o = {},
) {
  const metrics: string[] = [];
  const consumer = new SenderConsumer({
    sqs: sqs as never,
    urgentQueueUrl: URGENT,
    standardQueueUrl: STANDARD,
    handle,
    logger: silent,
    writeMetric: (l) => metrics.push(l),
    ...o,
  });
  return { consumer, metrics };
}

describe('SenderConsumer', () => {
  it('parses an array body, tolerates one object, rejects junk', () => {
    expect(parseBatches(JSON.stringify(msg('a', 2)))).toHaveLength(2);
    expect(parseBatches(JSON.stringify(makeBatch()))).toHaveLength(1);
    expect(parseBatches('nope')).toBeNull();
    expect(parseBatches('[]')).toBeNull();
    expect(parseBatches('[{"x":1}]')).toBeNull();
  });

  it('strict priority: standard is polled only when urgent returned nothing', async () => {
    const sqs = new FakeSqs(10_000);
    for (let i = 0; i < 3; i++) sqs.add(URGENT, msg(`u${i}`));
    for (let i = 0; i < 3; i++) sqs.add(STANDARD, msg(`s${i}`));
    const order: string[] = [];
    const { consumer } = make(sqs, async (b) => (order.push(b.batchKey), ok()), { concurrency: 1 });
    const run = consumer.run();
    await waitFor(() => order.length === 6);
    consumer.stop();
    await run;
    expect(order.map((k) => k[0])).toEqual(['u', 'u', 'u', 's', 's', 's']);
    // Every standard receive was preceded by an urgent receive that came back empty.
    sqs.receives.forEach((r, i) => {
      if (r.queue === STANDARD) expect(sqs.receives[i - 1]?.queue).toBe(URGENT);
    });
    // urgent is polled with MaxNumberOfMessages <= 10
    expect(Math.max(...sqs.receives.map((r) => r.max))).toBeLessThanOrEqual(10);
  });

  it('urgent arriving during a standard backlog is served before the next standard message', async () => {
    const sqs = new FakeSqs(10_000);
    for (let i = 0; i < 5; i++) sqs.add(STANDARD, msg(`s${i}`));
    const order: string[] = [];
    const { consumer } = make(
      sqs,
      async (b) => {
        order.push(b.batchKey);
        if (b.batchKey === 's1:0') sqs.add(URGENT, msg('u0'));
        return ok();
      },
      { concurrency: 1 },
    );
    const run = consumer.run();
    await waitFor(() => order.length === 6);
    consumer.stop();
    await run;
    expect(order.indexOf('u0:0')).toBe(2);
  });

  it('never has more than C messages in flight', async () => {
    const sqs = new FakeSqs(10_000);
    for (let i = 0; i < 40; i++) sqs.add(URGENT, msg(`u${i}`));
    let active = 0;
    let peak = 0;
    let done = 0;
    const { consumer } = make(
      sqs,
      async () => {
        active++;
        peak = Math.max(peak, active);
        await new Promise((r) => setTimeout(r, 10));
        active--;
        done++;
        return ok();
      },
      { concurrency: 7 },
    );
    const run = consumer.run();
    await waitFor(() => done === 40);
    consumer.stop();
    await run;
    expect(peak).toBe(7);
    expect(sqs.size(URGENT)).toBe(0);
  });

  it("runs a message's batches concurrently and deletes it only if all are acked", async () => {
    const sqs = new FakeSqs(10_000);
    sqs.add(URGENT, msg('good', 10));
    sqs.add(URGENT, msg('bad', 3));
    let active = 0;
    let peak = 0;
    const handled: string[] = [];
    const { consumer } = make(sqs, async (b) => {
      active++;
      peak = Math.max(peak, active);
      await new Promise((r) => setTimeout(r, 5));
      active--;
      handled.push(b.batchKey);
      return b.batchKey === 'bad:1' ? ok({ ack: false, reason: 'leased' }) : ok();
    });
    const run = consumer.run();
    await waitFor(() => handled.length === 13);
    consumer.stop();
    await run;
    expect(peak).toBeGreaterThanOrEqual(10);
    expect(sqs.size(URGENT)).toBe(1); // the message with a failed batch stays
    expect(sqs.deleteCalls.reduce((a, b) => a + b, 0)).toBe(1);
  });

  it('a throwing batch leaves its message on the queue and it is redelivered', async () => {
    const sqs = new FakeSqs(20);
    sqs.add(URGENT, msg('flaky'));
    let calls = 0;
    const { consumer } = make(sqs, async () => {
      if (++calls === 1) throw new Error('boom');
      return ok();
    });
    const run = consumer.run();
    await waitFor(() => sqs.size(URGENT) === 0);
    consumer.stop();
    await run;
    expect(calls).toBe(2);
  });

  it('partial DeleteMessageBatch failure is tolerated; deletes are batched by 10', async () => {
    const sqs = new FakeSqs(10_000);
    for (let i = 0; i < 25; i++) sqs.add(URGENT, msg(`u${i}`));
    sqs.failDeletes.add('m3#1');
    let done = 0;
    const { consumer } = make(sqs, async () => (done++, ok()));
    const run = consumer.run();
    await waitFor(() => done === 25);
    consumer.stop();
    await run;
    expect(sqs.deleteCalls.every((n) => n <= 10)).toBe(true);
    expect(sqs.deleteCalls.reduce((a, b) => a + b, 0)).toBe(25);
    expect(sqs.size(URGENT)).toBe(1); // only the one whose delete failed
  });

  it('extends visibility for a message still running, and not for a quick one', async () => {
    const sqs = new FakeSqs(10_000);
    sqs.add(URGENT, msg('slow'));
    let release: () => void = () => {};
    const gate = new Promise<void>((r) => (release = r));
    const { consumer } = make(
      sqs,
      async () => {
        await gate;
        return ok();
      },
      { extendAfterMs: 15, extendToSeconds: 60 },
    );
    const run = consumer.run();
    await waitFor(() => sqs.extensions.length >= 2);
    expect(sqs.extensions[0]).toEqual({ queue: URGENT, seconds: 60 });
    release();
    await waitFor(() => sqs.size(URGENT) === 0);
    const n = sqs.extensions.length;
    await new Promise((r) => setTimeout(r, 40));
    expect(sqs.extensions.length).toBe(n); // heartbeat stops with the message
    consumer.stop();
    await run;

    const quick = new FakeSqs(10_000);
    quick.add(URGENT, msg('quick'));
    const c2 = make(quick, async () => ok(), { extendAfterMs: 1000 });
    const r2 = c2.consumer.run();
    await waitFor(() => quick.size(URGENT) === 0);
    c2.consumer.stop();
    await r2;
    expect(quick.extensions).toHaveLength(0);
  });

  it('graceful shutdown: stops receiving, finishes in-flight work, deletes, returns', async () => {
    const sqs = new FakeSqs(10_000);
    sqs.add(URGENT, msg('a'));
    sqs.add(URGENT, msg('b'));
    sqs.add(URGENT, msg('late'));
    const finished: string[] = [];
    let started = 0;
    const { consumer } = make(
      sqs,
      async (b) => {
        started++;
        await new Promise((r) => setTimeout(r, 60));
        finished.push(b.batchKey);
        return ok();
      },
      { concurrency: 2 },
    );
    const run = consumer.run();
    await waitFor(() => started === 2);
    consumer.stop();
    const receivesAtStop = sqs.receives.length;
    await run;
    expect(finished.sort()).toEqual(['a:0', 'b:0']); // in-flight work completed
    expect(sqs.size(URGENT)).toBe(1); // and was deleted
    expect(sqs.visible(URGENT)).toBe(1); // 'late' was never received
    expect(sqs.receives.length).toBeLessThanOrEqual(receivesAtStop + 1);
    expect(consumer.inFlightCount).toBe(0);
  });

  it('shutdown gives up on work that outlasts the drain timeout', async () => {
    const sqs = new FakeSqs(10_000);
    sqs.add(URGENT, msg('stuck'));
    let started = false;
    const { consumer } = make(
      sqs,
      async () => {
        started = true;
        await new Promise(() => {}); // never finishes
        return ok();
      },
      { drainTimeoutMs: 30 },
    );
    const run = consumer.run();
    await waitFor(() => started);
    const t0 = Date.now();
    consumer.stop();
    await run;
    expect(Date.now() - t0).toBeLessThan(1000);
    expect(sqs.size(URGENT)).toBe(1); // not deleted: SQS redelivers it
  });

  it('emits EMF metrics in Relay/Sender including LagSeconds', async () => {
    const sqs = new FakeSqs(10_000);
    sqs.add(URGENT, msg('u'), Date.now() - 4000);
    const { consumer, metrics } = make(sqs, async () =>
      ok({ sent: 7, permanentFailed: 2, retried: 3, gaveUp: 1 }),
    );
    const run = consumer.run();
    await waitFor(() => sqs.size(URGENT) === 0);
    consumer.stop();
    await run;
    const line = JSON.parse(metrics[0] as string);
    expect(line._aws.CloudWatchMetrics[0].Namespace).toBe('Relay/Sender');
    expect(line._aws.CloudWatchMetrics[0].Metrics.map((m: { Name: string }) => m.Name)).toEqual([
      'Sent',
      'PermanentFailed',
      'Retried',
      'GaveUp',
      'BatchesProcessed',
      'LagSeconds',
    ]);
    expect(line).toMatchObject({
      Queue: 'urgent',
      Sent: 7,
      PermanentFailed: 2,
      Retried: 3,
      GaveUp: 1,
      BatchesProcessed: 1,
    });
    expect(line.LagSeconds[0]).toBeGreaterThanOrEqual(4);
  });
});
