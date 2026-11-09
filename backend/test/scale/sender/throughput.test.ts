import { describe, expect, it } from 'vitest';
import { InMemoryAlertStore } from '../../../src/adapters/memory/alert-store.js';
import { MemoryIdempotencyStore } from '../../../src/adapters/memory/idempotency-store.js';
import type {
  NativePushMessage,
  NativePushResult,
  NativePushSender,
  PushTarget,
  SendBatch,
} from '../../../src/scale/contracts.js';
import { MAX_BATCHES_PER_MESSAGE } from '../../../src/scale/contracts.js';
import { SenderConsumer } from '../../../src/scale/sender/consumer.js';
import { processBatch } from '../../../src/scale/sender/sender.js';
import { signed, verifyAlert } from '../../delivery/fakes.js';
import { FakeSendQueue, FakeSqs, silent, waitFor } from './fakes.js';

const TOTAL = 200_000;
const BATCH_SIZE = 300;
const URGENT = 'urgent-q';
const STANDARD = 'standard-q';

/** In-memory APNs/FCM stand-in: 5 ms per call; 1% fail once (retryable), 0.5% are dead. */
class SimulatedPush implements NativePushSender {
  readonly successes = new Map<string, number>();
  readonly attemptsByDevice = new Map<string, number>();
  calls = 0;
  async send(messages: NativePushMessage[]): Promise<NativePushResult[]> {
    this.calls++;
    await new Promise((r) => setTimeout(r, 5));
    return messages.map((m) => {
      const device = m.target.token; // token == device id in this simulation
      const n = (this.attemptsByDevice.get(device) ?? 0) + 1;
      this.attemptsByDevice.set(device, n);
      const i = Number(device.slice(1));
      if (i % 200 === 13) return { ok: false, kind: 'permanent', error: 'Unregistered' };
      if (i % 100 === 7 && n === 1) return { ok: false, kind: 'retryable', error: 'Unavailable' };
      this.successes.set(device, (this.successes.get(device) ?? 0) + 1);
      return { ok: true };
    });
  }
}

describe('sender throughput (200,000 recipients, fake APNs/FCM)', () => {
  it('delivers every device exactly once, clears dead targets, and reports the rate', async () => {
    const alerts = new InMemoryAlertStore();
    const urgentAlert = signed({ id: '22222222-2222-4222-8222-222222222222' });
    const standardAlert = signed({
      id: '33333333-3333-4333-8333-333333333333',
      relaySeverity: 'watch',
    });
    await alerts.save(urgentAlert);
    await alerts.save(standardAlert);

    const sqs = new FakeSqs(60_000);
    const sendQueue = new FakeSendQueue();
    sendQueue.onEnqueue = (queue, batches) => {
      for (let i = 0; i < batches.length; i += MAX_BATCHES_PER_MESSAGE) {
        sqs.add(
          queue === 'urgent' ? URGENT : STANDARD,
          batches.slice(i, i + MAX_BATCHES_PER_MESSAGE),
        );
      }
    };

    // 75% urgent (warning), 25% standard (watch).
    const batches: SendBatch[] = [];
    for (let start = 0, b = 0; start < TOTAL; start += BATCH_SIZE, b++) {
      const urgent = start < TOTAL * 0.75;
      const alert = urgent ? urgentAlert : standardAlert;
      batches.push({
        alertId: alert.id,
        sourceId: alert.sourceId,
        relaySeverity: alert.relaySeverity,
        batchKey: `${alert.id}:${alert.relaySeverity}:cell${b}:${b % 64}`,
        attempt: 0,
        recipients: Array.from({ length: Math.min(BATCH_SIZE, TOTAL - start) }, (_, k) => {
          const id = `d${start + k}`;
          return {
            deviceId: id,
            target: { platform: (start + k) % 2 ? 'android' : 'ios', token: id } as PushTarget,
            language: 'en' as const,
            checkInToken: `ct${id}`,
          };
        }),
      });
    }
    for (const urgent of [true, false]) {
      const mine = batches.filter((b) => (b.relaySeverity === 'warning') === urgent);
      for (let i = 0; i < mine.length; i += MAX_BATCHES_PER_MESSAGE) {
        sqs.add(urgent ? URGENT : STANDARD, mine.slice(i, i + MAX_BATCHES_PER_MESSAGE));
      }
    }

    const push = new SimulatedPush();
    const dead = new Map<string, number>();
    const clock = { now: () => new Date('2026-11-10T10:05:00Z') };
    const idempotency = new MemoryIdempotencyStore(clock);
    const deps = {
      idempotency,
      alerts,
      verifyAlert,
      isDeliveryPaused: async () => false,
      push,
      sendQueue,
      onDeadTarget: async (deviceId: string) =>
        void dead.set(deviceId, (dead.get(deviceId) ?? 0) + 1),
      clock,
      logger: silent,
      sleep: async () => {},
    };
    const totals = { sent: 0, permanent: 0, retried: 0, gaveUp: 0 };
    const consumer = new SenderConsumer({
      sqs: sqs as never,
      urgentQueueUrl: URGENT,
      standardQueueUrl: STANDARD,
      logger: silent,
      writeMetric: () => {},
      handle: async (b) => {
        const out = await processBatch(b, deps);
        totals.sent += out.sent;
        totals.permanent += out.permanentFailed;
        totals.retried += out.retried;
        totals.gaveUp += out.gaveUp;
        return out;
      },
    });

    const t0 = performance.now();
    const run = consumer.run();
    await waitFor(() => sqs.size(URGENT) === 0 && sqs.size(STANDARD) === 0, 90_000);
    consumer.stop();
    await run;
    const seconds = (performance.now() - t0) / 1000;

    const permanentCount = Array.from({ length: TOTAL }, (_, i) => i).filter(
      (i) => i % 200 === 13,
    ).length;
    const retryableCount = Array.from({ length: TOTAL }, (_, i) => i).filter(
      (i) => i % 100 === 7,
    ).length;
    expect(permanentCount).toBe(TOTAL * 0.005);
    expect(retryableCount).toBe(TOTAL * 0.01);

    // Exactly once per device (dead devices get none and are cleared exactly once).
    let wrong = 0;
    for (let i = 0; i < TOTAL; i++) {
      const want = i % 200 === 13 ? 0 : 1;
      if ((push.successes.get(`d${i}`) ?? 0) !== want) wrong++;
    }
    expect(wrong).toBe(0);
    expect(totals.sent).toBe(TOTAL - permanentCount);
    expect(totals.retried).toBe(retryableCount);
    expect(totals.gaveUp).toBe(0);
    expect(dead.size).toBe(permanentCount);
    expect([...dead.values()].every((n) => n === 1)).toBe(true);
    // Retry batches really went through the queues.
    expect(sendQueue.enqueued.length).toBeGreaterThan(0);

    const rate = Math.round(TOTAL / seconds);
    console.log(
      JSON.stringify({
        test: 'sender-throughput',
        recipients: TOTAL,
        seconds: Number(seconds.toFixed(2)),
        recipientsPerSecond: rate,
        pushCalls: push.calls,
      }),
    );
    expect(rate).toBeGreaterThan(1000);
  }, 120_000);
});
