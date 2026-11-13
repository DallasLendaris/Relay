import type { SQSEvent } from 'aws-lambda';
import { describe, expect, it } from 'vitest';
import { deliveryWorker, type WorkerDeps } from '../../src/delivery/index.js';
import { errorFields, redactPushTokens } from '../../src/delivery/redact.js';
import { MemoryIdempotencyStore } from '../../src/adapters/memory/idempotency-store.js';
import { MemoryPushSender } from '../../src/adapters/memory/push-sender.js';
import { runRecords } from '../../src/scale/fanout/handlers.js';
import { processBatch } from '../../src/scale/sender/sender.js';
import { InMemoryAlertStore } from '../../src/adapters/memory/alert-store.js';
import { signed, verifyAlert } from '../delivery/fakes.js';
import { makeBatch, makeDeps, recordingLogger, seedAlert } from '../scale/sender/fakes.js';

/** A string no log line may ever contain: it looks like what database and SDK errors quote. */
const SECRET = 'Failing row contains (37.3352, -121.8811, 1 Main St) ExponentPushToken[abc123]';

const APNS = 'a1b2c3d4'.repeat(8); // 64 hex characters
const APNS_LONG = 'F0E1D2C3'.repeat(16); // 128 hex characters (uppercase)
const FCM = `fAbCdE123456:APA91b${'x9Y_-'.repeat(30)}`;
const FCM_NEWER = `cQwErTy98765:${'Ab0_-'.repeat(40)}`;

describe('redactPushTokens', () => {
  it('still redacts Expo tokens', () => {
    expect(redactPushTokens('bad ExponentPushToken[abc] and ExpoPushToken[def]')).toBe(
      'bad [redacted-push-token] and [redacted-push-token]',
    );
  });

  it('redacts an APNs hex token of 64 and of more characters, either case', () => {
    expect(redactPushTokens(`APNs rejected ${APNS} (BadDeviceToken)`)).toBe(
      'APNs rejected [redacted-push-token] (BadDeviceToken)',
    );
    expect(redactPushTokens(`token=${APNS_LONG}`)).toBe('token=[redacted-push-token]');
    expect(redactPushTokens(APNS)).toBe('[redacted-push-token]');
  });

  it('redacts an FCM registration token (APA91b shape and the newer colon shape)', () => {
    const out = redactPushTokens(`fcm said UNREGISTERED for ${FCM} and ${FCM_NEWER}.`);
    expect(out).toBe('fcm said UNREGISTERED for [redacted-push-token] and [redacted-push-token].');
    expect(out).not.toContain('APA91b');
  });

  it('leaves ordinary text, short hex, uuids and short ids alone', () => {
    for (const text of [
      'send failed: 503 Service Unavailable',
      'commit deadbeef1234', // short hex
      '11111111-1111-4111-8111-111111111111', // a UUID is not 64 hex characters
      'alert 9f2c:ok', // short id with a colon
      'MessageRateExceeded',
    ]) {
      expect(redactPushTokens(text)).toBe(text);
    }
  });

  it('redacts several different tokens in one string', () => {
    const out = redactPushTokens(`${APNS} / ${FCM} / ExponentPushToken[z]`);
    expect(out).toBe('[redacted-push-token] / [redacted-push-token] / [redacted-push-token]');
  });
});

describe('errorFields', () => {
  it('is the class name plus a stable code, never the message', () => {
    class ThrottlingException extends Error {
      override name = 'ThrottlingException';
      code = 'ThrottlingException';
    }
    expect(errorFields(new ThrottlingException(SECRET))).toEqual({
      error: 'ThrottlingException',
      code: 'ThrottlingException',
    });
    expect(errorFields(new Error(SECRET))).toEqual({ error: 'Error', code: 'unknown' });
    expect(errorFields(Object.assign(new Error(SECRET), { code: 'ECONNRESET' }))).toEqual({
      error: 'Error',
      code: 'ECONNRESET',
    });
    expect(JSON.stringify(errorFields(new Error(SECRET)))).not.toContain('Failing row');
  });

  it('survives non-errors and a code that is not a short plain string', () => {
    expect(errorFields(SECRET)).toEqual({ error: 'Error', code: 'unknown' });
    expect(errorFields(undefined)).toEqual({ error: 'Error', code: 'unknown' });
    expect(errorFields(Object.assign(new Error('x'), { code: SECRET }))).toEqual({
      error: 'Error',
      code: 'unknown',
    });
    expect(errorFields(Object.assign(new Error('x'), { name: SECRET })).error).toBe('Error');
  });
});

describe('log lines carry no error message (PRIV-10)', () => {
  it('sender.mark_sent_failed logs the error name and code only', async () => {
    const t = makeDeps();
    const rec = recordingLogger();
    t.deps.logger = rec.logger;
    await seedAlert(t.alerts);
    t.idempotency.markSent = async () => {
      throw new Error(SECRET);
    };
    await processBatch(makeBatch(), t.deps);
    const line = rec.lines.find((l) => l.msg === 'sender.mark_sent_failed');
    expect(line?.fields).toMatchObject({ error: 'Error', code: 'unknown' });
    const all = JSON.stringify(rec.lines);
    expect(all).not.toContain('Failing row');
    expect(all).not.toContain('ExponentPushToken');
    expect(all).not.toContain('37.3352');
  });

  it('sender.release_failed logs the error name and code only', async () => {
    const t = makeDeps();
    const rec = recordingLogger();
    t.deps.logger = rec.logger;
    await seedAlert(t.alerts);
    t.idempotency.release = async () => {
      throw new Error(SECRET);
    };
    // The alert is missing, so the batch fails after claiming and releases its claim.
    await processBatch(makeBatch({ alertId: 'unknown-alert' }), t.deps);
    const all = JSON.stringify(rec.lines);
    expect(all).not.toContain('Failing row');
    expect(rec.lines.some((l) => l.msg === 'sender.release_failed')).toBe(true);
  });

  it('fan-out record failures log the error name and code only', async () => {
    const lines: Array<Record<string, unknown>> = [];
    const log = {
      info() {},
      warn() {},
      error: (msg: string, f?: Record<string, unknown>) => void lines.push({ msg, ...f }),
    };
    const event = { Records: [{ messageId: 'm1', body: '{}' }] } as unknown as SQSEvent;
    const res = await runRecords(
      event,
      (b): b is object => typeof b === 'object',
      async () => {
        throw new Error(SECRET);
      },
      log,
    );
    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'm1' }]);
    expect(lines).toEqual([
      { msg: 'fan-out record failed', messageId: 'm1', error: 'Error', code: 'unknown' },
    ]);
  });
});

describe('delivery worker logs carry no device id', () => {
  const NOW = new Date('2026-11-10T10:05:00Z');
  function harness() {
    const clock = { now: () => NOW };
    const alerts = new InMemoryAlertStore();
    const idempotency = new MemoryIdempotencyStore(clock);
    const logs: Array<{ msg: string; fields?: Record<string, unknown> }> = [];
    const deps: WorkerDeps = {
      sender: new MemoryPushSender(),
      alerts,
      idempotency,
      verifyAlert,
      isDeliveryPaused: async () => false,
      onDeadToken: async () => {},
      clock,
      logger: {
        info: (msg, fields) => void logs.push({ msg, ...(fields ? { fields } : {}) }),
        warn: (msg, fields) => void logs.push({ msg, ...(fields ? { fields } : {}) }),
        error: (msg, fields) => void logs.push({ msg, ...(fields ? { fields } : {}) }),
      },
      sleep: async () => {},
    };
    return { alerts, idempotency, deps, logs };
  }
  const DEVICE = 'device-1234-secret-id';

  async function run(h: ReturnType<typeof harness>) {
    const alert = signed();
    await h.alerts.save(alert);
    const job = {
      alertId: alert.id,
      deviceId: DEVICE,
      expoPushToken: 'ExponentPushToken[abc]',
      language: 'en' as const,
      checkInToken: 'ct',
      expiresAt: alert.expires,
      groupingKey: alert.id,
    };
    const event = {
      Records: [{ messageId: 'm0', body: JSON.stringify(job) }],
    } as unknown as SQSEvent;
    return deliveryWorker(event, h.deps);
  }

  it('a skipped duplicate logs the alert, not the device', async () => {
    const h = harness();
    await run(h); // sends and marks sent
    h.logs.length = 0;
    await run(h); // the same job again
    const dup = h.logs.find((l) => l.msg === 'duplicate skipped');
    expect(dup).toBeDefined();
    expect(JSON.stringify(h.logs)).not.toContain(DEVICE);
  });

  it('a failed claim logs the error name and code, not the key (it embeds the device id) or the message', async () => {
    const h = harness();
    h.idempotency.claim = async () => {
      throw new Error(SECRET);
    };
    await run(h);
    const failed = h.logs.find((l) => l.msg === 'claim failed');
    expect(failed?.fields).toMatchObject({ error: 'Error', code: 'unknown' });
    const all = JSON.stringify(h.logs);
    expect(all).not.toContain(DEVICE);
    expect(all).not.toContain('Failing row');
  });
});
