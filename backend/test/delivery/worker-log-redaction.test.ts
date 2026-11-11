import { describe, expect, it } from 'vitest';
import type { SQSEvent } from 'aws-lambda';
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../src/adapters/memory/alert-store.js';
import { MemoryIdempotencyStore } from '../../src/adapters/memory/idempotency-store.js';
import { MemoryPushSender } from '../../src/adapters/memory/push-sender.js';
import { deliveryWorker, type WorkerDeps } from '../../src/delivery/index.js';
import type { DeliveryJob } from '../../src/ports.js';
import { signed, verifyAlert } from './fakes.js';

// SI-11: the worker logs errors by class and code only. Every failure path below throws an
// error whose message quotes a push token, a device id and row text, and none of it may reach
// the log (review-2026-11.md A1).
const TOKEN = 'ExponentPushToken[leak-me-abc123]';
const DEVICE = 'device-secret-7f3a';
const ROW_TEXT = 'row text: lat 37.33 lon -121.88';

class LeakyError extends Error {
  code = 'ThrottlingException';
  constructor() {
    super(`boom for ${TOKEN} on ${DEVICE}: ${ROW_TEXT}`);
    this.name = 'LeakyError';
  }
}

const clock = { now: () => new Date('2026-11-10T10:05:00Z') };

function setup() {
  const lines: Array<{ level: string; msg: string; fields?: Record<string, unknown> }> = [];
  const logger = {
    info: (msg: string, fields?: Record<string, unknown>) =>
      void lines.push({ level: 'info', msg, ...(fields ? { fields } : {}) }),
    warn: (msg: string, fields?: Record<string, unknown>) =>
      void lines.push({ level: 'warn', msg, ...(fields ? { fields } : {}) }),
    error: (msg: string, fields?: Record<string, unknown>) =>
      void lines.push({ level: 'error', msg, ...(fields ? { fields } : {}) }),
  };
  const alerts = new InMemoryAlertStore();
  const sender = new MemoryPushSender();
  const idempotency = new MemoryIdempotencyStore(clock);
  const deps: WorkerDeps = {
    sender,
    alerts,
    idempotency,
    verifyAlert,
    isDeliveryPaused: async () => false,
    onDeadToken: async () => undefined,
    clock,
    logger,
  };
  const errors = () => lines.filter((l) => l.level === 'error');
  return { alerts, sender, idempotency, deps, lines, errors };
}

const job = (alert: Alert): DeliveryJob => ({
  alertId: alert.id,
  deviceId: DEVICE,
  expoPushToken: TOKEN,
  language: 'en',
  checkInToken: 'ct0',
  expiresAt: alert.expires,
  groupingKey: alert.id,
});
const event = (j: DeliveryJob): SQSEvent =>
  ({ Records: [{ messageId: 'm0', body: JSON.stringify(j) }] }) as unknown as SQSEvent;

/**
 * No line carries the token or the error text, and no error line carries the device id (the
 * idempotency key embeds it). send-jobs' own 'push failed' warning names the device on purpose.
 */
function expectClean(lines: Array<{ level: string }>) {
  const text = JSON.stringify(lines);
  expect(text).not.toContain('leak-me');
  expect(text).not.toContain('PushToken');
  expect(text).not.toContain('row text');
  expect(text).not.toContain('boom');
  expect(JSON.stringify(lines.filter((l) => l.level === 'error'))).not.toContain(DEVICE);
}

describe('deliveryWorker error logging (SI-11)', () => {
  it('reading an alert fails: logs class and code only', async () => {
    const t = setup();
    const alert = signed();
    t.deps.alerts = {
      get: async () => {
        throw new LeakyError();
      },
    };
    await deliveryWorker(event(job(alert)), t.deps);
    expect(t.errors()).toEqual([
      {
        level: 'error',
        msg: 'reading alert failed',
        fields: { alertId: alert.id, error: 'LeakyError', code: 'ThrottlingException' },
      },
    ]);
    expectClean(t.lines);
  });

  it('the sender throws, then releasing the claim fails: no message text, no key', async () => {
    const t = setup();
    const alert = signed();
    await t.alerts.save(alert);
    t.deps.sender = {
      send: async () => {
        throw new LeakyError();
      },
    };
    t.idempotency.release = async () => {
      throw new LeakyError();
    };
    const r = await deliveryWorker(event(job(alert)), t.deps);
    expect(r.batchItemFailures).toEqual([{ itemIdentifier: 'm0' }]);
    expect(t.errors().map((l) => l.msg)).toEqual(['send threw', 'releasing claim failed']);
    expect(t.errors()[0]?.fields).toEqual({ error: 'LeakyError', code: 'ThrottlingException' });
    expect(t.errors()[1]?.fields).toEqual({
      alertId: alert.id,
      error: 'LeakyError',
      code: 'ThrottlingException',
    });
    expectClean(t.lines);
  });

  it('marking sent fails after a send: no message text, no key', async () => {
    const t = setup();
    const alert = signed();
    await t.alerts.save(alert);
    t.idempotency.markSent = async () => {
      throw new LeakyError();
    };
    const r = await deliveryWorker(event(job(alert)), t.deps);
    expect(r.batchItemFailures).toEqual([]);
    expect(t.errors()).toEqual([
      {
        level: 'error',
        msg: 'marking sent failed',
        fields: { alertId: alert.id, error: 'LeakyError', code: 'ThrottlingException' },
      },
    ]);
    expectClean(t.lines);
  });

  it('clearing a dead token fails: no token, no message text', async () => {
    const t = setup();
    const alert = signed();
    await t.alerts.save(alert);
    t.sender.failTokens.set(TOKEN, 'DeviceNotRegistered');
    t.deps.onDeadToken = async () => {
      throw new LeakyError();
    };
    await deliveryWorker(event(job(alert)), t.deps);
    expect(t.errors().map((l) => l.msg)).toContain('clearing dead token failed');
    expect(t.errors().find((l) => l.msg === 'clearing dead token failed')?.fields).toEqual({
      alertId: alert.id,
      error: 'LeakyError',
      code: 'ThrottlingException',
    });
    expectClean(t.lines);
  });

  it('a non-Error throw is logged as a generic class with code unknown', async () => {
    const t = setup();
    const alert = signed();
    await t.alerts.save(alert);
    t.deps.sender = {
      send: async () => {
        throw `string throw with ${TOKEN}`;
      },
    };
    await deliveryWorker(event(job(alert)), t.deps);
    expect(t.errors()[0]).toEqual({
      level: 'error',
      msg: 'send threw',
      fields: { error: 'Error', code: 'unknown' },
    });
    expectClean(t.lines);
  });
});
