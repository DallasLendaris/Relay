import { describe, expect, it } from 'vitest';
import type { SQSEvent } from 'aws-lambda';
import { deliveryWorker, sendJobs } from '../../src/delivery/index.js';
import { MemoryPushSender } from '../../src/adapters/memory/push-sender.js';
import type { DeliveryJob } from '../../src/ports.js';
import { makeAlert } from './fakes.js';

const logger = { info() {}, warn() {}, error() {} };
const alert = makeAlert();
const alerts = { get: async (id: string) => (id === alert.id ? alert : null) };
const job = (n: number): DeliveryJob => ({
  alertId: alert.id,
  deviceId: `d${n}`,
  expoPushToken: `tok${n}`,
  language: 'en',
  checkInToken: `ct${n}`,
  expiresAt: alert.expires,
  groupingKey: alert.id,
});
const event = (jobs: DeliveryJob[]): SQSEvent =>
  ({
    Records: jobs.map((j, i) => ({ messageId: `m${i}`, body: JSON.stringify(j) })),
  }) as unknown as SQSEvent;

describe('sendJobs', () => {
  it('builds title, body, data and category', async () => {
    const sender = new MemoryPushSender();
    const res = await sendJobs([job(1)], { sender, alerts, logger });
    expect(res).toEqual([{ ok: true }]);
    expect(sender.sent[0]).toEqual({
      to: 'tok1',
      title: 'Warning: Flash Flood Warning',
      body: 'Flash Flood Warning until 6 PM',
      data: {
        alertId: alert.id,
        checkInToken: 'ct1',
        expiresAt: alert.expires,
        groupingKey: alert.id,
      },
      categoryId: 'alert-checkin',
    });
  });

  it('falls back to the first 140 chars of the description', async () => {
    const long = makeAlert({
      official: { ...alert.official, headline: null, description: 'x'.repeat(300) },
    });
    const sender = new MemoryPushSender();
    await sendJobs([job(1)], { sender, alerts: { get: async () => long }, logger });
    expect(sender.sent[0]?.body).toHaveLength(140);
  });

  it('fails jobs whose alert is missing', async () => {
    const sender = new MemoryPushSender();
    const res = await sendJobs([{ ...job(1), alertId: 'nope' }], { sender, alerts, logger });
    expect(res).toEqual([{ ok: false, error: 'AlertNotFound' }]);
  });
});

describe('deliveryWorker', () => {
  it('reports only failed records, treating dead tokens as final', async () => {
    const sender = new MemoryPushSender();
    sender.failTokens.set('tok1', 'MessageRateExceeded'); // retryable
    sender.failTokens.set('tok2', 'DeviceNotRegistered'); // permanent
    const res = await deliveryWorker(event([job(0), job(1), job(2)]), { sender, alerts, logger });
    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'm1' }]);
    expect(sender.sent.map((m) => m.to)).toEqual(['tok0']);
  });

  it('fails unparseable records', async () => {
    const ev = { Records: [{ messageId: 'bad', body: '{nope' }] } as unknown as SQSEvent;
    const res = await deliveryWorker(ev, { sender: new MemoryPushSender(), alerts, logger });
    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'bad' }]);
  });
});
