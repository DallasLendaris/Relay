import { describe, expect, it } from 'vitest';
import { sendJobs } from '../../src/delivery/index.js';
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
const spanishAlert = makeAlert({
  summaries: [
    { language: 'en', whatToDo: 'Go up.', text: 'English summary.' },
    { language: 'es', whatToDo: 'Suba.', text: 'Hay un aviso de inundación repentina.' },
  ],
});

describe('sendJobs', () => {
  it('uses the Spanish summary and severity word for a Spanish job', async () => {
    const sender = new MemoryPushSender();
    await sendJobs([{ ...job(1), language: 'es' }], {
      sender,
      alerts: { get: async () => spanishAlert },
      logger,
    });
    expect(sender.sent[0]).toMatchObject({
      title: 'Aviso: Flash Flood Warning',
      body: 'Hay un aviso de inundación repentina.',
    });
  });

  it('falls back to English when a Spanish job has no Spanish summary', async () => {
    const sender = new MemoryPushSender();
    await sendJobs([{ ...job(1), language: 'es' }], { sender, alerts, logger });
    expect(sender.sent[0]).toMatchObject({
      title: 'Warning: Flash Flood Warning',
      body: 'Flash Flood Warning until 6 PM',
    });
  });

  it('keeps English for an English job even when a Spanish summary exists', async () => {
    const sender = new MemoryPushSender();
    await sendJobs([job(1)], { sender, alerts: { get: async () => spanishAlert }, logger });
    expect(sender.sent[0]?.title).toBe('Warning: Flash Flood Warning');
  });

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
