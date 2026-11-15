import { PushDataSchema } from '@relay/shared';
import { describe, expect, it } from 'vitest';
import { fitPayload, PAYLOAD_LIMITS } from '../../../src/adapters/push/payload.js';
import type { NativePushMessage } from '../../../src/scale/contracts.js';
import { publicAlertDocKey } from '../../../src/scale/publish/paths.js';
import { buildMessages, processBatch } from '../../../src/scale/sender/sender.js';
import { signed } from '../../delivery/fakes.js';
import { makeBatch, makeDeps, seedAlert } from './fakes.js';

const BASE = 'https://d1234567890abc.cloudfront.net';

describe('push data carries the CDN alert URL', () => {
  it('every message of a sent batch has data.alertUrl = base + the alert version key', async () => {
    const t = makeDeps({ publicAlertBaseUrl: `${BASE}/` });
    const alert = await seedAlert(t.alerts);
    await processBatch(makeBatch(), t.deps);
    const messages = t.push.calls.flat();
    expect(messages).toHaveLength(3);
    for (const m of messages) {
      expect(m.data['alertUrl']).toBe(`${BASE}/${publicAlertDocKey(alert)}`);
      expect(PushDataSchema.safeParse(m.data).success).toBe(true);
    }
  });

  it('no base configured: no alertUrl at all', async () => {
    const t = makeDeps();
    await seedAlert(t.alerts);
    await processBatch(makeBatch(), t.deps);
    for (const m of t.push.calls.flat()) expect(m.data).not.toHaveProperty('alertUrl');
  });

  it('the URL changes when the alert version (sourceId) changes', () => {
    const batch = makeBatch();
    const now = new Date('2026-11-10T10:05:00Z');
    const v1 = buildMessages(batch, signed({ sourceId: 'urn:oid:v1' }), now, BASE);
    const v2 = buildMessages(batch, signed({ sourceId: 'urn:oid:v2' }), now, BASE);
    expect(v1[0]?.data['alertUrl']).toBeDefined();
    expect(v1[0]?.data['alertUrl']).not.toBe(v2[0]?.data['alertUrl']);
  });

  /** The APNs wire body, built exactly as ApnsSender.buildRequest does. */
  const apnsBody = (m: NativePushMessage): string | null =>
    fitPayload(
      m,
      (x) =>
        JSON.stringify({
          ...x.data,
          aps: {
            alert: { title: x.title, body: x.body },
            category: x.category,
            'thread-id': x.collapseKey,
            sound: 'default',
            'interruption-level': 'time-sensitive',
          },
        }),
      PAYLOAD_LIMITS.apnsBytes,
    );

  const longUrlBase = `https://${'a'.repeat(60)}.cloudfront.net`;
  const withRecipient = (text: string) => {
    const alert = signed({
      event: text.slice(0, 80),
      official: {
        headline: text,
        description: text,
        instruction: text,
        senderName: 'NWS Los Angeles/Oxnard CA',
        language: 'en',
      },
    });
    const batch = makeBatch({}, 1);
    const first = batch.recipients[0];
    if (!first) throw new Error('no recipient');
    batch.recipients[0] = { ...first, language: 'en', checkInToken: 'x'.repeat(400) };
    const [m] = buildMessages(batch, alert, new Date('2026-11-10T10:05:00Z'), longUrlBase);
    if (!m) throw new Error('no message');
    return m;
  };

  it('worst-case ASCII text with a long base URL stays under 4 KB and keeps alertUrl', () => {
    const body = apnsBody(withRecipient('Flash flood "danger" now. '.repeat(100)));
    expect(body).not.toBeNull();
    expect(Buffer.byteLength(body ?? '')).toBeLessThanOrEqual(4096);
    expect(JSON.parse(body ?? '{}')).toHaveProperty('alertUrl');
  });

  it('worst-case 4-byte characters still stay under 4 KB (optional data is trimmed first)', () => {
    const body = apnsBody(withRecipient('\u{1F30A}'.repeat(2000)));
    expect(body).not.toBeNull();
    expect(Buffer.byteLength(body ?? '')).toBeLessThanOrEqual(4096);
    // The required keys always survive, whether or not alertUrl had to go.
    expect(JSON.parse(body ?? '{}')).toMatchObject({ alertId: expect.any(String) });
    expect(JSON.parse(body ?? '{}')).toHaveProperty('checkInToken');
  });
});
