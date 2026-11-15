import { describe, expect, it } from 'vitest';
import {
  FOLLOW_UP_DELAY_SECONDS,
  followUpBatchKey,
  followUpCoversKeptCells,
  followUpDeviceKey,
  followUpKindFor,
  followUpPolicy,
  followUpPushText,
  followUpRefusal,
  parseFollowUpKey,
  versionTag,
} from '../../src/delivery/follow-up.js';
import { deliveryKey } from '../../src/delivery/eligibility.js';
import { bucketKey } from '../../src/scale/fanout/keys.js';
import { SENDER_LEASE_MS } from '../../src/scale/sender/sender.js';
import { signed, tampered, verifyAlert } from './fakes.js';

const NOW = new Date('2026-11-10T11:00:00Z');

describe('follow-up keys', () => {
  const alert = signed({ sourceId: 'urn:oid:cancel.1', state: 'cancelled', messageType: 'Cancel' });

  it('never collide with the original key, and differ per kind and per version', () => {
    const fu = followUpBatchKey(alert, 'cancel', 'c1', 7);
    expect(fu).not.toBe(bucketKey(alert, 'c1', 7));
    expect(fu.startsWith(`${alert.id}:warning:fu.cancel.`)).toBe(true);
    expect(followUpBatchKey(alert, 'update', 'c1', 7)).not.toBe(fu);
    expect(
      followUpBatchKey({ ...alert, sourceId: 'urn:oid:cancel.2' }, 'cancel', 'c1', 7),
    ).not.toBe(fu);
    expect(followUpDeviceKey(alert, 'cancel', 'd1')).not.toBe(deliveryKey(alert, 'd1'));
  });

  it('are deterministic, so a replayed or repeated Cancel builds the same keys', () => {
    expect(followUpBatchKey(alert, 'cancel', 'c1', 7)).toBe(
      followUpBatchKey(alert, 'cancel', 'c1', 7),
    );
    expect(versionTag('urn:oid:x')).toMatch(/^[0-9a-f]{12}$/);
    expect(versionTag('urn:oid:x')).toBe(versionTag('urn:oid:x'));
  });

  it('are read back from any key the sender derives (retry, re-check, split)', () => {
    const key = followUpBatchKey(alert, 'removed', 'c1', 3);
    const want = { kind: 'removed', version: versionTag(alert.sourceId) };
    for (const k of [key, `${key}:r2`, `${key}:v1`, `${key}:h1`, `${key}:u3`, `${key}:s01`]) {
      expect(parseFollowUpKey(k), k).toEqual(want);
    }
    expect(parseFollowUpKey(bucketKey(alert, 'c1', 3))).toBeNull();
    expect(parseFollowUpKey(`${alert.id}:warning:late:abc`)).toBeNull();
  });

  it('a cancel waits out an in-flight original batch (lease plus caches)', () => {
    expect(FOLLOW_UP_DELAY_SECONDS * 1000).toBeGreaterThan(SENDER_LEASE_MS + 5_000 + 10_000);
  });
});

describe('follow-up loudness', () => {
  it('cancel and removed are quiet whatever the severity', () => {
    for (const relaySeverity of ['emergency', 'warning', 'watch'] as const) {
      expect(followUpPolicy('cancel', { relaySeverity })).toEqual({
        push: true,
        priority: 'normal',
        sound: null,
      });
      expect(followUpPolicy('removed', { relaySeverity }).sound).toBeNull();
    }
  });

  it("an update keeps the new severity's policy, and a statement update is still sent quietly", () => {
    expect(followUpPolicy('update', { relaySeverity: 'emergency' })).toMatchObject({
      priority: 'high',
      sound: 'default',
    });
    expect(followUpPolicy('update', { relaySeverity: 'watch' })).toMatchObject({
      priority: 'normal',
      sound: null,
    });
    expect(followUpPolicy('update', { relaySeverity: 'statement' })).toEqual({
      push: true,
      priority: 'normal',
      sound: null,
    });
  });

  it('kept cells get an update follow-up only when the normal fan-out does not re-notify them', () => {
    const prev = { sourceId: 'a', relaySeverity: 'warning' as const };
    expect(followUpCoversKeptCells({ relaySeverity: 'warning', supersedes: prev })).toBe(true);
    expect(followUpCoversKeptCells({ relaySeverity: 'emergency', supersedes: prev })).toBe(false);
    expect(followUpCoversKeptCells({ relaySeverity: 'statement', supersedes: prev })).toBe(true);
    expect(followUpKindFor('update', true)).toBe('update');
    expect(followUpKindFor('update', false)).toBe('removed');
    expect(followUpKindFor('cancel', false)).toBe('cancel');
  });
});

describe('follow-up text', () => {
  const alert = signed({
    official: {
      headline: 'Flash Flood Warning until 6 PM',
      description: 'x',
      instruction: null,
      senderName: 'NWS Monterey CA',
      language: 'en-US',
    },
  });

  it('says plainly what happened, in English and Spanish', () => {
    expect(followUpPushText(alert, 'cancel', 'en')).toEqual({
      title: 'Cancelled: Flash Flood Warning',
      body: 'NWS Monterey cancelled this alert. Follow any other alerts still in effect.',
    });
    expect(followUpPushText(alert, 'cancel', 'es').title).toMatch(/^Cancelado: Aviso de inundaci/);
    expect(followUpPushText(alert, 'cancel', 'es').body).toMatch(/canceló esta alerta/);
    expect(followUpPushText(alert, 'removed', 'en').body).toMatch(/no longer includes your area/);
    expect(followUpPushText(alert, 'removed', 'es').body).toMatch(/Ya no incluye su zona/);
    expect(followUpPushText(alert, 'update', 'en')).toEqual({
      title: 'Updated: Flash Flood Warning',
      body: 'NWS Monterey: Flash Flood Warning until 6 PM',
    });
    expect(followUpPushText(alert, 'update', 'es').title).toMatch(/^Actualización: /);
  });
});

describe('followUpRefusal', () => {
  const cancelled = signed({
    sourceId: 'urn:oid:c',
    state: 'cancelled',
    messageType: 'Cancel',
  });
  const tag = versionTag('urn:oid:c');

  it('passes the current version in the state its kind needs', () => {
    expect(followUpRefusal(cancelled, 'cancel', tag, NOW, verifyAlert)).toBeNull();
    const updated = signed({ sourceId: 'urn:oid:c', state: 'updated' });
    expect(followUpRefusal(updated, 'update', tag, NOW, verifyAlert)).toBeNull();
    expect(followUpRefusal(updated, 'removed', tag, NOW, verifyAlert)).toBeNull();
  });

  it('drops an older version and the wrong state: the cancellation stays the last word', () => {
    expect(followUpRefusal(cancelled, 'cancel', versionTag('urn:oid:b'), NOW, verifyAlert)).toBe(
      'stale_version',
    );
    expect(followUpRefusal(cancelled, 'update', tag, NOW, verifyAlert)).toBe('stale_version');
    const active = signed({ sourceId: 'urn:oid:c' });
    expect(followUpRefusal(active, 'cancel', tag, NOW, verifyAlert)).toBe('stale_version');
  });

  it('refuses unsigned, non-Actual and ended alerts', () => {
    expect(
      followUpRefusal(tampered(cancelled, { event: 'x' }), 'cancel', tag, NOW, verifyAlert),
    ).toBe('unsigned');
    const test = signed({ sourceId: 'urn:oid:c', state: 'cancelled', status: 'Test' });
    expect(followUpRefusal(test, 'cancel', tag, NOW, verifyAlert)).toBe('not_actual');
    const later = new Date('2026-11-10T15:00:00Z');
    expect(followUpRefusal(cancelled, 'cancel', tag, later, verifyAlert)).toBe('expired');
  });
});
