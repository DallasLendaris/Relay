import { describe, expect, it } from 'vitest';
import {
  CELL_BUCKETS,
  MAX_BATCHES_PER_MESSAGE,
  type SendBatch,
} from '../../../src/scale/contracts.js';
import { bucketFor, fnv1a32 } from '../../../src/scale/fanout/hash.js';
import {
  MAX_MESSAGE_BYTES,
  packBatches,
  serializeMessage,
} from '../../../src/scale/fanout/pack.js';
import { target } from './helpers.js';

describe('fnv1a32', () => {
  it('matches the published FNV-1a test vectors, so batch keys can never silently move', () => {
    expect(fnv1a32('')).toBe(0x811c9dc5);
    expect(fnv1a32('a')).toBe(0xe40c292c);
    expect(fnv1a32('foobar')).toBe(0xbf9cf968);
  });

  it('hashes UTF-8 bytes, not UTF-16 units', () => {
    // "é" is 0xC3 0xA9 in UTF-8: FNV-1a of those two bytes.
    let h = 0x811c9dc5;
    for (const b of [0xc3, 0xa9]) h = Math.imul(h ^ b, 0x01000193);
    expect(fnv1a32('é')).toBe(h >>> 0);
  });

  it('pins the buckets of some device ids across runs', () => {
    expect(['dev-0', 'dev-1', 'dev-2', 'device-abc'].map(bucketFor)).toEqual(
      ['dev-0', 'dev-1', 'dev-2', 'device-abc'].map((d) => fnv1a32(d) % 64),
    );
    expect(CELL_BUCKETS).toBe(64);
    expect(bucketFor('dev-0')).toBe(bucketFor('dev-0'));
  });

  it('spreads devices over all 64 buckets roughly evenly', () => {
    const counts = new Array<number>(CELL_BUCKETS).fill(0);
    for (let i = 0; i < 64_000; i++) counts[bucketFor(`device-${i}`)]!++;
    expect(Math.min(...counts)).toBeGreaterThan(800);
    expect(Math.max(...counts)).toBeLessThan(1200);
  });
});

function batch(n: number, recipients: number): SendBatch {
  return {
    alertId: 'a',
    sourceId: 's',
    relaySeverity: 'warning',
    batchKey: `a:warning:cell:${n}`,
    attempt: 0,
    recipients: Array.from({ length: recipients }, (_, i) => ({
      deviceId: `d${n}-${i}`,
      target: target(`${n}-${i}`),
      language: 'en' as const,
      checkInToken: 'x'.repeat(150),
    })),
  };
}

describe('packBatches', () => {
  it('puts up to MAX_BATCHES_PER_MESSAGE small batches in one message', () => {
    const batches = Array.from({ length: 25 }, (_, i) => batch(i, 3));
    const messages = packBatches(batches);
    expect(messages.map((m) => m.length)).toEqual([MAX_BATCHES_PER_MESSAGE, 10, 5]);
    expect(messages.flat()).toEqual(batches); // order and content preserved
  });

  it('keeps every message under 240 KB by measured size, starting a new message when needed', () => {
    // ~400 recipients is about 180 KB per batch, so two never fit together.
    const batches = Array.from({ length: 6 }, (_, i) => batch(i, 400));
    const one = Buffer.byteLength(JSON.stringify(batches[0]));
    expect(one).toBeGreaterThan(100_000);
    const messages = packBatches(batches);
    expect(messages.length).toBeGreaterThan(1);
    expect(messages.flat().map((b) => b.batchKey)).toEqual(batches.map((b) => b.batchKey));
    for (const m of messages) {
      expect(Buffer.byteLength(serializeMessage(m))).toBeLessThanOrEqual(MAX_MESSAGE_BYTES);
    }
  });

  it('fills a message right up to the limit with mixed sizes', () => {
    const batches = Array.from({ length: 200 }, (_, i) => batch(i, 20 + ((i * 37) % 180)));
    for (const m of packBatches(batches)) {
      expect(m.length).toBeLessThanOrEqual(MAX_BATCHES_PER_MESSAGE);
      expect(Buffer.byteLength(serializeMessage(m))).toBeLessThanOrEqual(MAX_MESSAGE_BYTES);
    }
  });

  it('returns nothing for nothing', () => {
    expect(packBatches([])).toEqual([]);
  });
});
