import { randomUUID } from 'node:crypto';
import type { SendMessageBatchCommand } from '@aws-sdk/client-sqs';
import { describe, expect, it } from 'vitest';
import {
  CELL_BUCKETS,
  DENSE_PARTS,
  MAX_BATCHES_PER_MESSAGE,
  type SendBatch,
} from '../../../src/scale/contracts.js';
import { bucketFor, densePartFor, fnv1a32 } from '../../../src/scale/fanout/hash.js';
import {
  MAX_MESSAGE_BYTES,
  OversizeBatchError,
  packBatches,
  serializeMessage,
  splitToFit,
} from '../../../src/scale/fanout/pack.js';
import {
  SqsSendQueue as FanoutSqsSendQueue,
  sendBodies,
} from '../../../src/scale/fanout/queues.js';
import { SqsSendQueue as SenderSqsSendQueue } from '../../../src/scale/sender/sqs-send-queue.js';
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

describe('densePartFor', () => {
  it('pins the part of each first character (part keys must never move)', () => {
    const parts = '0123456789abcdef'.split('').map((c) => densePartFor(`${c}000-uuid`));
    expect(parts).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
    expect(densePartFor('')).toBe(0);
    expect(densePartFor('-x')).toBe(0);
    expect(densePartFor('A1')).toBe(9);
    expect(densePartFor('dev-1')).toBe(13);
    expect(densePartFor('zeta')).toBe(15);
    expect(DENSE_PARTS).toBe(16);
  });

  it('never decreases in cell-index sort order, for any ids (so a cell streams part by part)', () => {
    const ids = Array.from({ length: 2_000 }, (_, i) =>
      String.fromCharCode(32 + ((i * 7919) % 95), 32 + (i % 95)),
    ).sort();
    for (let i = 1; i < ids.length; i++) {
      expect(densePartFor(ids[i] as string)).toBeGreaterThanOrEqual(
        densePartFor(ids[i - 1] as string),
      );
    }
  });

  it('is balanced for UUIDs', () => {
    const counts = new Array<number>(16).fill(0);
    for (let i = 0; i < 16_000; i++) counts[densePartFor(randomUUID())]!++;
    expect(Math.min(...counts)).toBeGreaterThan(800);
    expect(Math.max(...counts)).toBeLessThan(1200);
  });
});

describe('oversize guards', () => {
  it('packBatches splits a single batch larger than a message into `:s` pieces that fit', () => {
    // 1,500 recipients is about 400 KB: the old packer gave it a message of its own, which SQS
    // rejects (over 256 KB), so those devices were never sent.
    const big = batch(7, 1_500);
    expect(Buffer.byteLength(JSON.stringify(big))).toBeGreaterThan(256 * 1024);
    const messages = packBatches([batch(1, 3), big, batch(2, 3)]);
    for (const m of messages) {
      expect(Buffer.byteLength(serializeMessage(m))).toBeLessThanOrEqual(MAX_MESSAGE_BYTES);
    }
    const flat = messages.flat();
    const pieces = flat.filter((b) => b.batchKey.startsWith(`${big.batchKey}:s`));
    expect(pieces.length).toBeGreaterThanOrEqual(2);
    expect(pieces.flatMap((b) => b.recipients)).toEqual(big.recipients);
    expect(flat[0]?.batchKey).toBe('a:warning:cell:1');
    // Deterministic for the same batch.
    expect(
      packBatches([big])
        .flat()
        .map((b) => b.batchKey),
    ).toEqual(pieces.map((b) => b.batchKey));
  });

  it('splitToFit throws a clear error when one recipient alone is too big', () => {
    const one = batch(1, 1);
    expect(() => splitToFit(one, 100)).toThrow(OversizeBatchError);
    expect(() => splitToFit(one, 100)).toThrow(/a:warning:cell:1 is \d+ bytes with one recipient/);
  });

  it('sendBodies refuses a body over the SQS limit before sending anything', async () => {
    let calls = 0;
    const sqs = {
      send: async () => {
        calls++;
        return {};
      },
    } as never;
    await expect(
      sendBodies(sqs, 'url', ['ok', 'x'.repeat(256 * 1024 + 1)], async () => {}),
    ).rejects.toThrow(/over the 262144 byte limit/);
    expect(calls).toBe(0);
  });

  it.each([
    ['fan-out', (s: never) => new FanoutSqsSendQueue(s, { urgent: 'U', standard: 'S' })],
    ['sender re-queue', (s: never) => new SenderSqsSendQueue(s, { urgent: 'U', standard: 'S' })],
  ])('%s SqsSendQueue packs by bytes: every message and request fits', async (_, make) => {
    const requests: string[][] = [];
    const sqs = {
      send: async (cmd: SendMessageBatchCommand) => {
        requests.push((cmd.input.Entries ?? []).map((e) => e.MessageBody ?? ''));
        return {};
      },
    } as never;
    // Ten 100 KB batches: packed by count alone they were one 1 MB message.
    const batches = Array.from({ length: 10 }, (_, i) => batch(i, 230));
    await make(sqs).enqueue('urgent', batches);
    const bodies = requests.flat();
    expect(bodies.length).toBeGreaterThan(1);
    for (const b of bodies) expect(Buffer.byteLength(b)).toBeLessThanOrEqual(MAX_MESSAGE_BYTES);
    for (const r of requests) {
      expect(r.reduce((n, b) => n + Buffer.byteLength(b), 0)).toBeLessThanOrEqual(256 * 1024);
    }
    const sent = bodies.flatMap((b) => JSON.parse(b) as SendBatch[]);
    expect(sent.map((b) => b.batchKey)).toEqual(batches.map((b) => b.batchKey));
  });
});
