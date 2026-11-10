import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { mockClient } from 'aws-sdk-client-mock';
import { cellToParent, gridDisk, latLngToCell } from 'h3-js';
import { beforeEach, describe, expect, it } from 'vitest';
import { InMemoryAlertStore } from '../../../src/adapters/memory/alert-store.js';
import { SigningAlertStore } from '../../../src/adapters/signing-alert-store.js';
import {
  AlertPublisher,
  cellsDigest,
  documentMatchesAlert,
  PUBLIC_ALERT_PATH,
  PUBLIC_CACHE_CONTROL,
  PUBLIC_DOC_CACHE_CONTROL,
  publicAlertDocKey,
  S3PublicObjectWriter,
  type ActiveIndexFile,
  type PublicObjectWriter,
} from '../../../src/scale/publish/index.js';
import {
  AlertPointerSchema,
  PublicAlertDocumentSchema,
  type Alert,
  type AlertPointer,
  type PublicAlertDocument,
} from '@relay/shared';
import { createAlertVerifier } from '../../../src/signing/index.js';
import { TEST_SIGNING } from '../../signing/test-signing.js';

const KEY = TEST_SIGNING.signer;
const NOW = new Date('2026-06-01T12:00:00Z');

const LA = latLngToCell(34.05, -118.25, 7);
const LA2 = latLngToCell(34.06, -118.26, 7);
const SF = latLngToCell(37.77, -122.42, 7);
const LA_SHARD = cellToParent(LA, 4);
const SF_SHARD = cellToParent(SF, 4);

const A = '11111111-1111-4111-8111-111111111111';
const B = '22222222-2222-4222-8222-222222222222';

function alert(id: string, cells: string[], over: Partial<Alert> = {}): Alert {
  return {
    id,
    source: 'nws',
    sourceId: `urn:${id}`,
    event: 'Flash Flood Warning',
    eventCode: 'FFW',
    relaySeverity: 'warning',
    capSeverity: 'Severe',
    urgency: 'Immediate',
    certainty: 'Likely',
    status: 'Actual',
    messageType: 'Alert',
    state: 'active',
    references: [],
    area: { description: 'Test', geometry: null, ugcCodes: [], sameCodes: [], h3Cells: cells },
    sent: '2026-06-01T11:00:00Z',
    effective: '2026-06-01T11:00:00Z',
    onset: null,
    expires: '2026-06-01T18:00:00Z',
    ends: null,
    official: {
      headline: `Headline ${id}`,
      description: 'd',
      instruction: null,
      senderName: 'NWS',
      language: 'en',
    },
    summaries: [],
    signature: null,
    ...over,
  };
}

class FakeWriter implements PublicObjectWriter {
  puts: Array<{ key: string; body: string; cacheControl: string }> = [];
  fail = false;
  async putJson(key: string, body: string, cacheControl: string) {
    if (this.fail) throw new Error('s3 down');
    this.puts.push({ key, body, cacheControl });
  }
  json<T>(key: string): T {
    const hit = [...this.puts].reverse().find((p) => p.key === key);
    if (!hit) throw new Error(`no put for ${key}`);
    return JSON.parse(hit.body) as T;
  }
}

function setup(writer: PublicObjectWriter, errors: unknown[][] = []) {
  const inner = new InMemoryAlertStore();
  const publisher = new AlertPublisher({
    writer,
    alerts: inner,
    cellResolution: 7,
    clock: { now: () => NOW },
    logger: { info() {}, warn() {}, error: (...a) => errors.push(a) },
  });
  const signing = new SigningAlertStore(inner, KEY);
  // Publishing is no longer part of save: the pipeline runs it afterwards.
  const store = {
    async save(a: Alert) {
      await signing.save(a);
      const saved = await inner.get(a.id);
      if (saved) await publisher.publish(saved);
    },
  };
  return { inner, publisher, store };
}

describe('AlertPublisher via SigningAlertStore', () => {
  let writer: FakeWriter;
  beforeEach(() => {
    writer = new FakeWriter();
  });

  it('writes a cell-free signed document at an immutable key and a short-TTL pointer', async () => {
    const { store, inner } = setup(writer);
    await store.save(alert(A, [LA, LA2]));
    const saved = await inner.get(A);
    if (!saved) throw new Error('not saved');

    const docKey = publicAlertDocKey(saved);
    expect(docKey).toMatch(new RegExp(`^alerts/v1/doc/${A}/[0-9a-f]{16}\\.json$`));
    const put = writer.puts.find((p) => p.key === docKey);
    expect(put?.cacheControl).toBe('public, max-age=31536000, immutable');
    expect(PUBLIC_DOC_CACHE_CONTROL).toBe(put?.cacheControl);
    const body = JSON.parse(put?.body ?? '{}') as PublicAlertDocument;
    expect(PublicAlertDocumentSchema.safeParse(body).success).toBe(true);
    expect(body.id).toBe(A);
    expect(body.area).not.toHaveProperty('h3Cells');
    expect(put?.body).not.toContain(LA);
    expect(body.area.cellCount).toBe(2);
    expect(body.official.description).toBe('d');
    expect(body.summaries.length).toBeGreaterThan(0);
    expect(body.signature).toMatch(/^v2\.a1\./);

    // Authenticity: the signature is the alert's own and still verifies on the full alert; the
    // document is exactly what the full, verified alert produces (cells bound by digest).
    expect(body.signature).toBe(saved.signature);
    expect(createAlertVerifier(TEST_SIGNING.publicKeys)(saved)).toBe(true);
    expect(documentMatchesAlert(body, saved)).toBe(true);
    expect(body.area.cellsSha256).toBe(cellsDigest([LA, LA2]));
    expect(documentMatchesAlert(body, { ...saved, area: { ...saved.area, h3Cells: [LA] } })).toBe(
      false,
    );

    const pointer = writer.puts.find((p) => p.key === `alerts/v1/latest/${A}.json`);
    expect(pointer?.cacheControl).toBe('public, max-age=15, stale-while-revalidate=60');
    const ptr = AlertPointerSchema.parse(JSON.parse(pointer?.body ?? '{}'));
    expect(ptr).toMatchObject({ alertId: A, sourceId: saved.sourceId, state: 'active' });
    expect(ptr.doc).toBe(`/${docKey}`);
    // The full-alert object no longer exists.
    expect(writer.puts.map((p) => p.key)).not.toContain(`alerts/v1/${A}.json`);
  });

  it('a statewide alert (~77,000 cells) publishes a document under 10 KB with no cell list', async () => {
    const cells = gridDisk(LA, 160); // about 77,000 res-7 cells, like a statewide alert
    expect(cells.length).toBeGreaterThan(75_000);
    const inner = new InMemoryAlertStore();
    const signing = new SigningAlertStore(inner, KEY);
    const big = alert(A, cells, {
      area: {
        description: 'Los Angeles; Orange; Ventura; San Diego; Riverside; '.repeat(10),
        geometry: null,
        ugcCodes: Array.from({ length: 500 }, (_, i) => `CAZ${String(i).padStart(3, '0')}`),
        sameCodes: [],
        h3Cells: cells,
      },
      official: {
        headline: 'Flash Flood Warning issued for a very large area',
        description: 'WHAT...Flash flooding is occurring. '.repeat(60),
        instruction: 'Move to higher ground now. '.repeat(20),
        senderName: 'NWS Los Angeles/Oxnard CA',
        language: 'en',
      },
    });
    await signing.save(big);
    const saved = await inner.get(A);
    if (!saved) throw new Error('not saved');
    const publisher = new AlertPublisher({
      writer,
      alerts: inner,
      cellResolution: 7,
      clock: { now: () => NOW },
    });
    await publisher.publish(saved);
    const doc = writer.puts.find((p) => p.key === publicAlertDocKey(saved));
    expect(Buffer.byteLength(doc?.body ?? '')).toBeLessThan(10_000);
    expect(JSON.stringify(saved).length).toBeGreaterThan(1_000_000);
    expect(doc?.body).not.toContain('h3Cells');
  });

  it('the document URL changes when the alert version (sourceId) changes', async () => {
    const { store, inner } = setup(writer);
    await store.save(alert(A, [LA], { sourceId: 'urn:v1' }));
    const v1 = await inner.get(A);
    await store.save(
      alert(A, [LA], { sourceId: 'urn:v2', messageType: 'Update', state: 'updated' }),
    );
    const v2 = await inner.get(A);
    if (!v1 || !v2) throw new Error('not saved');
    expect(publicAlertDocKey(v1)).not.toBe(publicAlertDocKey(v2));
    // Same version, same key (a republish overwrites an identical object).
    expect(publicAlertDocKey(v2)).toBe(publicAlertDocKey({ ...v2 }));
    // Both documents exist (old phones keep a valid document); the pointer names the newest.
    const keys = writer.puts.map((p) => p.key);
    expect(keys).toContain(publicAlertDocKey(v1));
    expect(keys).toContain(publicAlertDocKey(v2));
    const ptr = writer.json<AlertPointer>(`alerts/v1/latest/${A}.json`);
    expect(ptr.doc).toBe(`/${publicAlertDocKey(v2)}`);
    expect(ptr.sourceId).toBe('urn:v2');
  });

  it('exposes the public path for the app (the pointer)', () => {
    expect(PUBLIC_ALERT_PATH('abc')).toBe('/alerts/v1/latest/abc.json');
  });

  it('writes one active index per res-4 shard with summaries, listing only that shard', async () => {
    const { store } = setup(writer);
    await store.save(alert(A, [LA, SF]));
    await store.save(alert(B, [SF], { relaySeverity: 'watch', event: 'Flood Watch' }));

    const keys = new Set(writer.puts.map((p) => p.key));
    expect(keys).toContain(`alerts/v1/active/${LA_SHARD}.json`);
    expect(keys).toContain(`alerts/v1/active/${SF_SHARD}.json`);
    for (const p of writer.puts.filter((x) => x.key.includes('/active/'))) {
      expect(p.cacheControl).toBe(PUBLIC_CACHE_CONTROL);
    }

    const la = writer.json<ActiveIndexFile>(`alerts/v1/active/${LA_SHARD}.json`);
    expect(la.shard).toBe(LA_SHARD);
    expect(la.alerts.map((x) => x.id)).toEqual([A]);
    expect(la.alerts[0]).toEqual({
      id: A,
      event: 'Flash Flood Warning',
      relaySeverity: 'warning',
      state: 'active',
      expires: '2026-06-01T18:00:00Z',
      headline: `Headline ${A}`,
      shards: [LA_SHARD, SF_SHARD].sort(),
    });

    const sf = writer.json<ActiveIndexFile>(`alerts/v1/active/${SF_SHARD}.json`);
    expect(sf.alerts.map((x) => x.id).sort()).toEqual([A, B].sort());
  });

  it('drops a cancelled alert from the shard index but still publishes its object', async () => {
    const { store } = setup(writer);
    await store.save(alert(A, [LA]));
    await store.save(alert(A, [LA], { state: 'cancelled', messageType: 'Cancel' }));

    expect(writer.json<ActiveIndexFile>(`alerts/v1/active/${LA_SHARD}.json`).alerts).toEqual([]);
    // A cancelled alert still gets a (new) document, and the pointer says cancelled.
    const ptr = writer.json<AlertPointer>(`alerts/v1/latest/${A}.json`);
    expect(ptr.state).toBe('cancelled');
    expect(writer.json<PublicAlertDocument>(ptr.doc.slice(1)).state).toBe('cancelled');
  });

  it('drops an already expired alert from the index', async () => {
    const { store } = setup(writer);
    await store.save(alert(A, [LA], { expires: '2026-06-01T10:00:00Z' }));
    expect(writer.json<ActiveIndexFile>(`alerts/v1/active/${LA_SHARD}.json`).alerts).toEqual([]);
  });

  it('a publish failure is logged as publish.failed and never fails the save', async () => {
    writer.fail = true;
    const errors: unknown[][] = [];
    const { store, inner } = setup(writer, errors);
    await expect(store.save(alert(A, [LA]))).resolves.toBeUndefined();
    expect(await inner.get(A)).not.toBeNull();
    expect(errors[0]?.[0]).toBe('publish.failed');
    expect(errors[0]?.[1]).toMatchObject({ alertId: A, error: 's3 down' });
  });

  it('SigningAlertStore.save never calls a publisher: it only signs and stores', async () => {
    const inner = new InMemoryAlertStore();
    const store = new SigningAlertStore(inner, KEY);
    await store.save(alert(A, [LA]));
    expect((await inner.get(A))?.signature).toMatch(/^v2\./);
    expect(writer.puts).toEqual([]);
  });

  it('writes the document first, then the pointer, then shard indexes with at most `concurrency` in flight', async () => {
    const cells = [LA, SF, latLngToCell(32.7, -117.1, 7), latLngToCell(38.58, -121.49, 7)];
    const inner = new InMemoryAlertStore();
    const order: string[] = [];
    let inFlight = 0;
    let maxInFlight = 0;
    const slow: PublicObjectWriter = {
      async putJson(key) {
        order.push(key);
        inFlight++;
        maxInFlight = Math.max(maxInFlight, inFlight);
        await new Promise((r) => setTimeout(r, 10));
        inFlight--;
      },
    };
    const publisher = new AlertPublisher({
      writer: slow,
      alerts: inner,
      cellResolution: 7,
      clock: { now: () => NOW },
    });
    await publisher.publish(alert(A, cells), { concurrency: 2 });
    expect(order[0]).toMatch(new RegExp(`^alerts/v1/doc/${A}/`));
    expect(order[1]).toBe(`alerts/v1/latest/${A}.json`);
    expect(order.length).toBe(2 + 4);
    expect(maxInFlight).toBe(2);
  });

  it('stops after the total time cap: no new shards start and publish still resolves', async () => {
    const cells = [LA, SF, latLngToCell(32.7, -117.1, 7), latLngToCell(38.58, -121.49, 7)];
    const inner = new InMemoryAlertStore();
    const started: string[] = [];
    const warns: unknown[][] = [];
    const hung: PublicObjectWriter = {
      async putJson(key) {
        started.push(key);
        if (key.includes('/active/')) await new Promise(() => {}); // never settles
      },
    };
    const publisher = new AlertPublisher({
      writer: hung,
      alerts: inner,
      cellResolution: 7,
      clock: { now: () => NOW },
      logger: { info() {}, warn: (...a) => warns.push(a), error() {} },
    });
    const t0 = Date.now();
    await publisher.publish(alert(A, cells), { concurrency: 1, deadlineMs: 50 });
    expect(Date.now() - t0).toBeLessThan(1000);
    // Document, pointer and the single shard that was in flight; the other three never started.
    expect(started.length).toBe(3);
    expect(warns[0]?.[0]).toBe('publish.deadline_exceeded');
  });

  it('one failing shard is logged and the others are still written', async () => {
    const inner = new InMemoryAlertStore();
    const errors: unknown[][] = [];
    const puts: string[] = [];
    const writer2: PublicObjectWriter = {
      async putJson(key) {
        if (key === `alerts/v1/active/${LA_SHARD}.json`) throw new Error('boom');
        puts.push(key);
      },
    };
    const publisher = new AlertPublisher({
      writer: writer2,
      alerts: inner,
      cellResolution: 7,
      clock: { now: () => NOW },
      logger: { info() {}, warn() {}, error: (...a) => errors.push(a) },
    });
    await publisher.publish(alert(A, [LA, SF]));
    expect(puts).toContain(`alerts/v1/active/${SF_SHARD}.json`);
    expect(errors[0]?.[0]).toBe('publish.shard_failed');
  });
});

describe('publish reads the store once for all shards (PERF-11)', () => {
  it('one listActiveForCells call regardless of shard count, and the same indexes as before', async () => {
    const cells = [LA, SF, latLngToCell(32.7, -117.1, 7), latLngToCell(38.58, -121.49, 7)];
    const inner = new InMemoryAlertStore();
    await inner.save(alert(B, [SF], { relaySeverity: 'watch', event: 'Flood Watch' }));
    let calls = 0;
    let cellsAsked = 0;
    const counting = {
      listActiveForCells: async (c: string[], now: Date) => {
        calls++;
        cellsAsked += c.length;
        return inner.listActiveForCells(c, now);
      },
    };
    const w = new FakeWriter();
    const publisher = new AlertPublisher({
      writer: w,
      alerts: counting,
      cellResolution: 7,
      clock: { now: () => NOW },
    });
    await publisher.publish(alert(A, cells));
    expect(calls).toBe(1); // was one per shard (4 here, about 1,700 statewide)
    expect(cellsAsked).toBe(4 * 343);
    // The other active alert still lands in its own shard's index only.
    const sf = w.json<ActiveIndexFile>(`alerts/v1/active/${SF_SHARD}.json`);
    expect(sf.alerts.map((x) => x.id).sort()).toEqual([A, B].sort());
    const la = w.json<ActiveIndexFile>(`alerts/v1/active/${LA_SHARD}.json`);
    expect(la.alerts.map((x) => x.id)).toEqual([A]);
  });
});

describe('S3PublicObjectWriter', () => {
  const s3Mock = mockClient(S3Client);
  beforeEach(() => s3Mock.reset());

  it('sends PutObject with JSON content type and Cache-Control', async () => {
    s3Mock.on(PutObjectCommand).resolves({});
    const w = new S3PublicObjectWriter(new S3Client({ region: 'us-west-2' }), 'pub-bucket');
    await w.putJson('alerts/v1/x.json', '{"a":1}', PUBLIC_CACHE_CONTROL);
    expect(s3Mock.commandCalls(PutObjectCommand)[0]?.args[0].input).toEqual({
      Bucket: 'pub-bucket',
      Key: 'alerts/v1/x.json',
      Body: '{"a":1}',
      ContentType: 'application/json',
      CacheControl: 'public, max-age=15, stale-while-revalidate=60',
    });
  });

  it('end to end: an S3 error does not fail save', async () => {
    s3Mock.on(PutObjectCommand).rejects(new Error('AccessDenied'));
    const inner = new InMemoryAlertStore();
    const publisher = new AlertPublisher({
      writer: new S3PublicObjectWriter(new S3Client({ region: 'us-west-2' }), 'pub-bucket'),
      alerts: inner,
      cellResolution: 7,
      clock: { now: () => NOW },
    });
    await expect(publisher.publish(alert(A, [LA]))).resolves.toBeUndefined();
    expect(s3Mock.commandCalls(PutObjectCommand).length).toBeGreaterThan(0);
  });
});
