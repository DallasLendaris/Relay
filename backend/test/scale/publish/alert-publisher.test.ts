import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import type { Alert } from '@relay/shared';
import { mockClient } from 'aws-sdk-client-mock';
import { cellToParent, latLngToCell } from 'h3-js';
import { beforeEach, describe, expect, it } from 'vitest';
import { InMemoryAlertStore } from '../../../src/adapters/memory/alert-store.js';
import { SigningAlertStore } from '../../../src/adapters/signing-alert-store.js';
import {
  AlertPublisher,
  PUBLIC_ALERT_PATH,
  PUBLIC_CACHE_CONTROL,
  S3PublicObjectWriter,
  type ActiveIndexFile,
  type PublicObjectWriter,
} from '../../../src/scale/publish/index.js';
import { createAlertVerifier } from '../../../src/signing/index.js';

const KEY = { id: 'k1', secret: 'test-secret-not-real' };
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
  return { inner, store: new SigningAlertStore(inner, KEY, publisher) };
}

describe('AlertPublisher via SigningAlertStore', () => {
  let writer: FakeWriter;
  beforeEach(() => {
    writer = new FakeWriter();
  });

  it('writes the full signed alert at alerts/v1/{id}.json with cache headers', async () => {
    const { store } = setup(writer);
    await store.save(alert(A, [LA, LA2]));

    const put = writer.puts.find((p) => p.key === `alerts/v1/${A}.json`);
    expect(put?.cacheControl).toBe('public, max-age=15, stale-while-revalidate=60');
    expect(PUBLIC_CACHE_CONTROL).toBe(put?.cacheControl);
    const body = JSON.parse(put?.body ?? '{}') as Alert;
    expect(body.id).toBe(A);
    expect(body.area.h3Cells).toEqual([LA, LA2].sort());
    expect(body.official.description).toBe('d');
    expect(body.summaries.length).toBeGreaterThan(0);
    expect(body.signature).toMatch(/^v1\.k1\./);
    // The published copy verifies: it is exactly what was signed.
    expect(createAlertVerifier(new Map([[KEY.id, KEY.secret]]))(body)).toBe(true);
  });

  it('exposes the public path for the app', () => {
    expect(PUBLIC_ALERT_PATH('abc')).toBe('/alerts/v1/abc.json');
  });

  it('writes one active index per res-4 shard with summaries, listing only that shard', async () => {
    const { store } = setup(writer);
    await store.save(alert(A, [LA, SF]));
    await store.save(alert(B, [SF], { relaySeverity: 'watch', event: 'Flood Watch' }));

    const keys = new Set(writer.puts.map((p) => p.key));
    expect(keys).toContain(`alerts/v1/active/${LA_SHARD}.json`);
    expect(keys).toContain(`alerts/v1/active/${SF_SHARD}.json`);
    for (const p of writer.puts) expect(p.cacheControl).toBe(PUBLIC_CACHE_CONTROL);

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
    expect(writer.json<Alert>(`alerts/v1/${A}.json`).state).toBe('cancelled');
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

  it('a publisher that throws still does not fail the save', async () => {
    const inner = new InMemoryAlertStore();
    const store = new SigningAlertStore(inner, KEY, {
      publish: async () => {
        throw new Error('boom');
      },
    });
    await expect(store.save(alert(A, [LA]))).resolves.toBeUndefined();
    expect(await inner.get(A)).not.toBeNull();
  });

  it('works unchanged without a publisher', async () => {
    const inner = new InMemoryAlertStore();
    const store = new SigningAlertStore(inner, KEY);
    await store.save(alert(A, [LA]));
    expect((await inner.get(A))?.signature).toMatch(/^v1\./);
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
    const store = new SigningAlertStore(inner, KEY, publisher);
    await expect(store.save(alert(A, [LA]))).resolves.toBeUndefined();
    expect(s3Mock.commandCalls(PutObjectCommand).length).toBeGreaterThan(0);
  });
});
