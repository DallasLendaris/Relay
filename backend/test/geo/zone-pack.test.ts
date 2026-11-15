import { gzipSync } from 'node:zlib';
import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import type { GetObjectCommandOutput } from '@aws-sdk/client-s3';
import type { MultiPolygon, Polygon } from 'geojson';
import { mockClient } from 'aws-sdk-client-mock';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { LayeredZoneProvider } from '../../src/geo/layered-zone-provider.js';
import {
  decodeZonePack,
  encodeZonePack,
  S3ZoneProvider,
  ZonePackError,
  type ZonePack,
} from '../../src/geo/s3-zone-provider.js';
import { NwsZoneProvider } from '../../src/geo/zones.js';
import {
  buildZonePack,
  type FetchFn,
  listCaliforniaZones,
  parseCodes,
  parseS3Uri,
  uploadZonePack,
} from '../../scripts/build-zone-pack.js';

const SQUARE: Polygon = {
  type: 'Polygon',
  coordinates: [
    [
      [-122, 37],
      [-121, 37],
      [-121, 38],
      [-122, 37],
    ],
  ],
};
const MULTI: MultiPolygon = { type: 'MultiPolygon', coordinates: [SQUARE.coordinates] };
const BUILT_AT = '2026-11-09T00:00:00.000Z';
const KEY = 'zones/v1/california.json.gz';

const pack = (zones: ZonePack['zones'] = { CAZ513: SQUARE, CAC085: MULTI }): ZonePack => ({
  version: 1,
  builtAt: BUILT_AT,
  zones,
});

const respond = (status: number, body: unknown = {}) => ({
  ok: status >= 200 && status < 300,
  status,
  json: async () => body,
});
const feature = (id: string) => ({ properties: { id } });
const geoBody = (geometry: unknown) => respond(200, { geometry });

const s3Mock = mockClient(S3Client);
const s3 = new S3Client({ region: 'us-west-2' });

/** A GetObject response whose Body reads as the given bytes, as the SDK's stream does. */
const objectWith = (bytes: Uint8Array) => ({
  Body: {
    transformToByteArray: async () => bytes,
  } as unknown as GetObjectCommandOutput['Body'],
});

const getCalls = () => s3Mock.commandCalls(GetObjectCommand);

describe('zone pack encoding', () => {
  it('round-trips a pack through gzip', async () => {
    const original = pack();
    const gz = await encodeZonePack(original);
    expect(gz[0]).toBe(0x1f);
    expect(gz[1]).toBe(0x8b);
    expect(await decodeZonePack(gz)).toEqual(original);
  });

  it('rejects a pack with an unknown version or a non-geometry entry', async () => {
    const wrongVersion = await encodeZonePack({ version: 2 } as unknown as ZonePack);
    await expect(decodeZonePack(wrongVersion)).rejects.toThrow('unsupported zone pack version 2');

    const badEntry = await encodeZonePack({
      version: 1,
      builtAt: BUILT_AT,
      zones: { CAZ513: { type: 'Point', coordinates: [0, 0] } },
    } as unknown as ZonePack);
    await expect(decodeZonePack(badEntry)).rejects.toThrow('CAZ513 is not a Polygon');
  });
});

describe('S3ZoneProvider', () => {
  beforeEach(() => s3Mock.reset());

  it('serves hits and misses from the pack, with UGC codes matched case-insensitively', async () => {
    s3Mock.on(GetObjectCommand).resolves(objectWith(await encodeZonePack(pack())));
    const provider = new S3ZoneProvider('bucket', KEY, s3);

    expect(await provider.getZoneGeometry('caz513')).toEqual(SQUARE);
    expect(await provider.getZoneGeometry('CAZ999')).toBeNull();
    expect(getCalls()[0]?.args[0].input).toEqual({ Bucket: 'bucket', Key: KEY });
  });

  it('does not read S3 until the first lookup, then loads the pack only once', async () => {
    s3Mock.on(GetObjectCommand).resolves(objectWith(await encodeZonePack(pack())));
    const provider = new S3ZoneProvider('bucket', KEY, s3);
    expect(getCalls()).toHaveLength(0);

    await Promise.all([
      provider.getZoneGeometry('CAZ513'),
      provider.getZoneGeometry('CAC085'),
      provider.getZoneGeometry('CAZ513'),
    ]);
    await provider.getZoneGeometry('CAC085');

    expect(getCalls()).toHaveLength(1);
  });

  it('throws a ZonePackError when the pack cannot be read, and retries only after the cooldown', async () => {
    s3Mock.on(GetObjectCommand).rejects(new Error('NoSuchKey'));
    let now = 1_000;
    const provider = new S3ZoneProvider('bucket', KEY, s3, {
      retryAfterMs: 60_000,
      now: () => now,
    });

    const first = provider.getZoneGeometry('CAZ513');
    await expect(first).rejects.toThrow(ZonePackError);
    await expect(first).rejects.toThrow(`zone pack s3://bucket/${KEY} unavailable: NoSuchKey`);

    now += 30_000;
    await expect(provider.getZoneGeometry('CAZ513')).rejects.toBeInstanceOf(ZonePackError);
    expect(getCalls()).toHaveLength(1);

    now += 31_000;
    s3Mock.reset();
    s3Mock.on(GetObjectCommand).resolves(objectWith(await encodeZonePack(pack())));
    expect(await provider.getZoneGeometry('CAZ513')).toEqual(SQUARE);
    // The mock was reset, so this count is the retry alone.
    expect(getCalls()).toHaveLength(1);
  });

  it('treats a corrupt pack as a load error', async () => {
    s3Mock.on(GetObjectCommand).resolves(objectWith(gzipSync(Buffer.from('not json'))));
    const provider = new S3ZoneProvider('bucket', KEY, s3);
    await expect(provider.getZoneGeometry('CAZ513')).rejects.toBeInstanceOf(ZonePackError);
  });
});

describe('LayeredZoneProvider', () => {
  beforeEach(() => s3Mock.reset());

  /** NWS stub: forecast CAZ999 has geometry, every other zone is unknown (404). */
  const nwsFetch = vi.fn<FetchFn>(async (url: string) =>
    url.endsWith('/zones/forecast/CAZ999') ? geoBody(MULTI) : respond(404),
  );
  const nws = () => new NwsZoneProvider(nwsFetch);
  const logger = () => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn() });

  it('answers from the pack without asking NWS', async () => {
    s3Mock.on(GetObjectCommand).resolves(objectWith(await encodeZonePack(pack())));
    nwsFetch.mockClear();
    const layered = new LayeredZoneProvider([new S3ZoneProvider('bucket', KEY, s3), nws()]);

    expect(await layered.getZoneGeometry('CAZ513')).toEqual(SQUARE);
    expect(nwsFetch).not.toHaveBeenCalled();
  });

  it('falls back to NWS on a pack miss and logs zones.pack_miss once per code', async () => {
    s3Mock.on(GetObjectCommand).resolves(objectWith(await encodeZonePack(pack())));
    nwsFetch.mockClear();
    const log = logger();
    const layered = new LayeredZoneProvider([new S3ZoneProvider('bucket', KEY, s3), nws()], log);

    expect(await layered.getZoneGeometry('CAZ999')).toEqual(MULTI);
    expect(await layered.getZoneGeometry('CAZ999')).toEqual(MULTI);
    expect(await layered.getZoneGeometry('CAZ998')).toBeNull();

    expect(log.info.mock.calls.map((c) => c[1])).toEqual([{ ugc: 'CAZ999' }, { ugc: 'CAZ998' }]);
    expect(log.info.mock.calls.every((c) => c[0] === 'zones.pack_miss')).toBe(true);
  });

  it('falls back to NWS when the pack cannot load, and logs the error once', async () => {
    s3Mock.on(GetObjectCommand).rejects(new Error('AccessDenied'));
    nwsFetch.mockClear();
    const log = logger();
    const layered = new LayeredZoneProvider([new S3ZoneProvider('bucket', KEY, s3), nws()], log);

    expect(await layered.getZoneGeometry('CAZ999')).toEqual(MULTI);
    expect(await layered.getZoneGeometry('CAZ999')).toEqual(MULTI);
    // NwsZoneProvider caches the hit, so the second lookup does not reach the network.
    expect(nwsFetch).toHaveBeenCalledTimes(1);
    expect(log.warn).toHaveBeenCalledTimes(1);
    expect(log.warn.mock.calls[0]?.[0]).toBe('zones.pack_error');
  });

  it('keeps the NWS error semantics: a 404 is null, a network error rejects', async () => {
    s3Mock.on(GetObjectCommand).resolves(objectWith(await encodeZonePack(pack())));
    const layered = new LayeredZoneProvider([new S3ZoneProvider('bucket', KEY, s3), nws()]);
    expect(await layered.getZoneGeometry('CAZ998')).toBeNull();

    const down = new NwsZoneProvider(async () => {
      throw new TypeError('fetch failed');
    });
    const offline = new LayeredZoneProvider([new S3ZoneProvider('bucket', KEY, s3), down]);
    await expect(offline.getZoneGeometry('CAZ998')).rejects.toThrow('fetch failed');
  });

  it('rejects when both the pack and NWS fail', async () => {
    s3Mock.on(GetObjectCommand).rejects(new Error('AccessDenied'));
    const down = new NwsZoneProvider(async () => {
      throw new TypeError('fetch failed');
    });
    const layered = new LayeredZoneProvider([new S3ZoneProvider('bucket', KEY, s3), down]);
    await expect(layered.getZoneGeometry('CAZ999')).rejects.toThrow('fetch failed');
  });

  it('needs at least one provider', () => {
    expect(() => new LayeredZoneProvider([])).toThrow('at least one provider');
  });
});

describe('zone pack builder', () => {
  const routes = (table: Record<string, () => ReturnType<typeof respond>>): FetchFn =>
    vi.fn<FetchFn>(async (url: string) => {
      const handler = table[url.replace('https://api.weather.gov', '')];
      return handler ? handler() : respond(404);
    });

  it('lists California zones across types and follows pagination', async () => {
    const fetchFn = routes({
      '/zones?area=CA&type=forecast': () =>
        respond(200, {
          features: [feature('CAZ001'), feature('CAZ002')],
          pagination: { next: 'https://api.weather.gov/zones?area=CA&type=forecast&cursor=2' },
        }),
      '/zones?area=CA&type=forecast&cursor=2': () =>
        respond(200, { features: [feature('CAZ002'), feature('CAZ003')] }),
      '/zones?area=CA&type=fire': () => respond(200, { features: [feature('CAZ050')] }),
      '/zones?area=CA&type=county': () => respond(200, { features: [feature('CAC085')] }),
    });

    const refs = await listCaliforniaZones(fetchFn);

    expect(refs.map((r) => [r.ugc, r.types.join('|')])).toEqual([
      ['CAZ001', 'forecast'],
      ['CAZ002', 'forecast'],
      ['CAZ003', 'forecast'],
      ['CAZ050', 'fire'],
      ['CAC085', 'county'],
    ]);
  });

  it('fetches geometry by type, trying forecast then fire for Z codes, and reports missing codes', async () => {
    const fetchFn = routes({
      '/zones/county/CAC085': () => geoBody(MULTI),
      '/zones/forecast/CAZ513': () => respond(404),
      '/zones/fire/CAZ513': () => geoBody(SQUARE),
      '/zones/forecast/CAZ999': () => respond(404),
      '/zones/fire/CAZ999': () => respond(404),
    });

    const { pack: built, missing } = await buildZonePack({
      refs: parseCodes('CAC085,CAZ513,CAZ999'),
      fetchFn,
      builtAt: new Date(BUILT_AT),
    });

    expect(built).toEqual(pack());
    expect(missing).toEqual(['CAZ999']);
    const urls = vi.mocked(fetchFn).mock.calls.map((c) => c[0]);
    expect(urls).not.toContain('https://api.weather.gov/zones/forecast/CAC085');
  });

  it('keeps at most four requests in flight and still builds the whole pack', async () => {
    let inFlight = 0;
    let peak = 0;
    const fetchFn = vi.fn<FetchFn>(async () => {
      inFlight++;
      peak = Math.max(peak, inFlight);
      await new Promise((resolve) => setTimeout(resolve, 5));
      inFlight--;
      return geoBody(SQUARE);
    });
    const codes = Array.from({ length: 12 }, (_, i) => `CAZ${String(100 + i)}`).join(',');

    const { pack: built, missing } = await buildZonePack({ refs: parseCodes(codes), fetchFn });

    expect(peak).toBe(4);
    expect(Object.keys(built.zones)).toHaveLength(12);
    expect(missing).toEqual([]);
  });

  it('fails the build on a non-404 error rather than writing a partial pack', async () => {
    const fetchFn = routes({
      '/zones/forecast/CAZ513': () => respond(500),
    });
    await expect(buildZonePack({ refs: parseCodes('CAZ513'), fetchFn })).rejects.toThrow(
      'CAZ513: HTTP 500',
    );
  });

  it('rejects codes that are not UGC codes', () => {
    expect(() => parseCodes('CAZ513,CAZ51')).toThrow('not a UGC zone code: CAZ51');
  });

  it('uploads the gzipped pack to the given S3 key', async () => {
    s3Mock.reset();
    s3Mock.on(PutObjectCommand).resolves({});
    const gz = await encodeZonePack(pack());

    await uploadZonePack(s3, 's3://pack-bucket/zones/v1/california.json.gz', gz);

    const input = s3Mock.commandCalls(PutObjectCommand)[0]?.args[0].input;
    expect(input).toMatchObject({
      Bucket: 'pack-bucket',
      Key: KEY,
      ContentType: 'application/gzip',
    });
    expect(input?.Body).toEqual(gz);
  });

  it('parses s3 URIs and rejects anything else', () => {
    expect(parseS3Uri('s3://b/zones/v1/x.json.gz')).toEqual({
      bucket: 'b',
      key: 'zones/v1/x.json.gz',
    });
    expect(() => parseS3Uri('https://example.com/x')).toThrow('expected s3://bucket/key');
  });

  it('a built pack is served by S3ZoneProvider after the gzip round trip', async () => {
    const fetchFn = routes({ '/zones/forecast/CAZ513': () => geoBody(SQUARE) });
    const { pack: built } = await buildZonePack({ refs: parseCodes('CAZ513'), fetchFn });
    const gz = await encodeZonePack(built);

    s3Mock.reset();
    s3Mock.on(GetObjectCommand).resolves(objectWith(gz));
    const provider = new S3ZoneProvider('bucket', KEY, s3);
    expect(await provider.getZoneGeometry('CAZ513')).toEqual(SQUARE);
  });
});
