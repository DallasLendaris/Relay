/**
 * Builds the California NWS zone pack (zones/v1/california.json.gz) and optionally uploads it.
 *
 *   npx tsx scripts/build-zone-pack.ts                       # all CA forecast, fire and county zones
 *   npx tsx scripts/build-zone-pack.ts --codes CAZ513,CAC085 # just these codes
 *   npx tsx scripts/build-zone-pack.ts --out /tmp/pack.json.gz --upload s3://bucket/zones/v1/california.json.gz
 *
 * Run it from a machine that can reach api.weather.gov. Do not run it in tests.
 */
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import {
  encodeZonePack,
  isZoneGeometry,
  ZONE_PACK_VERSION,
  type ZoneGeometry,
  type ZonePack,
} from '../src/geo/s3-zone-provider.js';

export type FetchFn = (
  url: string,
  init?: { headers?: Record<string, string>; signal?: AbortSignal },
) => Promise<{ ok: boolean; status: number; json(): Promise<unknown> }>;

export type ZoneType = 'forecast' | 'fire' | 'county';

/** A zone to fetch, with the NWS zone types to try in order. */
export interface ZoneRef {
  ugc: string;
  types: readonly ZoneType[];
}

export const NWS_API = 'https://api.weather.gov';
export const USER_AGENT = 'relay-capstone (SJSU)';
export const CALIFORNIA_ZONE_TYPES: readonly ZoneType[] = ['forecast', 'fire', 'county'];
export const BUILD_CONCURRENCY = 4;
const REQUEST_TIMEOUT_MS = 10_000;
const MAX_LIST_PAGES = 500;
const UGC_PATTERN = /^[A-Z]{2}[ZC]\d{3}$/;

const jsonHeaders = { Accept: 'application/geo+json', 'User-Agent': USER_AGENT };

/** Forecast zone ids (Z) can also be fire zones, so try forecast then fire, as NwsZoneProvider does. */
export function typesForUgc(ugc: string): readonly ZoneType[] {
  return ugc.charAt(2) === 'C' ? ['county'] : ['forecast', 'fire'];
}

/** Turns "CAZ513, cac085" into refs, rejecting anything that is not a UGC code. */
export function parseCodes(list: string): ZoneRef[] {
  const seen = new Set<string>();
  const refs: ZoneRef[] = [];
  for (const raw of list.split(',')) {
    const ugc = raw.trim().toUpperCase();
    if (!ugc || seen.has(ugc)) continue;
    if (!UGC_PATTERN.test(ugc)) throw new Error(`not a UGC zone code: ${raw.trim()}`);
    seen.add(ugc);
    refs.push({ ugc, types: typesForUgc(ugc) });
  }
  return refs;
}

async function getJson(fetchFn: FetchFn, url: string): Promise<unknown> {
  const res = await fetchFn(url, {
    headers: jsonHeaders,
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  if (!res.ok) throw new Error(`NWS request failed: ${url} HTTP ${res.status}`);
  return res.json();
}

interface ZoneListPage {
  features?: Array<{ properties?: { id?: unknown } }>;
  pagination?: { next?: unknown };
}

/**
 * Lists every zone of the given types in California, following pagination.next.
 * A zone id that appears under two types is kept once, under the first type listed.
 */
export async function listCaliforniaZones(
  fetchFn: FetchFn,
  types: readonly ZoneType[] = CALIFORNIA_ZONE_TYPES,
  baseUrl = NWS_API,
): Promise<ZoneRef[]> {
  const refs: ZoneRef[] = [];
  const seen = new Set<string>();
  for (const type of types) {
    let url: string | undefined = `${baseUrl}/zones?area=CA&type=${type}`;
    for (let page = 0; url && page < MAX_LIST_PAGES; page++) {
      const body = (await getJson(fetchFn, url)) as ZoneListPage;
      for (const feature of body.features ?? []) {
        const id = feature.properties?.id;
        if (typeof id !== 'string' || seen.has(id)) continue;
        seen.add(id);
        refs.push({ ugc: id.toUpperCase(), types: [type] });
      }
      url = typeof body.pagination?.next === 'string' ? body.pagination.next : undefined;
    }
  }
  return refs;
}

/** Fetches one zone's geometry, or null if NWS has none (404 or no geometry). */
async function fetchGeometry(
  fetchFn: FetchFn,
  ref: ZoneRef,
  baseUrl: string,
): Promise<ZoneGeometry | null> {
  for (const type of ref.types) {
    const res = await fetchFn(`${baseUrl}/zones/${type}/${ref.ugc}`, {
      headers: jsonHeaders,
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
    if (res.status === 404) continue;
    if (!res.ok) throw new Error(`NWS zone lookup failed for ${ref.ugc}: HTTP ${res.status}`);
    const body = (await res.json()) as { geometry?: unknown };
    if (isZoneGeometry(body.geometry)) return body.geometry;
  }
  return null;
}

export interface BuildOptions {
  refs: readonly ZoneRef[];
  fetchFn: FetchFn;
  builtAt?: Date;
  concurrency?: number;
  baseUrl?: string;
  onProgress?: (done: number, total: number) => void;
}

export interface BuildResult {
  pack: ZonePack;
  /** Codes NWS had no geometry for. They are not in the pack, so lookups fall back to NWS. */
  missing: string[];
}

/**
 * Fetches each zone with at most `concurrency` requests in flight. Output keys follow
 * the order of `refs`, so the same input always gives the same pack. A non-404 error
 * fails the build rather than writing a silently partial pack.
 */
export async function buildZonePack(options: BuildOptions): Promise<BuildResult> {
  const { refs, fetchFn } = options;
  const baseUrl = options.baseUrl ?? NWS_API;
  const concurrency = Math.max(1, Math.min(options.concurrency ?? BUILD_CONCURRENCY, refs.length));
  const results: Array<ZoneGeometry | null> = refs.map(() => null);
  let next = 0;
  let done = 0;
  let aborted = false;

  const worker = async (): Promise<void> => {
    while (!aborted && next < refs.length) {
      const index = next++;
      const ref = refs[index];
      if (!ref) break;
      try {
        results[index] = await fetchGeometry(fetchFn, ref, baseUrl);
      } catch (err) {
        aborted = true;
        throw err;
      }
      done++;
      options.onProgress?.(done, refs.length);
    }
  };
  await Promise.all(Array.from({ length: concurrency }, worker));

  const zones: Record<string, ZoneGeometry> = {};
  const missing: string[] = [];
  refs.forEach((ref, index) => {
    const geometry = results[index];
    if (geometry) zones[ref.ugc] = geometry;
    else missing.push(ref.ugc);
  });
  return {
    pack: {
      version: ZONE_PACK_VERSION,
      builtAt: (options.builtAt ?? new Date()).toISOString(),
      zones,
    },
    missing,
  };
}

/** Splits s3://bucket/key into its bucket and key. */
export function parseS3Uri(uri: string): { bucket: string; key: string } {
  const match = /^s3:\/\/([^/]+)\/(.+)$/.exec(uri);
  const [, bucket, key] = match ?? [];
  if (!bucket || !key) throw new Error(`expected s3://bucket/key, got ${uri}`);
  return { bucket, key };
}

export async function uploadZonePack(s3: S3Client, s3Uri: string, gz: Uint8Array): Promise<void> {
  const { bucket, key } = parseS3Uri(s3Uri);
  await s3.send(
    new PutObjectCommand({ Bucket: bucket, Key: key, Body: gz, ContentType: 'application/gzip' }),
  );
}

const DEFAULT_OUT = 'zones/v1/california.json.gz';

function log(msg: string, fields: Record<string, unknown> = {}, level = 'info'): void {
  console.error(JSON.stringify({ level, msg, ...fields }));
}

async function main(argv: string[]): Promise<void> {
  const { values } = parseArgs({
    args: argv,
    options: {
      codes: { type: 'string' },
      out: { type: 'string', default: DEFAULT_OUT },
      upload: { type: 'string' },
    },
    strict: true,
  });
  const fetchFn = fetch as unknown as FetchFn;

  const refs = values.codes
    ? parseCodes(values.codes)
    : await listCaliforniaZones(fetchFn, CALIFORNIA_ZONE_TYPES);
  log('zones.listed', { count: refs.length });

  const { pack, missing } = await buildZonePack({
    refs,
    fetchFn,
    onProgress: (done, total) => {
      if (done % 50 === 0 || done === total) log('zones.progress', { done, total });
    },
  });
  const gz = await encodeZonePack(pack);

  const out = values.out ?? DEFAULT_OUT;
  await mkdir(dirname(out), { recursive: true });
  await writeFile(out, gz);
  if (values.upload) {
    await uploadZonePack(new S3Client({}), values.upload, gz);
  }
  log('zones.pack_written', {
    out,
    uploaded: values.upload ?? null,
    zones: Object.keys(pack.zones).length,
    missing: missing.length,
    bytes: gz.byteLength,
  });
  if (missing.length > 0) log('zones.missing', { ugc: missing });
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch((err: unknown) => {
    log('zones.build_failed', { error: err instanceof Error ? err.message : String(err) }, 'error');
    process.exitCode = 1;
  });
}
