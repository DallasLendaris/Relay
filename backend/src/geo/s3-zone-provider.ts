import { promisify } from 'node:util';
import { gunzip, gzip } from 'node:zlib';
import { GetObjectCommand, type S3Client } from '@aws-sdk/client-s3';
import type { MultiPolygon, Polygon } from 'geojson';
import type { ZoneGeometryProvider } from './zones.js';

export type ZoneGeometry = Polygon | MultiPolygon;

export const ZONE_PACK_VERSION = 1;

/**
 * Pre-built geometry for many NWS zones, stored as one gzipped JSON object at
 * zones/v1/california.json.gz. Keys are UGC codes (CAZ513, CAC085).
 */
export interface ZonePack {
  version: typeof ZONE_PACK_VERSION;
  builtAt: string;
  zones: Record<string, ZoneGeometry>;
}

/** Raised when the pack cannot be read or decoded. Callers fall back to NWS on this. */
export class ZonePackError extends Error {}

const gzipAsync = promisify(gzip);
const gunzipAsync = promisify(gunzip);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

export const isZoneGeometry = (value: unknown): value is ZoneGeometry =>
  isRecord(value) &&
  (value.type === 'Polygon' || value.type === 'MultiPolygon') &&
  Array.isArray(value.coordinates);

/** Checks the shape of a decoded pack. Throws on anything the lookup could not trust. */
export function parseZonePack(value: unknown): ZonePack {
  if (!isRecord(value)) throw new Error('zone pack is not a JSON object');
  if (value.version !== ZONE_PACK_VERSION) {
    throw new Error(`unsupported zone pack version ${String(value.version)}`);
  }
  if (typeof value.builtAt !== 'string') throw new Error('zone pack has no builtAt');
  if (!isRecord(value.zones)) throw new Error('zone pack has no zones object');
  for (const [ugc, geometry] of Object.entries(value.zones)) {
    if (!isZoneGeometry(geometry)) {
      throw new Error(`zone pack entry ${ugc} is not a Polygon or MultiPolygon`);
    }
  }
  return value as unknown as ZonePack;
}

export async function encodeZonePack(pack: ZonePack): Promise<Buffer> {
  return gzipAsync(Buffer.from(JSON.stringify(pack), 'utf8'));
}

export async function decodeZonePack(gz: Uint8Array): Promise<ZonePack> {
  const json = (await gunzipAsync(gz)).toString('utf8');
  return parseZonePack(JSON.parse(json) as unknown);
}

/** How long a failed load is remembered before the next lookup tries S3 again. */
const RETRY_AFTER_MS = 60_000;

export interface S3ZoneProviderOptions {
  retryAfterMs?: number;
  now?: () => number;
}

/**
 * Serves zone geometry from a pack in S3. The pack is read once per warm Lambda,
 * on first use. A successful load is kept for the life of the container. A failed
 * load is remembered for a short cooldown, so an S3 outage does not hit S3 on every
 * alert. Lookups throw ZonePackError while the pack is unavailable.
 */
export class S3ZoneProvider implements ZoneGeometryProvider {
  private loaded: ZonePack | null = null;
  private inFlight: Promise<ZonePack> | null = null;
  private lastError: ZonePackError | null = null;
  private failedAt = 0;
  private readonly retryAfterMs: number;
  private readonly now: () => number;

  constructor(
    private readonly bucket: string,
    private readonly key: string,
    private readonly s3: S3Client,
    options: S3ZoneProviderOptions = {},
  ) {
    this.retryAfterMs = options.retryAfterMs ?? RETRY_AFTER_MS;
    this.now = options.now ?? Date.now;
  }

  async getZoneGeometry(ugc: string): Promise<ZoneGeometry | null> {
    const pack = await this.ensurePack();
    return pack.zones[ugc.toUpperCase()] ?? null;
  }

  private ensurePack(): Promise<ZonePack> {
    if (this.loaded) return Promise.resolve(this.loaded);
    if (this.lastError && this.now() - this.failedAt < this.retryAfterMs) {
      return Promise.reject(this.lastError);
    }
    if (!this.inFlight) {
      const pending = this.load();
      this.inFlight = pending;
      // Clear after the assignment above, so a synchronous failure cannot leave a stale promise.
      const clear = (): void => {
        this.inFlight = null;
      };
      pending.then(clear, clear);
    }
    return this.inFlight;
  }

  private async load(): Promise<ZonePack> {
    try {
      this.loaded = await this.fetchPack();
      this.lastError = null;
      return this.loaded;
    } catch (cause) {
      this.lastError = new ZonePackError(
        `zone pack s3://${this.bucket}/${this.key} unavailable: ${errorMessage(cause)}`,
        { cause },
      );
      this.failedAt = this.now();
      throw this.lastError;
    }
  }

  private async fetchPack(): Promise<ZonePack> {
    const res = await this.s3.send(new GetObjectCommand({ Bucket: this.bucket, Key: this.key }));
    if (!res.Body) throw new Error('empty response body');
    return decodeZonePack(await res.Body.transformToByteArray());
  }
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
