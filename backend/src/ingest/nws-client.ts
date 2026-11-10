/**
 * Minimal NWS client for the California active-alerts feed.
 * It remembers Last-Modified / ETag from the previous response and sends them back,
 * so NWS answers 304 when nothing changed. It never throws: failures come back as a result.
 * It also keeps the request rate polite: a minimum spacing between requests, backoff after
 * 429/503, Retry-After, and a cap on how much body it will read.
 */
export const NWS_BASE_URL = 'https://api.weather.gov';
const TIMEOUT_MS = 8000;

/**
 * NWS: "We recommend you make requests of the server no more than every 30 seconds"
 * (https://www.weather.gov/documentation/services-web-alerts). Nothing in Relay may request the
 * same URL faster than this; configured intervals below it are raised to it.
 */
export const MIN_POLL_INTERVAL_MS = 30_000;
/** A feed response bigger than this is refused (SEC-B-10). California's feed is a few MB at most. */
export const MAX_RESPONSE_BYTES = 16 * 1024 * 1024;
/** Backoff after 429/503 with no Retry-After: 30 s, doubling, capped. */
const BACKOFF_BASE_MS = 30_000;
const BACKOFF_MAX_MS = 5 * 60_000;
/** A Retry-After longer than this is capped (we would rather probe again than sleep for hours). */
const RETRY_AFTER_MAX_MS = 10 * 60_000;

export type NwsFetchResult =
  | { status: 'not_modified' }
  | { status: 'ok'; body: string; fetchedAt: Date }
  /** Not requested: too soon after the last request, or backing off. Not a failure. */
  | { status: 'throttled'; retryInMs: number; reason: 'min_interval' | 'backoff' }
  | { status: 'error'; httpStatus?: number; message: string; retryAfterMs?: number };

/**
 * Request pacing shared by every client in one warm container, so two invocations that run one
 * after the other cannot exceed the NWS rate. Lambda reserved concurrency keeps a second
 * container from running at all.
 */
export class RequestGate {
  lastStartedAt = 0;
  notBefore = 0;
  consecutiveBackoffs = 0;
}
/** The gate the real handler uses. */
export const sharedGate = new RequestGate();

/** Retry-After as milliseconds: delta-seconds or an HTTP date. null when absent or unreadable. */
export function parseRetryAfter(raw: string | null, nowMs: number): number | null {
  if (raw === null || raw.trim() === '') return null;
  const v = raw.trim();
  if (/^\d+$/.test(v)) return Math.min(Number(v) * 1000, RETRY_AFTER_MAX_MS);
  const at = Date.parse(v);
  if (Number.isNaN(at)) return null;
  return Math.min(Math.max(0, at - nowMs), RETRY_AFTER_MAX_MS);
}

class ResponseTooLargeError extends Error {
  constructor(maxBytes: number) {
    super(`NWS response larger than ${maxBytes} bytes; refused`);
    this.name = 'ResponseTooLargeError';
  }
}

/** Reads a body, refusing anything over maxBytes (Content-Length first, then while streaming). */
async function readBounded(res: Response, maxBytes: number): Promise<string> {
  const declared = Number(res.headers.get('Content-Length'));
  if (Number.isFinite(declared) && declared > maxBytes) {
    await res.body?.cancel().catch(() => undefined);
    throw new ResponseTooLargeError(maxBytes);
  }
  if (!res.body) {
    const text = await res.text();
    if (Buffer.byteLength(text) > maxBytes) throw new ResponseTooLargeError(maxBytes);
    return text;
  }
  const reader = res.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > maxBytes) {
      await reader.cancel().catch(() => undefined);
      throw new ResponseTooLargeError(maxBytes);
    }
    chunks.push(value);
  }
  return Buffer.concat(chunks).toString('utf8');
}

export interface NwsClient {
  fetchActiveCalifornia(): Promise<NwsFetchResult>;
  /**
   * Keep the ETag / Last-Modified of the last "ok" response, so the next fetch can get a 304.
   * Call it only after the response was fully processed. If it is not called (processing
   * failed), the next fetch re-sends the older validators and gets the full body again.
   */
  commit(): void;
}

export interface NwsClientOptions {
  fetch: typeof fetch;
  /** NWS asks every client to identify itself, e.g. "Relay (contact@example.org)". */
  userAgent: string;
  baseUrl?: string;
  /** Largest body accepted (default MAX_RESPONSE_BYTES). */
  maxBodyBytes?: number;
  /**
   * Minimum time between request starts, enforced through `gate`. Default 0 (off, for tests); the
   * handler passes MIN_POLL_INTERVAL_MS. A non-zero value below MIN_POLL_INTERVAL_MS is raised.
   */
  minIntervalMs?: number;
  gate?: RequestGate;
  now?(): number;
}

export function createNwsClient(opts: NwsClientOptions): NwsClient {
  const baseUrl = opts.baseUrl ?? NWS_BASE_URL;
  const maxBytes = opts.maxBodyBytes ?? MAX_RESPONSE_BYTES;
  const minInterval = opts.minIntervalMs ? Math.max(opts.minIntervalMs, MIN_POLL_INTERVAL_MS) : 0;
  const gate = opts.gate ?? new RequestGate();
  const nowMs = opts.now ?? Date.now;
  let lastModified: string | null = null;
  let etag: string | null = null;
  // Validators of the latest response, not trusted until commit().
  let pending: { lastModified: string | null; etag: string | null } | null = null;

  return {
    commit() {
      if (pending) ({ lastModified, etag } = pending);
      pending = null;
    },
    async fetchActiveCalifornia() {
      const startedAt = nowMs();
      if (gate.notBefore > startedAt) {
        return { status: 'throttled', retryInMs: gate.notBefore - startedAt, reason: 'backoff' };
      }
      if (minInterval > 0 && startedAt - gate.lastStartedAt < minInterval) {
        return {
          status: 'throttled',
          retryInMs: minInterval - (startedAt - gate.lastStartedAt),
          reason: 'min_interval',
        };
      }
      gate.lastStartedAt = startedAt;
      pending = null;

      const headers: Record<string, string> = {
        'User-Agent': opts.userAgent,
        Accept: 'application/geo+json',
      };
      if (lastModified) headers['If-Modified-Since'] = lastModified;
      if (etag) headers['If-None-Match'] = etag;

      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
      try {
        const res = await opts.fetch(`${baseUrl}/alerts/active?area=CA`, {
          method: 'GET',
          headers,
          signal: controller.signal,
        });
        if (res.status === 304) {
          gate.consecutiveBackoffs = 0;
          return { status: 'not_modified' };
        }
        if (!res.ok) {
          const retryAfter = parseRetryAfter(res.headers.get('Retry-After'), nowMs());
          let retryAfterMs: number | undefined;
          if (res.status === 429 || res.status === 503 || retryAfter !== null) {
            gate.consecutiveBackoffs += 1;
            const backoff = Math.min(
              BACKOFF_BASE_MS * 2 ** (gate.consecutiveBackoffs - 1),
              BACKOFF_MAX_MS,
            );
            retryAfterMs = Math.max(retryAfter ?? 0, backoff);
            gate.notBefore = nowMs() + retryAfterMs;
          }
          return {
            status: 'error',
            httpStatus: res.status,
            message: `NWS returned HTTP ${res.status}`,
            ...(retryAfterMs !== undefined ? { retryAfterMs } : {}),
          };
        }
        gate.consecutiveBackoffs = 0;
        const body = await readBounded(res, maxBytes);
        // Held back until the caller commits: a failed poll must not turn into a 304 next time.
        pending = { lastModified: res.headers.get('Last-Modified'), etag: res.headers.get('ETag') };
        return { status: 'ok', body, fetchedAt: new Date() };
      } catch (err) {
        return {
          status: 'error',
          message: controller.signal.aborted
            ? `NWS request timed out after ${TIMEOUT_MS} ms`
            : err instanceof Error
              ? err.message
              : String(err),
        };
      } finally {
        clearTimeout(timer);
      }
    },
  };
}
