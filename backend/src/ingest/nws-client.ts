/**
 * Minimal NWS client for the California active-alerts feed.
 * It remembers Last-Modified / ETag from the previous response and sends them back,
 * so NWS answers 304 when nothing changed. It never throws: failures come back as a result.
 */
export const NWS_BASE_URL = 'https://api.weather.gov';
const TIMEOUT_MS = 8000;

export type NwsFetchResult =
  | { status: 'not_modified' }
  | { status: 'ok'; body: string; fetchedAt: Date }
  | { status: 'error'; httpStatus?: number; message: string };

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
}

export function createNwsClient(opts: NwsClientOptions): NwsClient {
  const baseUrl = opts.baseUrl ?? NWS_BASE_URL;
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
        if (res.status === 304) return { status: 'not_modified' };
        if (!res.ok) {
          return {
            status: 'error',
            httpStatus: res.status,
            message: `NWS returned HTTP ${res.status}`,
          };
        }
        const body = await res.text();
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
