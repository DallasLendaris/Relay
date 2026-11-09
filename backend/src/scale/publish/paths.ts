import { createHash } from 'node:crypto';

/**
 * Public (CDN-served) object layout. The app builds URLs from these helpers, relative to the
 * CloudFront domain, e.g. `https://<domain>${PUBLIC_ALERT_PATH(id)}`.
 *
 *   alerts/v1/doc/{alertId}/{hash}.json   cell-free alert document, immutable, cached for a year.
 *                                         {hash} covers sourceId, signature and state, so any new
 *                                         version of an alert has a new URL.
 *   alerts/v1/latest/{alertId}.json       short-TTL pointer naming the current document.
 *   alerts/v1/active/{shard}.json         summaries of the alerts active in one res-4 shard.
 *
 * Staleness bound: a document never changes, so an update or cancellation is visible only through
 * the pointer (and the shard index), which phones may see up to PUBLIC_MAX_STALENESS_SECONDS (75 s)
 * after it was written. The old full-alert object alerts/v1/{alertId}.json is gone.
 */
export const PUBLIC_PREFIX = 'alerts/v1';

/** The fields that make one alert version distinct. */
export interface AlertVersionRef {
  id: string;
  sourceId: string;
  signature: string | null;
  state: string;
}

/** Short content address of one alert version (16 hex chars of SHA-256). */
export function alertVersionHash(a: AlertVersionRef): string {
  return createHash('sha256')
    .update(`${a.sourceId}\n${a.signature ?? ''}\n${a.state}`)
    .digest('hex')
    .slice(0, 16);
}

/** S3 key of one alert version's cell-free document. */
export function publicAlertDocKey(a: AlertVersionRef): string {
  return `${PUBLIC_PREFIX}/doc/${a.id}/${alertVersionHash(a)}.json`;
}

/** S3 key of one alert's "latest" pointer. */
export function publicAlertPointerKey(alertId: string): string {
  return `${PUBLIC_PREFIX}/latest/${alertId}.json`;
}

/** S3 key of one res-4 shard's active-alert index. */
export function activeShardKey(shardId: string): string {
  return `${PUBLIC_PREFIX}/active/${shardId}.json`;
}

/** URL path (leading slash) of one alert version's document. */
export function PUBLIC_ALERT_DOC_PATH(a: AlertVersionRef): string {
  return `/${publicAlertDocKey(a)}`;
}

/** URL path of one alert's latest pointer. */
export function PUBLIC_ALERT_PATH(alertId: string): string {
  return `/${publicAlertPointerKey(alertId)}`;
}

/** URL path of one shard's active index. */
export function PUBLIC_ACTIVE_SHARD_PATH(shardId: string): string {
  return `/${activeShardKey(shardId)}`;
}

/**
 * Absolute URL of one alert version's document for the push `data`, or undefined when no base is
 * configured. `base` is the CDN origin, e.g. https://dxxxx.cloudfront.net (a trailing slash is ok).
 */
export function publicAlertDocUrl(
  base: string | undefined,
  a: AlertVersionRef,
): string | undefined {
  const origin = base?.trim().replace(/\/+$/, '');
  return origin ? `${origin}${PUBLIC_ALERT_DOC_PATH(a)}` : undefined;
}

/**
 * Short TTL for pointers and shard indexes so updates and cancellations reach phones within
 * seconds, with stale-while-revalidate so a million phones collapse into about one origin fetch
 * per CloudFront edge.
 */
export const PUBLIC_CACHE_CONTROL = 'public, max-age=15, stale-while-revalidate=60';

/** Worst case between publishing a pointer or shard index and a phone seeing it: 15 s + 60 s. */
export const PUBLIC_MAX_STALENESS_SECONDS = 75;

/** Documents are immutable (their key changes with the content): cache for a year. */
export const PUBLIC_DOC_CACHE_CONTROL = 'public, max-age=31536000, immutable';
