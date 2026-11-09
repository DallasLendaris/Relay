/**
 * Public (CDN-served) object layout. The app team builds URLs from these helpers, relative to the
 * CloudFront domain, e.g. `https://<domain>${PUBLIC_ALERT_PATH(id)}`.
 */
export const PUBLIC_PREFIX = 'alerts/v1';

/** S3 key of one alert's public JSON. */
export function publicAlertKey(alertId: string): string {
  return `${PUBLIC_PREFIX}/${alertId}.json`;
}

/** S3 key of one res-4 shard's active-alert index. */
export function activeShardKey(shardId: string): string {
  return `${PUBLIC_PREFIX}/active/${shardId}.json`;
}

/** URL path (with leading slash) of one alert, as the phone requests it through CloudFront. */
export function PUBLIC_ALERT_PATH(alertId: string): string {
  return `/${publicAlertKey(alertId)}`;
}

/** URL path of one shard's active index. */
export function PUBLIC_ACTIVE_SHARD_PATH(shardId: string): string {
  return `/${activeShardKey(shardId)}`;
}

/**
 * Short TTL so updates and cancellations reach phones within seconds, with stale-while-revalidate so
 * a million phones opening one alert collapse into about one origin fetch per CloudFront edge.
 */
export const PUBLIC_CACHE_CONTROL = 'public, max-age=15, stale-while-revalidate=60';
