/**
 * Parses a Retry-After header value: delta-seconds ("30") or an HTTP date. Returns whole seconds
 * to wait (never negative), or undefined when the header is absent or unusable.
 */
export function parseRetryAfter(
  value: string | string[] | undefined,
  nowMs: number = Date.now(),
): number | undefined {
  const raw = (Array.isArray(value) ? value[0] : value)?.trim();
  if (!raw) return undefined;
  if (/^\d+(\.\d+)?$/.test(raw)) return Math.min(86_400, Math.ceil(Number(raw)));
  const at = Date.parse(raw);
  if (Number.isNaN(at)) return undefined;
  return Math.min(86_400, Math.max(0, Math.ceil((at - nowMs) / 1000)));
}
