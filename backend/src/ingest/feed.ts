/** Parses an NWS active-alerts response body. Never throws. */
export function parseFeed(body: string): { features: unknown[] } | { error: string } {
  let json: unknown;
  try {
    json = JSON.parse(body);
  } catch {
    return { error: 'body is not valid JSON' };
  }
  if (typeof json !== 'object' || json === null) return { error: 'body is not a JSON object' };
  const doc = json as Record<string, unknown>;
  if (doc['type'] !== 'FeatureCollection') return { error: 'body is not a FeatureCollection' };
  if (!Array.isArray(doc['features'])) return { error: 'FeatureCollection has no features array' };
  return { features: doc['features'] };
}
