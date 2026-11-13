/**
 * Jurisdictions are sets of California county FIPS codes ("06085" is Santa Clara County).
 *
 * A user's jurisdiction comes only from the `custom:jurisdiction` attribute, which only an
 * administrator can write (the app client has no write access to it), copied into the access
 * token by the pre-token-generation trigger. Nothing the client sends widens it: a request may
 * only NARROW its scope to some of those counties, and asking for any county outside it is
 * refused as a whole (and audited) rather than silently trimmed, so probing is visible.
 */

/** California's state FIPS code. Relay is California-only today. */
export const CA_STATE_FIPS = '06';
/** California has 58 counties, FIPS 06001 to 06115, odd numbers only. */
export const CA_COUNTY_COUNT = 58;
/** Most counties one request may name (all of California). */
export const MAX_AREAS = CA_COUNTY_COUNT;

/** True for a real California county FIPS code (06 + an odd number from 001 to 115). */
export function isCaliforniaCountyFips(code: string): boolean {
  if (!/^06\d{3}$/.test(code)) return false;
  const county = Number(code.slice(2));
  return county >= 1 && county <= 115 && county % 2 === 1;
}

/**
 * Parses a comma-separated county list (the attribute and the `areas` query parameter use the
 * same format). Returns the sorted, de-duplicated codes, or null when ANY entry is invalid: a
 * malformed jurisdiction is a configuration error and must not degrade to a partial one.
 */
export function parseCountyList(raw: string): string[] | null {
  const parts = raw
    .split(',')
    .map((p) => p.trim())
    .filter((p) => p !== '');
  if (parts.length > MAX_AREAS * 2) return null;
  if (!parts.every(isCaliforniaCountyFips)) return null;
  return [...new Set(parts)].sort();
}

/** The NWS county zone (UGC) of a California county: 06085 -> CAC085. */
export const countyUgc = (fips: string): string => `CAC${fips.slice(2)}`;

export type AreaCheck =
  | { ok: true; areas: string[] }
  | { ok: false; reason: 'invalid_areas' }
  | { ok: false; reason: 'area_outside_jurisdiction'; requested: string[] };

/**
 * The counties a request may read: every county in the jurisdiction when the request names none,
 * otherwise the named ones, all of which must be inside the jurisdiction.
 */
export function resolveRequestedAreas(
  requested: string | undefined,
  jurisdiction: ReadonlySet<string>,
): AreaCheck {
  if (requested === undefined || requested.trim() === '') {
    return { ok: true, areas: [...jurisdiction].sort() };
  }
  const parsed = parseCountyList(requested);
  if (parsed === null || parsed.length === 0) return { ok: false, reason: 'invalid_areas' };
  if (parsed.some((c) => !jurisdiction.has(c))) {
    return { ok: false, reason: 'area_outside_jurisdiction', requested: parsed };
  }
  return { ok: true, areas: parsed };
}
