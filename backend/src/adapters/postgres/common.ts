/** Shared SQL snippets for the Postgres stores. */

/** Max cells per query; keeps the Data API request under its 64 KB limit. */
export const CELL_CHUNK = 2000;

/** ISO-8601 UTC text for a timestamptz column, identical on every driver. */
export const isoUtc = (column: string): string =>
  `to_char(${column} AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`;

/** Turn a JSON-array parameter (as text) into a text[] without needing array parameters. */
export const textArrayParam = (n: number): string =>
  `ARRAY(SELECT jsonb_array_elements_text($${n}::jsonb))`;

export function chunk<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}
