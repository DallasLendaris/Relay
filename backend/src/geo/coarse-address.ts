/**
 * Coarse address text for a saved place (audit PRIV-5).
 *
 * Decision: the server stores NO house number. `shortAddress` is "<street name>, <locality>"
 * (for example "Elm St, San José"), or the locality alone when the street cannot be separated
 * from the number, or nothing at all. A street name is shared by many homes, so it does not
 * identify one. The full address the user typed stays on the device.
 */

/** Digits with an optional letter or unit suffix: "1600", "12-B", "221B", "12-14", "12 1/2". */
const LEADING_NUMBER =
  /^\s*\d+(?:-[0-9A-Za-z]+|[A-Za-z](?![A-Za-z]))?(?:\s+\d\/\d)?(?=\s|,|$)[\s,]*/;

/** Spelled-out building numbers ("One Market St"). Stripped only when a street name follows. */
const LEADING_NUMBER_WORD =
  /^\s*(?:one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|hundred|thousand)\s+(?=\S)/i;

/** Remove leading house-number tokens from street text. May return ''. */
export function stripHouseNumber(text: string): string {
  let out = text.trim();
  for (let i = 0; i < 3; i++) {
    const next = out.replace(LEADING_NUMBER, '').replace(LEADING_NUMBER_WORD, '');
    if (next === out) break;
    out = next;
  }
  return out.trim();
}

/** Build the coarse value from separate street and locality parts. May return ''. */
export function coarseAddress(street: string | undefined, locality: string | undefined): string {
  const name = stripHouseNumber(street ?? '');
  const place = (locality ?? '').trim();
  return [name, place].filter(Boolean).join(', ');
}

/**
 * Defense in depth for a value that is already joined ("12 Elm St, San José"): drop a leading
 * house number. Idempotent.
 */
export function coarsenShortAddress(value: string): string {
  return stripHouseNumber(value);
}
