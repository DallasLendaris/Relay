import { z } from 'zod';
import { AlertSchema, AlertStateSchema } from './alert.js';

/**
 * What the CDN serves for one alert version, and what the push `data` carries to find it.
 * Layout (relative to the CDN base URL; see backend/src/scale/publish/paths.ts):
 *   /alerts/v1/doc/{alertId}/{hash}.json   PublicAlertDocument, immutable (cached for a year)
 *   /alerts/v1/latest/{alertId}.json       AlertPointer, short TTL (names the current document)
 *   /alerts/v1/active/{shard}.json         per-shard index of active alerts
 */

/**
 * The cell-free, immutable document for one alert version. It carries everything the app shows
 * and NOT the H3 cell list (a statewide alert lists about 82,000 cells, about 1.5 MB). Strict:
 * an unexpected field such as `area.h3Cells` fails validation.
 *
 * Authenticity: `signature` is the alert's own signature (`v1.<kid>.<mac>` today), computed over
 * the full alert including its cells. `area.cellsSha256` and `area.cellCount` bind this document
 * to those cells, so anyone holding the full alert can rebuild this document and compare. The
 * signature is symmetric, so a phone cannot verify it; a phone trusts the document because it came
 * from the Relay CDN origin over TLS, and the bucket is writable only by the publisher.
 */
export const PublicAlertDocumentSchema = z.strictObject({
  ...AlertSchema.omit({ area: true }).shape,
  version: z.literal(1),
  area: z.strictObject({
    description: z.string(),
    /** Number of H3 cells in the full alert. */
    cellCount: z.int().nonnegative(),
    /** Hex SHA-256 of the alert's sorted cell ids joined with "\n". */
    cellsSha256: z.string().regex(/^[0-9a-f]{64}$/),
  }),
});
export type PublicAlertDocument = z.infer<typeof PublicAlertDocumentSchema>;

/**
 * Short-TTL pointer to the current document of one alert. Phones that opened a document from a
 * push re-read the pointer to learn about updates and cancellations.
 */
export const AlertPointerSchema = z.strictObject({
  version: z.literal(1),
  alertId: z.guid(),
  sourceId: z.string(),
  state: AlertStateSchema,
  expires: z.iso.datetime({ offset: true }),
  /** Path (with leading slash, relative to the CDN base URL) of the current document. */
  doc: z.string().startsWith('/alerts/v1/doc/'),
});
export type AlertPointer = z.infer<typeof AlertPointerSchema>;

/** The `data` map of a push notification. Every value is a string on the wire. */
export const PushDataSchema = z.looseObject({
  alertId: z.guid(),
  checkInToken: z.string(),
  expiresAt: z.iso.datetime({ offset: true }),
  /**
   * Absolute URL of this alert's PublicAlertDocument on the CDN. Absent when the sender has no
   * CDN base configured, or when the payload had to be trimmed: then read GET /alerts/{alertId}.
   */
  alertUrl: z.url().optional(),
  /**
   * Set only on a follow-up push (docs/cancellation.md): `cancel` (the alert was cancelled),
   * `update` (it changed and still covers this device), `removed` (it changed and no longer covers
   * this device). The app should not offer "I'm okay" for `cancel` and `removed`.
   */
  followUp: z.enum(['cancel', 'update', 'removed']).optional(),
});
export type PushData = z.infer<typeof PushDataSchema>;
