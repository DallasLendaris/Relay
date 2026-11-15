import { z } from 'zod';

/**
 * Input bounds. One place, used by the zod schemas, the API handler and (checked by a contract
 * test) the OpenAPI spec. Every string, array and record that accepts external input is bounded
 * so an oversized or hostile payload is rejected cheaply. Defence in depth for zod advisory
 * GHSA-mrhx-c7xc-m3w5 (validating huge constrained collections can exhaust memory; fixed in zod
 * 4.6.6, and the repo pins zod ^4.6.7).
 */
export const LIMITS = {
  /** Largest request body the API reads, in bytes. */
  maxRequestBodyBytes: 64 * 1024,
  labelMax: 60,
  addressMax: 200,
  /** APNs device tokens are 32 bytes (64 hex); Apple reserves room to grow, so allow up to 200. */
  apnsTokenMin: 64,
  apnsTokenMax: 200,
  fcmTokenMin: 100,
  fcmTokenMax: 4096,
  expoTokenMax: 200,
  idempotencyKeyMax: 200,
  /** Signed check-in token (payload.signature); real ones are well under 300 characters. */
  checkInTokenMax: 1024,
  /** Largest free-text field of an NWS alert (description, instruction). */
  alertTextMax: 50_000,
  alertShortMax: 1_000,
  /** Coordinate rings: NWS polygons are small, 50k vertices is far above any real one. */
  maxPolygonRings: 1_000,
  maxRingPositions: 50_000,
  maxMultiPolygonParts: 1_000,
  maxUgcCodes: 2_000,
  maxSameCodes: 2_000,
  /** Roughly every res-7 cell in California (about 90k) with headroom. */
  maxH3Cells: 200_000,
  maxReferences: 200,
  maxSummaries: 20,
} as const;

/** True when the string holds a NUL or other C0/C1 control character (tab and newline included). */
export const hasControlChars = (s: string): boolean => {
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c <= 0x1f || (c >= 0x7f && c <= 0x9f)) return true;
  }
  return false;
};

const noControl = (s: string): boolean => !hasControlChars(s);
const noControlMsg = { error: 'must not contain control characters' };

/** A place label as sent by a client (the API trims it before checking). */
export const PlaceLabelSchema = z
  .string()
  .min(1)
  .max(LIMITS.labelMax)
  .refine(noControl, noControlMsg);

export const PlaceAddressSchema = z
  .string()
  .min(1)
  .max(LIMITS.addressMax)
  .refine(noControl, noControlMsg);

export const ApnsTokenSchema = z
  .string()
  .regex(new RegExp(`^[0-9a-fA-F]{${LIMITS.apnsTokenMin},${LIMITS.apnsTokenMax}}$`));

export const FcmTokenSchema = z
  .string()
  .regex(new RegExp(`^[A-Za-z0-9_:-]{${LIMITS.fcmTokenMin},${LIMITS.fcmTokenMax}}$`));

export const ExpoTokenSchema = z
  .string()
  .max(LIMITS.expoTokenMax)
  .regex(/^Expo(nent)?PushToken\[[^\]\s]+\]$/);
