import { fileURLToPath } from 'node:url';
import SwaggerParser from '@apidevtools/swagger-parser';
import { beforeAll, describe, expect, it } from 'vitest';
import {
  AlertSchema,
  ApnsTokenSchema,
  ExpoTokenSchema,
  FcmTokenSchema,
  LIMITS,
  PlaceAddressSchema,
  PlaceLabelSchema,
} from '../src/index.js';
import { validAlert } from './fixtures.js';

type Schema = { maxLength?: number; minLength?: number; properties?: Record<string, Schema> };
let schemas: Record<string, Schema>;
beforeAll(async () => {
  const api = (await SwaggerParser.dereference(
    fileURLToPath(new URL('../openapi/relay.yaml', import.meta.url)),
  )) as { components: { schemas: Record<string, Schema> } };
  schemas = api.components.schemas;
});
const prop = (schema: string, name: string): Schema => {
  const p = schemas[schema]?.properties?.[name];
  if (!p) throw new Error(`spec has no ${schema}.${name}`);
  return p;
};

describe('spec and zod agree on request bounds (SEC-B-11)', () => {
  it('label', () => {
    for (const s of ['CreatePlaceByAddress', 'CreatePlaceByCoordinates', 'Place']) {
      expect(prop(s, 'label').maxLength).toBe(LIMITS.labelMax);
    }
    expect(PlaceLabelSchema.safeParse('a'.repeat(LIMITS.labelMax)).success).toBe(true);
    expect(PlaceLabelSchema.safeParse('a'.repeat(LIMITS.labelMax + 1)).success).toBe(false);
  });

  it('address', () => {
    expect(prop('CreatePlaceByAddress', 'address').maxLength).toBe(LIMITS.addressMax);
    expect(PlaceAddressSchema.safeParse('a'.repeat(LIMITS.addressMax)).success).toBe(true);
    expect(PlaceAddressSchema.safeParse('a'.repeat(LIMITS.addressMax + 1)).success).toBe(false);
    expect(PlaceAddressSchema.safeParse('a\u0000b').success).toBe(false);
  });

  it('native push token covers every platform', () => {
    const token = prop('NativePushTokenRequest', 'token');
    expect(token.maxLength).toBe(Math.max(LIMITS.fcmTokenMax, LIMITS.apnsTokenMax));
    expect(token.minLength).toBe(LIMITS.apnsTokenMin);
    const fcm = (n: number) => FcmTokenSchema.safeParse('a'.repeat(n)).success;
    expect(fcm(LIMITS.fcmTokenMax)).toBe(true);
    expect(fcm(LIMITS.fcmTokenMax + 1)).toBe(false);
    const apns = (n: number) => ApnsTokenSchema.safeParse('a'.repeat(n)).success;
    expect(apns(LIMITS.apnsTokenMax)).toBe(true);
    expect(apns(LIMITS.apnsTokenMax + 1)).toBe(false);
    expect(apns(LIMITS.apnsTokenMin - 1)).toBe(false);
  });

  it('expo token', () => {
    expect(prop('ExpoPushTokenRequest', 'expoPushToken').maxLength).toBe(LIMITS.expoTokenMax);
    const pad = 'ExponentPushToken[]'.length;
    const expo = (n: number) =>
      ExpoTokenSchema.safeParse(`ExponentPushToken[${'a'.repeat(n - pad)}]`).success;
    expect(expo(LIMITS.expoTokenMax)).toBe(true);
    expect(expo(LIMITS.expoTokenMax + 1)).toBe(false);
  });

  it('check-in token and idempotency key', () => {
    for (const s of ['CreateCheckInRequest', 'CheckIn']) {
      expect(prop(s, 'token').maxLength).toBe(LIMITS.checkInTokenMax);
      expect(prop(s, 'idempotencyKey').maxLength).toBe(LIMITS.idempotencyKeyMax);
    }
  });
});

describe('AlertSchema bounds (DEP-1)', () => {
  const withArea = (area: Partial<typeof validAlert.area>) => ({
    ...validAlert,
    area: { ...validAlert.area, ...area },
  });
  const many = (n: number) => Array.from({ length: n }, () => 'x');

  it('rejects oversized arrays and strings', () => {
    const ok = (v: unknown) => AlertSchema.safeParse(v).success;
    expect(ok(withArea({ ugcCodes: many(LIMITS.maxUgcCodes + 1) }))).toBe(false);
    expect(ok(withArea({ sameCodes: many(LIMITS.maxSameCodes + 1) }))).toBe(false);
    expect(ok(withArea({ h3Cells: many(LIMITS.maxH3Cells + 1) }))).toBe(false);
    expect(ok(withArea({ description: 'x'.repeat(LIMITS.alertTextMax + 1) }))).toBe(false);
    expect(ok({ ...validAlert, event: 'x'.repeat(LIMITS.alertShortMax + 1) })).toBe(false);
    const ref = validAlert.references[0];
    expect(
      ok({
        ...validAlert,
        references: Array.from({ length: LIMITS.maxReferences + 1 }, () => ref),
      }),
    ).toBe(false);
    // And the maximum itself is fine.
    expect(ok(withArea({ ugcCodes: many(LIMITS.maxUgcCodes) }))).toBe(true);
  });

  it('rejects an oversized polygon ring and accepts the maximum', () => {
    const ring = (n: number) => Array.from({ length: n }, () => [-121.9, 37.3]);
    const poly = (n: number) => withArea({ geometry: { type: 'Polygon', coordinates: [ring(n)] } });
    expect(AlertSchema.safeParse(poly(LIMITS.maxRingPositions)).success).toBe(true);
    expect(AlertSchema.safeParse(poly(LIMITS.maxRingPositions + 1)).success).toBe(false);
  });

  it('rejects a position with more than 3 numbers', () => {
    const bad = withArea({ geometry: { type: 'Polygon', coordinates: [[[1, 2, 3, 4]]] } });
    expect(AlertSchema.safeParse(bad).success).toBe(false);
  });
});
