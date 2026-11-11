import { describe, expect, it } from 'vitest';
import {
  AlertPointerSchema,
  AlertSchema,
  CheckInSchema,
  PlaceLabelSchema,
  PublicAlertDocumentSchema,
  PushDataSchema,
} from '../src/index.js';
import { validAlert, validCheckIn } from './fixtures.js';

// Pins the behaviour that changed, or could have changed, in the zod 3 -> 4 upgrade. Each case was
// checked against zod 3.25.76 with the pre-upgrade schemas; see the commit message for the diff.

const { area, ...rest } = validAlert;
const doc = {
  ...rest,
  version: 1 as const,
  area: { description: area.description, cellCount: 1, cellsSha256: 'a'.repeat(64) },
};
const pointer = {
  version: 1 as const,
  alertId: validAlert.id,
  sourceId: validAlert.sourceId,
  state: 'active' as const,
  expires: validAlert.expires,
  doc: '/alerts/v1/doc/x/y.json',
};
const push = { alertId: validAlert.id, checkInToken: 't', expiresAt: validAlert.expires };

describe('ids stay 8-4-4-4-12 hex, as in zod 3 (z.guid, not the stricter z.uuid)', () => {
  // zod 4's z.uuid() also checks the RFC 9562 version and variant bits. Relay never relied on
  // them: the API path check (backend/src/api/index.ts) accepts any 8-4-4-4-12 hex id.
  const loose = [
    '00000000-0000-0000-0000-000000000000',
    '11111111-1111-1111-1111-111111111111',
    'FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF',
  ];
  it.each(loose)('accepts %s everywhere an id is checked', (id) => {
    expect(AlertSchema.shape.id.safeParse(id).success).toBe(true);
    expect(CheckInSchema.safeParse({ ...validCheckIn, alertId: id, deviceId: id }).success).toBe(
      true,
    );
    expect(AlertPointerSchema.safeParse({ ...pointer, alertId: id }).success).toBe(true);
    expect(PushDataSchema.safeParse({ ...push, alertId: id }).success).toBe(true);
  });
  it.each(['3f1c2d4e5a6b4c7d8e9f0a1b2c3d4e5f', '{3f1c2d4e-5a6b-4c7d-8e9f-0a1b2c3d4e5f}', ''])(
    'still rejects %j',
    (id) => expect(AlertSchema.shape.id.safeParse(id).success).toBe(false),
  );
});

describe('date-times are RFC 3339 with an offset', () => {
  const date = AlertSchema.shape.sent;
  it.each([
    '2026-11-09T10:00:00-08:00',
    '2026-11-09T18:00:00Z',
    '2026-11-09T10:00:00.123-08:00',
    '2026-11-09T10:00:00.123456789Z',
    '2026-11-09T10:00:00+05:45',
  ])('accepts %s', (d) => expect(date.safeParse(d).success).toBe(true));
  // zod 3 also accepted the two forms below. RFC 3339 (the OpenAPI `format: date-time`) and
  // CAP 1.2 both require seconds and a colon in the offset, so zod 4 rejecting them is kept.
  it.each([
    '2026-11-09T10:00-08:00',
    '2026-11-09T10:00:00-0800',
    '2026-11-09T10:00:00-08',
    '2026-11-09T10:00:00',
    '2026-11-09 10:00:00-08:00',
  ])('rejects %s', (d) => expect(date.safeParse(d).success).toBe(false));
});

describe('object strictness', () => {
  it('PublicAlertDocument stays strict at both levels and keeps the field order', () => {
    expect(PublicAlertDocumentSchema.safeParse({ ...doc, extra: 1 }).success).toBe(false);
    expect(
      PublicAlertDocumentSchema.safeParse({ ...doc, area: { ...doc.area, h3Cells: [] } }).success,
    ).toBe(false);
    // Documents are content-addressed, so the output key order must not depend on input order.
    const reversed = Object.fromEntries(Object.entries(doc).reverse());
    const out = PublicAlertDocumentSchema.parse(reversed);
    expect(Object.keys(out)).toEqual([...Object.keys(rest), 'version', 'area']);
    expect(JSON.stringify(out)).toBe(JSON.stringify(PublicAlertDocumentSchema.parse(doc)));
  });
  it('AlertPointer is strict; PushData keeps unknown keys', () => {
    expect(AlertPointerSchema.safeParse({ ...pointer, x: 1 }).success).toBe(false);
    expect(PushDataSchema.parse({ ...push, extra: 'x' })).toEqual({ ...push, extra: 'x' });
  });
  it('Alert strips unknown keys and does not add an absent optional pds', () => {
    const out = AlertSchema.parse({ ...validAlert, junk: 1 });
    expect('junk' in out).toBe(false);
    expect('pds' in out).toBe(false);
  });
});

describe('numbers', () => {
  it('rejects a non-finite coordinate (zod 3 let Infinity through to validateGeometry)', () => {
    const geometry = { type: 'Polygon' as const, coordinates: [[[Infinity, 37.3]]] };
    expect(AlertSchema.safeParse({ ...validAlert, area: { ...area, geometry } }).success).toBe(
      false,
    );
  });
  it.each([-1, 1.5, 2 ** 53])('cellCount rejects %d', (n) => {
    expect(
      PublicAlertDocumentSchema.safeParse({ ...doc, area: { ...doc.area, cellCount: n } }).success,
    ).toBe(false);
  });
});

it('a refine keeps its custom error text', () => {
  const r = PlaceLabelSchema.safeParse('a\nb');
  expect(r.success).toBe(false);
  expect(r.error?.issues[0]?.message).toBe('must not contain control characters');
});

it('PushData.alertUrl must still be an absolute URL', () => {
  expect(
    PushDataSchema.safeParse({ ...push, alertUrl: 'https://cdn.example.test/a.json' }).success,
  ).toBe(true);
  for (const bad of ['not a url', '//cdn.example.test/a.json', '/alerts/v1/doc/x.json']) {
    expect(PushDataSchema.safeParse({ ...push, alertUrl: bad }).success).toBe(false);
  }
});
