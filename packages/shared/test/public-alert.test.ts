import { describe, expect, it } from 'vitest';
import {
  AlertPointerSchema,
  PublicAlertDocumentSchema,
  PushDataSchema,
  type AlertPointer,
  type PublicAlertDocument,
  type PushData,
  type components,
} from '../src/index.js';
import { validAlert } from './fixtures.js';

// Compile-time drift check between the zod schemas and the OpenAPI components (see contract.test.ts).
type Mutual<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;
type Schemas = components['schemas'];
const docMatches: Mutual<PublicAlertDocument, Schemas['PublicAlertDocument']> = true;
const pointerMatches: Mutual<AlertPointer, Schemas['AlertPointer']> = true;
const pushMatches: Mutual<PushData, Schemas['PushData']> = true;

const { area, ...rest } = validAlert;
const doc: PublicAlertDocument = {
  ...rest,
  version: 1,
  area: {
    description: area.description,
    cellCount: area.h3Cells.length,
    cellsSha256: 'a'.repeat(64),
  },
};

describe('public alert schemas', () => {
  it('match the OpenAPI components (enforced by typecheck)', () => {
    expect([docMatches, pointerMatches, pushMatches]).toEqual([true, true, true]);
  });

  it('PublicAlertDocument accepts a cell-free document and rejects h3Cells', () => {
    expect(PublicAlertDocumentSchema.safeParse(doc).success).toBe(true);
    const withCells = { ...doc, area: { ...doc.area, h3Cells: ['872830828ffffff'] } };
    expect(PublicAlertDocumentSchema.safeParse(withCells).success).toBe(false);
  });

  it('AlertPointer needs a document path under /alerts/v1/doc/', () => {
    const p = {
      version: 1,
      alertId: validAlert.id,
      sourceId: 's',
      state: 'active',
      expires: validAlert.expires,
      doc: `/alerts/v1/doc/${validAlert.id}/abc.json`,
    };
    expect(AlertPointerSchema.safeParse(p).success).toBe(true);
    expect(AlertPointerSchema.safeParse({ ...p, doc: '/elsewhere.json' }).success).toBe(false);
  });

  it('PushData: alertUrl is optional and must be a URL', () => {
    const base = { alertId: validAlert.id, checkInToken: 't', expiresAt: validAlert.expires };
    expect(PushDataSchema.safeParse(base).success).toBe(true);
    expect(
      PushDataSchema.safeParse({ ...base, alertUrl: 'https://cdn.example.org/x.json' }).success,
    ).toBe(true);
    expect(PushDataSchema.safeParse({ ...base, alertUrl: 'not a url' }).success).toBe(false);
  });
});
