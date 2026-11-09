import { describe, expect, it } from 'vitest';
import { AlertSchema, H3_RESOLUTION, relaySeverityFor } from '../src/index.js';
import { validAlert } from './fixtures.js';

describe('AlertSchema', () => {
  it('accepts a valid alert', () => {
    expect(AlertSchema.safeParse(validAlert).success).toBe(true);
  });

  it('accepts null geometry and a MultiPolygon', () => {
    const noGeometry = { ...validAlert, area: { ...validAlert.area, geometry: null } };
    expect(AlertSchema.safeParse(noGeometry).success).toBe(true);
    const multi = {
      ...validAlert,
      area: {
        ...validAlert.area,
        geometry: {
          type: 'MultiPolygon',
          coordinates: [
            [
              [
                [0, 0],
                [1, 0],
                [1, 1],
                [0, 0],
              ],
            ],
          ],
        },
      },
    };
    expect(AlertSchema.safeParse(multi).success).toBe(true);
  });

  it('rejects an invalid alert', () => {
    const bad = { ...validAlert, relaySeverity: 'panic', expires: 'tomorrow', id: 'not-a-uuid' };
    const result = AlertSchema.safeParse(bad);
    expect(result.success).toBe(false);
    if (!result.success) {
      const paths = result.error.issues.map((i) => i.path[0]);
      expect(paths).toEqual(expect.arrayContaining(['relaySeverity', 'expires', 'id']));
    }
  });

  it('rejects a missing required field', () => {
    const { signature: _omit, ...rest } = validAlert;
    expect(AlertSchema.safeParse(rest).success).toBe(false);
  });
});

describe('relaySeverityFor', () => {
  it.each([
    ['Flash Flood Emergency', 'emergency'],
    ['Tornado Emergency', 'emergency'],
    ['Flash Flood Warning', 'warning'],
    ['Red Flag Warning', 'warning'],
    ['Flood Watch', 'watch'],
    ['Wind Advisory', 'advisory'],
    ['Special Weather Statement', 'statement'],
    ['Hydrologic Outlook', 'statement'],
  ])('%s -> %s', (event, expected) => {
    expect(relaySeverityFor(event, 'Unknown')).toBe(expected);
  });
});

describe('H3_RESOLUTION', () => {
  it('is 7', () => {
    expect(H3_RESOLUTION).toBe(7);
  });
});
