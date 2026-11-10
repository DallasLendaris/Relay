import { describe, expect, it } from 'vitest';
import {
  coarseAddress,
  coarsenShortAddress,
  stripHouseNumber,
} from '../../src/geo/coarse-address.js';

describe('coarse address', () => {
  it.each([
    ['1600 Amphitheatre Pkwy', 'Amphitheatre Pkwy'],
    ['12-B Main St', 'Main St'],
    ['221B Baker St', 'Baker St'],
    ['12-14 Elm St', 'Elm St'],
    ['12 1/2 Elm St', 'Elm St'],
    ['One Market St', 'Market St'],
    ['1st St', '1st St'],
    ['5th Ave', '5th Ave'],
    ['El Camino Real', 'El Camino Real'],
    ['', ''],
  ])('strips %j to %j', (input, expected) => {
    expect(stripHouseNumber(input)).toBe(expected);
  });

  it('joins street and locality, or falls back to what exists', () => {
    expect(coarseAddress('1600 Amphitheatre Pkwy', 'Mountain View')).toBe(
      'Amphitheatre Pkwy, Mountain View',
    );
    expect(coarseAddress('1234', 'San José')).toBe('San José');
    expect(coarseAddress(undefined, undefined)).toBe('');
  });

  it('is idempotent on joined values', () => {
    const once = coarsenShortAddress('12-B Main St, San Jose');
    expect(once).toBe('Main St, San Jose');
    expect(coarsenShortAddress(once)).toBe(once);
  });
});
