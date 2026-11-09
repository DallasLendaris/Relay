import { describe, expect, it } from 'vitest';
import { CheckInSchema } from '../src/index.js';
import { validCheckIn } from './fixtures.js';

describe('CheckInSchema', () => {
  it('accepts a valid check-in', () => {
    expect(CheckInSchema.safeParse(validCheckIn).success).toBe(true);
  });

  it('accepts need_help in the format (the demo API rejects it, not the schema)', () => {
    expect(CheckInSchema.safeParse({ ...validCheckIn, status: 'need_help' }).success).toBe(true);
  });

  it('rejects an invalid check-in', () => {
    const result = CheckInSchema.safeParse({
      ...validCheckIn,
      status: 'fine',
      checkedInAt: 'now',
      deviceId: 'x',
    });
    expect(result.success).toBe(false);
    if (!result.success) {
      const paths = result.error.issues.map((i) => i.path[0]);
      expect(paths).toEqual(expect.arrayContaining(['status', 'checkedInAt', 'deviceId']));
    }
  });

  it('rejects a missing h3Cell', () => {
    const { h3Cell: _omit, ...rest } = validCheckIn;
    expect(CheckInSchema.safeParse(rest).success).toBe(false);
  });
});
