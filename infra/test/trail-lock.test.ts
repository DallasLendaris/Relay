import { describe, expect, it, vi } from 'vitest';
import { DEFAULT_TRAIL_OBJECT_LOCK_DAYS, TRAIL_RETENTION_DAYS } from '../lib/monitoring-stack.js';
import { synthAll } from './helpers.js';

vi.setConfig({ testTimeout: 60_000 });

// NIST AU-9 (review-2026-11 A2): the trail bucket was versioned but any administrator could
// delete objects and versions. Object Lock in governance mode makes that an explicit bypass.

const trailBucket = (stage: 'dev' | 'prod', monitoring: Record<string, unknown> = {}) => {
  const t = synthAll(stage, { monitoring: { alarmEmail: 'ops@example.com', ...monitoring } });
  const found = Object.entries(t.monitoring.toJSON().Resources as Record<string, any>).filter(
    ([id, r]) => r.Type === 'AWS::S3::Bucket' && id.startsWith('TrailBucket'),
  );
  expect(found).toHaveLength(1);
  return found[0]![1].Properties;
};

describe('trail bucket Object Lock', () => {
  it.each(['dev', 'prod'] as const)('%s: enabled, governance mode, stage default', (stage) => {
    const p = trailBucket(stage);
    expect(p.ObjectLockEnabled).toBe(true);
    expect(p.ObjectLockConfiguration).toEqual({
      ObjectLockEnabled: 'Enabled',
      Rule: {
        DefaultRetention: { Mode: 'GOVERNANCE', Days: DEFAULT_TRAIL_OBJECT_LOCK_DAYS[stage] },
      },
    });
    // Object Lock needs versioning; the lock never outlives the lifecycle by default.
    expect(p.VersioningConfiguration).toEqual({ Status: 'Enabled' });
    expect(DEFAULT_TRAIL_OBJECT_LOCK_DAYS[stage]).toBeLessThanOrEqual(TRAIL_RETENTION_DAYS);
  });

  it('prod defaults to a year, dev to a day', () => {
    expect(DEFAULT_TRAIL_OBJECT_LOCK_DAYS).toEqual({ dev: 1, prod: 365 });
  });

  it('takes the retention from trailObjectLockDays', () => {
    const p = trailBucket('dev', { trailObjectLockDays: 30 });
    expect(p.ObjectLockConfiguration.Rule.DefaultRetention).toEqual({
      Mode: 'GOVERNANCE',
      Days: 30,
    });
  });

  it.each([0, 1.5, -3, 40_000])('refuses trailObjectLockDays %s', (days) => {
    expect(() => trailBucket('dev', { trailObjectLockDays: days })).toThrow(/trailObjectLockDays/);
  });

  it('only the trail bucket is locked (other buckets keep normal deletes)', () => {
    const ts = synthAll('dev');
    for (const [name, t] of Object.entries(ts)) {
      for (const [id, r] of Object.entries(t.toJSON().Resources as Record<string, any>)) {
        if (r.Type !== 'AWS::S3::Bucket') continue;
        const locked = r.Properties.ObjectLockEnabled === true;
        expect(locked, `${name}/${id}`).toBe(name === 'monitoring' && id.startsWith('TrailBucket'));
      }
    }
  });
});
