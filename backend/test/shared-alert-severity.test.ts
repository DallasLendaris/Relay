import { describe, expect, it } from 'vitest';
import { AlertSchema, isPds, relaySeverityFor } from '@relay/shared';

describe('relaySeverityFor with damage threat and urgency', () => {
  it('keeps the old mapping with no options', () => {
    expect(relaySeverityFor('Flash Flood Emergency')).toBe('emergency');
    expect(relaySeverityFor('Tornado Warning')).toBe('warning');
    expect(relaySeverityFor('Flood Watch')).toBe('watch');
    expect(relaySeverityFor('Heat Advisory')).toBe('advisory');
    expect(relaySeverityFor('Special Weather Statement', 'Extreme')).toBe('statement');
  });

  it('CATASTROPHIC upgrades a warning to an emergency, in any case', () => {
    expect(relaySeverityFor('Tornado Warning', 'Extreme', { damageThreat: 'CATASTROPHIC' })).toBe(
      'emergency',
    );
    expect(
      relaySeverityFor('Flash Flood Warning', 'Severe', { damageThreat: 'catastrophic' }),
    ).toBe('emergency');
    expect(relaySeverityFor('Tornado Warning', 'Extreme', { damageThreat: 'CONSIDERABLE' })).toBe(
      'warning',
    );
    expect(relaySeverityFor('Tornado Warning', 'Extreme', { damageThreat: null })).toBe('warning');
    expect(relaySeverityFor('Tornado Warning', 'Extreme', { damageThreat: 'huge' })).toBe(
      'warning',
    );
  });

  it('does not upgrade a watch or an advisory', () => {
    for (const event of ['Tornado Watch', 'Wind Advisory']) {
      expect(relaySeverityFor(event, 'Severe', { damageThreat: 'CATASTROPHIC' })).not.toBe(
        'emergency',
      );
    }
  });

  it('promotes only an Immediate Severe/Extreme statement to advisory', () => {
    const s = (cap: Parameters<typeof relaySeverityFor>[1], urgency: 'Immediate' | 'Expected') =>
      relaySeverityFor('Special Weather Statement', cap, { urgency });
    expect(s('Severe', 'Immediate')).toBe('advisory');
    expect(s('Extreme', 'Immediate')).toBe('advisory');
    expect(s('Moderate', 'Immediate')).toBe('statement');
    expect(s('Severe', 'Expected')).toBe('statement');
    expect(s(undefined, 'Immediate')).toBe('statement');
  });

  it('isPds is true only for CONSIDERABLE on a warning', () => {
    expect(isPds('Tornado Warning', 'CONSIDERABLE')).toBe(true);
    expect(isPds('Flash Flood Warning', 'considerable')).toBe(true);
    expect(isPds('Tornado Warning', 'CATASTROPHIC')).toBe(false);
    expect(isPds('Tornado Warning', 'BASE')).toBe(false);
    expect(isPds('Tornado Watch', 'CONSIDERABLE')).toBe(false);
    expect(isPds('Tornado Warning', undefined)).toBe(false);
  });
});

describe('Alert.pds', () => {
  const base = {
    id: '6f1c1d2e-3b4a-4c5d-8e9f-0a1b2c3d4e5f',
    source: 'nws',
    sourceId: 's',
    event: 'Tornado Warning',
    eventCode: 'TOR',
    relaySeverity: 'warning',
    capSeverity: 'Extreme',
    urgency: 'Immediate',
    certainty: 'Observed',
    status: 'Actual',
    messageType: 'Alert',
    state: 'active',
    references: [],
    area: { description: 'x', geometry: null, ugcCodes: [], sameCodes: [], h3Cells: [] },
    sent: '2026-11-09T10:00:00-08:00',
    effective: '2026-11-09T10:00:00-08:00',
    onset: null,
    expires: '2026-11-09T11:00:00-08:00',
    ends: null,
    official: {
      headline: null,
      description: '',
      instruction: null,
      senderName: '',
      language: 'en',
    },
    summaries: [],
    signature: null,
  };

  it('is optional and must be a boolean', () => {
    expect(AlertSchema.safeParse(base).success).toBe(true);
    expect(AlertSchema.safeParse({ ...base, pds: true }).success).toBe(true);
    expect(AlertSchema.safeParse({ ...base, pds: 'yes' }).success).toBe(false);
  });
});
