import type { Alert } from '../src/alert.js';
import type { CheckIn } from '../src/checkin.js';

export const validAlert: Alert = {
  id: '3f1c2d4e-5a6b-4c7d-8e9f-0a1b2c3d4e5f',
  source: 'nws',
  sourceId: 'urn:oid:2.49.0.1.840.0.abc123',
  event: 'Flash Flood Warning',
  eventCode: 'FFW',
  relaySeverity: 'warning',
  capSeverity: 'Severe',
  urgency: 'Immediate',
  certainty: 'Likely',
  status: 'Actual',
  messageType: 'Alert',
  state: 'active',
  references: [{ sourceId: 'urn:oid:2.49.0.1.840.0.older', sent: '2026-11-09T10:00:00-08:00' }],
  area: {
    description: 'Santa Clara County',
    geometry: {
      type: 'Polygon',
      coordinates: [
        [
          [-121.9, 37.3],
          [-121.8, 37.3],
          [-121.8, 37.4],
          [-121.9, 37.3],
        ],
      ],
    },
    ugcCodes: ['CAC085'],
    sameCodes: ['006085'],
    h3Cells: ['8728306a4ffffff'],
  },
  sent: '2026-11-09T12:00:00-08:00',
  effective: '2026-11-09T12:00:00-08:00',
  onset: null,
  expires: '2026-11-09T15:00:00-08:00',
  ends: null,
  official: {
    headline: 'Flash Flood Warning issued November 9',
    description: 'Heavy rain is causing flooding.',
    instruction: 'Move to higher ground.',
    senderName: 'NWS San Francisco CA',
    language: 'en-US',
  },
  summaries: [],
  signature: null,
};

export const validCheckIn: CheckIn = {
  alertId: '3f1c2d4e-5a6b-4c7d-8e9f-0a1b2c3d4e5f',
  deviceId: '9b8a7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d',
  status: 'ok',
  checkedInAt: '2026-11-09T12:05:00Z',
  token: 'signed.token.value',
  idempotencyKey: 'alert-device-1',
  h3Cell: '8728306a4ffffff',
};
