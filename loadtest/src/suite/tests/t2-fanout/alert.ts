/**
 * The signed test alert (real ECDSA P-256 signing with the in-memory signer, the same algorithm as
 * the KMS key in AWS) and the helpers every child uses to hold and verify it.
 */
import { createPublicKey } from 'node:crypto';
import type { Alert } from '@relay/shared';
import { cellsForAlertArea } from '../../../../../backend/src/geo/h3-cells.js';
import {
  createAlertVerifier,
  createMemoryAlertSigning,
  signAlertSync,
  type AlertVerifier,
} from '../../../../../backend/src/signing/index.js';

export const SIGNING_KID = 't2-alert';
export const TOKEN_KEY = { id: 't2-token', secret: 't2-token-secret' };

export interface AlertKeys {
  sign(alert: Alert): Alert;
  publicPem: string;
}

export function createAlertKeys(): AlertKeys {
  const s = createMemoryAlertSigning(SIGNING_KID);
  return {
    sign: (a) => signAlertSync(a, s.signer),
    publicPem: s.publicKey.export({ type: 'spki', format: 'pem' }) as string,
  };
}

export function verifierFromPem(pem: string): AlertVerifier {
  return createAlertVerifier(new Map([[SIGNING_KID, createPublicKey(pem)]]));
}

/** Deterministic UUID for scenario n (each scenario gets its own alert id and batch keys). */
export const alertIdFor = (n: number): string =>
  `7a2e0c11-5b3d-4c8e-9a6f-${(0x1000 + n).toString(16).padStart(12, '0')}`;

export async function buildAlert(
  keys: AlertKeys,
  n: number,
  description: string,
  ring: Array<[number, number]>,
): Promise<Alert> {
  const geometry = { type: 'Polygon' as const, coordinates: [ring] };
  const h3Cells = await cellsForAlertArea(
    { geometry, ugcCodes: [] },
    {
      getZoneGeometry: () => {
        throw new Error('zone lookup not expected');
      },
    },
  );
  const now = Date.now();
  return keys.sign({
    id: alertIdFor(n),
    source: 'nws',
    sourceId: `urn:oid:t2-${n}`,
    event: 'Flash Flood Warning',
    eventCode: 'FFW',
    relaySeverity: 'warning',
    capSeverity: 'Severe',
    urgency: 'Immediate',
    certainty: 'Likely',
    status: 'Actual',
    messageType: 'Alert',
    state: 'active',
    references: [],
    area: { description, geometry, ugcCodes: [], sameCodes: [], h3Cells },
    sent: new Date(now - 60_000).toISOString(),
    effective: new Date(now - 60_000).toISOString(),
    onset: null,
    expires: new Date(now + 4 * 3600_000).toISOString(),
    ends: null,
    official: {
      headline: 'Flash Flood Warning (scalability test)',
      description: 'Simulated alert for the local scalability suite.',
      instruction: null,
      senderName: 'NWS',
      language: 'en-US',
    },
    summaries: [],
    signature: 'unsigned',
  });
}
