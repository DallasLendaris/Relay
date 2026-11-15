import { parseCountyList } from './jurisdiction.js';
import { AGENCY_SCOPED_ROLES, isAgencyRole, type AgencyRole } from './roles.js';

/**
 * The signed-in agency user, built only from claims of a Cognito access token that the API
 * Gateway JWT authorizer has already verified (signature, issuer, audience/client_id, expiry).
 * The backend re-checks the claims it relies on (defence in depth) and refuses anything odd.
 */
export interface AgencyPrincipal {
  sub: string;
  /** Agency the user works for; null only for users who hold no agency-scoped role. */
  agencyId: string | null;
  roles: AgencyRole[];
  /** County FIPS codes the user may see. Empty for users without an agency-scoped role. */
  jurisdiction: ReadonlySet<string>;
}

/** Claim names the pre-token-generation trigger adds to the access token (pre-token.ts). */
export const AGENCY_ID_CLAIM = 'agency_id';
export const JURISDICTION_CLAIM = 'jurisdiction';

export interface ExpectedToken {
  /** The agency app client id. The authorizer checks it; this is the second check. */
  clientId?: string;
  /** https://cognito-idp.<region>.amazonaws.com/<poolId> */
  issuer?: string;
}

export type PrincipalDenyReason =
  | 'missing_claims'
  | 'not_access_token'
  | 'wrong_client'
  | 'wrong_issuer'
  | 'no_role'
  | 'no_agency'
  | 'invalid_jurisdiction';

export type PrincipalResult =
  | { ok: true; principal: AgencyPrincipal }
  | {
      ok: false;
      reason: PrincipalDenyReason;
      /** What could be read before the refusal, for the audit record. */
      partial: { sub: string | null; agencyId: string | null; roles: string[] };
    };

const AGENCY_ID = /^[a-z0-9][a-z0-9-]{0,62}$/;

/**
 * Groups as API Gateway hands them over. An HTTP API JWT authorizer flattens array claims to a
 * string like "[agency-viewer relay-admin]"; a real array (tests, other front doors) also works.
 */
export function parseGroups(value: unknown): string[] {
  if (Array.isArray(value)) return value.filter((v): v is string => typeof v === 'string');
  if (typeof value !== 'string') return [];
  return value
    .replace(/^\[/, '')
    .replace(/\]$/, '')
    .split(/[\s,]+/)
    .filter((g) => g !== '');
}

const str = (v: unknown): string | null => (typeof v === 'string' && v !== '' ? v : null);

export function principalFromClaims(
  claims: Record<string, unknown> | undefined,
  expected: ExpectedToken = {},
): PrincipalResult {
  const sub = str(claims?.['sub']);
  const groups = parseGroups(claims?.['cognito:groups']);
  const rawAgency = str(claims?.[AGENCY_ID_CLAIM]);
  const partial = { sub, agencyId: rawAgency, roles: groups };
  const deny = (reason: PrincipalDenyReason): PrincipalResult => ({ ok: false, reason, partial });

  if (!claims || !sub) return deny('missing_claims');
  // ID tokens carry the same user but are meant for the client, not for APIs.
  if (claims['token_use'] !== 'access') return deny('not_access_token');
  if (expected.clientId && claims['client_id'] !== expected.clientId) return deny('wrong_client');
  if (expected.issuer && claims['iss'] !== expected.issuer) return deny('wrong_issuer');

  // Unknown groups are ignored, never mapped to a role.
  const roles = [...new Set(groups.filter(isAgencyRole))].sort();
  if (roles.length === 0) return deny('no_role');

  const scoped = roles.some((r) => AGENCY_SCOPED_ROLES.has(r));
  const agencyId = rawAgency && AGENCY_ID.test(rawAgency) ? rawAgency : null;
  if (scoped && !agencyId) return deny('no_agency');

  const rawJurisdiction = str(claims[JURISDICTION_CLAIM]);
  let jurisdiction: string[] = [];
  if (rawJurisdiction !== null) {
    const parsed = parseCountyList(rawJurisdiction);
    if (parsed === null) return deny('invalid_jurisdiction');
    jurisdiction = parsed;
  }
  return { ok: true, principal: { sub, agencyId, roles, jurisdiction: new Set(jurisdiction) } };
}
