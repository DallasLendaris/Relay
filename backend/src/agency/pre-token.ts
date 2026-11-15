import { AGENCY_ID_CLAIM, JURISDICTION_CLAIM } from './principal.js';

/**
 * Cognito pre-token-generation trigger (event version V2_0, needs the Essentials or Plus feature
 * plan). Copies the two admin-only attributes into the ACCESS token, which is what the agency API
 * accepts (ID tokens are refused). Cognito does not put custom attributes in access tokens on its
 * own.
 *
 * The values come from Cognito's own user record (event.request.userAttributes), never from the
 * client. The app client cannot write these attributes, so only an administrator
 * (AdminUpdateUserAttributes, logged by CloudTrail) or, later, a federated IdP mapping can change
 * them. A change takes effect at the user's next token refresh (access tokens live 15 minutes).
 */
export interface PreTokenGenerationV2Event {
  version: string;
  triggerSource: string;
  userPoolId: string;
  userName: string;
  request: { userAttributes: Record<string, string | undefined> };
  response: {
    claimsAndScopeOverrideDetails?: {
      accessTokenGeneration?: { claimsToAddOrOverride?: Record<string, string> };
      idTokenGeneration?: { claimsToAddOrOverride?: Record<string, string> };
    } | null;
  };
}

export const AGENCY_ID_ATTRIBUTE = 'custom:agency_id';
export const JURISDICTION_ATTRIBUTE = 'custom:jurisdiction';

export function agencyClaims(attrs: Record<string, string | undefined>): Record<string, string> {
  const claims: Record<string, string> = {};
  const agency = attrs[AGENCY_ID_ATTRIBUTE];
  const jurisdiction = attrs[JURISDICTION_ATTRIBUTE];
  if (agency) claims[AGENCY_ID_CLAIM] = agency;
  if (jurisdiction) claims[JURISDICTION_CLAIM] = jurisdiction;
  return claims;
}

export function handler(event: PreTokenGenerationV2Event): Promise<PreTokenGenerationV2Event> {
  const claims = agencyClaims(event.request.userAttributes ?? {});
  event.response = {
    claimsAndScopeOverrideDetails: {
      accessTokenGeneration: { claimsToAddOrOverride: claims },
      idTokenGeneration: { claimsToAddOrOverride: claims },
    },
  };
  return Promise.resolve(event);
}
