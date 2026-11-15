/**
 * Agency roles and what each may do (NIST AC-3, AC-6). DRAFT: the agency-side owner and the State
 * confirm this matrix (docs/agency-auth.md, "Decisions").
 *
 * A role is a Cognito group. Cognito puts the user's groups in the access token
 * (`cognito:groups`), so only an administrator who can call AdminAddUserToGroup changes them.
 *
 * Deny by default: an action that is not in PERMISSIONS, or a role that is not listed for it, is
 * refused. Adding an endpoint therefore means adding its action here first.
 */
export const AGENCY_ROLES = ['agency-viewer', 'agency-operator', 'relay-admin'] as const;
export type AgencyRole = (typeof AGENCY_ROLES)[number];

/** Cognito group precedence (lower wins when a token needs one role, e.g. an IAM role mapping). */
export const ROLE_PRECEDENCE: Record<AgencyRole, number> = {
  'relay-admin': 0,
  'agency-operator': 10,
  'agency-viewer': 20,
};

export const ROLE_DESCRIPTIONS: Record<AgencyRole, string> = {
  'agency-viewer': 'Agency staff: reads check-in summaries for their own jurisdiction',
  'agency-operator':
    'Agency staff who will run agency actions (none yet; the statewide phase adds them). Same reads as viewer',
  'relay-admin':
    'Relay operations: manages agency users. No check-in data unless also in an agency role',
};

export type AgencyAction = 'agency.me.read' | 'agency.alert.summary.read';

/**
 * Role matrix. relay-admin is deliberately NOT allowed to read check-in summaries: administering
 * accounts and reading response data are separate duties (AC-5, AC-6). An admin who also needs
 * data gets an agency group and a jurisdiction like anyone else.
 */
export const PERMISSIONS: Readonly<Record<AgencyAction, readonly AgencyRole[]>> = {
  'agency.me.read': ['agency-viewer', 'agency-operator', 'relay-admin'],
  'agency.alert.summary.read': ['agency-viewer', 'agency-operator'],
};

/** Roles that work on behalf of an agency, so they need an agency id and a jurisdiction. */
export const AGENCY_SCOPED_ROLES: ReadonlySet<AgencyRole> = new Set([
  'agency-viewer',
  'agency-operator',
]);

export const isAgencyRole = (value: string): value is AgencyRole =>
  (AGENCY_ROLES as readonly string[]).includes(value);

/** True only when one of `roles` is listed for `action`. Unknown actions are denied. */
export function roleAllows(roles: readonly AgencyRole[], action: string): boolean {
  // Own keys only: "constructor" or "__proto__" must not resolve to something on the prototype.
  if (!Object.hasOwn(PERMISSIONS, action)) return false;
  const allowed = (PERMISSIONS as Record<string, readonly AgencyRole[]>)[action];
  if (!Array.isArray(allowed)) return false;
  return roles.some((r) => allowed.includes(r));
}
