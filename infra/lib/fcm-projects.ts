import type { Construct } from 'constructs';

/** CDK context key: comma-separated Firebase project ids, e.g. `-c fcmProjectIds=relay-a,relay-b`. */
export const FCM_PROJECT_IDS_CONTEXT = 'fcmProjectIds';

/** Firebase project ids are 6 to 30 characters of a-z, 0-9 and hyphens. */
const PROJECT_ID = /^[a-z][a-z0-9-]{4,28}[a-z0-9]$/;

/**
 * The Firebase projects Android pushes are spread over (audit PERF-1), from the `fcmProjectIds`
 * context. Empty (the default) means one project that is not named here: the sender uses the
 * project of its service account and the API does no assignment. The order is part of the
 * assignment (stable hash mod N), and the first id is the default for devices without a project,
 * so only ever APPEND ids; reordering or removing one moves devices to a project their token
 * does not belong to.
 */
export function fcmProjectIds(scope: Construct): string[] {
  const raw = scope.node.tryGetContext(FCM_PROJECT_IDS_CONTEXT) as unknown;
  if (raw === undefined || raw === null || raw === '') return [];
  const list = (Array.isArray(raw) ? raw.map(String) : String(raw).split(','))
    .map((s) => s.trim())
    .filter((s) => s !== '');
  for (const id of list) {
    if (!PROJECT_ID.test(id)) {
      throw new Error(`${FCM_PROJECT_IDS_CONTEXT}: invalid project id "${id}"`);
    }
  }
  if (new Set(list).size !== list.length) {
    throw new Error(`${FCM_PROJECT_IDS_CONTEXT}: duplicate project id`);
  }
  return list;
}
