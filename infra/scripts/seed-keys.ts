/**
 * One-time (and rotation) helper: puts random keys into the Relay keyring secrets
 * (token-keys and device-credential-keys). Alert signing is a KMS asymmetric key and is not
 * seeded here (README "Alert signing key").
 *
 *   npm run seed-keys -- <stage>            replace the placeholder keyring (k1)
 *   npm run seed-keys -- <stage> --rotate   add a new key and make it current
 *
 * Uses the AWS CLI, so it needs no extra dependencies and uses whatever
 * credentials/profile your shell already has. Old keys stay in the keyring so
 * tokens signed with them keep verifying until you remove them by hand.
 *
 * SEC-C-5: kids are monotonic. A rotation adds k<n+1>, where n is the highest kid ever issued
 * (the larger of the highest kid in `keys` and the ring's `highestKid` high-water mark), so a
 * kid removed by hand is never reissued. The script refuses to overwrite any existing kid.
 */
import { execFileSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { pathToFileURL } from 'node:url';

const PLACEHOLDER = 'REPLACE_ME_RUN_SEED_KEYS'; // same marker as lib/config-stack.ts

/** Keyrings this script manages. */
export const KEYRING_NAMES = ['token-keys', 'device-credential-keys'] as const;

export interface Keyring {
  current: string;
  keys: Record<string, string>;
  /** Highest kid number ever issued (never decreases). Loaders ignore it. */
  highestKid?: number;
}

export type SeedPlan =
  { action: 'skip'; reason: string } | { action: 'write'; ring: Keyring; current: string };

const KID = /^k([1-9][0-9]*)$/;

function kidNumber(kid: string): number {
  const m = KID.exec(kid);
  if (!m) throw new Error(`unexpected kid "${kid}" (expected k<number>); fix the secret by hand`);
  return Number(m[1]);
}

/**
 * Decides what to write. Pure, so it is tested without AWS. `fresh` is the new base64 key.
 * Throws instead of writing anything that would replace an existing real key.
 */
export function planSeed(ring: Keyring, rotate: boolean, fresh: string): SeedPlan {
  if (!ring || typeof ring.current !== 'string' || !ring.keys || typeof ring.keys !== 'object') {
    throw new Error('secret is not a keyring ({"current": ..., "keys": {...}})');
  }
  const kids = Object.keys(ring.keys);
  const numbers = kids.map(kidNumber);
  const placeholders = kids.filter((k) => ring.keys[k] === PLACEHOLDER);

  if (placeholders.length > 0) {
    // Only the untouched CDK placeholder ring ({"current":"k1","keys":{"k1":PLACEHOLDER}}) is
    // replaced in place. A placeholder next to real keys means someone edited it by hand.
    if (kids.length !== 1 || placeholders[0] !== ring.current) {
      throw new Error('keyring mixes a placeholder with real keys; fix it by hand');
    }
    const n = Math.max(ring.highestKid ?? 0, ...numbers);
    return {
      action: 'write',
      current: ring.current,
      ring: { current: ring.current, keys: { [ring.current]: fresh }, highestKid: n },
    };
  }
  if (!rotate) return { action: 'skip', reason: 'already seeded (use --rotate to add a key)' };

  const highest = Math.max(ring.highestKid ?? 0, ...numbers);
  const kid = `k${highest + 1}`;
  if (Object.prototype.hasOwnProperty.call(ring.keys, kid)) {
    throw new Error(`refusing to overwrite existing key ${kid}`);
  }
  return {
    action: 'write',
    current: kid,
    ring: { current: kid, keys: { ...ring.keys, [kid]: fresh }, highestKid: highest + 1 },
  };
}

function aws(args: string[]): string {
  return execFileSync('aws', [...args, '--region', 'us-west-2'], { encoding: 'utf8' });
}

function seed(secretId: string, rotate: boolean): void {
  const ring = JSON.parse(
    aws([
      'secretsmanager',
      'get-secret-value',
      '--secret-id',
      secretId,
      '--query',
      'SecretString',
      '--output',
      'text',
    ]),
  ) as Keyring;
  const plan = planSeed(ring, rotate, randomBytes(32).toString('base64'));
  if (plan.action === 'skip') {
    console.log(`${secretId}: ${plan.reason}, skipping`);
    return;
  }
  aws([
    'secretsmanager',
    'put-secret-value',
    '--secret-id',
    secretId,
    '--secret-string',
    JSON.stringify(plan.ring),
  ]);
  console.log(`${secretId}: current key is now ${plan.current}`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const [stage, flag] = process.argv.slice(2);
  if (stage !== 'dev' && stage !== 'prod') {
    console.error('usage: npm run seed-keys -- <dev|prod> [--rotate]');
    process.exit(1);
  }
  for (const name of KEYRING_NAMES) seed(`relay/${stage}/${name}`, flag === '--rotate');
}
