/**
 * One-time (and rotation) helper: puts random keys into the Relay key secrets.
 *
 *   npm run seed-keys -- <stage>            replace the placeholder keyring (k1)
 *   npm run seed-keys -- <stage> --rotate   add a new key and make it current
 *
 * Uses the AWS CLI, so it needs no extra dependencies and uses whatever
 * credentials/profile your shell already has. Old keys stay in the keyring so
 * tokens signed with them keep verifying until you remove them by hand.
 */
import { execFileSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';

const PLACEHOLDER = 'REPLACE_ME_RUN_SEED_KEYS'; // same marker as lib/config-stack.ts

interface Keyring {
  current: string;
  keys: Record<string, string>;
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
  const fresh = randomBytes(32).toString('base64');
  const hasPlaceholder = Object.values(ring.keys).includes(PLACEHOLDER);

  let next: Keyring;
  if (hasPlaceholder) {
    next = { current: ring.current, keys: { [ring.current]: fresh } };
  } else if (rotate) {
    const kid = `k${Object.keys(ring.keys).length + 1}`;
    next = { current: kid, keys: { ...ring.keys, [kid]: fresh } };
  } else {
    console.log(`${secretId}: already seeded, skipping (use --rotate to add a key)`);
    return;
  }
  aws([
    'secretsmanager',
    'put-secret-value',
    '--secret-id',
    secretId,
    '--secret-string',
    JSON.stringify(next),
  ]);
  console.log(`${secretId}: current key is now ${next.current}`);
}

const [stage, flag] = process.argv.slice(2);
if (stage !== 'dev' && stage !== 'prod') {
  console.error('usage: npm run seed-keys -- <dev|prod> [--rotate]');
  process.exit(1);
}
for (const name of ['token-keys', 'alert-keys'])
  seed(`relay/${stage}/${name}`, flag === '--rotate');
