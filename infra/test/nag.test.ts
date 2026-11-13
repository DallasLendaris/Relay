import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { App } from 'aws-cdk-lib';
import { Annotations, Match } from 'aws-cdk-lib/assertions';
import { describe, expect, it } from 'vitest';
import { applyCompliance, NIST } from '../lib/compliance.js';
import { buildRelayApp } from '../lib/relay-app.js';
import { SKIP_IMAGE_CONTEXT } from '../lib/sender-stack.js';

/** Builds the app the way bin/relay.ts does, with the NIST 800-53 rev 5 pack on. */
function nagApp(stage: 'dev' | 'prod') {
  const app = new App({ context: { [SKIP_IMAGE_CONTEXT]: 'true' } });
  const stacks = buildRelayApp(app, {
    stage,
    env: { account: '111111111111', region: 'us-west-2' },
    edgeEnv: { account: '111111111111', region: 'us-east-1' },
    monitoring: { alarmEmail: 'ops@example.com' },
    ...(stage === 'prod' ? { publicAlertBaseUrl: 'https://alerts.example.org' } : {}),
  });
  applyCompliance(app, stacks, { stage, reports: false });
  return stacks;
}

const nistMessage = Match.stringLikeRegexp(`${NIST.replace(/\./g, '\\.')}-`);

describe.each(['dev', 'prod'] as const)('cdk-nag NIST 800-53 rev 5 (%s)', (stage) => {
  const stacks = nagApp(stage);

  it.each(Object.keys(stacks))('%s stack has no unsuppressed NIST errors or warnings', (key) => {
    const stack = stacks[key as keyof typeof stacks];
    const annotations = Annotations.fromStack(stack);
    const errors = annotations.findError('*', nistMessage);
    const warnings = annotations.findWarning('*', nistMessage);
    const show = (xs: typeof errors) => xs.map((x) => `${x.id}: ${String(x.entry.data)}`);
    expect(show(errors)).toEqual([]);
    expect(show(warnings)).toEqual([]);
  });
});

describe('cdk-nag exceptions register', () => {
  const doc = readFileSync(
    fileURLToPath(new URL('../../docs/compliance/cdk-nag-exceptions.md', import.meta.url)),
    'utf8',
  );
  const source = readFileSync(
    fileURLToPath(new URL('../lib/compliance.ts', import.meta.url)),
    'utf8',
  );

  it('documents every rule the code suppresses', () => {
    const suppressed = new Set(
      [...source.matchAll(/rule\('([A-Za-z0-9]+)'\)/g)].map((m) => m[1] as string),
    );
    expect(suppressed.size).toBeGreaterThan(0);
    for (const r of suppressed) expect(doc, `${r} missing from the exceptions doc`).toContain(r);
  });

  it('never suppresses a rule for a whole stack', () => {
    expect(source).not.toMatch(/addStackSuppressions/);
  });
});
