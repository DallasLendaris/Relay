import { Match, type Template } from 'aws-cdk-lib/assertions';
import { describe, expect, it, vi } from 'vitest';
import { synthAll, type Templates } from './helpers.js';

vi.setConfig({ testTimeout: 60_000 });

/*
 * SEC-C-7 / SEC-A-8 / RED-2: alert signing is a KMS asymmetric key. Only the alert writer (the
 * ingest Lambda) may hold kms:Sign; every other principal can only fetch the public key, so it
 * cannot produce a signature that verifies. No alert-signing secret exists in Secrets Manager.
 */

type Res = { Type: string; Properties: Record<string, any> };
const stages = { dev: synthAll('dev'), prod: synthAll('prod') } as const;

const all = (ts: Templates) => Object.entries(ts) as [string, Template][];
const resources = (t: Template): Record<string, Res> => t.toJSON().Resources ?? {};

/** Every IAM statement in a stack with the roles it is attached to. */
function statements(t: Template): { roles: string[]; actions: string[]; resource: unknown }[] {
  const out: { roles: string[]; actions: string[]; resource: unknown }[] = [];
  for (const r of Object.values(resources(t))) {
    if (r.Type !== 'AWS::IAM::Policy') continue;
    const roles = (r.Properties['Roles'] ?? []).map((x: any) => x.Ref as string);
    for (const s of r.Properties['PolicyDocument'].Statement) {
      if (s.Effect !== 'Allow') continue;
      out.push({ roles, actions: [s.Action].flat(), resource: s.Resource });
    }
  }
  for (const [id, r] of Object.entries(resources(t))) {
    if (r.Type !== 'AWS::IAM::Role') continue;
    for (const p of r.Properties['Policies'] ?? []) {
      for (const s of p.PolicyDocument.Statement) {
        out.push({ roles: [id], actions: [s.Action].flat(), resource: s.Resource });
      }
    }
  }
  return out;
}

const canSign = (a: string) => a === 'kms:Sign' || a === 'kms:*' || a === '*';

/** Role logical id of each Lambda that has env var `name`. */
function rolesOfFunctionsWith(t: Template, name: string): string[] {
  return Object.values(resources(t))
    .filter(
      (r) =>
        r.Type === 'AWS::Lambda::Function' &&
        r.Properties['Environment']?.Variables?.[name] !== undefined,
    )
    .map((r) => r.Properties['Role']['Fn::GetAtt'][0] as string);
}

describe.each(Object.keys(stages) as (keyof typeof stages)[])('alert signing (%s)', (stage) => {
  const ts = stages[stage];

  it('config has one ECC_NIST_P256 SIGN_VERIFY key with an alias, and no alert-keys secret', () => {
    ts.config.resourceCountIs('AWS::KMS::Key', 1);
    ts.config.hasResourceProperties('AWS::KMS::Key', {
      KeySpec: 'ECC_NIST_P256',
      KeyUsage: 'SIGN_VERIFY',
    });
    ts.config.hasResourceProperties('AWS::KMS::Alias', {
      AliasName: `alias/relay/${stage}/alert-signing-v1`,
    });
    for (const [name, t] of all(ts)) {
      const json = JSON.stringify(t.toJSON());
      expect(json, name).not.toContain('alert-keys');
      expect(json, name).not.toContain('ALERT_KEYS_SECRET_ARN');
    }
  });

  it('the key policy grants nothing beyond the account root (IAM decides)', () => {
    const key = Object.values(resources(ts.config)).find((r) => r.Type === 'AWS::KMS::Key');
    for (const s of key?.Properties['KeyPolicy'].Statement ?? []) {
      expect(JSON.stringify(s.Principal)).toMatch(/:root/);
    }
  });

  it('only the ingest Lambda holds kms:Sign, on the signing key only', () => {
    const signers: string[] = [];
    for (const [name, t] of all(ts)) {
      for (const s of statements(t)) {
        if (!s.actions.some(canSign)) continue;
        expect(s.actions, `${name} ${s.roles.join()}`).toEqual(['kms:Sign']);
        expect(JSON.stringify(s.resource)).toContain('AlertSigningKey');
        signers.push(...s.roles.map((r) => `${name}/${r}`));
      }
    }
    const ingestRoles = rolesOfFunctionsWith(ts.ingest, 'ALERT_SIGNING_KMS_KEY');
    expect(ingestRoles).toHaveLength(1);
    expect(signers).toEqual(ingestRoles.map((r) => `ingest/${r}`));
  });

  it('every verifier gets ALERT_VERIFY_KEYS and kms:GetPublicKey, and nothing that signs', () => {
    const verifierStacks = ['fanout', 'latejoin', 'delivery', 'ingest'] as const;
    for (const name of verifierStacks) {
      const t = ts[name];
      const roles = rolesOfFunctionsWith(t, 'ALERT_VERIFY_KEYS');
      expect(roles.length, name).toBeGreaterThan(0);
      for (const role of roles) {
        const granted = statements(t)
          .filter((s) => s.roles.includes(role))
          .flatMap((s) => s.actions);
        expect(granted, `${name}/${role}`).toContain('kms:GetPublicKey');
      }
    }
    // Planner and shard both verify.
    expect(rolesOfFunctionsWith(ts.fanout, 'ALERT_VERIFY_KEYS')).toHaveLength(2);
    // The sender task (ECS) verifies too.
    ts.sender.hasResourceProperties('AWS::ECS::TaskDefinition', {
      ContainerDefinitions: Match.arrayWith([
        Match.objectLike({
          Environment: Match.arrayWith([Match.objectLike({ Name: 'ALERT_VERIFY_KEYS' })]),
        }),
      ]),
    });
    expect(statements(ts.sender).flatMap((s) => s.actions)).toContain('kms:GetPublicKey');
    // The API neither signs nor reads the key.
    expect(statements(ts.api).flatMap((s) => s.actions)).not.toContain('kms:GetPublicKey');
  });

  it('the verify list names the kid the writer signs with', () => {
    const fn = Object.values(resources(ts.ingest)).find(
      (r) => r.Properties?.['Environment']?.Variables?.ALERT_SIGNING_KID !== undefined,
    );
    const vars = fn?.Properties['Environment'].Variables;
    expect(vars.ALERT_SIGNING_KID).toBe(`${stage}-alert-v1`);
    expect(JSON.stringify(vars.ALERT_VERIFY_KEYS)).toContain(`${stage}-alert-v1`);
  });
});
