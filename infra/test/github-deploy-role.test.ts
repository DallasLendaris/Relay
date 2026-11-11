import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { App, Aspects } from 'aws-cdk-lib';
import { Annotations, Match, Template } from 'aws-cdk-lib/assertions';
import { NIST80053R5Checks } from 'cdk-nag';
import { describe, expect, it } from 'vitest';
import {
  GithubDeployStack,
  deployRoleName,
  diffRoleName,
  type GithubDeployStackProps,
} from '../bootstrap/github-deploy-stack.js';
import { NIST } from '../lib/compliance.js';

// S-06 / CM-5 / SEC-C-10: the GitHub OIDC roles the deploy workflow assumes. Separate CDK app
// (infra/bootstrap/app.ts), deployed by hand once per account, never part of bin/relay.ts.

const ACCOUNT = '111111111111';

function build(props: Partial<GithubDeployStackProps> = {}) {
  const app = new App();
  const stack = new GithubDeployStack(app, 'relay-github-oidc', {
    env: { account: ACCOUNT, region: 'us-west-2' },
    stages: ['dev', 'prod'],
    ...props,
  });
  Aspects.of(app).add(new NIST80053R5Checks({ verbose: true, reports: false }));
  app.synth();
  return { stack, template: Template.fromStack(stack) };
}

const { stack, template } = build();
const resources = (type: string) =>
  Object.values(template.findResources(type)) as { Properties: any }[];
const role = (name: string) => {
  const r = resources('AWS::IAM::Role').find((x) => x.Properties.RoleName === name);
  if (!r) throw new Error(`role ${name} not found`);
  return r.Properties;
};
/** The ARNs (with the partition token flattened) a role's managed policy lets it assume. */
const assumable = (name: string): string[] => {
  const ref = role(name).ManagedPolicyArns[0].Ref as string;
  const policy = template.toJSON().Resources[ref].Properties.PolicyDocument;
  expect(policy.Statement).toHaveLength(1);
  const [s] = policy.Statement;
  expect(s.Effect).toBe('Allow');
  expect(s.Action).toBe('sts:AssumeRole');
  return s.Resource.map((r: any) =>
    typeof r === 'string' ? r : r['Fn::Join'][1].map((p: any) => (p.Ref ? 'aws' : p)).join(''),
  );
};

describe('GitHub OIDC provider', () => {
  it('trusts token.actions.githubusercontent.com for audience sts.amazonaws.com only', () => {
    template.resourceCountIs('AWS::IAM::OIDCProvider', 1);
    template.hasResourceProperties('AWS::IAM::OIDCProvider', {
      Url: 'https://token.actions.githubusercontent.com',
      ClientIdList: ['sts.amazonaws.com'],
    });
  });

  it('reuses an existing provider instead of creating a second one', () => {
    const arn = `arn:aws:iam::${ACCOUNT}:oidc-provider/token.actions.githubusercontent.com`;
    const t = build({ existingOidcProviderArn: arn }).template;
    t.resourceCountIs('AWS::IAM::OIDCProvider', 0);
    t.hasResourceProperties('AWS::IAM::Role', {
      RoleName: 'relay-github-deploy-prod',
      AssumeRolePolicyDocument: {
        Statement: [Match.objectLike({ Principal: { Federated: arn } })],
      },
    });
  });
});

describe.each([
  ['dev', 'relay-github-deploy-dev', 'environment:dev'],
  ['prod', 'relay-github-deploy-prod', 'environment:prod'],
  ['dev', 'relay-github-diff-dev', 'environment:dev-diff'],
  ['prod', 'relay-github-diff-prod', 'environment:prod-diff'],
] as const)('%s role %s', (_stage, name, env) => {
  it(`trusts only repo DallasLendaris/Relay in GitHub ${env}, via OIDC`, () => {
    const doc = role(name).AssumeRolePolicyDocument;
    expect(doc.Statement).toHaveLength(1);
    const [s] = doc.Statement;
    expect(s.Effect).toBe('Allow');
    expect(s.Action).toBe('sts:AssumeRoleWithWebIdentity');
    expect(Object.keys(s.Principal)).toEqual(['Federated']);
    // StringEquals, not StringLike: no wildcard can widen it to other branches, PRs or forks.
    expect(s.Condition).toEqual({
      StringEquals: {
        'token.actions.githubusercontent.com:aud': 'sts.amazonaws.com',
        'token.actions.githubusercontent.com:sub': `repo:DallasLendaris/Relay:${env}`,
      },
    });
  });

  it('has no inline policy and exactly one managed policy (its own)', () => {
    const p = role(name);
    expect(p.Policies).toBeUndefined();
    expect(p.ManagedPolicyArns).toHaveLength(1);
    expect(p.ManagedPolicyArns[0]).toHaveProperty('Ref');
  });
});

describe('least privilege', () => {
  const kinds = (arns: string[]) =>
    arns.map((a) => a.replace(/^arn:aws:iam::111111111111:role\/cdk-hnb659fds-/, '')).sort();

  it.each(['dev', 'prod'] as const)(
    'the %s deploy role may only assume the CDK bootstrap roles, in both Relay regions',
    (stage) => {
      expect(kinds(assumable(deployRoleName(stage)))).toEqual(
        [
          'deploy-role-111111111111-us-east-1',
          'deploy-role-111111111111-us-west-2',
          'file-publishing-role-111111111111-us-east-1',
          'file-publishing-role-111111111111-us-west-2',
          'image-publishing-role-111111111111-us-east-1',
          'image-publishing-role-111111111111-us-west-2',
          'lookup-role-111111111111-us-east-1',
          'lookup-role-111111111111-us-west-2',
        ].sort(),
      );
    },
  );

  it.each(['dev', 'prod'] as const)(
    'the %s diff role may only assume the lookup roles',
    (stage) => {
      expect(kinds(assumable(diffRoleName(stage)))).toEqual([
        'lookup-role-111111111111-us-east-1',
        'lookup-role-111111111111-us-west-2',
      ]);
    },
  );

  it('grants nothing but sts:AssumeRole anywhere in the stack', () => {
    const actions = resources('AWS::IAM::ManagedPolicy').flatMap((p) =>
      p.Properties.PolicyDocument.Statement.flatMap((s: any) => [s.Action].flat()),
    );
    expect(new Set(actions)).toEqual(new Set(['sts:AssumeRole']));
    template.resourceCountIs('AWS::IAM::Policy', 0);
    template.resourceCountIs('AWS::IAM::User', 0);
    template.resourceCountIs('AWS::IAM::AccessKey', 0);
  });

  it('honours a custom bootstrap qualifier', () => {
    const t = build({ stages: ['dev'], qualifier: 'relay1' }).template;
    expect(JSON.stringify(t.toJSON())).toContain('cdk-relay1-deploy-role-');
    expect(JSON.stringify(t.toJSON())).not.toContain('hnb659fds');
  });

  it('caps sessions at 2 hours (long enough for a first deploy)', () => {
    for (const r of resources('AWS::IAM::Role')) expect(r.Properties.MaxSessionDuration).toBe(7200);
  });
});

describe('stages', () => {
  it('creates roles only for the stages deployed into this account', () => {
    const t = build({ stages: ['prod'] }).template;
    const names = Object.values(t.findResources('AWS::IAM::Role')).map(
      (r: any) => r.Properties.RoleName,
    );
    expect(names.sort()).toEqual(['relay-github-deploy-prod', 'relay-github-diff-prod']);
  });

  it('refuses no stage or an unknown one', () => {
    expect(() => build({ stages: [] })).toThrow(/at least one stage/);
    expect(() => build({ stages: ['qa' as 'dev'] })).toThrow(/unknown stage/);
  });
});

describe('cdk-nag NIST 800-53 rev 5', () => {
  it('has no NIST errors or warnings, and no suppressions', () => {
    const nist = Match.stringLikeRegexp(`${NIST.replace(/\./g, '\\.')}-`);
    const a = Annotations.fromStack(stack);
    const show = (xs: ReturnType<typeof a.findError>) =>
      xs.map((x) => `${x.id}: ${String(x.entry.data)}`);
    expect(show(a.findError('*', nist))).toEqual([]);
    expect(show(a.findWarning('*', nist))).toEqual([]);
    expect(JSON.stringify(template.toJSON())).not.toContain('cdk_nag');
  });
});

describe('kept out of the Relay app', () => {
  const read = (p: string) => readFileSync(fileURLToPath(new URL(p, import.meta.url)), 'utf8');

  it('bin/relay.ts and lib/ never import the bootstrap stack', () => {
    expect(read('../bin/relay.ts')).not.toMatch(/bootstrap\//);
    expect(read('../lib/relay-app.ts')).not.toMatch(/bootstrap\//);
  });

  it('the bootstrap app runs the same NIST pack', () => {
    expect(read('../bootstrap/app.ts')).toContain('new NIST80053R5Checks(');
  });
});
