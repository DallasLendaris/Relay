import { DefaultStackSynthesizer, Duration, Stack, type StackProps } from 'aws-cdk-lib';
import {
  Effect,
  ManagedPolicy,
  OidcProviderNative,
  PolicyStatement,
  Role,
  WebIdentityPrincipal,
} from 'aws-cdk-lib/aws-iam';
import type { Construct } from 'constructs';

/**
 * S-06 / CM-5 / SEC-C-10: the IAM roles GitHub Actions assumes to diff and deploy Relay.
 *
 * Not part of the Relay app (bin/relay.ts): the account owner deploys this stack once per AWS
 * account, by hand, with their own credentials, after `cdk bootstrap` (docs/runbooks/deploy.md).
 *
 * No long-lived keys: GitHub's OIDC token is exchanged for short-lived credentials. Each role
 * trusts one GitHub repository and one GitHub environment only, so a fork, another branch's
 * pull request or another repository cannot assume it.
 *
 * Least privilege: neither role can touch any Relay resource directly. Their only permission is
 * sts:AssumeRole on the CDK bootstrap roles of this account in Relay's two regions:
 *   - deploy role -> cdk-<q>-deploy-role, -file-publishing-role, -image-publishing-role,
 *     -lookup-role (what `cdk deploy` needs: upload assets, then drive CloudFormation)
 *   - diff role   -> cdk-<q>-lookup-role only (read-only; `cdk diff --method=template`)
 * Resource changes are made by CloudFormation through the bootstrap execution role, which is
 * the place to narrow further (`cdk bootstrap --cloudformation-execution-policies`).
 */

/** GitHub's OIDC issuer; IAM validates its TLS chain itself, so no thumbprint is pinned. */
export const GITHUB_OIDC_URL = 'https://token.actions.githubusercontent.com';
export const GITHUB_OIDC_AUDIENCE = 'sts.amazonaws.com';
/** The only repository whose workflows may assume these roles. */
export const GITHUB_REPO = 'DallasLendaris/Relay';
/** Relay's regions: the main region and us-east-1 for CloudFront/WAF (bin/relay.ts). */
export const RELAY_REGIONS = ['us-west-2', 'us-east-1'] as const;
/** CDK's default bootstrap qualifier. */
export const DEFAULT_QUALIFIER = 'hnb659fds';

export type DeployStage = 'dev' | 'prod';

/** Role names are fixed so the workflow's environment variables can name them. */
export const deployRoleName = (stage: DeployStage) => `relay-github-deploy-${stage}`;
export const diffRoleName = (stage: DeployStage) => `relay-github-diff-${stage}`;
/** GitHub environments: `<stage>` gates the deploy (prod has required reviewers); `<stage>-diff` the diff. */
export const deployEnvironment = (stage: DeployStage) => stage;
export const diffEnvironment = (stage: DeployStage) => `${stage}-diff`;
/** The OIDC `sub` claim GitHub issues to a job that runs in that environment. */
export const environmentSubject = (environment: string, repo = GITHUB_REPO) =>
  `repo:${repo}:environment:${environment}`;

export interface GithubDeployStackProps extends StackProps {
  /** Stages deployed into this account (one account per stage is the plan; both is allowed). */
  stages: DeployStage[];
  /** CDK bootstrap qualifier (default hnb659fds). */
  qualifier?: string;
  /**
   * ARN of an existing GitHub OIDC provider in the account (IAM allows one per URL). Unset: the
   * stack creates it.
   */
  existingOidcProviderArn?: string;
  /** Repository allowed to assume the roles. Default DallasLendaris/Relay. */
  repo?: string;
}

export class GithubDeployStack extends Stack {
  readonly oidcProviderArn: string;
  readonly deployRoles: Partial<Record<DeployStage, Role>> = {};
  readonly diffRoles: Partial<Record<DeployStage, Role>> = {};

  constructor(scope: Construct, id: string, props: GithubDeployStackProps) {
    // A custom qualifier applies to this stack's own deploy too (its assets and bootstrap check).
    super(scope, id, {
      ...props,
      ...(props.qualifier && !props.synthesizer
        ? { synthesizer: new DefaultStackSynthesizer({ qualifier: props.qualifier }) }
        : {}),
    });
    if (props.stages.length === 0) throw new Error('GithubDeployStack needs at least one stage');
    for (const s of props.stages) {
      if (s !== 'dev' && s !== 'prod') throw new Error(`unknown stage "${String(s)}"`);
    }
    const qualifier = props.qualifier ?? DEFAULT_QUALIFIER;
    if (!/^[a-z0-9]{1,10}$/.test(qualifier)) throw new Error('qualifier must be [a-z0-9]{1,10}');
    const repo = props.repo ?? GITHUB_REPO;

    this.oidcProviderArn =
      props.existingOidcProviderArn ??
      new OidcProviderNative(this, 'GithubOidcProvider', {
        url: GITHUB_OIDC_URL,
        clientIds: [GITHUB_OIDC_AUDIENCE],
      }).oidcProviderArn;

    const bootstrapRoleArns = (kinds: string[]) =>
      RELAY_REGIONS.flatMap((region) =>
        kinds.map(
          (kind) =>
            `arn:${this.partition}:iam::${this.account}:role/cdk-${qualifier}-${kind}-role-${this.account}-${region}`,
        ),
      );

    const githubRole = (
      id: string,
      roleName: string,
      environment: string,
      kinds: string[],
      description: string,
    ) => {
      const role = new Role(this, id, {
        roleName,
        description,
        maxSessionDuration: Duration.hours(2),
        assumedBy: new WebIdentityPrincipal(this.oidcProviderArn, {
          StringEquals: {
            'token.actions.githubusercontent.com:aud': GITHUB_OIDC_AUDIENCE,
            'token.actions.githubusercontent.com:sub': environmentSubject(environment, repo),
          },
        }),
      });
      // A customer managed policy (not an inline one) keeps cdk-nag's IAMNoInlinePolicy satisfied
      // without an exception; it is attached to this one role only.
      role.addManagedPolicy(
        new ManagedPolicy(this, `${id}Policy`, {
          description: `Lets ${roleName} assume the CDK bootstrap roles it needs, nothing else.`,
          statements: [
            new PolicyStatement({
              sid: 'AssumeCdkBootstrapRoles',
              effect: Effect.ALLOW,
              actions: ['sts:AssumeRole'],
              resources: bootstrapRoleArns(kinds),
            }),
          ],
        }),
      );
      return role;
    };

    for (const stage of props.stages) {
      const Stage = stage === 'dev' ? 'Dev' : 'Prod';
      this.deployRoles[stage] = githubRole(
        `DeployRole${Stage}`,
        deployRoleName(stage),
        deployEnvironment(stage),
        ['deploy', 'file-publishing', 'image-publishing', 'lookup'],
        `GitHub Actions (${repo}, environment ${deployEnvironment(stage)}): cdk deploy of relay-${stage}`,
      );
      this.diffRoles[stage] = githubRole(
        `DiffRole${Stage}`,
        diffRoleName(stage),
        diffEnvironment(stage),
        ['lookup'],
        `GitHub Actions (${repo}, environment ${diffEnvironment(stage)}): read-only cdk diff of relay-${stage}`,
      );
    }
  }
}
