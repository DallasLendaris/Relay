import { App, Aspects, Tags } from 'aws-cdk-lib';
import { NIST80053R5Checks } from 'cdk-nag';
import { GithubDeployStack, type DeployStage } from './github-deploy-stack.js';

/**
 * Separate CDK app for the GitHub OIDC deploy roles (S-06). The account owner runs it once per
 * AWS account, by hand, after `cdk bootstrap` (docs/runbooks/deploy.md):
 *
 *   cd infra/bootstrap
 *   npx cdk deploy -c stages=dev                  # the dev account
 *   npx cdk deploy -c stages=prod                 # the prod account
 *   # optional: -c existingOidcProviderArn=arn:aws:iam::<acct>:oidc-provider/token.actions.githubusercontent.com
 *
 * It is not part of bin/relay.ts, so the Relay synth and its tests never include it.
 */
const app = new App();

const stagesCtx = String(app.node.tryGetContext('stages') ?? '');
const stages = stagesCtx
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean) as DeployStage[];
if (stages.length === 0)
  throw new Error('pass -c stages=dev, -c stages=prod or -c stages=dev,prod');

const existingOidcProviderArn = app.node.tryGetContext('existingOidcProviderArn');
const qualifier = app.node.tryGetContext('qualifier');

const stack = new GithubDeployStack(app, 'relay-github-oidc', {
  // IAM is global; the stack itself lives in the main region. Account from the caller's credentials.
  env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: 'us-west-2' },
  stages,
  ...(existingOidcProviderArn ? { existingOidcProviderArn: String(existingOidcProviderArn) } : {}),
  ...(qualifier ? { qualifier: String(qualifier) } : {}),
});
Tags.of(stack).add('project', 'relay');
Tags.of(stack).add('purpose', 'github-deploy');

// Same NIST 800-53 rev 5 pack as the Relay app; an unsuppressed finding fails synth.
Aspects.of(app).add(new NIST80053R5Checks({ verbose: true }));
