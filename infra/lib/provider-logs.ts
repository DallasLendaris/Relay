import { CfnResource, RemovalPolicy, Stack } from 'aws-cdk-lib';
import { CfnFunction } from 'aws-cdk-lib/aws-lambda';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import type { IConstruct } from 'constructs';

/**
 * Log groups for the Lambdas of CDK's own custom-resource providers (NIST AU-11, SC-28).
 *
 * CDK's providers (`Custom::VpcRestrictDefaultSG`, dev `Custom::S3AutoDeleteObjects`, and the
 * `Custom::CrossRegionExportWriter` / `Reader` behind `crossRegionReferences`) are plain
 * `AWS::Lambda::Function` CfnResources, not `aws-lambda.Function`, so they take no `logGroup` prop.
 * Left alone, Lambda creates `/aws/lambda/<generated name>` at run time: no retention (kept
 * forever) and no customer-managed key. Instead each one gets a declared LogGroup in its stack and
 * `LoggingConfig.LogGroup` pointing at it. The LogGroup is an ordinary construct, so the stage's
 * retention (LogRetentionAspect) and the logs key (encryptResources) apply to it like any other.
 * The provider roles carry AWSLambdaBasicExecutionRole (logs:CreateLogStream / PutLogEvents on
 * any log group), so no extra permission is needed.
 *
 * Two kinds:
 *  - Providers created with their construct (VPC, bucket): found by `declareProviderLogs`.
 *  - Cross-region export providers: CDK creates them while resolving references, after every
 *    aspect has run, so neither this function nor an aspect can see them. The caller names them
 *    in `late`; their LogGroup is declared now and attached by a validation hook once they exist.
 *    The hook fails synth when a provider appears that has no declared log group, or when a
 *    declared one is never used, so this list cannot silently go stale.
 */

/** Construct id of the provider CDK creates for a cross-region reference (core custom-resource-provider). */
export const CROSS_REGION_WRITER = 'Custom::CrossRegionExportWriterCustomResourceProvider';
export const CROSS_REGION_READER = 'Custom::CrossRegionExportReaderCustomResourceProvider';

/** The stack-level construct id of the LogGroup declared for a provider. */
export const providerLogGroupId = (providerId: string): string =>
  `${providerId.replace(/[^A-Za-z0-9]/g, '')}Logs`;

const isProviderHandler = (node: IConstruct): node is CfnResource =>
  node instanceof CfnResource &&
  !(node instanceof CfnFunction) &&
  node.cfnResourceType === 'AWS::Lambda::Function';

/** The provider construct (direct child of the stack) a handler belongs to. */
const providerOf = (handler: CfnResource): IConstruct => {
  const parent = handler.node.scope;
  if (!parent || !(parent.node.scope instanceof Stack)) {
    throw new Error(`${handler.node.path}: unexpected provider Lambda location`);
  }
  return parent;
};

function declare(stack: Stack, providerId: string, isProd: boolean): LogGroup {
  const id = providerLogGroupId(providerId);
  const existing = stack.node.tryFindChild(id);
  if (existing instanceof LogGroup) return existing;
  return new LogGroup(stack, id, {
    // Overridden by LogRetentionAspect; stated so the intent survives without it.
    retention: RetentionDays.ONE_MONTH,
    removalPolicy: isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
  });
}

const attach = (handler: CfnResource, group: LogGroup): void =>
  handler.addPropertyOverride('LoggingConfig', { LogGroup: group.logGroupName });

/**
 * Declares a log group for every CDK provider Lambda of `stack`, plus one for each provider in
 * `late` (created later by CDK; see above). Call before encryptResources so the groups get the key.
 */
export function declareProviderLogs(
  stack: Stack,
  opts: { isProd: boolean; late?: readonly string[] },
): void {
  for (const handler of stack.node.findAll().filter(isProviderHandler)) {
    attach(handler, declare(stack, providerOf(handler).node.id, opts.isProd));
  }
  const late = new Map((opts.late ?? []).map((id) => [id, declare(stack, id, opts.isProd)]));

  stack.node.addValidation({
    validate: () => {
      const errors: string[] = [];
      const used = new Set<string>();
      for (const handler of stack.node.findAll().filter(isProviderHandler)) {
        const providerId = providerOf(handler).node.id;
        const group = stack.node.tryFindChild(providerLogGroupId(providerId));
        if (!(group instanceof LogGroup)) {
          errors.push(
            `${handler.node.path}: CDK provider Lambda without a declared log group ` +
              '(add it to `late` in relay-app.ts, see provider-logs.ts)',
          );
          continue;
        }
        if (late.has(providerId)) {
          // Created after declareProviderLogs ran: attach now (idempotent).
          attach(handler, group);
          used.add(providerId);
        }
      }
      for (const id of late.keys()) {
        if (!used.has(id)) {
          errors.push(
            `${stack.node.path}: log group declared for ${id}, which this stack never creates`,
          );
        }
      }
      return errors;
    },
  });
}
