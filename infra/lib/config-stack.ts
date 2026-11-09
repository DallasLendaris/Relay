import { CfnOutput, RemovalPolicy, SecretValue, Stack, StackProps, Tags } from 'aws-cdk-lib';
import { Secret } from 'aws-cdk-lib/aws-secretsmanager';
import { StringParameter } from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';

export interface ConfigStackProps extends StackProps {
  stage: 'dev' | 'prod';
  /**
   * Region of the edge stack (us-east-1). The origin-verify secret is replicated there so the
   * edge stack can resolve it by name at deploy time (see originVerifySecretName).
   */
  originVerifyReplicaRegion?: string;
}

/** Name of the origin-verify secret; the edge stack resolves its replica by this name. */
export const originVerifySecretName = (stage: string): string => `relay/${stage}/origin-verify`;

/** Marker the seed-keys script looks for. Never a usable key. */
export const KEY_PLACEHOLDER = 'REPLACE_ME_RUN_SEED_KEYS';

/**
 * Shared secrets and runtime switches. Lives in its own stack so that tearing down
 * ingest or delivery never deletes the signing keys.
 *
 * Key secrets use the keyring shape {"current":"k1","keys":{"k1":"<base64>"}}.
 * CDK's generateSecretString cannot put a generated string inside a nested object,
 * so the stack creates the secret with a placeholder and `npm run seed-keys`
 * replaces it with random bytes (see README). No custom resource needed.
 */
export class ConfigStack extends Stack {
  readonly tokenKeysSecretArn: string;
  readonly alertKeysSecretArn: string;
  readonly deviceCredentialKeysSecretArn: string;
  readonly expoSecretArn: string;
  /** Shared secret CloudFront sends as x-relay-origin-verify; the API Lambda checks it. */
  readonly originVerifySecretArn: string;
  readonly originVerifySecretName: string;
  readonly killSwitchParamName: string;
  readonly killSwitchParamArn: string;

  constructor(scope: Construct, id: string, props: ConfigStackProps) {
    super(scope, id, props);
    const { stage } = props;
    const removalPolicy = stage === 'prod' ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY;

    const keyring = (logicalId: string, name: string) =>
      new Secret(this, logicalId, {
        secretName: `relay/${stage}/${name}`,
        description: `Relay ${stage} ${name} keyring (rotate with the README procedure)`,
        secretStringValue: SecretValue.unsafePlainText(
          JSON.stringify({ current: 'k1', keys: { k1: KEY_PLACEHOLDER } }),
        ),
        removalPolicy,
      });

    const tokenKeys = keyring('TokenKeys', 'token-keys');
    const alertKeys = keyring('AlertKeys', 'alert-keys');
    // Signs and verifies stateless device credentials (check-ins). Same placeholder shape, so the
    // backend keyring loader rejects it until `npm run seed-keys` has run.
    const deviceCredentialKeys = keyring('DeviceCredentialKeys', 'device-credential-keys');

    // Optional. The value is the raw Expo access token; replace it by hand.
    const expo = new Secret(this, 'ExpoAccessToken', {
      secretName: `relay/${stage}/expo-access-token`,
      description: 'Expo push access token (placeholder until a real one is set)',
      secretStringValue: SecretValue.unsafePlainText('placeholder-set-a-real-expo-token'),
      removalPolicy,
    });

    // Origin verification. Anyone can call the API's execute-api URL directly and skip the WAF
    // on CloudFront, so CloudFront adds this secret as an origin header and the API Lambda
    // refuses requests without it. CloudFront needs the literal value when the distribution is
    // deployed, but the edge stack lives in us-east-1 and this one in us-west-2. A Secrets Manager
    // replica solves it without copying the value anywhere: the secret is generated once here,
    // replicated to the edge region under the same name, and the edge stack uses the CloudFormation
    // dynamic reference {{resolve:secretsmanager:<name>}}, which CloudFormation resolves at
    // deploy time (the value never appears in a template or in cdk.out). The API Lambda reads
    // the primary by ARN. To rotate: update the primary's value, then redeploy the edge stack
    // so CloudFront picks it up; the API Lambda refreshes within 5 minutes (cache TTL), so
    // deploy edge first and expect up to 5 minutes of 403s on rotation, or rotate in two steps.
    const originVerify = new Secret(this, 'OriginVerify', {
      secretName: originVerifySecretName(stage),
      description: `Relay ${stage} CloudFront to API origin verification header value`,
      generateSecretString: { passwordLength: 48, excludePunctuation: true },
      ...(props.originVerifyReplicaRegion
        ? { replicaRegions: [{ region: props.originVerifyReplicaRegion }] }
        : {}),
      removalPolicy,
    });

    // "paused" stops all delivery; any other value means running.
    const killSwitch = new StringParameter(this, 'DeliveryKillSwitch', {
      parameterName: `/relay/${stage}/delivery-kill-switch`,
      stringValue: 'running',
      description: 'Set to "paused" to stop all push delivery',
    });

    this.tokenKeysSecretArn = tokenKeys.secretArn;
    this.alertKeysSecretArn = alertKeys.secretArn;
    this.deviceCredentialKeysSecretArn = deviceCredentialKeys.secretArn;
    this.expoSecretArn = expo.secretArn;
    this.originVerifySecretArn = originVerify.secretArn;
    this.originVerifySecretName = originVerifySecretName(stage);
    this.killSwitchParamName = killSwitch.parameterName;
    this.killSwitchParamArn = killSwitch.parameterArn;

    new CfnOutput(this, 'KillSwitchParam', { value: this.killSwitchParamName });

    Tags.of(this).add('project', 'relay');
    Tags.of(this).add('stage', stage);
  }
}
