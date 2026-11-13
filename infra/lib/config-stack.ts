import {
  CfnOutput,
  Duration,
  RemovalPolicy,
  SecretValue,
  Stack,
  StackProps,
  Tags,
} from 'aws-cdk-lib';
import { Key, KeySpec, KeyUsage } from 'aws-cdk-lib/aws-kms';
import { Secret } from 'aws-cdk-lib/aws-secretsmanager';
import { StringParameter } from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';
import { createRegionalKeys, type RegionalKeys } from './encryption.js';
import type { AlertSigningRefs } from './grants.js';

export interface ConfigStackProps extends StackProps {
  stage: 'dev' | 'prod';
  /**
   * Region of the edge stack (us-east-1). The origin-verify secret is replicated there so the
   * edge stack can resolve it by name at deploy time (see originVerifySecretName).
   */
  originVerifyReplicaRegion?: string;
  /**
   * Alert signing key versions (default [1]). Each is its own KMS ECC_NIST_P256 key; the highest
   * signs, and verifiers accept all of them. To rotate, add the next version, deploy, and drop
   * the old one once alerts signed with it have expired (README "Alert signing key").
   */
  alertSigningVersions?: number[];
}

/** The kid written into signatures by alert signing key `version` (no "." allowed). */
export const alertSigningKid = (stage: string, version: number): string =>
  `${stage}-alert-v${version}`;

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
  /** KMS asymmetric alert signing key(s): writers get kms:Sign, verifiers kms:GetPublicKey. */
  readonly alertSigning: AlertSigningRefs;
  readonly deviceCredentialKeysSecretArn: string;
  readonly expoSecretArn: string;
  /** Shared secret CloudFront sends as x-relay-origin-verify; the API Lambda checks it. */
  readonly originVerifySecretArn: string;
  readonly originVerifySecretName: string;
  readonly killSwitchParamName: string;
  readonly killSwitchParamArn: string;
  /** Customer-managed KMS keys of the main region (data, messaging, logs); see encryption.ts. */
  readonly keys: RegionalKeys;

  constructor(scope: Construct, id: string, props: ConfigStackProps) {
    super(scope, id, props);
    const { stage } = props;
    const removalPolicy = stage === 'prod' ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY;
    this.keys = createRegionalKeys(this, stage);

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
    // Alert signing (SEC-C-7, RED-2): an asymmetric KMS key, not a shared secret. The private key
    // never leaves KMS. Grants are IAM policies on the principals (see grants.ts): kms:Sign for
    // the ingest Lambda only, kms:GetPublicKey for verifiers. The default key policy delegates to
    // IAM in this account. KMS asymmetric keys cannot rotate automatically; rotate by version.
    const versions = [...new Set(props.alertSigningVersions ?? [1])].sort((a, b) => a - b);
    if (versions.length === 0 || versions.some((v) => !Number.isInteger(v) || v < 1)) {
      throw new Error('alertSigningVersions must be positive integers');
    }
    const signingKeys = versions.map((version) => {
      const key = new Key(this, version === 1 ? 'AlertSigningKey' : `AlertSigningKeyV${version}`, {
        keySpec: KeySpec.ECC_NIST_P256,
        keyUsage: KeyUsage.SIGN_VERIFY,
        alias: `alias/relay/${stage}/alert-signing-v${version}`,
        description: `Relay ${stage} alert signing key v${version} (ECDSA P-256)`,
        removalPolicy,
        pendingWindow: Duration.days(stage === 'prod' ? 30 : 7),
      });
      return { kid: alertSigningKid(stage, version), keyArn: key.keyArn };
    });
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

    // Only "running" and "paused" are valid (after trim and lower-casing). "paused" stops all
    // delivery; any other value (a typo, empty) counts as paused too and raises an alarm; a missing
    // parameter counts as running (so deleting it is alarmed). See backend/src/config/kill-switch.ts.
    const killSwitch = new StringParameter(this, 'DeliveryKillSwitch', {
      parameterName: `/relay/${stage}/delivery-kill-switch`,
      stringValue: 'running',
      description: 'Set to "paused" to stop all push delivery',
    });

    this.tokenKeysSecretArn = tokenKeys.secretArn;
    const current = signingKeys[signingKeys.length - 1];
    if (!current) throw new Error('no alert signing key');
    this.alertSigning = { current, verify: [...signingKeys].reverse() };
    this.deviceCredentialKeysSecretArn = deviceCredentialKeys.secretArn;
    this.expoSecretArn = expo.secretArn;
    this.originVerifySecretArn = originVerify.secretArn;
    this.originVerifySecretName = originVerifySecretName(stage);
    this.killSwitchParamName = killSwitch.parameterName;
    this.killSwitchParamArn = killSwitch.parameterArn;

    new CfnOutput(this, 'KillSwitchParam', { value: this.killSwitchParamName });
    new CfnOutput(this, 'AlertSigningKeyArn', { value: current.keyArn });
    new CfnOutput(this, 'AlertSigningKid', { value: current.kid });

    Tags.of(this).add('project', 'relay');
    Tags.of(this).add('stage', stage);
  }
}
