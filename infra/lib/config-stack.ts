import { CfnOutput, RemovalPolicy, SecretValue, Stack, StackProps, Tags } from 'aws-cdk-lib';
import { Secret } from 'aws-cdk-lib/aws-secretsmanager';
import { StringParameter } from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';

export interface ConfigStackProps extends StackProps {
  stage: 'dev' | 'prod';
}

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
  readonly expoSecretArn: string;
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

    // Optional. The value is the raw Expo access token; replace it by hand.
    const expo = new Secret(this, 'ExpoAccessToken', {
      secretName: `relay/${stage}/expo-access-token`,
      description: 'Expo push access token (placeholder until a real one is set)',
      secretStringValue: SecretValue.unsafePlainText('placeholder-set-a-real-expo-token'),
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
    this.expoSecretArn = expo.secretArn;
    this.killSwitchParamName = killSwitch.parameterName;
    this.killSwitchParamArn = killSwitch.parameterArn;

    new CfnOutput(this, 'KillSwitchParam', { value: this.killSwitchParamName });

    Tags.of(this).add('project', 'relay');
    Tags.of(this).add('stage', stage);
  }
}
