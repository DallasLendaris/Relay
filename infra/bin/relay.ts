import { App } from 'aws-cdk-lib';
import { ApiStack } from '../lib/api-stack.js';
import { ConfigStack } from '../lib/config-stack.js';
import { DataStack } from '../lib/data-stack.js';
import { DeliveryStack } from '../lib/delivery-stack.js';
import { IngestStack } from '../lib/ingest-stack.js';

const app = new App();

const stage = String(app.node.tryGetContext('stage') ?? 'dev');
if (stage !== 'dev' && stage !== 'prod') {
  throw new Error(`stage must be "dev" or "prod", got "${stage}"`);
}

// Account comes from the environment (CDK_DEFAULT_ACCOUNT); never hard-code it.
const env = { account: process.env.CDK_DEFAULT_ACCOUNT, region: 'us-west-2' };

// One stack per concern. Values flow between stacks as plain props (ARNs and names).
const data = new DataStack(app, `relay-${stage}-data`, { env, stage });
const db = {
  clusterArn: data.clusterArn,
  secretArn: data.secretArn,
  databaseName: data.databaseName,
};
const config = new ConfigStack(app, `relay-${stage}-config`, { env, stage });
const delivery = new DeliveryStack(app, `relay-${stage}-delivery`, {
  env,
  stage,
  db,
  expoSecretArn: config.expoSecretArn,
});
new IngestStack(app, `relay-${stage}-ingest`, {
  env,
  stage,
  db,
  deliveryQueueArn: delivery.queueArn,
  deliveryQueueUrl: delivery.queueUrl,
  idempotencyTableName: delivery.idempotencyTableName,
  idempotencyTableArn: delivery.idempotencyTableArn,
  tokenKeysSecretArn: config.tokenKeysSecretArn,
  alertKeysSecretArn: config.alertKeysSecretArn,
  killSwitchParamName: config.killSwitchParamName,
  killSwitchParamArn: config.killSwitchParamArn,
});
new ApiStack(app, `relay-${stage}-api`, { env, stage, db });
