import { App } from 'aws-cdk-lib';
import { ApiStack } from '../lib/api-stack.js';
import { ConfigStack } from '../lib/config-stack.js';
import { DataStack } from '../lib/data-stack.js';
import { DeliveryStack } from '../lib/delivery-stack.js';
import { EdgeStack, publicAlertsBucketName } from '../lib/edge-stack.js';
import { IngestStack } from '../lib/ingest-stack.js';

const app = new App();

const stage = String(app.node.tryGetContext('stage') ?? 'dev');
if (stage !== 'dev' && stage !== 'prod') {
  throw new Error(`stage must be "dev" or "prod", got "${stage}"`);
}

// Account comes from the environment (CDK_DEFAULT_ACCOUNT); never hard-code it.
const env = { account: process.env.CDK_DEFAULT_ACCOUNT, region: 'us-west-2' };
// CloudFront's WAF web ACL must live in us-east-1, so the whole edge stack does.
const edgeEnv = { account: process.env.CDK_DEFAULT_ACCOUNT, region: 'us-east-1' };
const publicAlerts = { bucketName: publicAlertsBucketName(stage), region: edgeEnv.region };

// API stage throttle: from context (-c apiThrottleRate=... -c apiThrottleBurst=...) or stage defaults.
const rateCtx = app.node.tryGetContext('apiThrottleRate');
const burstCtx = app.node.tryGetContext('apiThrottleBurst');
const throttle =
  rateCtx !== undefined && burstCtx !== undefined
    ? { rateLimit: Number(rateCtx), burstLimit: Number(burstCtx) }
    : undefined;

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
  alertKeysSecretArn: config.alertKeysSecretArn,
  killSwitchParamName: config.killSwitchParamName,
  killSwitchParamArn: config.killSwitchParamArn,
});
new IngestStack(app, `relay-${stage}-ingest`, {
  env,
  stage,
  db,
  deliveryQueueArn: delivery.queueArn,
  deliveryQueueUrl: delivery.queueUrl,
  tokenKeysSecretArn: config.tokenKeysSecretArn,
  alertKeysSecretArn: config.alertKeysSecretArn,
  killSwitchParamName: config.killSwitchParamName,
  killSwitchParamArn: config.killSwitchParamArn,
  publicAlerts,
});
const api = new ApiStack(app, `relay-${stage}-api`, {
  env,
  stage,
  db,
  tokenKeysSecretArn: config.tokenKeysSecretArn,
  throttle,
  crossRegionReferences: true,
});
new EdgeStack(app, `relay-${stage}-edge`, {
  env: edgeEnv,
  stage,
  apiDomainName: api.apiDomainName,
  crossRegionReferences: true,
});
