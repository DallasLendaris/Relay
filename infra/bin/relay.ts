import { App, Aws } from 'aws-cdk-lib';
import { ApiStack } from '../lib/api-stack.js';
import { ConfigStack } from '../lib/config-stack.js';
import { DataStack } from '../lib/data-stack.js';
import { DeliveryStack } from '../lib/delivery-stack.js';
import { EdgeStack, publicAlertsBucketName } from '../lib/edge-stack.js';
import { FanoutStack } from '../lib/fanout-stack.js';
import { IngestStack } from '../lib/ingest-stack.js';
import { SenderStack } from '../lib/sender-stack.js';

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
// Parallel fan-out for statewide alerts. Ingest uses it only when FANOUT_MODE=queue (default off).
// Its cell-index table and send-queue ARNs/URLs are exposed as properties for the api and
// sender stacks.
const _fanout = new FanoutStack(app, `relay-${stage}-fanout`, {
  env,
  stage,
  db,
  tokenKeysSecretArn: config.tokenKeysSecretArn,
  alertKeysSecretArn: config.alertKeysSecretArn,
  killSwitchParamName: config.killSwitchParamName,
  killSwitchParamArn: config.killSwitchParamArn,
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

// Sender service. The send queues and the cell index come from the fan-out stack, which is not
// merged yet, so these are PLACEHOLDER names and ARNs that keep the app synthesizing. Replace
// them with the fan-out stack's outputs when it lands. Do not deploy this stack until then:
// the placeholders point at queues and a table that do not exist.
const queueRefs = (name: string) => ({
  name: `relay-${stage}-${name}`,
  arn: `arn:aws:sqs:us-west-2:${Aws.ACCOUNT_ID}:relay-${stage}-${name}`,
  url: `https://sqs.us-west-2.amazonaws.com/${Aws.ACCOUNT_ID}/relay-${stage}-${name}`,
});
new SenderStack(app, `relay-${stage}-sender`, {
  env,
  stage,
  db,
  urgentQueue: queueRefs('send-urgent'),
  standardQueue: queueRefs('send-standard'),
  idempotencyTableName: delivery.idempotencyTableName,
  idempotencyTableArn: delivery.idempotencyTableArn,
  cellIndexTableName: `relay-${stage}-cell-index`,
  cellIndexTableArn: `arn:aws:dynamodb:us-west-2:${Aws.ACCOUNT_ID}:table/relay-${stage}-cell-index`,
  alertKeysSecretArn: config.alertKeysSecretArn,
  killSwitchParamName: config.killSwitchParamName,
  killSwitchParamArn: config.killSwitchParamArn,
});
