import { App } from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { ApiStack } from '../lib/api-stack.js';
import { ConfigStack } from '../lib/config-stack.js';
import { DataStack } from '../lib/data-stack.js';
import { DeliveryStack } from '../lib/delivery-stack.js';
import { IngestStack } from '../lib/ingest-stack.js';

/** Builds the whole app the same way bin/relay.ts does and returns each stack's template. */
export function synthAll(stage: 'dev' | 'prod' = 'dev') {
  const app = new App();
  const env = { region: 'us-west-2' };
  const data = new DataStack(app, `data-${stage}`, { env, stage });
  const db = {
    clusterArn: data.clusterArn,
    secretArn: data.secretArn,
    databaseName: data.databaseName,
  };
  const config = new ConfigStack(app, `config-${stage}`, { env, stage });
  const delivery = new DeliveryStack(app, `delivery-${stage}`, {
    env,
    stage,
    db,
    expoSecretArn: config.expoSecretArn,
  });
  const ingest = new IngestStack(app, `ingest-${stage}`, {
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
  const api = new ApiStack(app, `api-${stage}`, { env, stage, db });
  return {
    data: Template.fromStack(data),
    config: Template.fromStack(config),
    delivery: Template.fromStack(delivery),
    ingest: Template.fromStack(ingest),
    api: Template.fromStack(api),
  };
}
