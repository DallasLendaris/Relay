import type { App, Environment } from 'aws-cdk-lib';
import { ApiStack } from './api-stack.js';
import { ConfigStack } from './config-stack.js';
import { DataStack } from './data-stack.js';
import { DeliveryStack } from './delivery-stack.js';
import { EdgeStack, publicAlertsBucketName } from './edge-stack.js';
import { FanoutStack } from './fanout-stack.js';
import { IngestStack, type FanoutMode } from './ingest-stack.js';
import { SenderStack } from './sender-stack.js';

export interface RelayAppOptions {
  stage: 'dev' | 'prod';
  /** Main region (us-west-2). */
  env: Environment;
  /** CloudFront's WAF web ACL must live in us-east-1, so the whole edge stack does. */
  edgeEnv: Environment;
  throttle?: { rateLimit: number; burstLimit: number };
  fanoutMode?: FanoutMode;
  /** Prefix of every stack id. Default `relay-${stage}`. */
  prefix?: string;
}

/**
 * Builds every stack. bin/relay.ts and the infra tests both call it, so the tests exercise the
 * real wiring. Values flow between stacks as plain props (ARNs, names, URLs).
 *
 * Dependency order (acyclic): config -> data -> fanout -> api, sender, ingest -> edge. Delivery
 * sits beside fanout (it needs config and data only). Nothing depends back on edge: ingest names
 * the public-alerts bucket deterministically instead of referencing it.
 */
export function buildRelayApp(app: App, o: RelayAppOptions) {
  const { stage, env, edgeEnv } = o;
  const id = (name: string) => `${o.prefix ?? `relay-${stage}`}-${name}`;

  const config = new ConfigStack(app, id('config'), { env, stage });
  const data = new DataStack(app, id('data'), { env, stage });
  const db = {
    clusterArn: data.clusterArn,
    secretArn: data.secretArn,
    databaseName: data.databaseName,
  };
  const killSwitch = {
    killSwitchParamName: config.killSwitchParamName,
    killSwitchParamArn: config.killSwitchParamArn,
  };

  const delivery = new DeliveryStack(app, id('delivery'), {
    env,
    stage,
    db,
    expoSecretArn: config.expoSecretArn,
    alertKeysSecretArn: config.alertKeysSecretArn,
    ...killSwitch,
  });
  const fanout = new FanoutStack(app, id('fanout'), {
    env,
    stage,
    db,
    tokenKeysSecretArn: config.tokenKeysSecretArn,
    alertKeysSecretArn: config.alertKeysSecretArn,
    ...killSwitch,
  });
  const api = new ApiStack(app, id('api'), {
    env,
    stage,
    db,
    tokenKeysSecretArn: config.tokenKeysSecretArn,
    deviceCredentialKeysSecretArn: config.deviceCredentialKeysSecretArn,
    cellIndexTableName: fanout.cellIndexTableName,
    cellIndexTableArn: fanout.cellIndexTableArn,
    ...(o.throttle ? { throttle: o.throttle } : {}),
    crossRegionReferences: true,
  });
  const ingest = new IngestStack(app, id('ingest'), {
    env,
    stage,
    db,
    deliveryQueueArn: delivery.queueArn,
    deliveryQueueUrl: delivery.queueUrl,
    planQueueArn: fanout.planQueueArn,
    planQueueUrl: fanout.planQueueUrl,
    tokenKeysSecretArn: config.tokenKeysSecretArn,
    alertKeysSecretArn: config.alertKeysSecretArn,
    ...killSwitch,
    publicAlerts: { bucketName: publicAlertsBucketName(stage), region: edgeEnv.region ?? '' },
    ...(o.fanoutMode ? { fanoutMode: o.fanoutMode } : {}),
  });
  const sender = new SenderStack(app, id('sender'), {
    env,
    stage,
    db,
    urgentQueue: {
      name: fanout.urgentQueueName,
      arn: fanout.urgentQueueArn,
      url: fanout.urgentQueueUrl,
    },
    standardQueue: {
      name: fanout.standardQueueName,
      arn: fanout.standardQueueArn,
      url: fanout.standardQueueUrl,
    },
    idempotencyTableName: delivery.idempotencyTableName,
    idempotencyTableArn: delivery.idempotencyTableArn,
    cellIndexTableName: fanout.cellIndexTableName,
    cellIndexTableArn: fanout.cellIndexTableArn,
    alertKeysSecretArn: config.alertKeysSecretArn,
    ...killSwitch,
  });
  const edge = new EdgeStack(app, id('edge'), {
    env: edgeEnv,
    stage,
    apiDomainName: api.apiDomainName,
    crossRegionReferences: true,
  });
  // Edge is deployed last: it fronts the API and the bucket that ingest writes to.
  edge.addDependency(ingest);

  return { config, data, delivery, fanout, api, ingest, sender, edge };
}
