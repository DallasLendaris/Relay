import type { App, Environment } from 'aws-cdk-lib';
import { ApiStack } from './api-stack.js';
import { ConfigStack } from './config-stack.js';
import { DataStack } from './data-stack.js';
import { DeliveryStack } from './delivery-stack.js';
import { EdgeStack, publicAlertsBucketName } from './edge-stack.js';
import { FanoutStack } from './fanout-stack.js';
import { IngestStack, type FanoutMode } from './ingest-stack.js';
import { LateJoinStack } from './latejoin-stack.js';
import {
  addAggregatorAlarms,
  addDeliveryAlarms,
  addFanoutAlarms,
  applyMonitoring,
  MonitoringStack,
  type MonitoringOptions,
} from './monitoring-stack.js';
import { SenderStack } from './sender-stack.js';

export interface RelayAppOptions {
  stage: 'dev' | 'prod';
  /** Main region (us-west-2). */
  env: Environment;
  /** CloudFront's WAF web ACL must live in us-east-1, so the whole edge stack does. */
  edgeEnv: Environment;
  throttle?: { rateLimit: number; burstLimit: number };
  fanoutMode?: FanoutMode;
  /** Capacity settings (bin/relay.ts reads them from CDK context). All optional; see each stack. */
  capacity?: {
    /** Aurora max ACU (default prod 16, dev 2). */
    auroraMaxAcu?: number;
    /** Reserved concurrency by Lambda; unset means unreserved (a fresh account has a low quota). */
    apiReservedConcurrency?: number;
    aggregatorReservedConcurrency?: number;
    repairReservedConcurrency?: number;
    /** Check-ins table warm write throughput (default prod 40,000, dev none; 0 disables). */
    checkinsWarmWritesPerSecond?: number;
  };
  /** Alarm email, GuardDuty, log retention and budget (bin/relay.ts reads them from context). */
  monitoring?: MonitoringOptions;
  /** Prefix of every stack id. Default `relay-${stage}`. */
  prefix?: string;
}

/**
 * Builds every stack. bin/relay.ts and the infra tests both call it, so the tests exercise the
 * real wiring. Values flow between stacks as plain props (ARNs, names, URLs).
 *
 * Dependency order (acyclic): config -> monitoring -> data -> fanout -> latejoin -> api, and sender, ingest -> edge. Delivery
 * sits beside fanout (it needs config and data only). Nothing depends back on edge: ingest names
 * the public-alerts bucket deterministically instead of referencing it.
 */
export function buildRelayApp(app: App, o: RelayAppOptions) {
  const { stage, env, edgeEnv } = o;
  const id = (name: string) => `${o.prefix ?? `relay-${stage}`}-${name}`;

  const config = new ConfigStack(app, id('config'), {
    env,
    stage,
    originVerifyReplicaRegion: edgeEnv.region,
  });
  // Every other stack depends on monitoring: they all notify its SNS topic.
  const monitoring = new MonitoringStack(app, id('monitoring'), {
    env,
    stage,
    killSwitchParamName: config.killSwitchParamName,
    ...o.monitoring,
  });
  const data = new DataStack(app, id('data'), {
    env,
    stage,
    ...(o.capacity?.auroraMaxAcu !== undefined ? { maxAcu: o.capacity.auroraMaxAcu } : {}),
  });
  const db = {
    clusterArn: data.clusterArn,
    roleSecretArns: data.roleSecretArns,
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
    alertSigning: config.alertSigning,
    ...killSwitch,
  });
  const fanout = new FanoutStack(app, id('fanout'), {
    env,
    stage,
    db,
    tokenKeysSecretArn: config.tokenKeysSecretArn,
    alertSigning: config.alertSigning,
    ...killSwitch,
  });
  const latejoin = new LateJoinStack(app, id('latejoin'), {
    env,
    stage,
    db,
    tokenKeysSecretArn: config.tokenKeysSecretArn,
    alertSigning: config.alertSigning,
    ...killSwitch,
    urgentQueue: { arn: fanout.urgentQueueArn, url: fanout.urgentQueueUrl },
    standardQueue: { arn: fanout.standardQueueArn, url: fanout.standardQueueUrl },
    idempotencyTableName: delivery.idempotencyTableName,
    idempotencyTableArn: delivery.idempotencyTableArn,
  });
  const api = new ApiStack(app, id('api'), {
    env,
    stage,
    db,
    tokenKeysSecretArn: config.tokenKeysSecretArn,
    deviceCredentialKeysSecretArn: config.deviceCredentialKeysSecretArn,
    cellIndexTableName: fanout.cellIndexTableName,
    cellIndexTableArn: fanout.cellIndexTableArn,
    originVerifySecretArn: config.originVerifySecretArn,
    lateJoinQueue: { arn: latejoin.queueArn, url: latejoin.queueUrl },
    ...(o.capacity?.checkinsWarmWritesPerSecond !== undefined
      ? { checkinsWarmWritesPerSecond: o.capacity.checkinsWarmWritesPerSecond }
      : {}),
    reservedConcurrency: {
      ...(o.capacity?.apiReservedConcurrency !== undefined
        ? { api: o.capacity.apiReservedConcurrency }
        : {}),
      ...(o.capacity?.aggregatorReservedConcurrency !== undefined
        ? { aggregator: o.capacity.aggregatorReservedConcurrency }
        : {}),
      ...(o.capacity?.repairReservedConcurrency !== undefined
        ? { repair: o.capacity.repairReservedConcurrency }
        : {}),
    },
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
    alertSigning: config.alertSigning,
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
    alertSigning: config.alertSigning,
    ...killSwitch,
  });
  const edge = new EdgeStack(app, id('edge'), {
    env: edgeEnv,
    stage,
    apiDomainName: api.apiDomainName,
    originVerifySecretName: config.originVerifySecretName,
    crossRegionReferences: true,
  });
  // The origin-verify secret must have been replicated to the edge region first.
  edge.addDependency(config);
  // Edge is deployed last: it fronts the API and the bucket that ingest writes to.
  edge.addDependency(ingest);

  // Alarms on resources the stacks do not expose are added in the owning stack's scope.
  addFanoutAlarms(fanout, {
    planQueueArn: fanout.planQueueArn,
    standardQueueName: fanout.standardQueueName,
  });
  addAggregatorAlarms(api);
  addDeliveryAlarms(delivery);

  // Every CloudWatch alarm notifies the alerts topic; every log group gets the stage's retention.
  // Edge (us-east-1) has no alarms, and an SNS topic in another region could not serve them.
  // Config is retention-only: monitoring reads its kill-switch name, so config must not depend on it.
  for (const stack of [data, delivery, fanout, latejoin, api, ingest, sender, monitoring]) {
    applyMonitoring(stack, monitoring, { alarms: true });
  }
  applyMonitoring(config, monitoring, { alarms: false });
  applyMonitoring(edge, monitoring, { alarms: false });

  return { config, monitoring, data, delivery, fanout, latejoin, api, ingest, sender, edge };
}
