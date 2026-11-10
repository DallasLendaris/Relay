import type { App, Environment } from 'aws-cdk-lib';
import { ApiStack } from './api-stack.js';
import { APIGW_BURST, DEFAULT_APIGW_RPS_QUOTA, planConcurrency } from './capacity.js';
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
  /** CDN origin the sender puts in push data (PUBLIC_ALERT_BASE_URL); bin/relay.ts reads `-c publicAlertBaseUrl`. */
  publicAlertBaseUrl?: string;
  /**
   * `-c allowNoPublicAlertUrl=true`: lets prod synth without publicAlertBaseUrl. Without the URL,
   * pushes carry no alertUrl and every phone opens its alert through the API, which is the load the
   * CDN exists to absorb (PERF-3). Prod therefore refuses to synth without one.
   */
  allowNoPublicAlertUrl?: boolean;
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
    /** Check-in totals table warm throughput (default prod 40,000, dev none; 0 disables). */
    checkinTotalsWarmWritesPerSecond?: number;
    /**
     * Lambda concurrent executions the account really has (default 1,000, the AWS default; see
     * docs/scale-quotas.md). Prod reserves concurrency for the planner, shard workers, ingest,
     * aggregator and delivery worker, and caps the API at what is left, so the total never
     * exceeds this minus the 100 AWS keeps unreserved. synth throws when it does not fit.
     */
    lambdaConcurrencyQuota?: number;
    /** HTTP API account throttle quota in rps (default 10,000). The prod stage throttle never exceeds it. */
    apiGatewayRpsQuota?: number;
    plannerReservedConcurrency?: number;
    shardReservedConcurrency?: number;
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
 * Dependency order (acyclic): config -> monitoring -> data -> delivery -> fanout -> latejoin -> api, and sender, ingest -> edge.
 * Delivery needs config and data only; fanout reads its idempotency table (dense-cell tombstones). Nothing depends back on edge: ingest names
 * the public-alerts bucket deterministically instead of referencing it.
 */
export function buildRelayApp(app: App, o: RelayAppOptions) {
  const { stage, env, edgeEnv } = o;
  const id = (name: string) => `${o.prefix ?? `relay-${stage}`}-${name}`;
  if (stage === 'prod' && !o.publicAlertBaseUrl && !o.allowNoPublicAlertUrl) {
    throw new Error(
      'prod needs the CDN origin for alert links: pass -c publicAlertBaseUrl=https://alerts.example.org ' +
        '(or -c allowNoPublicAlertUrl=true to deploy knowing every phone will open alerts through the API, ' +
        'not the CDN)',
    );
  }

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
    alertSigningKeyArns: config.alertSigning.verify.map((k) => k.keyArn),
    ...o.monitoring,
  });
  const data = new DataStack(app, id('data'), {
    env,
    stage,
    ...(o.capacity?.auroraMaxAcu !== undefined ? { maxAcu: o.capacity.auroraMaxAcu } : {}),
  });
  // Prod reserves concurrency so an API flood cannot starve the send path (SEC-C-2 / RED-4).
  // Dev stays unreserved, apart from the functions that always had a reservation.
  const plan = stage === 'prod' ? planConcurrency(o.capacity) : undefined;
  const rpsQuota = o.capacity?.apiGatewayRpsQuota ?? DEFAULT_APIGW_RPS_QUOTA;
  const throttle =
    o.throttle ?? (stage === 'prod' ? { rateLimit: rpsQuota, burstLimit: APIGW_BURST } : undefined);
  if (stage === 'prod' && throttle && throttle.rateLimit > rpsQuota) {
    throw new Error(
      `API throttle ${throttle.rateLimit} rps exceeds the HTTP API account quota of ${rpsQuota} rps ` +
        `(-c apiGatewayRpsQuota, docs/scale-quotas.md): the excess would never be served`,
    );
  }
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
    idempotencyTable: { name: delivery.idempotencyTableName, arn: delivery.idempotencyTableArn },
    ...(plan
      ? { plannerReservedConcurrency: plan.planner, shardReservedConcurrency: plan.shard }
      : {}),
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
    ...(o.capacity?.checkinTotalsWarmWritesPerSecond !== undefined
      ? { checkinTotalsWarmWritesPerSecond: o.capacity.checkinTotalsWarmWritesPerSecond }
      : {}),
    reservedConcurrency: plan
      ? { api: plan.api, aggregator: plan.aggregator, repair: plan.repair }
      : {
          // Dev: only what was asked for explicitly.
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
    ...(throttle ? { throttle } : {}),
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
    ...(o.publicAlertBaseUrl ? { publicAlertBaseUrl: o.publicAlertBaseUrl } : {}),
  });
  const edge = new EdgeStack(app, id('edge'), {
    env: edgeEnv,
    stage,
    apiDomainName: api.apiDomainName,
    originVerifySecretName: config.originVerifySecretName,
    ...(o.monitoring?.alarmEmail ? { alarmEmail: o.monitoring.alarmEmail } : {}),
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
  // Edge (us-east-1) has one alarm (WAF) that notifies the edge stack's own SNS topic, because an
  // alarm cannot notify a topic in another region, so the shared aspect is not applied to it.
  // Config is retention-only: monitoring reads its kill-switch name, so config must not depend on it.
  for (const stack of [data, delivery, fanout, latejoin, api, ingest, sender, monitoring]) {
    applyMonitoring(stack, monitoring, { alarms: true });
  }
  applyMonitoring(config, monitoring, { alarms: false });
  applyMonitoring(edge, monitoring, { alarms: false });

  return { config, monitoring, data, delivery, fanout, latejoin, api, ingest, sender, edge };
}
