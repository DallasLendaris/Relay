import { App } from 'aws-cdk-lib';
import { applyCompliance } from '../lib/compliance.js';
import { buildRelayApp } from '../lib/relay-app.js';

const app = new App();

const stage = String(app.node.tryGetContext('stage') ?? 'dev');
if (stage !== 'dev' && stage !== 'prod') {
  throw new Error(`stage must be "dev" or "prod", got "${stage}"`);
}

// Account comes from the environment (CDK_DEFAULT_ACCOUNT); never hard-code it.
const env = { account: process.env.CDK_DEFAULT_ACCOUNT, region: 'us-west-2' };
// CloudFront's WAF web ACL must live in us-east-1, so the whole edge stack does.
const edgeEnv = { account: process.env.CDK_DEFAULT_ACCOUNT, region: 'us-east-1' };

// API stage throttle: from context (-c apiThrottleRate=... -c apiThrottleBurst=...) or stage defaults.
const rateCtx = app.node.tryGetContext('apiThrottleRate');
const burstCtx = app.node.tryGetContext('apiThrottleBurst');
const throttle =
  rateCtx !== undefined && burstCtx !== undefined
    ? { rateLimit: Number(rateCtx), burstLimit: Number(burstCtx) }
    : undefined;

// FANOUT_MODE for ingest: -c fanoutMode=queue|inline, else prod "queue" and dev "inline".
const modeCtx = app.node.tryGetContext('fanoutMode');
if (modeCtx !== undefined && modeCtx !== 'queue' && modeCtx !== 'inline') {
  throw new Error(`fanoutMode must be "queue" or "inline", got "${String(modeCtx)}"`);
}

// CDN origin for the alert documents the sender links in pushes: -c publicAlertBaseUrl=https://...
// Unset: pushes carry no alertUrl and phones open alerts through the API.
const publicAlertBaseUrl = app.node.tryGetContext('publicAlertBaseUrl');
if (
  publicAlertBaseUrl !== undefined &&
  !/^https:\/\/[^/\s]+\/?$/.test(String(publicAlertBaseUrl))
) {
  throw new Error('publicAlertBaseUrl must be an https origin such as https://alerts.example.org');
}

// Prod must name the CDN origin (PERF-3) unless -c allowNoPublicAlertUrl=true says it is deliberate.
const allowNoPublicAlertUrl = app.node.tryGetContext('allowNoPublicAlertUrl');

// Capacity settings from context, e.g. -c auroraMaxAcu=32 -c apiReservedConcurrency=3000.
// Prod reserves concurrency by default, sized from -c lambdaConcurrencyQuota (default 1000, the
// AWS default; raise the real quota first, see docs/scale-quotas.md). Dev stays unreserved.
const numberCtx = (name: string): number | undefined => {
  const raw = app.node.tryGetContext(name);
  if (raw === undefined) return undefined;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) throw new Error(`${name} must be a non-negative number`);
  return n;
};
const capacity = Object.fromEntries(
  (
    [
      'auroraMaxAcu',
      'apiReservedConcurrency',
      'aggregatorReservedConcurrency',
      'repairReservedConcurrency',
      'checkinsWarmWritesPerSecond',
      'checkinTotalsWarmWritesPerSecond',
      'lambdaConcurrencyQuota',
      'apiGatewayRpsQuota',
      'plannerReservedConcurrency',
      'shardReservedConcurrency',
    ] as const
  ).flatMap((k) => {
    const v = numberCtx(k);
    return v === undefined ? [] : [[k, v]];
  }),
);

// Monitoring: -c alarmEmail=... (required on prod unless -c allowNoAlarmEmail=true),
// -c enableGuardDuty, -c logRetentionDays, -c monthlyBudgetUsd.
const boolCtx = (name: string): boolean | undefined => {
  const raw = app.node.tryGetContext(name);
  return raw === undefined ? undefined : String(raw) === 'true';
};
const alarmEmail = app.node.tryGetContext('alarmEmail');
const enableGuardDuty = boolCtx('enableGuardDuty');
const allowNoAlarmEmail = boolCtx('allowNoAlarmEmail');
const logRetentionDays = numberCtx('logRetentionDays');
const monthlyBudgetUsd = numberCtx('monthlyBudgetUsd');
const monitoring = {
  ...(alarmEmail ? { alarmEmail: String(alarmEmail) } : {}),
  ...(allowNoAlarmEmail !== undefined ? { allowNoAlarmEmail } : {}),
  ...(enableGuardDuty !== undefined ? { enableGuardDuty } : {}),
  ...(logRetentionDays !== undefined ? { logRetentionDays } : {}),
  ...(monthlyBudgetUsd !== undefined ? { monthlyBudgetUsd } : {}),
};

const stacks = buildRelayApp(app, {
  capacity,
  monitoring,
  stage,
  env,
  edgeEnv,
  ...(throttle ? { throttle } : {}),
  ...(publicAlertBaseUrl ? { publicAlertBaseUrl: String(publicAlertBaseUrl) } : {}),
  ...(allowNoPublicAlertUrl !== undefined
    ? { allowNoPublicAlertUrl: String(allowNoPublicAlertUrl) === 'true' }
    : {}),
  ...(modeCtx ? { fanoutMode: modeCtx as 'queue' | 'inline' } : {}),
});

// cdk-nag NIST 800-53 rev 5 checks on every stack: an unsuppressed finding is an error and fails
// synth. `-c nag=off` skips them (faster local iteration; CI and the infra tests keep them on).
if (String(app.node.tryGetContext('nag') ?? 'on') !== 'off') {
  applyCompliance(app, stacks, { stage });
}
