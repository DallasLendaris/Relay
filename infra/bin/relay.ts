import { App } from 'aws-cdk-lib';
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

buildRelayApp(app, {
  stage,
  env,
  edgeEnv,
  ...(throttle ? { throttle } : {}),
  ...(modeCtx ? { fanoutMode: modeCtx as 'queue' | 'inline' } : {}),
});
