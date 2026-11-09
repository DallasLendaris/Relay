import { App } from 'aws-cdk-lib';
import { DataStack } from '../lib/data-stack.js';
import { IngestStack } from '../lib/ingest-stack.js';

const app = new App();

const stage = String(app.node.tryGetContext('stage') ?? 'dev');
if (stage !== 'dev' && stage !== 'prod') {
  throw new Error(`stage must be "dev" or "prod", got "${stage}"`);
}

// Account comes from the environment (CDK_DEFAULT_ACCOUNT); never hard-code it.
const env = { account: process.env.CDK_DEFAULT_ACCOUNT, region: 'us-west-2' };

new IngestStack(app, `relay-${stage}-ingest`, { env, stage });
new DataStack(app, `relay-${stage}-data`, { env, stage });
