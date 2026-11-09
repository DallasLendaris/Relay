import { App } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { describe, expect, it } from 'vitest';
import { DataStack } from '../lib/data-stack.js';

function synth(stage: 'dev' | 'prod') {
  const app = new App();
  const stack = new DataStack(app, `test-${stage}`, { stage, env: { region: 'us-west-2' } });
  return Template.fromStack(stack);
}

describe('DataStack', () => {
  const t = synth('dev');

  it('is an encrypted Aurora PostgreSQL cluster with the Data API on', () => {
    t.hasResourceProperties('AWS::RDS::DBCluster', {
      Engine: 'aurora-postgresql',
      EnableHttpEndpoint: true,
      StorageEncrypted: true,
      DatabaseName: 'relay',
    });
  });

  it('scales serverless v2 between 0 and 2 ACU', () => {
    t.hasResourceProperties('AWS::RDS::DBCluster', {
      ServerlessV2ScalingConfiguration: Match.objectLike({ MinCapacity: 0, MaxCapacity: 2 }),
    });
    t.hasResourceProperties('AWS::RDS::DBInstance', { DBInstanceClass: 'db.serverless' });
  });

  it('has no NAT gateways and only isolated subnets', () => {
    t.resourceCountIs('AWS::EC2::NatGateway', 0);
    t.resourceCountIs('AWS::EC2::InternetGateway', 0);
  });

  it('generates the credentials secret and outputs both ARNs', () => {
    t.resourceCountIs('AWS::SecretsManager::Secret', 1);
    expect(Object.keys(t.toJSON().Outputs)).toEqual(
      expect.arrayContaining(['ClusterArn', 'SecretArn']),
    );
  });

  it('destroys in dev and snapshots in prod', () => {
    t.hasResource('AWS::RDS::DBCluster', { DeletionPolicy: 'Delete' });
    const prod = synth('prod');
    prod.hasResource('AWS::RDS::DBCluster', { DeletionPolicy: 'Snapshot' });
    prod.hasResourceProperties('AWS::RDS::DBCluster', { DeletionProtection: true });
  });
});
