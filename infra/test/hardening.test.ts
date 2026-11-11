import { Match } from 'aws-cdk-lib/assertions';
import { describe, expect, it } from 'vitest';
import { wafLogGroupName } from '../lib/edge-stack.js';
import { synthAll } from './helpers.js';

const dev = synthAll('dev');
const prod = synthAll('prod');

const buckets = (t: (typeof dev)['ingest']) =>
  Object.entries(t.findResources('AWS::S3::Bucket')) as [string, any][];

describe('NIST logging and recovery configuration (cdk-nag part A)', () => {
  it('records VPC flow logs (all traffic) in the data and sender VPCs', () => {
    for (const t of [dev.data, dev.sender]) {
      t.hasResourceProperties('AWS::EC2::FlowLog', {
        ResourceType: 'VPC',
        TrafficType: 'ALL',
        LogDestinationType: 'cloud-watch-logs',
      });
    }
  });

  it('strips the rules of the default security groups', () => {
    for (const t of [dev.data, dev.sender]) {
      expect(Object.keys(t.findResources('Custom::VpcRestrictDefaultSG'))).toHaveLength(1);
    }
  });

  it('exports Postgres logs to a log group the stack owns, and enables enhanced monitoring', () => {
    dev.data.hasResourceProperties('AWS::RDS::DBCluster', {
      DBClusterIdentifier: 'relay-dev',
      EnableCloudwatchLogsExports: ['postgresql'],
    });
    dev.data.hasResourceProperties('AWS::RDS::DBInstance', { MonitoringInterval: 60 });
    dev.data.hasResourceProperties('AWS::Logs::LogGroup', {
      LogGroupName: '/aws/rds/cluster/relay-dev/postgresql',
    });
  });

  it('keeps point-in-time recovery on every prod table', () => {
    for (const t of [prod.api, prod.delivery, prod.fanout, prod.ingest]) {
      for (const table of Object.values(t.findResources('AWS::DynamoDB::Table')) as any[]) {
        expect(table.Properties.PointInTimeRecoverySpecification.PointInTimeRecoveryEnabled).toBe(
          true,
        );
      }
    }
  });

  it('versions every data bucket and sends its access logs to the stack access log bucket', () => {
    for (const t of [dev.ingest, dev.edge, dev.monitoring]) {
      const all = buckets(t);
      const logs = all.filter(([id]) => id.startsWith('AccessLogsBucket'));
      expect(logs).toHaveLength(1);
      const logsId = logs[0]![0];
      for (const [id, b] of all) {
        expect(b.Properties.VersioningConfiguration, id).toEqual({ Status: 'Enabled' });
        if (id === logsId) continue;
        expect(b.Properties.LoggingConfiguration.DestinationBucketName, id).toEqual({
          Ref: logsId,
        });
      }
      // Delivery through a bucket policy, not ACLs.
      t.hasResourceProperties('AWS::S3::BucketPolicy', {
        Bucket: { Ref: logsId },
        PolicyDocument: {
          Statement: Match.arrayWith([
            Match.objectLike({
              Principal: { Service: 'logging.s3.amazonaws.com' },
              Action: 's3:PutObject',
            }),
          ]),
        },
      });
    }
  });

  it('logs blocked and counted WAF requests with the authorization header redacted', () => {
    dev.edge.hasResourceProperties('AWS::WAFv2::LoggingConfiguration', {
      RedactedFields: [{ SingleHeader: { Name: 'authorization' } }],
      LoggingFilter: Match.objectLike({ DefaultBehavior: 'DROP' }),
    });
    dev.edge.hasResourceProperties('AWS::Logs::LogGroup', {
      LogGroupName: wafLogGroupName('dev'),
    });
  });
});
