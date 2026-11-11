import { Match, type Template } from 'aws-cdk-lib/assertions';
import { describe, expect, it } from 'vitest';
import { FIPS_ENV, FIPS_EXCLUDED_FUNCTIONS, keyAlias } from '../lib/encryption.js';
import { synthAll, synthApp, type Templates } from './helpers.js';

type Res = { Type: string; Properties: Record<string, any>; DeletionPolicy?: string };
const resources = (t: Template) => t.toJSON().Resources as Record<string, Res>;
const ofType = (t: Template, type: string) =>
  Object.entries(resources(t)).filter(([, r]) => r.Type === type);

/** Logical id of the key with alias `alias` in the template (config or edge). */
function keyId(t: Template, alias: string): string {
  const a = ofType(t, 'AWS::KMS::Alias').find(([, r]) => r.Properties['AliasName'] === alias);
  expect(a, alias).toBeDefined();
  return a![1].Properties['TargetKeyId']['Fn::GetAtt'][0] as string;
}

/** True when `ref` points at key `id` of the config stack (an import) or of this stack (a GetAtt). */
function isKey(ref: unknown, id: string): boolean {
  return JSON.stringify(ref).includes(id);
}

describe.each(['dev', 'prod'] as const)('customer-managed KMS keys (%s)', (stage) => {
  const ts: Templates = synthAll(stage);
  const regional = Object.entries(ts).filter(([k]) => k !== 'edge') as [string, Template][];
  const data = keyId(ts.config, keyAlias(stage, 'data'));
  const messaging = keyId(ts.config, keyAlias(stage, 'messaging'));
  const logs = keyId(ts.config, keyAlias(stage, 'logs'));
  const edge = keyId(ts.edge, keyAlias(stage, 'edge'));

  it('has four symmetric keys with yearly rotation, retained on prod', () => {
    const symmetric = [...ofType(ts.config, 'AWS::KMS::Key'), ...ofType(ts.edge, 'AWS::KMS::Key')]
      .map(([, r]) => r)
      .filter((r) => r.Properties['KeyUsage'] !== 'SIGN_VERIFY');
    expect(symmetric).toHaveLength(4);
    for (const k of symmetric) {
      expect(k.Properties['EnableKeyRotation']).toBe(true);
      expect(k.Properties['RotationPeriodInDays']).toBe(365);
      expect(k.DeletionPolicy).toBe(stage === 'prod' ? 'Retain' : 'Delete');
    }
  });

  it('encrypts every DynamoDB table, Aurora cluster and secret with the data key', () => {
    for (const [name, t] of regional) {
      for (const [id, r] of ofType(t, 'AWS::DynamoDB::Table')) {
        expect(r.Properties['SSESpecification'], `${name}/${id}`).toMatchObject({
          SSEEnabled: true,
          SSEType: 'KMS',
        });
        expect(isKey(r.Properties['SSESpecification'].KMSMasterKeyId, data), id).toBe(true);
      }
      for (const [id, r] of ofType(t, 'AWS::RDS::DBCluster')) {
        expect(r.Properties['StorageEncrypted'], id).toBe(true);
        expect(isKey(r.Properties['KmsKeyId'], data), id).toBe(true);
      }
      for (const [id, r] of ofType(t, 'AWS::SecretsManager::Secret')) {
        expect(isKey(r.Properties['KmsKeyId'], data), `${name}/${id}`).toBe(true);
      }
    }
    expect(ofType(ts.data, 'AWS::DynamoDB::Table')).toHaveLength(0);
    expect(ofType(ts.api, 'AWS::DynamoDB::Table').length).toBeGreaterThan(0);
  });

  it('encrypts every queue and topic with the messaging key, every log group and the trail with the logs key', () => {
    for (const [name, t] of regional) {
      for (const [id, r] of ofType(t, 'AWS::SQS::Queue')) {
        expect(r.Properties['SqsManagedSseEnabled'], `${name}/${id}`).toBeUndefined();
        expect(isKey(r.Properties['KmsMasterKeyId'], messaging), `${name}/${id}`).toBe(true);
      }
      for (const [id, r] of ofType(t, 'AWS::SNS::Topic')) {
        expect(isKey(r.Properties['KmsMasterKeyId'], messaging), `${name}/${id}`).toBe(true);
      }
      for (const [id, r] of ofType(t, 'AWS::Logs::LogGroup')) {
        expect(isKey(r.Properties['KmsKeyId'], logs), `${name}/${id}`).toBe(true);
      }
    }
    for (const [, r] of ofType(ts.monitoring, 'AWS::CloudTrail::Trail')) {
      expect(isKey(r.Properties['KMSKeyId'], logs)).toBe(true);
    }
  });

  it('encrypts data buckets with SSE-KMS and bucket keys; only access log buckets stay SSE-S3', () => {
    for (const [name, t] of Object.entries(ts) as [string, Template][]) {
      for (const [id, r] of ofType(t, 'AWS::S3::Bucket')) {
        const rule = r.Properties['BucketEncryption'].ServerSideEncryptionConfiguration[0];
        if (id.startsWith('AccessLogsBucket')) {
          expect(rule.ServerSideEncryptionByDefault.SSEAlgorithm, id).toBe('AES256');
          continue;
        }
        expect(rule.ServerSideEncryptionByDefault.SSEAlgorithm, `${name}/${id}`).toBe('aws:kms');
        expect(rule.BucketKeyEnabled, `${name}/${id}`).toBe(true);
        const key = rule.ServerSideEncryptionByDefault.KMSMasterKeyID;
        const expected = name === 'edge' ? edge : id.startsWith('TrailBucket') ? logs : data;
        expect(isKey(key, expected), `${name}/${id}`).toBe(true);
      }
    }
  });

  it('the edge stack (us-east-1) uses its own key for its bucket, topic and WAF logs', () => {
    for (const [, r] of ofType(ts.edge, 'AWS::SNS::Topic')) {
      expect(isKey(r.Properties['KmsMasterKeyId'], edge)).toBe(true);
    }
    for (const [, r] of ofType(ts.edge, 'AWS::Logs::LogGroup')) {
      expect(isKey(r.Properties['KmsKeyId'], edge)).toBe(true);
    }
    expect(JSON.stringify(ts.edge.toJSON())).not.toContain('relay-' + stage + '-config:Exports');
  });

  it('lets CloudFront (OAC) decrypt public alerts only for distributions of this account', () => {
    const key = resources(ts.edge)[edge]!;
    const st = key.Properties['KeyPolicy'].Statement.find(
      (s: any) => s.Sid === 'AllowCloudFrontOacDecrypt',
    );
    expect(st.Principal).toEqual({ Service: 'cloudfront.amazonaws.com' });
    expect(st.Action).toBe('kms:Decrypt');
    expect(JSON.stringify(st.Condition)).toContain(':distribution/*');
  });

  it('limits account principals to use through the owning services (kms:ViaService)', () => {
    for (const [t, id] of [
      [ts.config, data],
      [ts.config, messaging],
      [ts.config, logs],
      [ts.edge, edge],
    ] as [Template, string][]) {
      for (const s of resources(t)[id]!.Properties['KeyPolicy'].Statement as any[]) {
        if (JSON.stringify(s.Principal) !== '{"AWS":"*"}') continue;
        expect(s.Condition.StringEquals['kms:CallerAccount'], `${id} ${s.Sid}`).toBeDefined();
        expect(s.Condition.StringEquals['kms:ViaService'], `${id} ${s.Sid}`).toBeDefined();
      }
    }
  });
});

describe('TLS', () => {
  const ts = synthAll('dev');

  it('every bucket denies requests below TLS 1.2 and without TLS', () => {
    for (const [name, t] of Object.entries(ts) as [string, Template][]) {
      const buckets = ofType(t, 'AWS::S3::Bucket').map(([id]) => id);
      const policies = ofType(t, 'AWS::S3::BucketPolicy').map(([, r]) => r);
      for (const b of buckets) {
        const policy = policies.find((p) => JSON.stringify(p.Properties['Bucket']).includes(b));
        expect(policy, `${name}/${b}`).toBeDefined();
        const st = policy!.Properties['PolicyDocument'].Statement as any[];
        expect(
          st.some((s) => s.Effect === 'Deny' && s.Condition?.NumericLessThan?.['s3:TlsVersion']),
          `${name}/${b} TLS 1.2`,
        ).toBe(true);
        expect(
          st.some((s) => s.Effect === 'Deny' && s.Condition?.Bool?.['aws:SecureTransport']),
          `${name}/${b} TLS`,
        ).toBe(true);
      }
    }
  });

  it('every queue denies requests without TLS, and every topic enforces TLS', () => {
    for (const [name, t] of Object.entries(ts) as [string, Template][]) {
      const queues = ofType(t, 'AWS::SQS::Queue').map(([id]) => id);
      const policies = ofType(t, 'AWS::SQS::QueuePolicy').map(([, r]) => r);
      for (const q of queues) {
        const policy = policies.find((p) => JSON.stringify(p.Properties['Queues']).includes(q));
        expect(policy, `${name}/${q}`).toBeDefined();
        const st = policy!.Properties['PolicyDocument'].Statement as any[];
        expect(
          st.some(
            (s) => s.Effect === 'Deny' && s.Condition?.Bool?.['aws:SecureTransport'] === 'false',
          ),
          `${name}/${q}`,
        ).toBe(true);
      }
      for (const [id] of ofType(t, 'AWS::SNS::Topic')) {
        const policy = ofType(t, 'AWS::SNS::TopicPolicy').find(([, p]) =>
          JSON.stringify(p.Properties['Topics']).includes(id),
        );
        expect(JSON.stringify(policy?.[1].Properties), `${name}/${id}`).toContain(
          'aws:SecureTransport',
        );
      }
    }
  });

  it('CloudFront talks TLS 1.2 to the API origin', () => {
    const [, d] = ofType(ts.edge, 'AWS::CloudFront::Distribution')[0]!;
    const api = d.Properties['DistributionConfig'].Origins.find((o: any) => o.CustomOriginConfig);
    expect(api.CustomOriginConfig.OriginSSLProtocols).toEqual(['TLSv1.2']);
  });

  it('with a custom domain, viewers need TLS 1.2 (TLSv1.2_2021) over SNI', () => {
    const t = synthApp('prod', {
      edgeCustomDomain: {
        domainName: 'alerts.example.org',
        certificateArn: 'arn:aws:acm:us-east-1:111111111111:certificate/abc',
      },
    }).templates.edge;
    t.hasResourceProperties('AWS::CloudFront::Distribution', {
      DistributionConfig: Match.objectLike({
        Aliases: ['alerts.example.org'],
        ViewerCertificate: {
          AcmCertificateArn: 'arn:aws:acm:us-east-1:111111111111:certificate/abc',
          MinimumProtocolVersion: 'TLSv1.2_2021',
          SslSupportMethod: 'sni-only',
        },
      }),
    });
  });
});

describe('FIPS endpoints', () => {
  const fnEnvs = (ts: Templates) =>
    (Object.entries(ts) as [string, Template][]).flatMap(([name, t]) =>
      ofType(t, 'AWS::Lambda::Function')
        // CDK's own custom-resource providers (Custom*) are not Relay code.
        .filter(([id]) => !id.startsWith('Custom'))
        .map(([id, r]) => ({ name, id, env: r.Properties['Environment']?.Variables ?? {} })),
    );
  const containerEnv = (t: Template) =>
    ofType(t, 'AWS::ECS::TaskDefinition')[0]![1].Properties['ContainerDefinitions'][0]
      .Environment as { Name: string; Value: string }[];

  it('prod sets AWS_USE_FIPS_ENDPOINT on every Lambda except the API (Amazon Location) and on the sender', () => {
    const ts = synthAll('prod');
    const fns = fnEnvs(ts);
    expect(fns.length).toBeGreaterThanOrEqual(12);
    for (const f of fns) {
      const excluded = [...FIPS_EXCLUDED_FUNCTIONS].some((x) => f.id.startsWith(x));
      expect(f.env[FIPS_ENV], `${f.name}/${f.id}`).toBe(excluded ? undefined : 'true');
    }
    expect(containerEnv(ts.sender)).toContainEqual({ Name: FIPS_ENV, Value: 'true' });
  });

  it('dev leaves it off unless asked', () => {
    const dev = synthAll('dev');
    for (const f of fnEnvs(dev)) expect(f.env[FIPS_ENV], f.id).toBeUndefined();
    expect(containerEnv(dev.sender).map((e) => e.Name)).not.toContain(FIPS_ENV);
    const on = synthApp('dev', { fips: true }).templates;
    expect(containerEnv(on.sender)).toContainEqual({ Name: FIPS_ENV, Value: 'true' });
  });
});
