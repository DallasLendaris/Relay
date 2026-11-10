import { App } from 'aws-cdk-lib';
import { describe, expect, it } from 'vitest';
import { buildRelayApp } from '../lib/relay-app.js';
import { SKIP_IMAGE_CONTEXT } from '../lib/sender-stack.js';
import { synthAll } from './helpers.js';

const entries = (t: any, type: string) =>
  Object.entries(t.findResources(type)) as Array<[string, any]>;
const values = (t: any, type: string) => entries(t, type).map(([, v]) => v);
const alarm = (t: any, idPrefix: string) => {
  const hit = entries(t, 'AWS::CloudWatch::Alarm').find(([k]) => k.startsWith(idPrefix));
  expect(hit, idPrefix).toBeDefined();
  return hit?.[1].Properties;
};

/** Event patterns of the rules whose source is `source`. */
const patterns = (t: any, source: string) =>
  values(t, 'AWS::Events::Rule')
    .map((r) => r.Properties)
    .filter((r) => r.EventPattern.source?.includes(source));
const eventNames = (rules: any[]): string[] =>
  rules.flatMap((r) => r.EventPattern.detail.eventName as string[]);

describe('kill-switch health alarms (silent fail-closed pause)', () => {
  for (const stage of ['dev', 'prod'] as const) {
    const t = synthAll(stage).monitoring;

    it(`${stage}: invalid value, failed-closed and missing parameter alarm on a single datapoint in 5 minutes`, () => {
      for (const [id, metric] of [
        ['KillSwitchInvalidValueAlarm', 'KillSwitchInvalidValue'],
        ['KillSwitchFailedClosedAlarm', 'KillSwitchFailedClosed'],
        ['KillSwitchParameterMissingAlarm', 'KillSwitchParameterMissing'],
      ] as const) {
        const a = alarm(t, id);
        expect(a.Namespace).toBe('Relay/Config');
        expect(a.MetricName).toBe(metric);
        expect(a.Dimensions).toEqual([{ Name: 'Source', Value: 'killswitch' }]);
        expect(a.Period).toBe(300);
        expect(a.Statistic).toBe('Sum');
        expect(a.Threshold).toBe(1);
        expect(a.EvaluationPeriods).toBe(1);
        expect(a.ComparisonOperator).toBe('GreaterThanOrEqualToThreshold');
        // The description holds the parameter name (an imported token), so look at its JSON.
        const description = JSON.stringify(a.AlarmDescription);
        expect(description.length).toBeGreaterThan(60);
        expect(description).toMatch(/check|set|recreate/i);
        expect(JSON.stringify(a.AlarmActions)).toContain('AlertsTopic');
      }
    });

    it(`${stage}: delivery paused for 15 minutes alarms (a reminder for an intentional pause)`, () => {
      const a = alarm(t, 'KillSwitchPausedAlarm');
      expect(a.MetricName).toBe('KillSwitchPaused');
      expect(a.Namespace).toBe('Relay/Config');
      expect(a.Statistic).toBe('Minimum');
      expect(a.Period).toBe(60);
      expect(a.EvaluationPeriods).toBe(15);
      expect(a.DatapointsToAlarm).toBeGreaterThanOrEqual(14);
      expect(a.Threshold).toBe(1);
      expect(a.ComparisonOperator).toBe('GreaterThanOrEqualToThreshold');
      expect(JSON.stringify(a.AlarmActions)).toContain('AlertsTopic');
    });
  }
});

describe('kill-switch parameter events (SEC-C-4)', () => {
  const t = synthAll('prod').monitoring;
  const ssm = patterns(t, 'aws.ssm');

  it('PutParameter, DeleteParameter and DeleteParameters are all matched, each on the right field', () => {
    expect(new Set(eventNames(ssm))).toEqual(
      new Set(['PutParameter', 'DeleteParameter', 'DeleteParameters']),
    );
    for (const r of ssm) {
      const names: string[] = r.EventPattern.detail.eventName;
      const rp = JSON.stringify(r.EventPattern.detail.requestParameters);
      expect(rp).toContain('DeliveryKillSwitch');
      // DeleteParameters carries `names`, the others `name`.
      expect(rp.startsWith(names.includes('DeleteParameters') ? '{"names"' : '{"name"')).toBe(true);
    }
  });

  it('deleting the parameter has its own notification that says a missing parameter fails open', () => {
    const del = ssm.filter((r) =>
      JSON.stringify(r.Targets).includes('KILL SWITCH PARAMETER DELETED'),
    );
    expect(new Set(eventNames(del))).toEqual(new Set(['DeleteParameter', 'DeleteParameters']));
    for (const r of del) expect(JSON.stringify(r.Targets)).toContain('AlertsTopic');
  });

  it('a missing parameter (which counts as running) raises the KillSwitchParameterMissing alarm', () => {
    expect(alarm(t, 'KillSwitchParameterMissingAlarm').MetricName).toBe(
      'KillSwitchParameterMissing',
    );
  });
});

describe('secret change rule (SEC-C-4)', () => {
  const [rule] = patterns(synthAll('prod').monitoring, 'aws.secretsmanager');

  it('matches every secret-changing event', () => {
    expect(rule.EventPattern.detail.eventName).toEqual(
      expect.arrayContaining([
        'PutSecretValue',
        'UpdateSecret',
        'DeleteSecret',
        'RotateSecret',
        'RestoreSecret',
        'PutResourcePolicy',
        'DeleteResourcePolicy',
        'ReplicateSecretToRegions',
      ]),
    );
    expect(JSON.stringify(rule.Targets)).toContain('AlertsTopic');
  });

  for (const stage of ['dev', 'prod'] as const) {
    it(`${stage}: every Secrets Manager secret in every stack is named relay/${stage}/..., so the rule covers it (Aurora admin secret included)`, () => {
      const all = synthAll(stage);
      const names: string[] = [];
      for (const tpl of Object.values(all)) {
        for (const s of values(tpl, 'AWS::SecretsManager::Secret')) {
          names.push(s.Properties.Name);
        }
      }
      expect(names.length).toBeGreaterThan(8);
      for (const n of names) {
        expect(typeof n, 'a secret has no fixed Name').toBe('string');
        expect(n.startsWith(`relay/${stage}/`), n).toBe(true);
      }
      expect(names).toContain(`relay/${stage}/db/relay_admin`);
      const pattern = JSON.stringify(
        patterns(all.monitoring, 'aws.secretsmanager')[0].EventPattern.detail.requestParameters,
      );
      expect(pattern).toContain(`*relay/${stage}/*`);
    });
  }
});

describe('alert-signing KMS key events (SEC-C-4)', () => {
  for (const stage of ['dev', 'prod'] as const) {
    const kms = patterns(synthAll(stage).monitoring, 'aws.kms');

    it(`${stage}: key events (delete, disable, enable, policy, grant, cancel) go to the topic`, () => {
      const keyRule = kms.find((r) => r.EventPattern.detail.requestParameters.keyId);
      expect(keyRule.EventPattern.detail.eventName).toEqual(
        expect.arrayContaining([
          'ScheduleKeyDeletion',
          'DisableKey',
          'PutKeyPolicy',
          'CreateGrant',
          'CancelKeyDeletion',
          'EnableKey',
        ]),
      );
      const ids = JSON.stringify(keyRule.EventPattern.detail.requestParameters.keyId);
      // The key itself (id taken from its imported ARN) and the alias, since callers use either.
      expect(ids).toContain('Fn::Split');
      expect(ids).toContain(`alias/relay/${stage}/alert-signing-`);
      expect(JSON.stringify(keyRule.Targets)).toContain('AlertsTopic');
    });

    it(`${stage}: alias events (update, delete) for the alert-signing alias go to the topic`, () => {
      const aliasRule = kms.find((r) => r.EventPattern.detail.requestParameters.aliasName);
      expect(aliasRule.EventPattern.detail.eventName).toEqual(
        expect.arrayContaining(['UpdateAlias', 'DeleteAlias']),
      );
      expect(JSON.stringify(aliasRule.EventPattern.detail.requestParameters)).toContain(
        `alias/relay/${stage}/alert-signing-`,
      );
      expect(JSON.stringify(aliasRule.Targets)).toContain('AlertsTopic');
    });
  }
});

describe('ingest feed stale alarm (OPS-2)', () => {
  for (const stage of ['dev', 'prod'] as const) {
    it(`${stage}: no successful poll for 10 minutes alarms, and missing data breaches`, () => {
      const a = alarm(synthAll(stage).ingest, 'IngestFeedStaleAlarm');
      expect(a.Namespace).toBe('Relay/Ingest');
      expect(a.MetricName).toBe('PollsOk');
      expect(a.Statistic).toBe('Sum');
      expect(a.Period).toBe(60);
      expect(a.EvaluationPeriods).toBe(10);
      expect(a.DatapointsToAlarm).toBe(10);
      expect(a.Threshold).toBe(1);
      expect(a.ComparisonOperator).toBe('LessThanThreshold');
      expect(a.TreatMissingData).toBe('breaching');
      expect(JSON.stringify(a.AlarmActions)).toContain('AlertsTopic');
    });
  }
});

describe('WAF blocked-requests alarm (OPS-2)', () => {
  for (const stage of ['dev', 'prod'] as const) {
    it(`${stage}: lives in the edge stack (us-east-1) and notifies an SNS topic in that stack`, () => {
      const { edge } = synthAll(stage);
      const a = alarm(edge, 'WafBlockedRequestsAlarm');
      expect(a.Namespace).toBe('AWS/WAFV2');
      expect(a.MetricName).toBe('BlockedRequests');
      expect(a.Threshold).toBe(1000);
      expect(a.Period).toBe(300);
      expect(a.Dimensions).toEqual(
        expect.arrayContaining([
          { Name: 'WebACL', Value: `relay-${stage}-web-acl` },
          { Name: 'Rule', Value: 'ALL' },
          { Name: 'Region', Value: 'Global' },
        ]),
      );
      for (const k of ['AlarmActions', 'OKActions']) {
        expect(JSON.stringify(a[k])).toContain('EdgeAlertsTopic');
      }
      edge.resourceCountIs('AWS::SNS::Topic', 1);
      // The web ACL really has the name the alarm dimension uses.
      edge.hasResourceProperties('AWS::WAFv2::WebACL', { Name: `relay-${stage}-web-acl` });
    });
  }

  it('the edge topic gets the alarm email as a subscription', () => {
    synthAll('prod').edge.hasResourceProperties('AWS::SNS::Subscription', {
      Protocol: 'email',
      Endpoint: 'ops@example.com',
    });
  });
});

describe('prod needs the CDN origin (PERF-3)', () => {
  const build = (stage: 'dev' | 'prod', extra: Record<string, unknown>) =>
    buildRelayApp(new App({ context: { [SKIP_IMAGE_CONTEXT]: 'true' } }), {
      stage,
      env: { account: '111111111111', region: 'us-west-2' },
      edgeEnv: { account: '111111111111', region: 'us-east-1' },
      prefix: `p-${stage}`,
      monitoring: { alarmEmail: 'ops@example.com' },
      ...extra,
    });

  it('prod synth fails without publicAlertBaseUrl', () => {
    expect(() => build('prod', {})).toThrow(/publicAlertBaseUrl/);
  });

  it('prod synth passes with the URL, or with the explicit escape', () => {
    expect(() => build('prod', { publicAlertBaseUrl: 'https://alerts.example.org' })).not.toThrow();
    expect(() => build('prod', { allowNoPublicAlertUrl: true })).not.toThrow();
    expect(() => build('prod', { allowNoPublicAlertUrl: false })).toThrow(/publicAlertBaseUrl/);
  });

  it('dev does not need it', () => {
    expect(() => build('dev', {})).not.toThrow();
  });
});
