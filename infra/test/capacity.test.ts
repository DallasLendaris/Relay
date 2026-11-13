import type { Template } from 'aws-cdk-lib/assertions';
import { describe, expect, it, vi } from 'vitest';
import {
  DEFAULT_LAMBDA_CONCURRENCY_QUOTA,
  LAMBDA_MIN_UNRESERVED,
  PAUSED_CONSUMER_MAX_CONCURRENCY,
  planConcurrency,
} from '../lib/capacity.js';
import { LATEJOIN_DEFAULT_CONCURRENCY } from '../lib/latejoin-stack.js';
import { synthAll } from './helpers.js';

vi.setConfig({ testTimeout: 60_000 });

const dev = synthAll('dev');
const prod = synthAll('prod');

type Res = { Properties: Record<string, any> };
const tables = (t: Template) => Object.values(t.findResources('AWS::DynamoDB::Table')) as Res[];
const fns = (t: Template) => Object.values(t.findResources('AWS::Lambda::Function')) as Res[];
const byHandler = (t: Template, handler: string): Res => {
  const f = fns(t).find((x) => x.Properties['Handler']?.endsWith(`.${handler}`));
  expect(f, handler).toBeDefined();
  return f as Res;
};
const reservedOf = (f: Res) => f.Properties['ReservedConcurrentExecutions'];
/** Every reserved-concurrency value across all stacks of one synthesized app. */
const allReserved = (all: typeof prod): number[] =>
  Object.values(all).flatMap((t) =>
    fns(t as Template).flatMap((f) => (reservedOf(f) === undefined ? [] : [reservedOf(f)])),
  );

describe('DynamoDB warm throughput (PERF-4a), prod only', () => {
  it('cell-index table: 30,000 reads/s and 10,000 writes/s on prod, none on dev', () => {
    const cellIndex = (t: Template) =>
      tables(t).find((x) => x.Properties.KeySchema.some((k: any) => k.AttributeName === 'h3Cell'));
    expect(cellIndex(prod.fanout)?.Properties.WarmThroughput).toEqual({
      ReadUnitsPerSecond: 30000,
      WriteUnitsPerSecond: 10000,
    });
    expect(cellIndex(dev.fanout)?.Properties.WarmThroughput).toBeUndefined();
  });

  it('check-in totals table: 40,000 writes/s on prod, none on dev; settable', () => {
    const totals = (t: Template) =>
      tables(t).find((x) => x.Properties.GlobalSecondaryIndexes?.[0]?.IndexName === 'byAlert');
    expect(totals(prod.api)?.Properties.WarmThroughput).toEqual({ WriteUnitsPerSecond: 40000 });
    expect(totals(dev.api)?.Properties.WarmThroughput).toBeUndefined();
    const custom = synthAll('prod', { capacity: { checkinTotalsWarmWritesPerSecond: 100000 } });
    expect(totals(custom.api)?.Properties.WarmThroughput).toEqual({
      WriteUnitsPerSecond: 100000,
    });
  });

  it('every prod table with warm throughput is a known one (no accidental extras)', () => {
    const warmed = Object.values(prod)
      .flatMap((t) => tables(t as Template))
      .filter((x) => x.Properties.WarmThroughput);
    // idempotency, cell index, check-ins, check-in totals
    expect(warmed).toHaveLength(4);
    const devWarmed = Object.values(dev)
      .flatMap((t) => tables(t as Template))
      .filter((x) => x.Properties.WarmThroughput);
    expect(devWarmed).toEqual([]);
  });
});

describe('Aurora minimum capacity (PERF-5 / OPS-10 / REL-9)', () => {
  it('prod keeps 0.5 ACU so a request never waits for a resume; dev pauses at 0', () => {
    const min = (t: Template) =>
      (Object.values(t.findResources('AWS::RDS::DBCluster'))[0] as Res).Properties
        .ServerlessV2ScalingConfiguration.MinCapacity;
    expect(min(prod.data)).toBe(0.5);
    expect(min(dev.data)).toBe(0);
  });

  it('the outbox drainer runs every minute on prod and every 5 minutes on dev', () => {
    const rate = (t: Template) =>
      (Object.values(t.findResources('AWS::Events::Rule')) as Res[]).map(
        (r) => r.Properties.ScheduleExpression,
      );
    expect(rate(prod.fanout)).toContain('rate(1 minute)');
    expect(rate(dev.fanout)).toContain('rate(5 minutes)');
    expect(rate(dev.fanout)).not.toContain('rate(1 minute)');
  });

  it('the drainer-not-running alarm spans at least 3 schedule periods on both stages', () => {
    const period = (t: Template) =>
      (
        (Object.values(t.findResources('AWS::CloudWatch::Alarm')) as Res[]).find(
          (a) => a.Properties.MetricName === 'OutboxPending',
        ) as Res
      ).Properties.Period;
    expect(period(prod.fanout)).toBe(300);
    expect(period(dev.fanout)).toBe(900);
  });
});

describe('Lambda reserved concurrency and the account quota (SEC-C-2 / RED-4)', () => {
  const quota = DEFAULT_LAMBDA_CONCURRENCY_QUOTA;

  it('prod reserves the planner, shard pool, ingest, aggregator and delivery worker', () => {
    expect(reservedOf(byHandler(prod.fanout, 'plannerHandler'))).toBe(10);
    expect(reservedOf(byHandler(prod.fanout, 'shardHandler'))).toBe(500);
    expect(reservedOf(byHandler(prod.api, 'handler'))).toBeDefined();
    const ingest = fns(prod.ingest).find((f) => reservedOf(f) !== undefined) as Res;
    expect(reservedOf(ingest)).toBe(1);
    const delivery = fns(prod.delivery).find((f) => reservedOf(f) !== undefined) as Res;
    expect(reservedOf(delivery)).toBe(10);
  });

  it('API cap + everything else reserved fits the assumed quota with 100 left unreserved', () => {
    const plan = planConcurrency();
    expect(plan.quota).toBe(1000);
    // What the templates actually say, summed over every stack.
    const total = allReserved(prod).reduce((a, b) => a + b, 0);
    expect(total).toBe(plan.reservedTotal);
    expect(total).toBeLessThanOrEqual(quota - LAMBDA_MIN_UNRESERVED);
    // The API gets exactly what is left (default quota: 1000 - 100 - 576 = 324).
    expect(plan.api).toBe(324);
    const apiFns = fns(prod.api).filter((f) => reservedOf(f) === plan.api);
    expect(apiFns).toHaveLength(1);
    expect(apiFns[0]?.Properties['Environment']?.Variables?.['REQUIRE_ORIGIN_HEADER']).toBe('true');
  });

  it('the quota is a context value: a bigger quota gives the API the difference', () => {
    const big = synthAll('prod', { capacity: { lambdaConcurrencyQuota: 15000 } });
    const total = allReserved(big).reduce((a, b) => a + b, 0);
    expect(total).toBe(15000 - LAMBDA_MIN_UNRESERVED);
    expect(planConcurrency({ lambdaConcurrencyQuota: 15000 }).api).toBe(15000 - 100 - 576);
  });

  it('dev API and aggregator stay unreserved', () => {
    expect(fns(dev.api).filter((f) => reservedOf(f) !== undefined)).toEqual([]);
  });

  it('a reservation set that does not fit the quota fails at synth time', () => {
    expect(() => planConcurrency({ lambdaConcurrencyQuota: 600 })).toThrow(/does not fit/);
    // An explicit API cap that overflows the quota.
    expect(() => planConcurrency({ apiReservedConcurrency: 900 })).toThrow(/does not fit/);
    // Exactly full is fine.
    expect(() => planConcurrency({ apiReservedConcurrency: 324 })).not.toThrow();
    expect(() => planConcurrency({ apiReservedConcurrency: 325 })).toThrow(/does not fit/);
  });

  // Review B2: late-join and the paused consumer are capped by their SQS triggers, and together
  // with the drainer (schedule, one run at a time) they must leave room in the unreserved 100.
  it('the capped unreserved functions fit in what AWS keeps unreserved', () => {
    const drainer = 1;
    const capped = LATEJOIN_DEFAULT_CONCURRENCY + PAUSED_CONSUMER_MAX_CONCURRENCY + drainer;
    expect(capped).toBeLessThanOrEqual(LAMBDA_MIN_UNRESERVED / 2);
    for (const t of [dev, prod]) {
      const caps = Object.values(t.fanout.findResources('AWS::Lambda::EventSourceMapping')).map(
        (m) => (m as Res).Properties.ScalingConfig?.MaximumConcurrency,
      );
      expect(caps).toContain(PAUSED_CONSUMER_MAX_CONCURRENCY);
    }
  });

  it('prod stage throttle never exceeds the HTTP API account quota', () => {
    const stage = (t: Template) =>
      (Object.values(t.findResources('AWS::ApiGatewayV2::Stage'))[0] as Res).Properties
        .DefaultRouteSettings;
    expect(stage(prod.api)).toEqual({ ThrottlingRateLimit: 10000, ThrottlingBurstLimit: 5000 });
    const raised = synthAll('prod', { capacity: { apiGatewayRpsQuota: 60000 } });
    expect(stage(raised.api).ThrottlingRateLimit).toBe(60000);
    expect(() => synthAll('prod', { throttle: { rateLimit: 20000, burstLimit: 5000 } })).toThrow(
      /exceeds the HTTP API account quota/,
    );
  });
});
