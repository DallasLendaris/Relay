import {
  Aws,
  CfnOutput,
  Duration,
  RemovalPolicy,
  SecretValue,
  Stack,
  StackProps,
  Tags,
} from 'aws-cdk-lib';
import {
  AllowedMethods,
  CachePolicy,
  Distribution,
  HttpVersion,
  OriginProtocolPolicy,
  OriginRequestPolicy,
  PriceClass,
  ViewerProtocolPolicy,
} from 'aws-cdk-lib/aws-cloudfront';
import { HttpOrigin, S3BucketOrigin } from 'aws-cdk-lib/aws-cloudfront-origins';
import { Alarm, ComparisonOperator, Metric, TreatMissingData } from 'aws-cdk-lib/aws-cloudwatch';
import { SnsAction } from 'aws-cdk-lib/aws-cloudwatch-actions';
import { Topic } from 'aws-cdk-lib/aws-sns';
import { EmailSubscription } from 'aws-cdk-lib/aws-sns-subscriptions';
import { BlockPublicAccess, Bucket, BucketEncryption } from 'aws-cdk-lib/aws-s3';
import { CfnWebACL } from 'aws-cdk-lib/aws-wafv2';
import { Construct } from 'constructs';

/** URL prefix of the public alert objects (must match backend/src/scale/publish/paths.ts). */
export const PUBLIC_ALERTS_PATH_PATTERN = '/alerts/v1/*';
/** Immutable, content-addressed alert documents: cached for up to a year (see publish/paths.ts). */
export const PUBLIC_ALERT_DOCS_PATH_PATTERN = '/alerts/v1/doc/*';

/**
 * WAF rate limits: requests per IP per 5 minutes (the longest window WAF offers besides 10 min).
 * A WAF rate rule counts one source IP, and on mobile networks carrier-grade NAT puts thousands
 * of phones behind a single IP. So the limits are tiered by what a legitimate crowd does:
 *  - REGISTRATION: a phone registers once; 300 per 5 min per IP is generous for a NAT and still
 *    stops device-spam against /devices.
 *  - GENERAL: ordinary API use (places, alert list), 2,000 per 5 min per IP.
 *  - CHECK_IN: during an alert the whole crowd answers within minutes. A busy NAT could carry
 *    thousands of phones, each sending one check-in (plus retries), so this path gets 20,000,
 *    which is still a hard ceiling for a single IP. It is excluded from GENERAL so the general
 *    limit cannot block real check-ins. The server-side token binding is the real abuse control.
 *  - PUBLIC_READ: cached alert JSON; every phone on one NAT opens the same alert, so same 20,000.
 */
export const RATE_LIMITS = {
  REGISTRATION: 300,
  GENERAL: 2_000,
  CHECK_IN: 20_000,
  PUBLIC_READ: 20_000,
} as const;

/** Check-in is POST /alerts/{alertId}/checkins. */
export const CHECKIN_PATH_REGEX = '^/alerts/[^/]+/checkins$';

/**
 * Deterministic so other stacks (ingest, in another region) can name the bucket without a
 * cross-region reference. Aws.ACCOUNT_ID resolves to the same account in every stack.
 */
export function publicAlertsBucketName(stage: string): string {
  return `relay-${stage}-public-alerts-${Aws.ACCOUNT_ID}`;
}

export interface EdgeStackProps extends StackProps {
  stage: 'dev' | 'prod';
  /** Host name of the HTTP API (no scheme, no path), e.g. abc.execute-api.us-west-2.amazonaws.com. */
  apiDomainName: string;
  /**
   * Name of the origin-verify secret (ConfigStack, replicated to this region). CloudFront sends
   * its value as x-relay-origin-verify so the API can refuse requests that skipped the WAF.
   * Resolved by CloudFormation at deploy time; the value never appears in the template.
   */
  originVerifySecretName?: string;
  /**
   * Alarm email (the same `alarmEmail` as the monitoring stack). CloudWatch alarms can only notify
   * an SNS topic in their own region, and this stack lives in us-east-1, so it owns a small topic
   * of its own; its email subscription needs its own confirmation click.
   */
  alarmEmail?: string;
  /** Requests blocked by the web ACL in 5 minutes that raise the alarm (default 1,000). */
  wafBlockedThreshold?: number;
}

/** Name of the web ACL (also the WebACL metric dimension). */
export const webAclName = (stage: string): string => `relay-${stage}-web-acl`;

/** Header CloudFront adds to API requests (must match backend/src/api/origin-guard.ts). */
export const ORIGIN_VERIFY_HEADER = 'x-relay-origin-verify';

/**
 * Public edge: a private S3 bucket of public alert JSON and a CloudFront distribution in front of
 * it and the API, protected by AWS WAF.
 *
 * Region: a WAF web ACL with scope CLOUDFRONT can only be created in us-east-1. The simplest
 * correct option is to deploy this whole stack in us-east-1 (CloudFront itself is global, so the
 * stack's region does not affect where users are served). The alternative, a cross-region
 * construct that creates only the web ACL there, needs custom-resource glue for one resource.
 * The cost of this choice is that the bucket lives in us-east-1; the ingest Lambda (us-west-2)
 * writes to it through the bucket's regional endpoint, which is why bin/relay.ts also passes the
 * bucket region.
 */
/** Default blocked requests per 5 minutes that raise the WAF alarm. */
export const WAF_BLOCKED_THRESHOLD = 1000;

export class EdgeStack extends Stack {
  readonly publicAlertsBucket: Bucket;
  readonly distribution: Distribution;
  readonly webAcl: CfnWebACL;

  constructor(scope: Construct, id: string, props: EdgeStackProps) {
    super(scope, id, props);
    const isProd = props.stage === 'prod';

    this.publicAlertsBucket = new Bucket(this, 'PublicAlertsBucket', {
      bucketName: publicAlertsBucketName(props.stage),
      // Private: only CloudFront (through Origin Access Control) can read it.
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      encryption: BucketEncryption.S3_MANAGED,
      removalPolicy: isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
      autoDeleteObjects: !isProd,
    });

    this.webAcl = this.buildWebAcl(props.stage);

    // Honor the origin's Cache-Control (max-age=15, stale-while-revalidate=60). TTLs only clamp:
    // min 0 so the origin may go as low as it likes, default 15 s when the header is missing.
    const publicAlertsCache = new CachePolicy(this, 'PublicAlertsCachePolicy', {
      minTtl: Duration.seconds(0),
      defaultTtl: Duration.seconds(15),
      maxTtl: Duration.hours(1),
      enableAcceptEncodingGzip: true,
      enableAcceptEncodingBrotli: true,
    });

    // Documents never change under their key (the key carries a hash of the version), so the edge
    // may keep them for a year: maxTtl must be raised or CloudFront would clamp the origin's
    // max-age=31536000 to the 1 hour of the policy above.
    const publicAlertDocsCache = new CachePolicy(this, 'PublicAlertDocsCachePolicy', {
      minTtl: Duration.days(1),
      defaultTtl: Duration.days(30),
      maxTtl: Duration.days(365),
      enableAcceptEncodingGzip: true,
      enableAcceptEncodingBrotli: true,
    });

    // One origin object for both behaviors, so there is one Origin Access Control.
    const publicAlertsOrigin = S3BucketOrigin.withOriginAccessControl(this.publicAlertsBucket);

    this.distribution = new Distribution(this, 'Distribution', {
      comment: `relay-${props.stage} edge`,
      webAclId: this.webAcl.attrArn,
      priceClass: PriceClass.PRICE_CLASS_100,
      httpVersion: HttpVersion.HTTP2_AND_3,
      // Everything not matched below is the API: never cached, every viewer header forwarded
      // except Host (API Gateway needs its own host name).
      defaultBehavior: {
        origin: new HttpOrigin(props.apiDomainName, {
          protocolPolicy: OriginProtocolPolicy.HTTPS_ONLY,
          ...(props.originVerifySecretName
            ? {
                customHeaders: {
                  [ORIGIN_VERIFY_HEADER]: SecretValue.secretsManager(
                    props.originVerifySecretName,
                  ).unsafeUnwrap(),
                },
              }
            : {}),
        }),
        allowedMethods: AllowedMethods.ALLOW_ALL,
        cachePolicy: CachePolicy.CACHING_DISABLED,
        originRequestPolicy: OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
        viewerProtocolPolicy: ViewerProtocolPolicy.HTTPS_ONLY,
      },
      // CloudFront matches behaviors in this order, so the specific doc path goes first.
      additionalBehaviors: {
        [PUBLIC_ALERT_DOCS_PATH_PATTERN]: {
          origin: publicAlertsOrigin,
          allowedMethods: AllowedMethods.ALLOW_GET_HEAD,
          cachePolicy: publicAlertDocsCache,
          compress: true,
          viewerProtocolPolicy: ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
        },
        [PUBLIC_ALERTS_PATH_PATTERN]: {
          origin: publicAlertsOrigin,
          allowedMethods: AllowedMethods.ALLOW_GET_HEAD,
          cachePolicy: publicAlertsCache,
          compress: true,
          viewerProtocolPolicy: ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
        },
      },
    });

    // Rate-limit and managed-rule blocks are normal in small numbers; a burst of them is an attack
    // (or a rule blocking real users). CloudFront web ACL metrics live in us-east-1, dimension
    // Region "Global", so the alarm and its SNS topic live in this stack.
    const edgeTopic = new Topic(this, 'EdgeAlertsTopic', {
      topicName: `relay-${props.stage}-edge-alerts`,
      displayName: `Relay ${props.stage} edge alerts`,
      enforceSSL: true,
    });
    const email = props.alarmEmail?.trim();
    if (email) edgeTopic.addSubscription(new EmailSubscription(email));
    const wafBlocked = new Alarm(this, 'WafBlockedRequestsAlarm', {
      alarmDescription:
        `The edge web ACL blocked at least ${props.wafBlockedThreshold ?? WAF_BLOCKED_THRESHOLD} requests in 5 minutes ` +
        '(rate limits or managed rules). Check WAF sampled requests for the web ACL: an attack, or a rule ' +
        'blocking real users.',
      metric: new Metric({
        namespace: 'AWS/WAFV2',
        metricName: 'BlockedRequests',
        dimensionsMap: { WebACL: webAclName(props.stage), Rule: 'ALL', Region: 'Global' },
        statistic: 'Sum',
        period: Duration.minutes(5),
      }),
      threshold: props.wafBlockedThreshold ?? WAF_BLOCKED_THRESHOLD,
      evaluationPeriods: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING,
    });
    wafBlocked.addAlarmAction(new SnsAction(edgeTopic));
    wafBlocked.addOkAction(new SnsAction(edgeTopic));

    new CfnOutput(this, 'DistributionDomainName', {
      value: this.distribution.distributionDomainName,
    });
    new CfnOutput(this, 'PublicAlertsBucketName', { value: this.publicAlertsBucket.bucketName });

    Tags.of(this).add('project', 'relay');
    Tags.of(this).add('stage', props.stage);
  }

  private buildWebAcl(stage: string): CfnWebACL {
    const vis = (name: string) => ({
      cloudWatchMetricsEnabled: true,
      sampledRequestsEnabled: true,
      metricName: `relay-${stage}-${name}`,
    });

    const uriPathEquals = (path: string): CfnWebACL.StatementProperty => ({
      byteMatchStatement: {
        searchString: path,
        fieldToMatch: { uriPath: {} },
        positionalConstraint: 'EXACTLY',
        textTransformations: [{ priority: 0, type: 'NONE' }],
      },
    });
    const uriPathStartsWith = (prefix: string): CfnWebACL.StatementProperty => ({
      byteMatchStatement: {
        searchString: prefix,
        fieldToMatch: { uriPath: {} },
        positionalConstraint: 'STARTS_WITH',
        textTransformations: [{ priority: 0, type: 'NONE' }],
      },
    });
    const checkInPath: CfnWebACL.StatementProperty = {
      regexMatchStatement: {
        regexString: CHECKIN_PATH_REGEX,
        fieldToMatch: { uriPath: {} },
        textTransformations: [{ priority: 0, type: 'NONE' }],
      },
    };

    const rateRule = (
      name: string,
      priority: number,
      limit: number,
      scopeDown: CfnWebACL.StatementProperty,
    ): CfnWebACL.RuleProperty => ({
      name,
      priority,
      action: { block: {} },
      statement: {
        rateBasedStatement: {
          limit,
          aggregateKeyType: 'IP',
          evaluationWindowSec: 300,
          scopeDownStatement: scopeDown,
        },
      },
      visibilityConfig: vis(name),
    });

    const managed = (name: string, priority: number): CfnWebACL.RuleProperty => ({
      name,
      priority,
      overrideAction: { none: {} },
      statement: { managedRuleGroupStatement: { vendorName: 'AWS', name } },
      visibilityConfig: vis(name),
    });

    return new CfnWebACL(this, 'WebAcl', {
      name: webAclName(stage),
      scope: 'CLOUDFRONT',
      defaultAction: { allow: {} },
      visibilityConfig: vis('web-acl'),
      rules: [
        rateRule('RegistrationRateLimit', 1, RATE_LIMITS.REGISTRATION, uriPathEquals('/devices')),
        rateRule('CheckInRateLimit', 2, RATE_LIMITS.CHECK_IN, checkInPath),
        rateRule(
          'PublicReadRateLimit',
          3,
          RATE_LIMITS.PUBLIC_READ,
          uriPathStartsWith('/alerts/v1/'),
        ),
        // Everything else: not check-ins, not cached public reads (they have their own limits).
        rateRule('GeneralRateLimit', 4, RATE_LIMITS.GENERAL, {
          notStatement: {
            statement: {
              orStatement: { statements: [checkInPath, uriPathStartsWith('/alerts/v1/')] },
            },
          },
        }),
        managed('AWSManagedRulesCommonRuleSet', 10),
        managed('AWSManagedRulesKnownBadInputsRuleSet', 11),
      ],
    });
  }
}
