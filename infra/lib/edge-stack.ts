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
import { BlockPublicAccess, Bucket, BucketEncryption } from 'aws-cdk-lib/aws-s3';
import { CfnWebACL } from 'aws-cdk-lib/aws-wafv2';
import { Construct } from 'constructs';

/** URL prefix of the public alert objects (must match backend/src/scale/publish/paths.ts). */
export const PUBLIC_ALERTS_PATH_PATTERN = '/alerts/v1/*';

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
}

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
      additionalBehaviors: {
        [PUBLIC_ALERTS_PATH_PATTERN]: {
          origin: S3BucketOrigin.withOriginAccessControl(this.publicAlertsBucket),
          allowedMethods: AllowedMethods.ALLOW_GET_HEAD,
          cachePolicy: publicAlertsCache,
          compress: true,
          viewerProtocolPolicy: ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
        },
      },
    });

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
