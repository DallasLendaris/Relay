import { fileURLToPath } from 'node:url';
import {
  Annotations,
  CfnOutput,
  Duration,
  Fn,
  RemovalPolicy,
  Stack,
  StackProps,
  Tags,
} from 'aws-cdk-lib';
import { AccessLogFormat } from 'aws-cdk-lib/aws-apigateway';
import {
  HttpApi,
  HttpMethod,
  HttpStage,
  LogGroupLogDestination,
} from 'aws-cdk-lib/aws-apigatewayv2';
import { HttpUserPoolAuthorizer } from 'aws-cdk-lib/aws-apigatewayv2-authorizers';
import { HttpLambdaIntegration } from 'aws-cdk-lib/aws-apigatewayv2-integrations';
import { Alarm, ComparisonOperator, TreatMissingData } from 'aws-cdk-lib/aws-cloudwatch';
import {
  AccountRecovery,
  CfnUserPoolGroup,
  ClientAttributes,
  FeaturePlan,
  LambdaVersion,
  Mfa,
  StandardThreatProtectionMode,
  StringAttribute,
  UserPool,
  UserPoolClient,
  UserPoolClientIdentityProvider,
  UserPoolOperation,
} from 'aws-cdk-lib/aws-cognito';
import { EventField, Rule, RuleTargetInput } from 'aws-cdk-lib/aws-events';
import { SnsTopic } from 'aws-cdk-lib/aws-events-targets';
import { AnyPrincipal, Effect, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { Architecture, Runtime } from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { FilterPattern, LogGroup, MetricFilter, RetentionDays } from 'aws-cdk-lib/aws-logs';
import {
  BlockPublicAccess,
  Bucket,
  BucketEncryption,
  ObjectLockRetention,
  ObjectOwnership,
} from 'aws-cdk-lib/aws-s3';
import type { ITopic } from 'aws-cdk-lib/aws-sns';
import { Construct } from 'constructs';
import { accessLogsBucket, logAccessTo } from './buckets.js';
import { allow, grantSecretRead } from './grants.js';

/**
 * DRAFT agency side: agency login (Cognito), the read-only agency API and its audit trail.
 * The agency side has no owner yet; every choice the owner or the State must confirm is listed in
 * docs/agency-auth.md under "Decisions". Nothing here can send, change or cancel an alert.
 *
 *   viewer -> CloudFront + WAF (/agency/*) -> agency HTTP API (JWT authorizer, access logs)
 *          -> AgencyFunction: origin header, claims, role, jurisdiction, data, audit -> reply
 *
 * The agency API is its own HTTP API (not routes on the public API): its stage access log is
 * the record of requests the JWT authorizer refuses before any code runs, it gets its own small
 * throttle, and the whole agency side can be changed or removed without touching the public API.
 */

/** Cognito groups (= roles). Keep in sync with backend/src/agency/roles.ts (a test checks). */
export const AGENCY_GROUPS = [
  { name: 'relay-admin', precedence: 0, description: 'Relay operations: manages agency users' },
  { name: 'agency-operator', precedence: 10, description: 'Agency staff, future agency actions' },
  { name: 'agency-viewer', precedence: 20, description: 'Agency staff, read-only summaries' },
] as const;

/** Routes of the agency API. Keep in sync with /agency/* in packages/shared/openapi/relay.yaml. */
export const AGENCY_ROUTES: ReadonlyArray<{ method: HttpMethod; path: string }> = [
  { method: HttpMethod.GET, path: '/agency/me' },
  { method: HttpMethod.GET, path: '/agency/alerts/{alertId}/summary' },
];

/** Session lifetimes (NIST AC-12). Cognito minimums: 5 min for access/ID, 60 min for refresh. */
export const AGENCY_TOKEN_LIFETIMES = {
  accessMinutes: 15,
  idMinutes: 15,
  /** One working shift: after it the user signs in again with password and TOTP. */
  refreshHours: 8,
  /** Time to finish a sign-in (password -> TOTP challenge). */
  authSessionMinutes: 3,
} as const;

export const AGENCY_PASSWORD_POLICY = { minLength: 14, historySize: 24, tempPasswordDays: 3 };

/** Audit record retention (lifecycle) and Object Lock, in days, by stage. */
export const DEFAULT_AGENCY_AUDIT_RETENTION_DAYS = { dev: 30, prod: 365 } as const;
export const DEFAULT_AGENCY_AUDIT_LOCK_DAYS = { dev: 1, prod: 365 } as const;
/** Small-cell suppression default (backend DEFAULT_MIN_CELL_COUNT), pending the privacy floor decision. */
export const DEFAULT_AGENCY_MIN_CELL_COUNT = 10;
/** 401/403 responses in 5 minutes that raise AgencyDeniedRequestsAlarm. */
export const DEFAULT_AGENCY_DENIED_THRESHOLD = 20;
/** Stage throttle: a few dozen agency users never need more. */
export const AGENCY_THROTTLE = { rateLimit: 20, burstLimit: 40 } as const;
export const AGENCY_AUDIT_PREFIX = 'agency-audit/v1/';
export const AGENCY_METRIC_NAMESPACE = 'Relay/Agency';

/** Role that may delete or re-retain audit objects. Not created here: see docs/agency-auth.md. */
export const auditBreakGlassRoleName = (stage: string) => `relay-${stage}-audit-break-glass`;

/** S3 actions on the audit bucket only the break-glass role may call. */
export const AUDIT_PROTECTED_ACTIONS = [
  's3:DeleteObject',
  's3:DeleteObjectVersion',
  's3:PutObjectRetention',
  's3:PutObjectLegalHold',
  's3:BypassGovernanceRetention',
] as const;

/** CloudTrail management events on the audit bucket's configuration that are reported. */
export const AUDIT_BUCKET_EVENTS = [
  'PutBucketPolicy',
  'DeleteBucketPolicy',
  'PutBucketLifecycle',
  'DeleteBucketLifecycle',
  'PutBucketVersioning',
  'PutObjectLockConfiguration',
  'PutBucketLogging',
  'DeleteBucket',
] as const;

/** Cognito management events that weaken or re-plumb sign-in, reported to the alerts topic. */
export const USER_POOL_SECURITY_EVENTS = [
  'SetUserPoolMfaConfig',
  'UpdateUserPool',
  'DeleteUserPool',
  'CreateUserPoolClient',
  'UpdateUserPoolClient',
  'DeleteUserPoolClient',
  'CreateIdentityProvider',
  'UpdateIdentityProvider',
  'DeleteIdentityProvider',
  'SetRiskConfiguration',
] as const;

export interface AgencyOptions {
  /** Context `agencyAuditRetentionDays`: audit objects expire after this (prod 365, dev 30). */
  auditRetentionDays?: number;
  /** Context `agencyAuditObjectLockDays`: Object Lock governance retention (prod 365, dev 1). */
  auditObjectLockDays?: number;
  /** Context `agencyMinCellCount`: small-cell threshold (default 10, pending the privacy floor). */
  minCellCount?: number;
  /** Context `agencyDeniedAlarmThreshold`: denied requests per 5 minutes that alarm (default 20). */
  deniedAlarmThreshold?: number;
}

export interface AgencyStackProps extends StackProps, AgencyOptions {
  stage: 'dev' | 'prod';
  /** Per-cell check-in totals (ApiStack). The agency API only queries its byAlert index. */
  checkinTotals: { tableName: string; tableArn: string };
  /** Zone pack (IngestStack): county boundaries used to place cells in counties. */
  zonePack: { bucketName: string; bucketArn: string };
  /** Secret CloudFront sends as x-relay-origin-verify (ConfigStack). */
  originVerifySecretArn: string;
  /** Default: prod true, dev false (as the public API). */
  requireOriginHeader?: boolean;
  /** The monitoring stack's alerts topic (EventBridge rules publish to it). */
  alertsTopic: ITopic;
}

function checkDays(name: string, days: number, max = 36_500): void {
  if (!Number.isInteger(days) || days < 1 || days > max) {
    throw new Error(`${name} ${days} must be a whole number of days, 1 to ${max}`);
  }
}

export class AgencyStack extends Stack {
  /** Host name of the agency HTTP API, for the CloudFront /agency/* behavior. */
  readonly apiDomainName: string;
  readonly userPool: UserPool;
  readonly userPoolClient: UserPoolClient;
  readonly auditBucket: Bucket;

  constructor(scope: Construct, id: string, props: AgencyStackProps) {
    super(scope, id, props);
    const { stage } = props;
    const isProd = stage === 'prod';
    const requireOrigin = props.requireOriginHeader ?? isProd;
    const retentionDays = props.auditRetentionDays ?? DEFAULT_AGENCY_AUDIT_RETENTION_DAYS[stage];
    const lockDays = props.auditObjectLockDays ?? DEFAULT_AGENCY_AUDIT_LOCK_DAYS[stage];
    checkDays('agencyAuditRetentionDays', retentionDays);
    checkDays('agencyAuditObjectLockDays', lockDays);
    if (lockDays > retentionDays) {
      throw new Error(
        `agencyAuditObjectLockDays ${lockDays} exceeds agencyAuditRetentionDays ${retentionDays}`,
      );
    }
    const minCellCount = props.minCellCount ?? DEFAULT_AGENCY_MIN_CELL_COUNT;
    if (!Number.isInteger(minCellCount) || minCellCount < 1) {
      throw new Error(`agencyMinCellCount ${minCellCount} must be a positive integer`);
    }
    const deniedThreshold = props.deniedAlarmThreshold ?? DEFAULT_AGENCY_DENIED_THRESHOLD;
    if (!Number.isInteger(deniedThreshold) || deniedThreshold < 1) {
      throw new Error(`agencyDeniedAlarmThreshold ${deniedThreshold} must be a positive integer`);
    }

    const lambdaDefaults = {
      depsLockFilePath: fileURLToPath(new URL('../../package-lock.json', import.meta.url)),
      projectRoot: fileURLToPath(new URL('../../', import.meta.url)),
      handler: 'handler',
      runtime: Runtime.NODEJS_22_X,
      architecture: Architecture.ARM_64,
    };
    const logs = (logId: string) =>
      new LogGroup(this, logId, {
        // Overridden by LogRetentionAspect (365 days on prod); stated so the intent survives.
        retention: RetentionDays.ONE_MONTH,
        removalPolicy: isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
      });

    // ---- Login: Cognito user pool (IA-2, IA-2(1), IA-2(2), IA-5, AC-2, AC-7, AC-12) ----
    // Copies the admin-only attributes into the access token (pre-token generation V2).
    const preTokenFn = new NodejsFunction(this, 'AgencyPreTokenFunction', {
      ...lambdaDefaults,
      entry: fileURLToPath(new URL('../../backend/src/agency/pre-token.ts', import.meta.url)),
      timeout: Duration.seconds(5),
      memorySize: 128,
      logGroup: logs('AgencyPreTokenLogs'),
    });

    this.userPool = new UserPool(this, 'AgencyUserPool', {
      userPoolName: `relay-${stage}-agency`,
      // AC-2: accounts exist only when an administrator creates them (AdminCreateUser).
      selfSignUpEnabled: false,
      signInAliases: { email: true },
      signInCaseSensitive: false,
      // IA-2(1), IA-2(2): every user, privileged or not, needs TOTP. No SMS (SIM swap), no email OTP.
      mfa: Mfa.REQUIRED,
      mfaSecondFactor: { otp: true, sms: false, email: false },
      // IA-5(1): long passphrases, all classes, no reuse of the last 24.
      passwordPolicy: {
        minLength: AGENCY_PASSWORD_POLICY.minLength,
        requireLowercase: true,
        requireUppercase: true,
        requireDigits: true,
        requireSymbols: true,
        tempPasswordValidity: Duration.days(AGENCY_PASSWORD_POLICY.tempPasswordDays),
        passwordHistorySize: AGENCY_PASSWORD_POLICY.historySize,
      },
      // Plus: threat protection (adaptive auth, compromised-credential checks; AC-7, SI-4) and
      // pre-token V2. About $0.02 per monthly active user (docs/agency-auth.md, "Cost").
      featurePlan: FeaturePlan.PLUS,
      standardThreatProtectionMode: isProd
        ? StandardThreatProtectionMode.FULL_FUNCTION
        : StandardThreatProtectionMode.AUDIT_ONLY,
      accountRecovery: AccountRecovery.EMAIL_ONLY,
      // Not required: admins set it on creation, and the client must not be able to change it.
      standardAttributes: { email: { required: false, mutable: true } },
      customAttributes: {
        // Admin-only (the app client cannot write them; see writeAttributes below).
        agency_id: new StringAttribute({ minLen: 1, maxLen: 64, mutable: true }),
        // Comma-separated county FIPS codes, e.g. "06085,06081".
        jurisdiction: new StringAttribute({ minLen: 0, maxLen: 2048, mutable: true }),
      },
      deletionProtection: isProd,
      removalPolicy: isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
    });
    this.userPool.addTrigger(
      UserPoolOperation.PRE_TOKEN_GENERATION_CONFIG,
      preTokenFn,
      LambdaVersion.V2_0,
    );
    // SSO hook (not built): a state or county IdP is added as UserPoolIdentityProviderSaml /
    // UserPoolIdentityProviderOidc here, its attribute mapping set for custom:agency_id and
    // custom:jurisdiction from IdP claims the State controls, and the provider listed in the
    // client's supportedIdentityProviders below. See docs/agency-auth.md, "Federation (SSO) later".

    for (const g of AGENCY_GROUPS) {
      new CfnUserPoolGroup(this, `Group-${g.name}`, {
        userPoolId: this.userPool.userPoolId,
        groupName: g.name,
        precedence: g.precedence,
        description: g.description,
      });
    }

    this.userPoolClient = this.userPool.addClient('AgencyWebClient', {
      userPoolClientName: `relay-${stage}-agency-web`,
      // A browser app cannot keep a secret.
      generateSecret: false,
      // SRP only: the password never leaves the browser. No USER_PASSWORD_AUTH, no admin flows.
      authFlows: { userSrp: true },
      // No hosted UI / OAuth until the front end and SSO exist.
      disableOAuth: true,
      supportedIdentityProviders: [UserPoolClientIdentityProvider.COGNITO],
      preventUserExistenceErrors: true,
      enableTokenRevocation: true,
      accessTokenValidity: Duration.minutes(AGENCY_TOKEN_LIFETIMES.accessMinutes),
      idTokenValidity: Duration.minutes(AGENCY_TOKEN_LIFETIMES.idMinutes),
      refreshTokenValidity: Duration.hours(AGENCY_TOKEN_LIFETIMES.refreshHours),
      authSessionValidity: Duration.minutes(AGENCY_TOKEN_LIFETIMES.authSessionMinutes),
      readAttributes: new ClientAttributes()
        .withStandardAttributes({ email: true, emailVerified: true })
        .withCustomAttributes('agency_id', 'jurisdiction'),
      // Users may set only harmless display preferences. NOT email (recovery channel) and NOT
      // the custom attributes: only administrators change agency or jurisdiction.
      writeAttributes: new ClientAttributes().withStandardAttributes({
        locale: true,
        timezone: true,
      }),
    });

    // ---- Audit trail storage (AU-9, AU-11) ----
    this.auditBucket = new Bucket(this, 'AgencyAuditBucket', {
      // encryptResources (encryption.ts) switches this to SSE-KMS with the logs key.
      encryption: BucketEncryption.S3_MANAGED,
      versioned: true,
      // Same pattern as the trail bucket (monitoring-stack.ts): governance-mode Object Lock.
      objectLockEnabled: true,
      objectLockDefaultRetention: ObjectLockRetention.governance(Duration.days(lockDays)),
      enforceSSL: true,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      objectOwnership: ObjectOwnership.BUCKET_OWNER_ENFORCED,
      lifecycleRules: [
        {
          id: 'expire-agency-audit',
          expiration: Duration.days(retentionDays),
          noncurrentVersionExpiration: Duration.days(retentionDays),
        },
      ],
      // Audit evidence outlives the stack on every stage.
      removalPolicy: RemovalPolicy.RETAIN,
    });
    // Who read or tried to delete audit records (S3 server access logs).
    logAccessTo(this.auditBucket, accessLogsBucket(this, isProd), 'agency-audit/');

    // ---- Agency API function ----
    const apiLogs = logs('AgencyLogs');
    const issuer = `https://cognito-idp.${this.region}.${this.urlSuffix}/${this.userPool.userPoolId}`;
    const fn = new NodejsFunction(this, 'AgencyFunction', {
      ...lambdaDefaults,
      entry: fileURLToPath(new URL('../../backend/src/agency/handler.ts', import.meta.url)),
      timeout: Duration.seconds(15),
      memorySize: 512,
      logGroup: apiLogs,
      environment: {
        AGENCY_AUDIT_BUCKET: this.auditBucket.bucketName,
        AGENCY_AUDIT_PREFIX: AGENCY_AUDIT_PREFIX,
        CHECKIN_TOTALS_TABLE: props.checkinTotals.tableName,
        ZONE_PACK_BUCKET: props.zonePack.bucketName,
        AGENCY_USER_POOL_CLIENT_ID: this.userPoolClient.userPoolClientId,
        AGENCY_TOKEN_ISSUER: issuer,
        AGENCY_MIN_CELL_COUNT: String(minCellCount),
        ORIGIN_VERIFY_SECRET_ARN: props.originVerifySecretArn,
        REQUIRE_ORIGIN_HEADER: requireOrigin ? 'true' : 'false',
      },
    });
    // Write-only on the audit prefix: no read, no delete, no retention change.
    allow(fn, ['s3:PutObject'], [this.auditBucket.arnForObjects(`${AGENCY_AUDIT_PREFIX}*`)]);
    // Totals by alert (the byAlert index only; never the check-ins table).
    allow(fn, ['dynamodb:Query'], [`${props.checkinTotals.tableArn}/index/byAlert`]);
    allow(fn, ['s3:GetObject'], [`${props.zonePack.bucketArn}/zones/v1/*`]);
    grantSecretRead(fn, props.originVerifySecretArn);

    const fnRole = fn.role;
    if (!fnRole) throw new Error('AgencyFunction has no role');
    const breakGlass = `arn:${this.partition}:iam::${this.account}:role/${auditBreakGlassRoleName(stage)}`;
    // Nobody but the break-glass role may delete a record or touch its retention.
    this.auditBucket.addToResourcePolicy(
      new PolicyStatement({
        sid: 'DenyAuditTamperExceptBreakGlass',
        effect: Effect.DENY,
        principals: [new AnyPrincipal()],
        actions: [...AUDIT_PROTECTED_ACTIONS],
        resources: [this.auditBucket.bucketArn, this.auditBucket.arnForObjects('*')],
        conditions: { ArnNotLike: { 'aws:PrincipalArn': breakGlass } },
      }),
    );
    // Only the agency function writes records, so nobody else can plant one.
    this.auditBucket.addToResourcePolicy(
      new PolicyStatement({
        sid: 'DenyAuditWriteExceptAgencyFunction',
        effect: Effect.DENY,
        principals: [new AnyPrincipal()],
        actions: ['s3:PutObject'],
        resources: [this.auditBucket.arnForObjects('*')],
        conditions: { ArnNotEquals: { 'aws:PrincipalArn': fnRole.roleArn } },
      }),
    );

    // ---- Agency HTTP API ----
    const accessLogs = logs('AgencyApiAccessLogs');
    const api = new HttpApi(this, 'AgencyHttpApi', {
      apiName: `relay-${stage}-agency-api`,
      createDefaultStage: false,
    });
    new HttpStage(this, 'AgencyDefaultStage', {
      httpApi: api,
      stageName: '$default',
      autoDeploy: true,
      throttle: AGENCY_THROTTLE,
      // Every request, including the ones the JWT authorizer refuses before the function runs.
      accessLogSettings: {
        destination: new LogGroupLogDestination(accessLogs),
        format: AccessLogFormat.custom(
          JSON.stringify({
            requestId: '$context.requestId',
            time: '$context.requestTime',
            ip: '$context.identity.sourceIp',
            userAgent: '$context.identity.userAgent',
            method: '$context.httpMethod',
            routeKey: '$context.routeKey',
            status: '$context.status',
            sub: '$context.authorizer.claims.sub',
            authorizerError: '$context.authorizer.error',
            integrationError: '$context.integrationErrorMessage',
            responseLength: '$context.responseLength',
          }).replace('"$context.status"', '$context.status'),
        ),
      },
    });
    const authorizer = new HttpUserPoolAuthorizer('AgencyJwtAuthorizer', this.userPool, {
      authorizerName: `relay-${stage}-agency-jwt`,
      userPoolClients: [this.userPoolClient],
      identitySource: ['$request.header.Authorization'],
    });
    const integration = new HttpLambdaIntegration('AgencyIntegration', fn);
    for (const { method, path } of AGENCY_ROUTES) {
      api.addRoutes({ path, methods: [method], integration, authorizer });
    }
    // Anything else under the API still needs a valid token and is answered (404) AND audited by
    // the function, rather than by API Gateway with no record.
    api.addRoutes({ path: '/{proxy+}', methods: [HttpMethod.ANY], integration, authorizer });

    this.apiDomainName = Fn.select(2, Fn.split('/', api.apiEndpoint));
    new CfnOutput(this, 'AgencyApiUrl', { value: api.apiEndpoint });
    new CfnOutput(this, 'AgencyUserPoolId', { value: this.userPool.userPoolId });
    new CfnOutput(this, 'AgencyUserPoolClientId', { value: this.userPoolClient.userPoolClientId });
    new CfnOutput(this, 'AgencyAuditBucketName', { value: this.auditBucket.bucketName });
    if (isProd) {
      Annotations.of(this).addWarningV2(
        'relay:agencyApiDefaultEndpoint',
        'prod agency API keeps its default execute-api endpoint, so callers can skip CloudFront ' +
          'and the WAF (the origin-verify header and the JWT authorizer still refuse them). Give ' +
          'it a custom origin domain like the public API before go-live (docs/agency-auth.md).',
      );
    }

    // ---- Alarms (SI-4, AU-6): every alarm notifies the alerts topic (AlarmActionsAspect) ----
    const denied = new MetricFilter(this, 'AgencyDeniedRequestsFilter', {
      logGroup: accessLogs,
      filterPattern: FilterPattern.any(
        FilterPattern.numberValue('$.status', '=', 401),
        FilterPattern.numberValue('$.status', '=', 403),
      ),
      metricNamespace: AGENCY_METRIC_NAMESPACE,
      metricName: `DeniedRequests-${stage}`,
      metricValue: '1',
      defaultValue: 0,
    });
    new Alarm(this, 'AgencyDeniedRequestsAlarm', {
      alarmDescription:
        `At least ${deniedThreshold} agency API requests were refused (401/403) in 5 minutes: a ` +
        'credential-stuffing or cross-jurisdiction probing attempt, or a broken client. Read the ' +
        'agency audit records and the agency API access log for the request ids.',
      metric: denied.metric({ statistic: 'Sum', period: Duration.minutes(5) }),
      threshold: deniedThreshold,
      evaluationPeriods: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING,
    });
    const auditFailed = new MetricFilter(this, 'AgencyAuditWriteFailedFilter', {
      logGroup: apiLogs,
      filterPattern: FilterPattern.stringValue('$.event', '=', 'agency_audit_write_failed'),
      metricNamespace: AGENCY_METRIC_NAMESPACE,
      metricName: `AuditWriteFailed-${stage}`,
      metricValue: '1',
      defaultValue: 0,
    });
    new Alarm(this, 'AgencyAuditWriteFailedAlarm', {
      alarmDescription:
        'An agency audit record could not be written, so agency requests are being refused (fail ' +
        'closed). Check the audit bucket, its policy and the logs key.',
      metric: auditFailed.metric({ statistic: 'Sum', period: Duration.minutes(1) }),
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING,
    });

    // ---- Reported configuration changes (AU-9, AC-2(4)) ----
    // CloudTrail already records every Cognito admin action (AdminCreateUser, AdminDisableUser,
    // AdminAddUserToGroup, AdminUpdateUserAttributes, ...) as a management event in the trail; it
    // is not duplicated here. Changes that weaken sign-in or the audit store also page.
    const notify = (headline: string) =>
      new SnsTopic(props.alertsTopic, {
        message: RuleTargetInput.fromText(
          `Relay ${stage} ${headline}: ${EventField.fromPath('$.detail.eventName')} ` +
            `by ${EventField.fromPath('$.detail.userIdentity.arn')} ` +
            `at ${EventField.fromPath('$.detail.eventTime')}`,
        ),
      });
    new Rule(this, 'AgencyAuditBucketChangedRule', {
      description:
        'The agency audit bucket policy, lifecycle, versioning, Object Lock or logging changed',
      eventPattern: {
        source: ['aws.s3'],
        detailType: ['AWS API Call via CloudTrail'],
        detail: {
          eventSource: ['s3.amazonaws.com'],
          eventName: [...AUDIT_BUCKET_EVENTS],
          requestParameters: { bucketName: [this.auditBucket.bucketName] },
        },
      },
      targets: [notify('AGENCY AUDIT BUCKET configuration change')],
    });
    new Rule(this, 'AgencyUserPoolChangedRule', {
      description:
        'The agency user pool MFA, client, identity provider or threat protection changed',
      eventPattern: {
        source: ['aws.cognito-idp'],
        detailType: ['AWS API Call via CloudTrail'],
        detail: {
          eventSource: ['cognito-idp.amazonaws.com'],
          eventName: [...USER_POOL_SECURITY_EVENTS],
          requestParameters: { userPoolId: [this.userPool.userPoolId] },
        },
      },
      targets: [notify('AGENCY USER POOL security change')],
    });

    Tags.of(this).add('project', 'relay');
    Tags.of(this).add('stage', stage);
  }
}
