import { fileURLToPath } from 'node:url';
import { CfnOutput, Duration, RemovalPolicy, Stack, StackProps, Tags } from 'aws-cdk-lib';
import { HttpApi, HttpMethod, HttpStage } from 'aws-cdk-lib/aws-apigatewayv2';
import { HttpLambdaIntegration } from 'aws-cdk-lib/aws-apigatewayv2-integrations';
import { Architecture, Runtime } from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { CfnPlaceIndex } from 'aws-cdk-lib/aws-location';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import { allow, dbEnv, grantDb, type DbRefs } from './grants.js';

export interface ApiStackProps extends StackProps {
  stage: 'dev' | 'prod';
  db: DbRefs;
}

/**
 * Every path in packages/shared/openapi/relay.yaml except /agency/*, which belongs
 * to the Cognito-protected agency API (added later). Keep in sync with the spec.
 */
export const API_ROUTES: ReadonlyArray<{ method: HttpMethod; path: string }> = [
  { method: HttpMethod.POST, path: '/devices' },
  { method: HttpMethod.PUT, path: '/devices/me/push-token' },
  { method: HttpMethod.POST, path: '/places' },
  { method: HttpMethod.GET, path: '/places' },
  { method: HttpMethod.DELETE, path: '/places/{placeId}' },
  { method: HttpMethod.GET, path: '/alerts' },
  { method: HttpMethod.GET, path: '/alerts/{alertId}' },
  { method: HttpMethod.POST, path: '/alerts/{alertId}/checkins' },
];

export class ApiStack extends Stack {
  constructor(scope: Construct, id: string, props: ApiStackProps) {
    super(scope, id, props);
    const isProd = props.stage === 'prod';

    // Address search for saved places. SingleUse means results are not stored.
    const placeIndexName = `relay-${props.stage}-places`;
    const placeIndex = new CfnPlaceIndex(this, 'PlaceIndex', {
      indexName: placeIndexName,
      dataSource: 'Esri',
      dataSourceConfiguration: { intendedUse: 'SingleUse' },
    });

    const apiFn = new NodejsFunction(this, 'ApiFunction', {
      entry: fileURLToPath(new URL('../../backend/src/api/handler.ts', import.meta.url)),
      depsLockFilePath: fileURLToPath(new URL('../../package-lock.json', import.meta.url)),
      projectRoot: fileURLToPath(new URL('../../', import.meta.url)),
      handler: 'handler',
      runtime: Runtime.NODEJS_22_X,
      architecture: Architecture.ARM_64,
      timeout: Duration.seconds(15),
      memorySize: 256,
      logGroup: new LogGroup(this, 'ApiLogs', {
        retention: RetentionDays.ONE_MONTH,
        removalPolicy: isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
      }),
      environment: { ...dbEnv(props.db), PLACE_INDEX_NAME: placeIndexName },
    });

    grantDb(apiFn, props.db);
    allow(apiFn, ['geo:SearchPlaceIndexForText'], [placeIndex.attrArn]);

    // The stage is created by hand below so we can set throttling on $default.
    const api = new HttpApi(this, 'HttpApi', {
      apiName: `relay-${props.stage}-api`,
      createDefaultStage: false,
    });
    const stage = new HttpStage(this, 'DefaultStage', {
      httpApi: api,
      stageName: '$default',
      autoDeploy: true,
      throttle: { rateLimit: 50, burstLimit: 100 },
    });

    const integration = new HttpLambdaIntegration('ApiIntegration', apiFn);
    for (const { method, path } of API_ROUTES) {
      api.addRoutes({ path, methods: [method], integration });
    }

    new CfnOutput(this, 'ApiUrl', { value: stage.url });

    Tags.of(this).add('project', 'relay');
    Tags.of(this).add('stage', props.stage);
  }
}
