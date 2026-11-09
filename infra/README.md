# @relay/infra

AWS CDK app (TypeScript) for Relay, deployed to us-west-2. The account comes from `CDK_DEFAULT_ACCOUNT`; nothing is hard-coded. `stage` is `dev` (default) or `prod`; stack names are `relay-<stage>-<name>`.

## Stacks (one per concern)

Values move between stacks as plain props (ARNs and names) wired in `bin/relay.ts`. IAM grants use explicit ARNs and explicit action lists; no wildcard actions anywhere (a test enforces this).

| Stack      | Creates                                                                                                                                                                                                                                                                                                                                                                                                  |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `data`     | Aurora Serverless v2 PostgreSQL (Data API, isolated VPC, no NAT) and its generated credentials secret.                                                                                                                                                                                                                                                                                                   |
| `config`   | Secrets Manager: `relay/<stage>/token-keys`, `relay/<stage>/alert-keys` (keyrings), `relay/<stage>/expo-access-token` (placeholder). SSM parameter `/relay/<stage>/delivery-kill-switch` (default `running`).                                                                                                                                                                                            |
| `delivery` | SQS delivery queue (SSE, visibility timeout 30 s) with DLQ (100 receives), CloudWatch alarm when the DLQ has any visible message, DynamoDB idempotency table (`pk`, TTL `ttl`), and the push worker Lambda (20 s, reserved concurrency 10, batch size 10, reports batch item failures).                                                                                                                  |
| `ingest`   | Raw S3 bucket, state table (`sourceId`, TTL `expiresAt`), the ingest Lambda (70 s, reserved concurrency 1) on a 1-minute EventBridge rule, errors alarm.                                                                                                                                                                                                                                                 |
| `api`      | HTTP API with a Lambda integration for every route in `packages/shared/openapi/relay.yaml` except `/agency/*`, `$default` stage throttled at 50 rps / burst 100, Amazon Location place index `relay-<stage>-places` (Esri, SingleUse), and the check-in tables (`alertId`+`idempotencyKey` for check-ins, `alertId`+`h3Cell` for per-cell totals; on demand, PITR and RETAIN on prod). Outputs `ApiUrl`. |

Everything is tagged `project=relay`, `stage=<stage>`.

Tables, buckets and queues have no fixed names: CDK generates unique ones and passes them to the Lambdas through environment variables, so `dev` and `prod` can share an account. Anything that must have a fixed name (secrets, SSM parameters, the place index) includes the stage; a test enforces both.

The Lambda environment variables are checked against the handler source (`test/handler-env.ts`), so adding an `env.X` to a handler without adding it to its stack fails the tests.

## Commands

    npm install
    npx cdk synth -c stage=dev     # works offline (bundles with esbuild); builds @relay/shared first
    npm test                       # assertion tests
    npm run typecheck

## Deploy dev

Deploys will run from GitHub Actions using OIDC (ticket S-06). Once the AWS accounts exist and your shell has credentials for the dev account:

    npx cdk bootstrap              # first time only
    npx cdk deploy --all -c stage=dev
    npm run seed-keys -- dev       # first deploy only, see "Keys" below

Never deploy from a laptop with stored access keys.

## Run the migrations against Aurora

Migrations are the SQL files in `backend/migrations`, applied in name order by `runMigrations(exec, dir)` in `backend/src/db/sql.ts`. It talks to Aurora through the Data API (`DataApiExecutor`), so no VPC access is needed, and it records applied files in `schema_migrations`, so re-running is safe. Take `ClusterArn`, `SecretArn` and `DatabaseName` from the `data` stack outputs, then call `runMigrations` with a `DataApiExecutor` built from them, from CI or a shell with dev credentials. There is no CLI wrapper for this yet; a ten-line script in `backend/scripts/` would do it. To check what has been applied:

    aws rds-data execute-statement --region us-west-2 \
      --resource-arn <ClusterArn> --secret-arn <SecretArn> --database relay \
      --sql "select name from schema_migrations order by name"

Aurora pauses at 0 ACU (minimum capacity 0, kept on purpose for cost). The first request after an idle period can take about 15 seconds while the cluster resumes; the code is meant to retry the Data API call while the cluster resumes (it reports DatabaseResumingException), so callers should see a slow first request, not an error. Raise `serverlessV2MinCapacity` in `lib/data-stack.ts` if that delay is ever unacceptable.

## Keys

Check-in token keys and alert signing keys share the keyring shape `{"current":"k1","keys":{"k1":"<base64>"}}`.

CDK's `generateSecretString` can only generate a top-level string, not one nested inside `keys`. So the `config` stack creates each secret with a clearly fake placeholder key, and a small script replaces it. No custom resource is involved.

- First deploy: `npm run seed-keys -- <stage>` replaces the placeholder with 32 random bytes (base64) for `k1`. It skips secrets that are already seeded, so it is safe to re-run. Redeploying the stack does not overwrite seeded values.
- Rotate: `npm run seed-keys -- <stage> --rotate` adds `k2` (then `k3`, ...) and makes it `current`. New tokens use the new key; old keys remain in `keys`, so tokens already in people's hands still verify. Loaders cache for 5 minutes, so allow about 5 minutes before the new key is used everywhere. When old tokens or alerts have expired (check-in links live for the alert's lifetime), remove the old kid from `keys` with `aws secretsmanager put-secret-value`.

The Expo secret starts as a placeholder string. Put the real raw token in with:

    aws secretsmanager put-secret-value --region us-west-2 \
      --secret-id relay/<stage>/expo-access-token --secret-string '<token>'

## Kill switch

Stop all push delivery immediately (workers pick it up within about 30 seconds):

    aws ssm put-parameter --region us-west-2 --name /relay/<stage>/delivery-kill-switch \
      --type String --value paused --overwrite

While paused the worker fails every record, so SQS keeps redelivering them (visibility timeout 30 s). The DLQ takes a job only after 100 receives, roughly 50 minutes of pausing. For a longer pause, or if the DLQ alarm fires afterwards, redrive the DLQ back to the delivery queue once you resume.

Resume by writing `running` the same way. Any value other than `paused` means running.

## Fan-out stack (statewide scale)

`fanout` creates the plan queue, the planner and shard worker Lambdas, the urgent and standard send queues (30 s visibility, each with a DLQ), and the DynamoDB cell index (`h3Cell` + `sk` = `deviceId#placeId`, on demand, PITR on prod). Ingest publishes to the plan queue only when it runs with `FANOUT_MODE=queue` and `PLAN_QUEUE_URL`; otherwise the original in-line planning runs.

The planner hands each shard job to the shard Lambda with an async invoke (50 at a time). An SQS event source would add only about 300 concurrent invocations a minute, too slow for a 60 s fan-out. The shard function retries twice, gives up after 1 hour, and sends the job to the shard DLQ.

**Quota:** the shard function reserves 500 concurrent executions (`shardReservedConcurrency`). The account Lambda concurrency quota must be raised before deploying (see `docs/scale-quotas.md`); Lambda also needs at least 100 unreserved executions left in the account.

**Runbook: shard DLQ not empty** (the alarm `ShardDlqAlarm` fires). Find the cause in the shard function logs first (kill switch still paused, cell index throttled, a bad deploy). Once fixed, replay:

    aws lambda invoke --function-name <RedriveFunctionName output> --payload '{"max":1000}' --cli-binary-format raw-in-base64-out out.json

Each DLQ message is re-invoked asynchronously on the shard function and deleted only after that succeeds. Replaying is safe: batch keys are deterministic and the sender claims them. Messages that are not shard jobs are left in the DLQ.
