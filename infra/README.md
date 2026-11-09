# @relay/infra

AWS CDK app (TypeScript) for Relay, deployed to us-west-2. The account comes from `CDK_DEFAULT_ACCOUNT`; nothing is hard-coded. `stage` is `dev` (default) or `prod`; stack names are `relay-<stage>-<name>`.

## Stacks (one per concern)

Values move between stacks as plain props (ARNs and names) wired in `bin/relay.ts`. IAM grants use explicit ARNs and explicit action lists; no wildcard actions anywhere (a test enforces this).

| Stack      | Creates                                                                                                                                                                                                                                                                                                                                                                                                  |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `data`     | Aurora Serverless v2 PostgreSQL (Data API, isolated VPC, no NAT) and its generated credentials secret.                                                                                                                                                                                                                                                                                                   |
| `config`   | Secrets Manager: `relay/<stage>/token-keys`, `relay/<stage>/alert-keys`, `relay/<stage>/device-credential-keys` (keyrings), `relay/<stage>/expo-access-token` (placeholder). SSM parameter `/relay/<stage>/delivery-kill-switch` (default `running`).                                                                                                                                                    |
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

## Database roles

No Lambda or task uses the Aurora master secret (`relay_admin`). Migration `0011_roles.sql` creates six Postgres roles, and the `data` stack creates one Secrets Manager secret for each (`relay/<stage>/db/<role>`, fixed username, generated password, attached to the cluster). Each compute principal gets `rds-data` on the cluster and `GetSecretValue` on ITS OWN role secret only, and its `DB_SECRET_ARN` points at that secret. The admin secret (the `data` stack's `SecretArn` output) is for the operator running migrations and `scripts/db-roles.ts`; a test fails if any other stack references it.

| Role                  | Used by                                    | Can do                                                                                                                  |
| --------------------- | ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------- |
| `relay_api`           | API Lambda                                 | read/write `devices`, `places`; read alerts; `push_token_moves`, `cell_index_outbox`, `deleted_devices` when they exist |
| `relay_ingest`        | ingest (queue mode, prod)                  | read/write `alerts`, `alert_source_ids`, `alert_cells`; nothing on devices or places                                    |
| `relay_ingest_inline` | ingest (inline mode, dev)                  | `relay_ingest` plus read of `devices` and `places` (inline mode plans deliveries in-process)                            |
| `relay_fanout`        | planner, shard workers                     | read alerts                                                                                                             |
| `relay_sender`        | sender, legacy delivery worker             | read alerts, `devices`, `places`; update only the three push-target columns of `devices`                                |
| `relay_ops`           | index repair (and later drainer, backfill) | read `devices`, `places`; `cell_index_outbox`, `deleted_devices` (read) when they exist                                 |

Tables from migrations 0008-0010 (`push_token_moves`, `cell_index_outbox`, `deleted_devices`) are granted by name only if they exist. There are no default privileges: a new table is invisible to every role until it is added to `0011_roles.sql`. Grants are idempotent, and `db-roles.ts` re-applies them, so editing the file and re-running the script is how a new table gets access.

Run order, on first deploy and after any change to the roles:

    npx cdk deploy --all -c stage=dev     # creates the role secrets (roles cannot log in yet)
    # apply migrations with the admin secret (see "Run the migrations"); 0011 creates the NOLOGIN roles
    cd infra && npx tsx scripts/db-roles.ts dev \
      --cluster-arn <ClusterArn> --admin-secret-arn <SecretArn>

`db-roles.ts` re-applies the grants, then for each role reads its secret and runs `ALTER ROLE ... LOGIN PASSWORD`. The statement is built by Postgres `format('%I', '%L')` with the role name and password bound as parameters, and the script never prints a password. It refuses a secret whose username is not the role. Until it has run, Lambdas that use a role secret get an authentication error from the Data API. To rotate a role's password, update that secret and run the script again; the Data API reads the secret on every call, so no redeploy is needed.

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

## Edge hardening, repair outbox and capacity

**Origin verification.** The API's `execute-api` URL is public, so a caller could skip CloudFront and the WAF. CloudFront adds the header `x-relay-origin-verify` to every API request; the API Lambda returns 403 without the matching value when `REQUIRE_ORIGIN_HEADER=true` (prod). The value is a generated secret `relay/<stage>/origin-verify` in the config stack, replicated to us-east-1; the edge stack resolves it at deploy time with `{{resolve:secretsmanager:...}}`, so it never appears in a template. The Lambda reads it by `ORIGIN_VERIFY_SECRET_ARN` and caches it 5 minutes. To rotate, change the secret, redeploy `edge` (CloudFront picks up the new value), and expect up to 5 minutes of 403s until the Lambda cache refreshes.

**Quota: API Gateway.** The account-level API Gateway quota is 10,000 requests per second per region (adjustable), but the prod `$default` stage throttle is 20,000 rps. The stage cannot exceed the account quota, so request an increase to at least 20,000 rps (and verify the burst limit) before a statewide check-in burst, or lower `apiThrottleRate`/`apiThrottleBurst`.

**Cell-index repair.** When the API cannot write a place into the cell index it queues `{op, h3Cell, deviceId, placeId}` to `INDEX_REPAIR_QUEUE_URL`; a repair Lambda re-reads Aurora and redoes the write (idempotent, per-item failures). After 5 failed receives a message goes to the repair DLQ, which has an alarm (`IndexRepairDlqAlarm`). Fix the cause, then redrive the DLQ to the repair queue.

**Capacity context.** `-c auroraMaxAcu=N` (default prod 16, dev 2), `-c checkinsWarmWritesPerSecond=N` (default prod 40,000, dev none; 0 disables), and `-c apiReservedConcurrency=N`, `-c aggregatorReservedConcurrency=N`, `-c repairReservedConcurrency=N` (unset by default so a fresh account deploys). The idempotency table's warm throughput (20,000 writes/s on prod) belongs to the delivery stack and is not set here.

## Fan-out stack (statewide scale)

`fanout` creates the plan queue, the planner and shard worker Lambdas, the urgent and standard send queues (30 s visibility, each with a DLQ), and the DynamoDB cell index (`h3Cell` + `sk` = `deviceId#placeId`, on demand, PITR on prod). Ingest publishes to the plan queue only when it runs with `FANOUT_MODE=queue` and `PLAN_QUEUE_URL`; otherwise the original in-line planning runs.

The planner hands each shard job to the shard Lambda with an async invoke (50 at a time). An SQS event source would add only about 300 concurrent invocations a minute, too slow for a 60 s fan-out. The shard function retries twice, gives up after 1 hour, and sends the job to the shard DLQ.

**Quota:** the shard function reserves 500 concurrent executions (`shardReservedConcurrency`). The account Lambda concurrency quota must be raised before deploying (see `docs/scale-quotas.md`); Lambda also needs at least 100 unreserved executions left in the account.

**Runbook: shard DLQ not empty** (the alarm `ShardDlqAlarm` fires). Find the cause in the shard function logs first (kill switch still paused, cell index throttled, a bad deploy). Once fixed, replay:

    aws lambda invoke --function-name <RedriveFunctionName output> --payload '{"max":1000}' --cli-binary-format raw-in-base64-out out.json

Each DLQ message is re-invoked asynchronously on the shard function and deleted only after that succeeds. Replaying is safe: batch keys are deterministic and the sender claims them. Messages that are not shard jobs are left in the DLQ.

**Shard size:** a shard job is at most 16 cells (one res-4 parent split into sorted parts of 16), and the shard function has 2048 MB. A shard that is running out of time (under 10 s left) sends the rest of its cells to a new invoke of itself and returns; nothing is skipped or repeated. Planner and shard retry an alert whose signature fails (a key rotation not yet loaded) instead of dropping it, and log `fanout.signature_failed`.

**Runbook: urgent or standard send DLQ not empty** (`UrgentDlqAlarm` / `StandardDlqAlarm`). Send batches only reach the DLQ after 100 receives. Fix the cause (sender down, secrets, kill switch), then move them back with SQS's own redrive:

    aws lambda invoke --function-name <SendRedriveFunctionName output> --payload '{"queue":"urgent"}' --cli-binary-format raw-in-base64-out out.json

(`"standard"`, or leave `queue` out for both.) The sender claims each batch key, so replaying a batch that was in fact sent is harmless.

## Sender stack (statewide scale)

The sender fleet (Fargate, 1 vCPU / 2 GB tasks) is **off until you enable it**, because the APNs and FCM secrets are created with placeholder values and a sender that boots with them crash-loops. First deploy, in this order:

1. `npx cdk deploy ... -c stage=<stage>`: everything is created; the sender service has desiredCount 0 and no auto scaling.
2. Put the real values in `relay/<stage>/apns-key` and `relay/<stage>/fcm-service-account` (`aws secretsmanager put-secret-value`).
3. `npx cdk deploy ... -c stage=<stage> -c senderEnabled=true`: the fleet starts. Prod keeps at least 10 tasks (override with `-c senderMinTasks=<n>`) and scales to 100 on queue depth (1-minute periods, aggressive steps) and urgent queue age; it scales in only after 15 idle minutes. Dev scales from 0 to 10.

The planner pre-scales the fleet on an urgent alert (ECS `UpdateService`, scoped to the sender service, never lowering the count; failures are logged and never block fan-out), because queue-metric scaling alone takes 3-5 minutes. It derives the service from the stage (`relay-<stage>-sender`), and the planner reads the same `senderEnabled` flag, so a disabled fleet is never pre-scaled.

**Quota:** the Fargate On-Demand vCPU quota defaults to 6 vCPUs per Region. It must be raised to at least 100 tasks x 1 vCPU = 100 vCPUs (more with headroom) before enabling prod, or the fleet cannot grow past 6 tasks. See `docs/scale-quotas.md` (ECS Fargate section).
