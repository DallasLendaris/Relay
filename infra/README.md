# @relay/infra

AWS CDK app (TypeScript) for Relay, deployed to us-west-2. The account comes from `CDK_DEFAULT_ACCOUNT`; nothing is hard-coded. `stage` is `dev` (default) or `prod`; stack names are `relay-<stage>-<name>`.

## Stacks (one per concern)

Values move between stacks as plain props (ARNs and names) wired in `bin/relay.ts`. IAM grants use explicit ARNs and explicit action lists; no wildcard actions anywhere (a test enforces this).

| Stack      | Creates                                                                                                                                                                                                                                                                                                                                                                                                  |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `data`     | Aurora Serverless v2 PostgreSQL (Data API, isolated VPC, no NAT) and its generated credentials secret.                                                                                                                                                                                                                                                                                                   |
| `config`   | KMS key `alias/relay/<stage>/alert-signing-v1` (ECC_NIST_P256, alert signing). Secrets Manager: `relay/<stage>/token-keys`, `relay/<stage>/device-credential-keys` (keyrings), `relay/<stage>/expo-access-token` (placeholder). SSM parameter `/relay/<stage>/delivery-kill-switch` (default `running`).                                                                                                 |
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

Aurora minimum capacity: **prod keeps 0.5 ACU** (about $44 a month: 0.5 x $0.12 per ACU-hour x 730 h), because at 0 the cluster pauses and the first Data API call after a pause waits 15-30 s for the resume, longer than the DB client's roughly 17 s retry budget. The cell-index outbox drainer, which runs **every minute on prod**, already touches the cluster each minute, so on prod it would rarely pause anyway; 0.5 ACU makes that a guarantee. **Dev stays at 0 ACU and auto-pauses**, and its drainer runs **every 5 minutes** (the drainer-not-running alarm widens to 15 minutes to match), so dev can sleep between ticks (Aurora's default auto-pause delay is 5 minutes, so with a 5-minute tick dev pauses only part of the time; lengthen `DRAINER_MINUTES.dev` in `lib/fanout-stack.ts` if dev cost matters). See `MIN_ACU` in `lib/data-stack.ts`.

## Database roles

No Lambda or task uses the Aurora master secret (`relay_admin`). Migration `0011_roles.sql` creates seven Postgres roles, and the `data` stack creates one Secrets Manager secret for each (`relay/<stage>/db/<role>`, fixed username, generated password, attached to the cluster). Each compute principal gets `rds-data` on the cluster and `GetSecretValue` on ITS OWN role secret only, and its `DB_SECRET_ARN` points at that secret. The admin secret (the `data` stack's `SecretArn` output) is for the operator running migrations and `scripts/db-roles.ts`; a test fails if any other stack references it.

| Role                  | Used by                                | Can do                                                                                                                  |
| --------------------- | -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `relay_api`           | API Lambda                             | read/write `devices`, `places`; read alerts; `push_token_moves`, `cell_index_outbox`, `deleted_devices` when they exist |
| `relay_ingest`        | ingest (queue mode, prod)              | read/write `alerts`, `alert_source_ids`, `alert_cells`; nothing on devices or places                                    |
| `relay_ingest_inline` | ingest (inline mode, dev)              | `relay_ingest` plus read of `devices` and `places` (inline mode plans deliveries in-process)                            |
| `relay_fanout`        | planner, shard workers                 | read alerts                                                                                                             |
| `relay_sender`        | sender, legacy delivery worker         | read alerts, `devices`, `places`; update only the three push-target columns of `devices`                                |
| `relay_ops`           | index repair, outbox drainer, backfill | read `devices`, `places`; `cell_index_outbox`, `deleted_devices` (read) when they exist                                 |
| `relay_latejoin`      | late-join Lambda                       | read-only: `devices`, `places`, `alerts`, `alert_cells`; nothing else (its claims live in DynamoDB)                     |

Tables from migrations 0008-0010 (`push_token_moves`, `cell_index_outbox`, `deleted_devices`) are granted by name only if they exist. `relay_api` can also delete from `devices` and `places` and insert into `deleted_devices` (DELETE /devices/me), and read and write `push_token_moves` (the push-target move rule). There are no default privileges: a new table is invisible to every role until it is added to `0011_roles.sql`. Grants are idempotent, and `db-roles.ts` re-applies them, so editing the file and re-running the script is how a new table gets access.

Deploy order, on first deploy and after any change to the roles or the schema:

1. Deploy: creates the role secrets (roles cannot log in yet).
2. Migrations, with the admin secret (see "Run the migrations"), BEFORE the API serves traffic: 0008 (push-target move log and `devices` columns), 0009 (cell-index outbox), 0010 (no exact coordinates, `deleted_devices`) and 0011 (the NOLOGIN roles and grants). The new API code reads and writes those tables and columns.
3. The db-roles script, which sets each role's password and enables LOGIN.

   npx cdk deploy --all -c stage=dev

   # apply migrations 0008-0011 with the admin secret (see "Run the migrations")

   cd infra && npx tsx scripts/db-roles.ts dev \
   --cluster-arn <ClusterArn> --admin-secret-arn <SecretArn>

`db-roles.ts` re-applies the grants, then for each role reads its secret and runs `ALTER ROLE ... LOGIN PASSWORD`. The statement is built by Postgres `format('%I', '%L')` with the role name and password bound as parameters, and the script never prints a password. It refuses a secret whose username is not the role. Until it has run, Lambdas that use a role secret get an authentication error from the Data API. To rotate a role's password, update that secret and run the script again; the Data API reads the secret on every call, so no redeploy is needed.

## Keys

Check-in token keys and device-credential keys share the keyring shape `{"current":"k1","keys":{"k1":"<base64>"}}`. Alerts are signed with a KMS key instead (below).

CDK's `generateSecretString` can only generate a top-level string, not one nested inside `keys`. So the `config` stack creates each secret with a clearly fake placeholder key, and a small script replaces it. No custom resource is involved.

- First deploy: `npm run seed-keys -- <stage>` replaces the placeholder with 32 random bytes (base64) for `k1`. It skips secrets that are already seeded, so it is safe to re-run. Redeploying the stack does not overwrite seeded values.
- Rotate: `npm run seed-keys -- <stage> --rotate` adds the next kid and makes it `current`. Kids are monotonic: the new kid is one above the highest ever issued (the ring keeps a `highestKid` mark), so a kid deleted by hand is never reused, and the script refuses to overwrite an existing kid. New tokens use the new key; old keys remain in `keys`, so tokens already in people's hands still verify. Loaders cache for 5 minutes, so allow about 5 minutes before the new key is used everywhere. When old tokens have expired (check-in links live for the alert's lifetime), remove the old kid from `keys` with `aws secretsmanager put-secret-value` (keep `highestKid`).

### Alert signing key

Alerts are signed with ECDSA P-256 by a KMS asymmetric key (`ECC_NIST_P256`, `SIGN_VERIFY`) in the `config` stack; the private key never leaves KMS. Only the ingest Lambda has `kms:Sign` (env `ALERT_SIGNING_KMS_KEY`, `ALERT_SIGNING_KID`). Every verifier (planner, shard, sender task, late-join, delivery worker, and ingest) gets only `kms:GetPublicKey` and `ALERT_VERIFY_KEYS` (a JSON list of `{kid, kmsKeyId}`), fetches each public key once per container and verifies locally. The operator recall script (`backend/scripts/alert-recall.ts`) also signs: give the operator role `kms:Sign` on the key (output `AlertSigningKeyArn`) for the recall only. `infra/test/alert-signing.test.ts` fails if any other principal gets `kms:Sign`.

KMS cannot rotate asymmetric keys automatically. To rotate, pass `alertSigningVersions: [1, 2]` to the config stack and deploy: v2 signs, verifiers accept both (`<stage>-alert-v2`, `<stage>-alert-v1`). Once alerts signed by v1 have expired, set `[2]` and deploy; prod retains the old key until you schedule its deletion. Signatures are `v2.<kid>.<base64url DER>`; the old HMAC `v1` signatures no longer verify, so alerts stored before this change are refused until NWS updates them.

The Expo secret starts as a placeholder string. Put the real raw token in with:

    aws secretsmanager put-secret-value --region us-west-2 \
      --secret-id relay/<stage>/expo-access-token --secret-string '<token>'

## Kill switch

The parameter `/relay/<stage>/delivery-kill-switch` stops all push delivery. The only valid values are exactly `running` or `paused` (case and surrounding spaces are ignored). **Anything else counts as paused** (a typo such as `pause` or `true` stops delivery instead of leaving it running) and is logged as `killswitch.invalid_value` (with the metric `KillSwitchInvalidValue`); a missing parameter means running (metric `KillSwitchParameterMissing`, so deleting it is alarmed). If SSM cannot be read, the last good value is used for up to 10 minutes, then the switch fails closed (paused). The planner, shard workers, paused consumer, delivery worker, late-join Lambda and sender re-read it about every 30 seconds.

**Flip:**

    aws ssm put-parameter --region us-west-2 --name /relay/<stage>/delivery-kill-switch \
      --type String --value paused --overwrite

**Confirm** (give it about 30 seconds):

- The sender logs `sender.receiving_paused` once when it stops receiving (CloudWatch Logs, the sender log group; filter on that message).
- The urgent send queue depth (`ApproximateNumberOfMessagesVisible` on the urgent queue) stops falling and starts to grow: nothing is being taken off it.

While paused, nothing is dropped. The sender stops receiving. The planner and shard workers **park** their work in the paused queue (`relay-<stage>-shard-paused`) instead of failing it into the shard DLQ; a paused consumer re-checks the switch about every minute. The legacy delivery worker and the late-join Lambda fail their records so SQS redelivers them. The send DLQs and the delivery DLQ only take a message after many receives (send queues: 100, about 50 minutes), so a long pause can fill them.

**Resume:** write `running`. Parked shard work resumes by itself about 1 to 2 minutes later (the paused consumer's next check plus the 30 s switch cache): **there is no manual redrive for it**. Then replay anything that reached a DLQ while paused:

    aws ssm put-parameter --region us-west-2 --name /relay/<stage>/delivery-kill-switch \
      --type String --value running --overwrite

1. Shard DLQ (only if its alarm fired): run the shard redrive, see "Runbook: shard DLQ not empty" below. Parked work is not in this queue.
2. Send DLQs (urgent, standard): run the send redrive, see "Runbook: urgent or standard send DLQ not empty" below.
3. The delivery DLQ (legacy worker path): redrive it to the delivery queue from the SQS console.

After 24 hours of pause, parked work fails into the **paused DLQ** (alarm `ShardPausedDlqAlarm`). The shard redrive function does NOT cover it. If that alarm fires, move the messages back to the paused queue with SQS's own redrive (console or `aws sqs start-message-move-task`); the paused consumer then resumes them.

Every change to this parameter emails the alerts topic (EventBridge rules on `PutParameter`, `DeleteParameter` and `DeleteParameters`), so a flip is never silent. Deleting it gets its own, louder message, because a missing parameter fails open to running.

**It cannot fail silently either.** Every real SSM read emits metrics (`Relay/Config`, dimension `Source=killswitch`; ingest reads once a minute as a heartbeat). Alarms to the alerts topic: `KillSwitchInvalidValueAlarm` (the value is not `running`/`paused`: check the parameter for a typo), `KillSwitchFailedClosedAlarm` (SSM unreadable and the last good value older than 10 minutes, or never loaded: check SSM throttling/outage and the IAM grant), `KillSwitchParameterMissingAlarm` (parameter deleted: redeploy the config stack), each on a single datapoint in 5 minutes; and `KillSwitchPausedAlarm` when delivery has been paused for about 15 minutes (14 of 15 one-minute periods), which is a reminder when the pause is intentional.

## Monitoring, alerting and audit trail

The `monitoring` stack owns one SNS topic per stage, `relay-<stage>-alerts`. **Every CloudWatch alarm in every stack (except auto scaling's own scale-in/out alarms) notifies this topic on ALARM and on OK**, through a CDK Aspect (`AlarmActionsAspect`), so a new alarm is wired automatically. A test fails if one is missed. The topic emails the address in context `alarmEmail`; AWS sends a confirmation message that must be accepted once. A page to a phone needs that address to be a pager or an SMS-capable email gateway: the topic itself only emails.

    npx cdk deploy --all -c stage=prod -c alarmEmail=oncall@example.com

Prod synth fails without `alarmEmail`, unless `-c allowNoAlarmEmail=true` (alarms then reach nobody).

The edge stack lives in us-east-1 and a CloudWatch alarm can only notify an SNS topic in its own region, so the edge stack has its own topic `relay-<stage>-edge-alerts` for its one alarm (`WafBlockedRequestsAlarm`: 1,000 or more blocked requests in 5 minutes on the web ACL). It subscribes the same `alarmEmail`, and that second subscription needs its own confirmation click.

| Context            | Default              | Meaning                                                                              |
| ------------------ | -------------------- | ------------------------------------------------------------------------------------ |
| `alarmEmail`       | none                 | Email subscribed to the alerts topic                                                 |
| `enableGuardDuty`  | prod true, dev false | Creates the GuardDuty detector (one per region per account: set false if one exists) |
| `logRetentionDays` | prod 365, dev 30     | Retention of every Lambda, ECS and trail log group                                   |
| `monthlyBudgetUsd` | prod 600, dev 50     | AWS Budgets limit; emails at 80% and 100% of actual spend                            |

Alarms: DLQs (delivery, plan, shard, urgent, standard, aggregator, index repair), ingest errors, fetch failures and quarantines; sender running tasks below desired for 3 minutes (also the deployment-rollback alarm), `PermanentBreakerOpen`, `GaveUp`, `DeadTargetDropped`, urgent queue age; planner and shard Lambda `Errors`; plan and standard queue age over 120 s; aggregator `IteratorAge` over 60 s; `delivery.signature_failed` / `fanout.signature_failed` log counts; the kill-switch alarms above; `IngestFeedStaleAlarm` (no successful NWS poll, a 200 or 304, for 10 minutes; the poller runs every minute and missing data counts as breaching); and the WAF blocked-requests alarm. Not alarmed yet: Aurora resume latency after a pause, and planned-versus-sent recipient counts per alert.

**Audit trail.** A multi-region CloudTrail trail with log file validation writes to an encrypted, versioned bucket (kept 1 year, retained when the stack is deleted) and to a log group. EventBridge reports to the topic: writes or deletes of the kill-switch parameter, every change to any `relay/<stage>/` secret (`PutSecretValue`, `UpdateSecret`, `DeleteSecret`, `RotateSecret`, `RestoreSecret`, `PutResourcePolicy`, `DeleteResourcePolicy`, `ReplicateSecretToRegions`; the Aurora admin secret is named `relay/<stage>/db/relay_admin` so it is covered, and a test checks every secret's name), events on the alert-signing KMS keys and aliases (`ScheduleKeyDeletion`, `CancelKeyDeletion`, `DisableKey`, `EnableKey`, `PutKeyPolicy`, `CreateGrant`, and `CreateAlias`/`UpdateAlias`/`DeleteAlias`), and GuardDuty findings of severity 7 or higher. IAM policy changes are reported by an alarm on the trail's log group (IAM is a global service whose events EventBridge only sees in us-east-1), excluding CloudFormation's own changes.

**Cost.** One AWS Budgets budget per stage. On prod, also a Cost Anomaly Detection monitor per AWS service with an immediate SNS subscription. The budget has no tag filter (it needs the `stage` cost allocation tag activated); if dev and prod share an account, each budget sees the account total.

## Deploying safely

**Prod needs the CDN origin.** Prod synth fails without `-c publicAlertBaseUrl=https://alerts.example.org` (like `alarmEmail`). Without it pushes carry no `alertUrl`, every phone opens its alert through the API, and the CDN does nothing at the moment of an alert. `-c allowNoPublicAlertUrl=true` is the explicit escape for a deploy that really wants that. Example prod deploy:

    npx cdk deploy --all -c stage=prod -c alarmEmail=oncall@example.com -c publicAlertBaseUrl=https://alerts.example.org

Prod runs the sender fleet by default (`senderEnabled` defaults to true when `stage=prod`), so a deploy that forgets a flag cannot scale delivery to zero. Turning prod's fleet off needs both `-c senderEnabled=false -c iUnderstandSenderStops=true`; with only the first, synth fails. A bad image rolls back: the ECS circuit breaker plus a deployment alarm on running tasks below desired. The first prod deploy needs the explicit off switch once, because the APNs and FCM secrets hold placeholders until you fill them in (see "Sender stack").

Aurora keeps automated backups for 14 days on prod (7 on dev) with tags copied to snapshots. A restore creates a new cluster with a new ARN: point the `data` stack outputs and the Lambda environments at it.

## Edge hardening, repair outbox and capacity

**Origin verification.** The API's `execute-api` URL is public, so a caller could skip CloudFront and the WAF. CloudFront adds the header `x-relay-origin-verify` to every API request; the API Lambda returns 403 without the matching value when `REQUIRE_ORIGIN_HEADER=true` (prod). The value is a generated secret `relay/<stage>/origin-verify` in the config stack, replicated to us-east-1; the edge stack resolves it at deploy time with `{{resolve:secretsmanager:...}}`, so it never appears in a template. The Lambda reads it by `ORIGIN_VERIFY_SECRET_ARN` and caches it 5 minutes. To rotate, change the secret, redeploy `edge` (CloudFront picks up the new value), and expect up to 5 minutes of 403s until the Lambda cache refreshes.

**Quota: API Gateway.** The account-level HTTP API quota is 10,000 requests per second per region (adjustable) with a fixed 5,000 burst. The prod `$default` stage throttle therefore defaults to 10,000 rps / burst 5,000 (a stage limit above the account quota never applied). After the quota increase to 60,000 rps (docs/scale-quotas.md) pass `-c apiGatewayRpsQuota=60000` and the stage follows; an explicit `apiThrottleRate` above the quota fails at synth.

**Cell-index repair.** When the API cannot write a place into the cell index it queues `{op, h3Cell, deviceId, placeId}` to `INDEX_REPAIR_QUEUE_URL`; a repair Lambda re-reads Aurora and redoes the write (idempotent, per-item failures). After 5 failed receives a message goes to the repair DLQ, which has an alarm (`IndexRepairDlqAlarm`). Fix the cause, then redrive the DLQ to the repair queue.

**Capacity context.** `-c auroraMaxAcu=N` (default prod 16, dev 2); `-c checkinsWarmWritesPerSecond=N` (check-ins table, default prod 40,000, dev none; 0 disables); `-c checkinTotalsWarmWritesPerSecond=N` (totals table, default prod 40,000; after the DynamoDB quota raise try 100,000). Warm throughput is prod only: the idempotency table has 20,000 writes/s (delivery stack), the cell index 30,000 reads/s and 10,000 writes/s (fanout stack, justified in `lib/fanout-stack.ts`).

**Lambda reserved concurrency and the account quota (SEC-C-2 / RED-4).** Lambda concurrency is one pool per account and Region, so on prod the stacks reserve concurrency for the functions an API flood must never starve and cap the API with what is left: planner 10, shard workers 500, ingest 1, delivery worker 10, aggregator 50, repair 5, and **API = quota - 100 - those**. `-c lambdaConcurrencyQuota=N` is the account quota the plan assumes (default 1,000, the AWS default, which gives the API 324; at 15,000 it gets 14,324). AWS keeps at least 100 unreserved, shared by late-join, the paused consumer, the drainer and the redrive functions. `synth` fails when the reservations do not fit (`lib/capacity.ts`). Individual overrides: `-c apiReservedConcurrency`, `aggregatorReservedConcurrency`, `repairReservedConcurrency`, `plannerReservedConcurrency`, `shardReservedConcurrency`. Dev stays unreserved apart from its shard worker (500), ingest (1) and delivery worker (10); if dev shares an account with prod, add those 511 to the quota you pass. Raise the real quota in Service Quotas before deploying a plan that assumes more than 1,000.

**Remaining exposure: the default `execute-api` endpoint.** An HTTP API cannot turn the default endpoint off while CloudFront uses it as the origin without a custom domain, so it stays reachable and a flood aimed straight at it bypasses the WAF. What limits it now: the origin-header guard returns 403 before any work, the stage throttle caps the rate at the account quota, and the API function's reserved concurrency caps what a flood can run, so it can no longer starve the planner, shard workers, ingest or aggregator (it can still make real check-ins return 429/503 while it lasts, which is the remaining exposure). The follow-up is a custom domain for the API in front of CloudFront plus `disableExecuteApiEndpoint: true`; do that together with a regional WAF only if the API moves to a REST API.

**Not done: separate check-in function (PERF-8).** Check-ins share the API function and its reserved concurrency. A dedicated check-in Lambda (or route-level throttling) with provisioned concurrency for the burst is the follow-up; it needs its own reservation in `lib/capacity.ts`.

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

## Late-join stack

`latejoin` owns the late-join queue (`relay-<stage>-latejoin`, with a DLQ and an alarm) and one Lambda. A device that registers a place or a push target while an alert is active missed the fan-out, so the API enqueues a small intent `{deviceId, placeId?, reason}`; only this Lambda consumes it. It re-reads the device, places and active alerts from Aurora with the read-only `relay_latejoin` role, verifies the alert signature, applies the primary-cell rule, claims one check-in token per push target per alert in the idempotency table, and sends a single-recipient batch to the urgent or standard send queue. The API has no permission on the send queues and no `URGENT_QUEUE_URL`/`STANDARD_QUEUE_URL`; it gets only `LATEJOIN_QUEUE_URL`. The Lambda retries while the kill switch is paused. A late-join DLQ message means a late joiner may have missed an alert: fix the cause and redrive it to the late-join queue. Its batch keys carry a hash of the device id, never the id.

## Sender stack (statewide scale)

The sender fleet (Fargate, 1 vCPU / 2 GB tasks) runs by default on prod and is off by default on dev. The APNs and FCM secrets are created with placeholder values and a sender that boots with them crash-loops, so the first deploy of a stage keeps the fleet off:

1. Prod: `npx cdk deploy ... -c stage=prod -c alarmEmail=... -c publicAlertBaseUrl=https://alerts.example.org -c senderEnabled=false -c iUnderstandSenderStops=true`. Dev: no flag. Everything is created; the sender service has desiredCount 0 and no auto scaling.
2. Put the real values in `relay/<stage>/apns-key` and `relay/<stage>/fcm-service-account` (`aws secretsmanager put-secret-value`).
3. Prod: deploy again with no sender flags. Dev: `-c senderEnabled=true`. The fleet starts. Prod keeps at least 10 tasks (override with `-c senderMinTasks=<n>`) and scales to 100 on queue depth (1-minute periods, aggressive steps) and urgent queue age; it scales in only after 15 idle minutes. Dev scales from 0 to 10.

The planner pre-scales the fleet on an urgent alert (ECS `UpdateService`, scoped to the sender service, never lowering the count; failures are logged and never block fan-out), because queue-metric scaling alone takes 3-5 minutes. It derives the service from the stage (`relay-<stage>-sender`), and the planner reads the same `senderEnabled` flag, so a disabled fleet is never pre-scaled.

**FCM projects (statewide Android).** One Firebase project caps FCM HTTP v1 sends at about 600,000 per minute, so 10M statewide pushes would take 7 to 8 minutes through one. `-c fcmProjectIds=relay-a,relay-b,relay-c` spreads Android devices over N Firebase projects (N = 3 here gives about 1.6M per minute at the default rate; plan N from the largest alert, see `docs/scale-quotas.md`). It sets `FCM_PROJECT_IDS` on the sender and on the API. The `fcm-service-account` secret then becomes a JSON array of N Google service-account keys in the same order (a single object still works and means one project). The sender refuses to start when `FCM_PROJECT_IDS` and the secret's `project_id` list differ. Each project is rate-limited on its own token bucket, `FCM_PROJECT_RATE_PER_SEC` (default 9,000, about 540,000 per minute, under the 600,000 default quota); raise it only after the project's quota is raised.

- The server assigns each device a project (stable hash of the device id modulo N) and returns it as `fcmProject` from `POST /devices`. The app sends it back with the token in `PUT /devices/me/push-token`; the API rejects any other value with 400. Devices without a project use the first one.
- **Only append project ids.** The assignment depends on the order and count. A new project only receives devices that register after it is added; do not reorder or remove ids, because a device's FCM token only works with the project it registered with.
- **The mobile app must initialize secondary `FirebaseApp` instances.** The default Firebase app covers one project. For every other id in the list the app needs that project's `google-services` config (`FirebaseApp.initializeApp(context, options, name)` on Android) and must request its FCM token from the app named for the `fcmProject` the server returned, then send that project id with the token. Until the app does this, leave `fcmProjectIds` unset or keep N = 1.
- Raise each project's FCM quota too (Cloud console, Quotas, "FCM HTTP v1 send requests"); the per-project bucket only protects the default.
- Migration `0012_push_project.sql` adds `devices.push_project`; `relay_sender` may clear it with the token (also granted in `0011_roles.sql` so a `db-roles` re-run keeps it).

**Quota:** the Fargate On-Demand vCPU quota defaults to 6 vCPUs per Region. It must be raised to at least 100 tasks x 1 vCPU = 100 vCPUs (more with headroom) before enabling prod, or the fleet cannot grow past 6 tasks. See `docs/scale-quotas.md` (ECS Fargate section).
