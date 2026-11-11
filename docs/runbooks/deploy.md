# Runbook: deploy

> **Draft.** No AWS account exists yet (S-06), so nothing here has been run. The pipeline is built and tested offline and is inert until the repository variable `AWS_DEPLOY_ENABLED` is `true`. `<stage>` is `dev` or `prod`; the main region is `us-west-2`, the edge region `us-east-1`.

Deploys go through one workflow, [`.github/workflows/deploy.yml`](../../.github/workflows/deploy.yml). Nobody deploys from a laptop. The only exception is the one-time setup below, which the account owner runs by hand because it creates the roles the pipeline uses.

## How it works

| Job      | GitHub environment | AWS access                                               | What it does                                                                                                                                                                                                                             |
| -------- | ------------------ | -------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `build`  | none               | none                                                     | `npm ci`, lint, typecheck, every test (the infra tests run cdk-nag on dev and prod). Refuses `prod` from any branch but `main`.                                                                                                          |
| `diff`   | `<stage>-diff`     | `relay-github-diff-<stage>` (CDK lookup role only)       | `cdk synth` with the stage's real context and cdk-nag on (an unsuppressed finding fails here), `cdk diff --method=template` against what is deployed, written to the job summary. Uploads the cloud assembly (`cdk.out`) as an artifact. |
| `deploy` | `<stage>`          | `relay-github-deploy-<stage>` (CDK deploy/publish roles) | Downloads that artifact and runs `cdk deploy --app cdk.out --all`, so what is deployed is exactly what was nagged and diffed. On prod this job waits for a required reviewer.                                                            |

Triggers: **Run workflow** (`workflow_dispatch`, stage `dev` or `prod`), and push to `main`, which deploys **dev only** and only when `AWS_DEPLOY_DEV_ON_PUSH` is also `true`. The workflow never runs on `pull_request`, so a fork or PR branch never gets an OIDC token or an environment's variables. One deploy per stage runs at a time (`concurrency: deploy-<stage>`); a second run queues instead of cancelling a deploy halfway.

Every action is pinned to a full commit SHA, the checkout never keeps its token (`persist-credentials: false`), the top-level token is `contents: read`, and only `diff` and `deploy` get `id-token: write` (enforced by `infra/test/supply-chain.test.ts`).

### Why these roles (least privilege)

The roles are defined in [`infra/bootstrap/github-deploy-stack.ts`](../../infra/bootstrap/github-deploy-stack.ts), a separate CDK app that is not part of `bin/relay.ts`, and tested in `infra/test/github-deploy-role.test.ts`.

- **No long-lived keys.** GitHub issues an OIDC token per job; AWS exchanges it for credentials that last at most 2 hours. There is no IAM user and no access key anywhere.
- **Trust is one repository and one environment.** Each role's trust policy requires `aud = sts.amazonaws.com` and `sub = repo:DallasLendaris/Relay:environment:<env>` with `StringEquals` (no wildcards). The deploy role for prod trusts only `environment:prod`, so the only jobs that can assume it are ones that passed the prod environment's protection rules (required reviewers, `main` only). A workflow on another branch, a PR, a fork or another repository gets a different `sub` and is refused.
- **The roles can do one thing: assume CDK's bootstrap roles.** Not `AdministratorAccess`. The deploy role may assume `cdk-hnb659fds-deploy-role`, `-file-publishing-role`, `-image-publishing-role` and `-lookup-role` in us-west-2 and us-east-1 of its own account; the diff role only the lookup role (read-only; it cannot change anything). Those bootstrap roles are what `cdk deploy` uses anyway: the publishing roles can only write to the bootstrap asset bucket and ECR repository, and the deploy role can only drive CloudFormation and pass the CloudFormation execution role.
- **Residual risk: the CloudFormation execution role.** CloudFormation makes the actual changes with `cdk-hnb659fds-cfn-exec-role`, which by default has `AdministratorAccess`. So a malicious change that gets through review and the prod approval can still create anything. The controls against that are code review, the diff, and the required reviewer. Narrowing it (`cdk bootstrap --cloudformation-execution-policies <policy>` with a policy limited to the services Relay uses, plus a permissions boundary) is a follow-up once the first deploys show which actions are needed.

## One-time setup (per AWS account)

Do this once in the dev account and once in the prod account, signed in as an account administrator through IAM Identity Center (short-lived credentials, never access keys). Run every command from `infra/` after `npm ci` at the repo root, so the CDK CLI version is the one in `package-lock.json`.

1. **Bootstrap CDK in both regions**, with termination protection:

   ```sh
   npx cdk bootstrap aws://<account>/us-west-2 aws://<account>/us-east-1 \
     --termination-protection
   ```

   The bootstrap roles trust their own account by default, which is all the OIDC roles need (they live in the same account). Pass `--trust <account>` and `--trust-for-lookup <account>` only if the GitHub roles ever move to a separate tooling account. Optionally narrow the execution role now with `--cloudformation-execution-policies` (see "Residual risk" above). Re-run bootstrap after a CDK CLI major upgrade if a deploy says the bootstrap version is too old.

2. **Create the GitHub OIDC provider and roles.** Review first, then deploy:

   ```sh
   npm run oidc-roles -- diff   -c stages=dev       # in the dev account
   npm run oidc-roles -- deploy -c stages=dev
   npm run oidc-roles -- deploy -c stages=prod      # in the prod account
   ```

   `-c stages=dev,prod` creates both stages' roles if dev and prod ever share an account. If the account already has a GitHub OIDC provider (IAM allows one per URL), add `-c existingOidcProviderArn=arn:aws:iam::<account>:oidc-provider/token.actions.githubusercontent.com`. With a custom bootstrap qualifier, add `-c qualifier=<q>`. The stack is `relay-github-oidc`; cdk-nag NIST 800-53 runs on it during synth. The role ARNs are `arn:aws:iam::<account>:role/relay-github-deploy-<stage>` and `arn:aws:iam::<account>:role/relay-github-diff-<stage>`.

3. **Create the GitHub environments** (repository Settings > Environments): `dev-diff`, `dev`, `prod-diff`, `prod`.
   - **`prod`: Required reviewers.** At least one reviewer who is not the person who starts the run; turn on "Prevent self-review". Turn off "Allow administrators to bypass configured protection rules". Deployment branches and tags: selected branches, `main` only.
   - **`prod-diff`:** deployment branches `main` only. No reviewers (read-only role).
   - **`dev`, `dev-diff`:** deployment branches `main` (add other branches only if you want to deploy dev from them).

4. **Set the environment variables** (plain variables, not secrets: none of these is secret). Leave a variable unset rather than empty-but-set when it does not apply.

   | Variable                                   | Environments                        | Value                                                                                       |
   | ------------------------------------------ | ----------------------------------- | ------------------------------------------------------------------------------------------- |
   | `AWS_ACCOUNT_ID`                           | all four                            | The stage's 12-digit account ID; the credentials action fails if it gets any other account. |
   | `AWS_DIFF_ROLE_ARN`                        | `<stage>-diff`                      | `arn:aws:iam::<account>:role/relay-github-diff-<stage>`                                     |
   | `AWS_DEPLOY_ROLE_ARN`                      | `<stage>`                           | `arn:aws:iam::<account>:role/relay-github-deploy-<stage>`                                   |
   | `PUBLIC_ALERT_BASE_URL`                    | `prod-diff` (dev: optional)         | CDN origin for alert links, `https://...` (required on prod by `bin/relay.ts`).             |
   | `ALARM_EMAIL`                              | `prod-diff` (dev: optional)         | On-call address for the alarm topic (required on prod).                                     |
   | `EDGE_DOMAIN_NAME`, `EDGE_CERTIFICATE_ARN` | `prod-diff`, when the domain exists | Both or neither; the certificate must be in us-east-1.                                      |

   The context lives in the `-diff` environment because the diff job synthesizes; the deploy job deploys that cloud assembly and does not synthesize again.

5. **Turn the pipeline on** (repository Settings > Secrets and variables > Actions > Variables): `AWS_DEPLOY_ENABLED=true`, and `AWS_DEPLOY_DEV_ON_PUSH=true` if every merge to `main` should deploy dev.

6. **Protect `main`** (CM-3, SA-10): require a pull request with at least one approving review, require the `CI` check, block force pushes. This is a GitHub setting, not code; screenshot it for the evidence file.

7. **First deploy only:** after the first successful `deploy` job, seed the keys (`npm run seed-keys -- <stage>`), run the migrations and `scripts/db-roles.ts` as described in [`infra/README.md`](../../infra/README.md) ("Keys", "Run the migrations", "Database roles"). These are not in the pipeline yet (OPS-9: no migration runner with a lock).

## Routine deploy

**dev:** merge to `main` (if `AWS_DEPLOY_DEV_ON_PUSH` is on), or Actions > Deploy > Run workflow > `dev`.

**prod:**

1. Actions > Deploy > Run workflow, branch `main`, stage `prod`.
2. Wait for `build` and `diff`. Open the run's summary and read the `cdk diff`. Stop and ask if it shows:
   - a **replacement** (`replace`) of anything stateful (table, bucket, key, secret, Aurora cluster), or a change to a resource's name;
   - IAM changes (`IAM Statement Changes`, `IAM Policy Changes`) or security group changes nobody mentioned in the PR;
   - stacks or resources you did not expect.
3. A required reviewer approves the `deploy` job (Review deployments > `prod` > Approve). Approving is the change authorization; write the PR number in the comment.
4. Watch the `deploy` job. CloudFormation events are in the log and in the CloudFormation console per stack (`relay-prod-*`).
5. After it finishes: check the alarm topic and the dashboards for 15 minutes.

## Rollback

A CDK deploy is one CloudFormation change set per stack, applied in dependency order (`config`, `monitoring`, `data`, `delivery`, `fanout`, `latejoin`, `api`, `ingest`, `sender`, `edge`).

- **A stack fails mid-deploy:** CloudFormation rolls that stack back to its previous state on its own (the workflow does not pass `--no-rollback`), and the job fails. Stacks that already finished earlier in the same run stay on the new version. Usually the fix is a new PR; if the half-applied state is a problem, roll back as below.
- **A deploy succeeded but the change is bad:** redeploy the previous commit. Revert the PR on `main` (a revert PR, reviewed like any change) and run the workflow for the stage again. That keeps `main` equal to what is deployed. For an outage, the revert PR can be approved fast, but it still goes through the pipeline. Before that, if alerts are going out wrong, use the delivery kill switch (`/relay/<stage>/delivery-kill-switch`, "Kill switch" in [`infra/README.md`](../../infra/README.md), and [alert-recall.md](./alert-recall.md)).
- **Stack stuck in `UPDATE_ROLLBACK_FAILED`:** fix the cause (usually a resource changed by hand), then `aws cloudformation continue-update-rollback --stack-name relay-<stage>-<name>` (with `--resources-to-skip` only if you understand what you skip). This needs an administrator, not the pipeline role; record it as an emergency change.

### What a rollback does not undo

Redeploying an old commit restores the old **configuration**. It does not restore **data**: no DynamoDB items, S3 objects, secret values or Aurora rows come back, and migrations have no down scripts, so old code must still work with the newer schema (keep migrations additive).

On **prod**, these resources have `DeletionPolicy: Retain` (Aurora: `Snapshot` plus deletion protection). If a change removes or replaces one, CloudFormation leaves the old one in the account, still holding its data but no longer part of the stack, and a later deploy that recreates it with a fixed name fails until the orphan is deleted or imported (`cdk import`). Watch for these in the diff:

| Stack        | Retained on prod                                                                                                                                 |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `config`     | KMS keys `DataKey`, `MessagingKey`, `LogsKey`, `AlertSigningKey`; secrets `TokenKeys`, `DeviceCredentialKeys`, `ExpoAccessToken`, `OriginVerify` |
| `monitoring` | `TrailBucket` and `TrailLogGroup` (retained on dev too), `AccessLogsBucket`                                                                      |
| `data`       | Aurora `Cluster` (snapshot on delete, deletion protection), `VpcFlowLogs`, `PostgresLogs`                                                        |
| `delivery`   | `IdempotencyTable`, `WorkerLogs`                                                                                                                 |
| `fanout`     | `CellIndexTable`, every function log group                                                                                                       |
| `latejoin`   | `LateJoinLogs`                                                                                                                                   |
| `api`        | `CheckinsTable`, `CheckinTotalsTable`, function log groups                                                                                       |
| `ingest`     | `RawBucket`, `ZonePackBucket`, `AccessLogsBucket`, `StateTable`, `IngestLogs`                                                                    |
| `sender`     | secrets `ApnsKey`, `FcmServiceAccount`; `SenderLogs`, `VpcFlowLogs`                                                                              |
| `edge`       | KMS `EdgeKey`, `PublicAlertsBucket`, `AccessLogsBucket`, `WafLogs` (us-east-1)                                                                   |

The list comes from `grep -rn RemovalPolicy infra/lib`; re-check it when a stack changes. On dev almost everything is `Destroy`, so a dev rollback can lose dev data.

## Evidence this leaves (CM-3, CM-5, AC-17(4), AU)

- The workflow run: who started it, the commit, the diff in the summary, the deploy log.
- The environment's deployment history and the approval (who approved, when, comment).
- CloudTrail in the account: `AssumeRoleWithWebIdentity` for `relay-github-*` with the GitHub `sub` claim, then `AssumeRole` into the CDK roles and the CloudFormation calls.
