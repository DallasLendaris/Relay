# Runbook: key and secret rotation

> **Draft.** Nothing is deployed; the commands below have not been run against a real account. Run them with operator credentials in the stage's account. `<stage>` is `dev` or `prod`; the main region is `us-west-2`, the edge region `us-east-1`.

Push credentials issued by Apple, Google and Expo have their own runbook: [push-credential-rotation.md](./push-credential-rotation.md).

## Inventory and cadence

| Secret or key                                                      | Where                                 | Rotates automatically?                                  | Suggested cadence                                  | Procedure                                                 |
| ------------------------------------------------------------------ | ------------------------------------- | ------------------------------------------------------- | -------------------------------------------------- | --------------------------------------------------------- |
| KMS keys `alias/relay/<stage>/data`, `messaging`, `logs`           | config stack, us-west-2               | Yes, KMS rotates the key material every 365 days        | Automatic                                          | [Customer-managed KMS keys](#customer-managed-kms-keys)   |
| KMS key `alias/relay/<stage>/edge`                                 | edge stack, us-east-1                 | Yes, every 365 days                                     | Automatic                                          | same                                                      |
| Alert signing key `alias/relay/<stage>/alert-signing-v<n>`         | config stack (ECC_NIST_P256)          | No (KMS cannot rotate asymmetric keys)                  | Yearly, and at once on suspected misuse            | [Alert signing key](#alert-signing-key)                   |
| `relay/<stage>/token-keys`, `relay/<stage>/device-credential-keys` | config stack, Secrets Manager         | No (keyring format; see cdk-nag exceptions)             | Yearly, and at once on suspected exposure          | [Keyrings](#token-and-device-credential-keyrings)         |
| `relay/<stage>/origin-verify`                                      | config stack, replicated to us-east-1 | No (CloudFront holds a copy)                            | Every 90 days                                      | [Origin verification secret](#origin-verification-secret) |
| `relay/<stage>/db/relay_admin`                                     | data stack                            | No (rotation Lambda would need the VPC; see exceptions) | Every 90 days, and after anyone who knew it leaves | [Aurora admin password](#aurora-admin-password)           |
| `relay/<stage>/db/relay_<role>` (7 roles)                          | data stack                            | No (same)                                               | Every 90 days                                      | [Aurora role passwords](#aurora-role-passwords)           |

Every write to a `relay/<stage>/` secret and every key-policy or deletion event on the alert-signing key emails the alerts topic (monitoring stack), so each step below shows up there; that email is the audit record that the rotation happened. Record the date of each rotation in the change log the ISO keeps.

Why the secrets do not use Secrets Manager rotation: `docs/compliance/cdk-nag-exceptions.md`, rule `SecretsManagerRotationEnabled`.

## Customer-managed KMS keys

Nothing to do: `enableKeyRotation` is on with a 365-day period (`infra/lib/encryption.ts`). KMS keeps every old key version, so data encrypted before a rotation stays readable, and key ARNs and aliases do not change.

Check it once a year:

    aws kms get-key-rotation-status --region us-west-2 --key-id alias/relay/<stage>/data
    aws kms list-key-rotations      --region us-west-2 --key-id alias/relay/<stage>/data

Repeat for `messaging`, `logs` (us-west-2) and `edge` (us-east-1). Expect `KeyRotationEnabled: true` and a rotation within the last year.

On suspected key compromise, an on-demand rotation does not help (old material still decrypts). Instead: create a new key, re-encrypt (DynamoDB: `update-table --sse-specification`; S3: copy objects in place; SQS/SNS: set the new key, in-flight messages drain within the retention period; Aurora: snapshot, copy the snapshot with the new key, restore), then schedule deletion of the old key. That is a planned change, not a runbook step; open an incident (`docs/compliance/incident-response.md`).

## Alert signing key

KMS cannot rotate asymmetric keys, so the key is versioned (`infra/README.md`, "Alert signing key"):

1. In `infra/lib/relay-app.ts`, pass `alertSigningVersions: [1, 2]` to the config stack (next version after the highest one) and deploy `config`, then the stacks that read the key list (all verifiers, ingest). v2 signs; verifiers accept v1 and v2.
2. Wait until every alert signed with v1 has expired (the longest alert lifetime plus a day).
3. Set `alertSigningVersions: [2]` and deploy again. On prod the v1 key is retained; schedule its deletion by hand once you are sure: `aws kms schedule-key-deletion --key-id <v1 key id> --pending-window-in-days 30`.

On suspected misuse of `kms:Sign`, do steps 1 and 3 together (accept that unexpired v1 alerts stop verifying) and open an incident.

## Token and device-credential keyrings

Both use the keyring shape `{"current":"k<n>","keys":{...}}`. Rotation adds a key and keeps the old ones so tokens already issued keep verifying:

    npm run seed-keys -w @relay/infra -- <stage> --rotate

Loaders cache keyrings for 5 minutes, so new tokens use the new key within about 5 minutes. When tokens signed with the old kid have expired (check-in links live for the alert's lifetime), remove the old kid from `keys` with `aws secretsmanager put-secret-value` (keep `highestKid`). Details: `infra/README.md`, "Keys".

On suspected exposure: rotate, then remove the exposed kid at once. Outstanding check-in links and device credentials signed with it stop working; devices re-register.

## Origin verification secret

CloudFront sends this value as `x-relay-origin-verify`, and the edge stack resolves it from the us-east-1 replica only at deploy time. A rotation therefore needs a deploy, and a naive one causes up to 5 minutes of 403s (the API Lambda caches the value for 5 minutes). Do it outside an active alert:

1. `aws secretsmanager put-secret-value --region us-west-2 --secret-id relay/<stage>/origin-verify --secret-string "$(aws secretsmanager get-random-password --exclude-punctuation --password-length 48 --query RandomPassword --output text)"`
2. Wait for the replica: `aws secretsmanager describe-secret --region us-west-2 --secret-id relay/<stage>/origin-verify --query 'ReplicationStatus'` shows `InSync`.
3. Deploy the edge stack: `npx cdk deploy relay-<stage>-edge` (from `infra/`). CloudFront now sends the new value.
4. Expect 403s from the API for up to 5 minutes while Lambda containers still hold the old value; watch the API 4xx metric return to normal.

## Aurora admin password

The admin (`relay_admin`) is used only by operators for migrations and `infra/scripts/db-roles.ts`. No compute principal holds it.

1. Generate and set the new password on the cluster and in the secret, without echoing it:

       NEW=$(aws secretsmanager get-random-password --exclude-characters '"@/\' --password-length 32 --query RandomPassword --output text)
       aws rds modify-db-cluster --region us-west-2 --db-cluster-identifier relay-<stage> \
         --master-user-password "$NEW" --apply-immediately
       CUR=$(aws secretsmanager get-secret-value --region us-west-2 --secret-id relay/<stage>/db/relay_admin --query SecretString --output text)
       aws secretsmanager put-secret-value --region us-west-2 --secret-id relay/<stage>/db/relay_admin \
         --secret-string "$(jq -c --arg p "$NEW" '.password=$p' <<<"$CUR")"
       unset NEW CUR

2. Check: run a read-only Data API statement with the admin secret (`aws rds-data execute-statement ... --sql 'select 1'`).

## Aurora role passwords

The seven role secrets (`relay/<stage>/db/relay_<role>`) are what the Lambdas and the sender use through the Data API, which reads the secret on every call. Between writing the secret and running the script the role cannot log in (a few seconds), so rotate one role at a time outside an active alert:

1. `aws secretsmanager put-secret-value --region us-west-2 --secret-id relay/<stage>/db/relay_<role> --secret-string "$(jq -c --arg p "$(aws secretsmanager get-random-password --exclude-characters '"@/\' --password-length 32 --query RandomPassword --output text)" '.password=$p' <<<"$(aws secretsmanager get-secret-value --region us-west-2 --secret-id relay/<stage>/db/relay_<role> --query SecretString --output text)")"`
2. `npx tsx scripts/db-roles.ts <stage> --cluster-arn <ClusterArn> --admin-secret-arn <SecretArn>` (from `infra/`; both values are outputs of the data stack). It runs `ALTER ROLE ... LOGIN PASSWORD` for every role from its secret.
3. Watch the role's functions for Data API authentication errors for a few minutes.

Automatic rotation is possible later: Secrets Manager's hosted PostgreSQL single-user rotation Lambda, placed in the data VPC with a Secrets Manager interface endpoint (about $15/month for two AZs). That is the "revisit" item for this exception.
