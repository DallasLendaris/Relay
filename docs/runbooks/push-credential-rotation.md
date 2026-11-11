# Runbook: push credential rotation (APNs, FCM, Expo)

> **Draft.** Nothing is deployed; these steps have not been run against a real account. `<stage>` is `dev` or `prod`; secrets live in us-west-2.

These credentials are issued by Apple, Google and Expo, so AWS Secrets Manager cannot rotate them (see `docs/compliance/cdk-nag-exceptions.md`, rule `SecretsManagerRotationEnabled`). They are rotated by hand. APNs, FCM and Expo are external services: their endpoints are not AWS FIPS endpoints and are not FIPS 140 validated by AWS; the connections are TLS 1.2+ as negotiated by the Node.js runtime.

| Credential                         | Secret                              | Read by                                   | Picked up                                       | Suggested cadence                                                                 |
| ---------------------------------- | ----------------------------------- | ----------------------------------------- | ----------------------------------------------- | --------------------------------------------------------------------------------- |
| APNs auth key (`.p8`, token based) | `relay/<stage>/apns-key`            | sender tasks (Fargate)                    | at task start: force a new ECS deployment       | Yearly; at once when someone with access to it leaves or it may have been exposed |
| FCM service account key(s)         | `relay/<stage>/fcm-service-account` | sender tasks                              | at task start: force a new ECS deployment       | Every 90 days; at once on suspected exposure                                      |
| Expo access token                  | `relay/<stage>/expo-access-token`   | delivery worker Lambda (legacy Expo path) | on the next cold start of each worker container | Yearly; at once on suspected exposure                                             |

The sender refuses to start with a malformed or placeholder secret (`backend/src/scale/sender/main.ts`, `validateApnsSecret`, `validateFcmServiceAccounts`), so a bad value shows up as new tasks failing to start and the ECS deployment circuit breaker rolling back to the old tasks, which still hold the old credential. Keep the old credential valid until the new tasks are healthy.

Every `put-secret-value` on these secrets emails the alerts topic; that email is the record of the rotation.

## APNs auth key

1. In the Apple Developer account (Certificates, Identifiers & Profiles > Keys), create a new key with the Apple Push Notifications service enabled. Download the `.p8` once; note the Key ID. Apple limits how many APNs keys a team may hold, so you may have to plan the revoke of the oldest key first; never revoke the key in use before step 4.
2. Write the new secret (same JSON shape as today: `keyId`, `teamId`, `bundleId`, `p8`, `sandbox`). Keep the file off shared disks and delete it afterwards:

       aws secretsmanager put-secret-value --region us-west-2 --secret-id relay/<stage>/apns-key \
         --secret-string "$(jq -n --arg k <KEY_ID> --arg t <TEAM_ID> --arg b <BUNDLE_ID> \
           --rawfile p AuthKey_<KEY_ID>.p8 '{keyId:$k,teamId:$t,bundleId:$b,p8:$p,sandbox:false}')"

3. Roll the sender fleet so every task reads it:

       aws ecs update-service --region us-west-2 --cluster relay-<stage>-sender \
         --service relay-<stage>-sender --force-new-deployment

   Watch the deployment finish (`aws ecs describe-services ...`, `rolloutState: COMPLETED`) and the sender logs for APNs errors (`403 InvalidProviderToken` means the key, team or key ID is wrong).

4. Send a test alert on dev (or a test push to a staff device on prod) and confirm delivery on iOS.
5. Revoke the old key in the Apple Developer account. Apple keys do not expire, so revocation is the only way an old key stops working.

## FCM service account keys

The secret holds one service account object, or a JSON array with one per Firebase project (`FCM_PROJECT_IDS`); the order and `project_id` values must stay the same, or the sender refuses to start.

1. For each Firebase project, in Google Cloud IAM (Service accounts > the FCM sender account > Keys), add a new JSON key. Do not delete the old key yet.
2. Write the secret with every new key, in the same order as before:

       aws secretsmanager put-secret-value --region us-west-2 \
         --secret-id relay/<stage>/fcm-service-account --secret-string file://fcm-accounts.json

   Then delete `fcm-accounts.json` from local disk.

3. Roll the sender fleet (same `update-service --force-new-deployment` command as for APNs) and watch it complete and log no FCM `401`/`403`.
4. Send a test push to an Android device.
5. Delete the old keys in Google Cloud IAM. If the organization allows it, set a key expiry policy (`iam.serviceAccountKeyExpiryHours`) so a forgotten key cannot live forever.

Longer term, keyless access (Google workload identity federation trusting the sender's AWS task role) would remove this secret entirely; that is a design change, not part of this runbook.

## Expo access token

1. In the Expo account (Access tokens), create a new robot token for push.
2. `aws secretsmanager put-secret-value --region us-west-2 --secret-id relay/<stage>/expo-access-token --secret-string '<token>'` (raw token, no JSON). Prefer reading the token from a file or prompt so it does not land in shell history.
3. The delivery worker reads the token when a container starts. To make every container pick it up now, publish a configuration change, for example `aws lambda update-function-configuration --function-name <WorkerFunction name> --description "expo token rotated <date>"`, then confirm pushes still succeed.
4. Revoke the old token in Expo.

## After any rotation

- Note the date, who rotated, and the new key ID (never the secret) in the change log.
- If the rotation was triggered by suspected exposure, follow `docs/compliance/incident-response.md` as well.
