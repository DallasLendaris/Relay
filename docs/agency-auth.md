# Agency sign-in, authorization and audit trail (DRAFT)

> **Draft foundation, no owner yet.** The agency side of Relay has no team owner. This is a backend, infrastructure and API-contract starting point for whoever takes it on: it is meant to be reshaped. Every choice that the owner or the State must confirm is listed under [Decisions](#decisions). Nothing is deployed, and there is no front end yet (`agency/` holds only a README).

Branch `b2-agency`. Code: `infra/lib/agency-stack.ts`, `backend/src/agency/`, the `/agency/*` paths in `packages/shared/openapi/relay.yaml`. Tests: `infra/test/agency-stack.test.ts`, `backend/test/agency/`.

## What it does

- **Sign-in** for agency staff with Amazon Cognito: admin-created accounts only, password plus TOTP, short sessions.
- **Two read-only endpoints**: `GET /agency/me` and `GET /agency/alerts/{alertId}/summary` (check-in totals per county, only for the caller's counties, small counts withheld).
- **An audit record for every agency API call**, allowed or denied, in a tamper-resistant S3 bucket. If the record cannot be written, the call is refused.

It cannot send, change or cancel an alert. Agency-initiated sending belongs to the statewide phase and will need its own actions, roles and review.

## How a request flows

```
browser (future agency app)
  -> CloudFront + WAF                       /agency/* behavior (edge-stack.ts AGENCY_PATH_PATTERN)
  -> agency HTTP API, $default stage        throttle 20 rps / burst 40, access log of EVERY request
       JWT authorizer (Cognito)             signature, issuer, client id, expiry; 401 if not valid
  -> AgencyFunction (backend/src/agency/handler.ts -> service.ts)
       1. route known?                      else 404 unknown_route
       2. origin-verify header (prod)?      else 403 origin (the request skipped CloudFront/WAF)
       3. claims re-checked                 access token, client id, issuer, groups, agency id, jurisdiction
       4. role allows the action?           roles.ts PERMISSIONS, deny by default
       5. requested counties in jurisdiction?  else 403 area_outside_jurisdiction
       6. read totals, place cells in counties, suppress small counts
       7. write the audit record            fails -> 503 audit_unavailable, nothing served
  <- response (Cache-Control: no-store)
```

Steps 1 to 6 can only refuse. A refusal at any step is still audited in step 7. A request the JWT authorizer refuses never reaches the function; it is recorded in the agency API's access log (`AgencyApiAccessLogs`, request id, IP, route, status, authorizer error) and counted by the denied-request alarm.

### Why a separate HTTP API

`infra/lib/api-stack.ts` reserved `/agency/*` for "the Cognito-protected agency API (added later)". It is its own HTTP API in `AgencyStack` rather than routes on the public API because:

- its stage access log is the only record of requests the JWT authorizer refuses, and the public API (millions of anonymous phones) should not get an access log just for that;
- it gets its own small throttle, so agency traffic and public traffic cannot starve each other;
- the whole agency side can be reshaped or removed without touching the public API.

CloudFront sends `/agency/*` to it as a second API origin with the same settings as the public API (never cached, HTTPS only, TLS 1.2 to the origin, the `x-relay-origin-verify` header), so the same WAF web ACL, including the general rate rule and the AWS common rule set, applies. `ApiStack` changed only to expose the check-in totals table name and ARN.

### Why the HTTP API JWT authorizer (not a Lambda authorizer)

- It is managed by API Gateway: no code, no cold start, no extra cost, and it rejects missing, expired, forged or other-client tokens before any function runs.
- Authorization (role and jurisdiction) is done in the function anyway, where it can be unit-tested and audited. A Lambda authorizer would add a second place for that logic and its decisions are cached by API Gateway, which makes denies harder to audit.
- A Lambda authorizer would be the right choice later if the owner wants token revocation checks on every call or a non-Cognito IdP token format.

The function re-checks the claims it relies on (`backend/src/agency/principal.ts`): `token_use` must be `access` (ID tokens are refused), `client_id` and `iss` must match the pool, groups must be known Relay roles, and the agency id and jurisdiction must be well formed. Anything odd is a deny, not a partial allow.

## Sign-in (Cognito)

| Setting               | Value                                                                                                                                                                                                                                     | Control          |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| Self sign-up          | Off: `AllowAdminCreateUserOnly`. Accounts exist only when an admin creates them                                                                                                                                                           | AC-2             |
| MFA                   | **Required**, TOTP authenticator apps only. SMS and email OTP off (SIM swap, email compromise)                                                                                                                                            | IA-2(1), IA-2(2) |
| Password policy       | 14+ characters, upper, lower, digit, symbol; last 24 not reusable; temporary password valid 3 days                                                                                                                                        | IA-5(1)          |
| Sign-in flow          | SRP only (`ALLOW_USER_SRP_AUTH` + refresh). No `USER_PASSWORD_AUTH`, no admin flows on the client, no client secret (browser app)                                                                                                         | IA-5             |
| Sessions              | Access and ID tokens 15 min, refresh token 8 h (one shift), 3 min to finish a sign-in, token revocation on                                                                                                                                | AC-12            |
| User existence errors | Hidden (`PreventUserExistenceErrors`)                                                                                                                                                                                                     | AC-7, SI-11      |
| Feature plan          | **Plus**: threat protection (enforced on prod, audit-only on dev), pre-token-generation V2, password history                                                                                                                              | AC-7, SI-4       |
| Deletion protection   | On for prod; the pool is retained on stack deletion on prod                                                                                                                                                                               | CP-9             |
| Account recovery      | Verified email                                                                                                                                                                                                                            | IA-5             |
| Custom attributes     | `custom:agency_id`, `custom:jurisdiction` (comma-separated county FIPS). The app client can **read** them but not **write** them, so only an admin (AdminUpdateUserAttributes) changes them                                               | AC-2, AC-6       |
| Writable by users     | `locale`, `zoneinfo` only (not email, not the custom attributes)                                                                                                                                                                          | AC-6             |
| Groups (roles)        | `relay-admin` (precedence 0), `agency-operator` (10), `agency-viewer` (20)                                                                                                                                                                | AC-2, AC-3       |
| Encryption at rest    | Cognito encrypts user pool data with an AWS-owned key. `AWS::Cognito::UserPool` has no property for a customer-managed key for the pool's data (the only KMS setting is for custom sender triggers, not used), so no CMK is possible here | SC-28            |

### Unsuccessful sign-in attempts (AC-7)

Cognito's built-in lockout applies and cannot be configured: after repeated failed sign-ins (AWS documents five) Cognito locks the user out temporarily, starting at one second and doubling with each further failure up to about 15 minutes; a successful sign-in resets it. Check the current wording in the [Cognito authentication documentation](https://docs.aws.amazon.com/cognito/latest/developerguide/authentication.html) before relying on the numbers. On top of that:

- threat protection (Plus) scores each sign-in and checks passwords against known compromised-credential lists. **No risk configuration is set here**, so Cognito's defaults apply. The owner should decide whether to block high-risk sign-ins and compromised credentials with a `CfnUserPoolRiskConfigurationAttachment` ([Decisions](#decisions) D-5);
- the WAF rate rules apply to `/agency/*`;
- the agency API alarms on a spike of 401/403 responses (`AgencyDeniedRequestsAlarm`).

The State may require a configurable lockout (for example 3 attempts / 15 minutes). Cognito cannot do that; a federated state IdP can.

### Users, roles and jurisdictions

| Role                  | GET /agency/me | GET /agency/alerts/{alertId}/summary | Notes                                                                                                                         |
| --------------------- | -------------- | ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `agency-viewer`       | yes            | yes, own jurisdiction                | agency staff                                                                                                                  |
| `agency-operator`     | yes            | yes, own jurisdiction                | reserved for agency actions in the statewide phase; same reads today                                                          |
| `relay-admin`         | yes            | **no**                               | administers accounts; separation of duties (AC-5, AC-6). An admin who needs data also gets an agency group and a jurisdiction |
| any other or no group | no             | no                                   | deny by default                                                                                                               |

A user in `agency-viewer` or `agency-operator` must have `custom:agency_id` (lowercase, digits, hyphens) or every request is refused (`no_agency`). The jurisdiction is a set of California county FIPS codes (`06001` to `06115`, odd numbers); a malformed value refuses every request (`invalid_jurisdiction`) rather than degrading. An empty jurisdiction sees nothing (`no_jurisdiction`). There is no statewide wildcard: a statewide user lists all 58 counties.

The roles live in `backend/src/agency/roles.ts` (`PERMISSIONS`); the Cognito groups in `infra/lib/agency-stack.ts` (`AGENCY_GROUPS`), and a test keeps them in step.

Cognito does not put custom attributes into access tokens. The pre-token-generation trigger (`backend/src/agency/pre-token.ts`, V2) copies `custom:agency_id` and `custom:jurisdiction` from Cognito's own user record into the access token as `agency_id` and `jurisdiction`. The values never come from the client. A change by an admin takes effect at the user's next token refresh, at most 15 minutes later; disabling a user stops refreshes, but an already-issued access token stays valid at API Gateway until it expires (at most 15 minutes). [Decisions](#decisions) D-3 covers shortening that.

Creating a user (an admin with IAM permission on the pool; every call is in CloudTrail):

```sh
POOL=<AgencyUserPoolId output>
aws cognito-idp admin-create-user --user-pool-id "$POOL" --username jane@county.example.gov \
  --user-attributes Name=email,Value=jane@county.example.gov Name=email_verified,Value=true \
                    Name=custom:agency_id,Value=santa-clara-oes Name=custom:jurisdiction,Value=06085 \
  --desired-delivery-mediums EMAIL
aws cognito-idp admin-add-user-to-group --user-pool-id "$POOL" --username jane@county.example.gov \
  --group-name agency-viewer
# Off-boarding: disable first (stops sign-in and refresh), then remove.
aws cognito-idp admin-disable-user --user-pool-id "$POOL" --username jane@county.example.gov
aws cognito-idp admin-user-global-sign-out --user-pool-id "$POOL" --username jane@county.example.gov
```

On first sign-in the user sets a new password and then enrolls a TOTP app (Cognito's `MFA_SETUP` challenge); the future front end must implement both steps.

### Federation (SSO) later

Not built, on purpose: the State or county identity provider is not chosen (Q-14). The hook is in `AgencyStack` right after the user pool:

1. Add a `UserPoolIdentityProviderSaml` (or `UserPoolIdentityProviderOidc`) for the state/county IdP.
2. Map IdP claims the State controls to `custom:agency_id` and `custom:jurisdiction` in its attribute mapping, and decide whether groups come from an IdP claim (a pre-token trigger can map them) or stay admin-assigned in Cognito.
3. Add the provider to the client's `supportedIdentityProviders`, enable the OAuth code flow with PKCE on the client and add a Cognito domain for the hosted sign-in.
4. Decide where MFA happens. With federation, MFA is the IdP's job and Cognito's MFA does not apply to federated users; the State must confirm its IdP enforces it (IA-2(1), IA-2(2)).

The backend needs no change: it reads the same access-token claims.

## Endpoints

Both need `Authorization: Bearer <Cognito access token>`. Responses are JSON with `Cache-Control: no-store`. Errors use the shared `Error` shape (`code`, `message`); the code is stable (`role`, `area_outside_jurisdiction`, `invalid_request`, `audit_unavailable`, ...), the message is generic.

- `GET /agency/me`: `{ sub, agencyId, roles, jurisdiction }`, all from the token.
- `GET /agency/alerts/{alertId}/summary[?areas=06085,06081]`: per-county totals.
  - `areas` may only narrow the jurisdiction. Default: the whole jurisdiction. Any county outside it refuses the whole request with 403 `area_outside_jurisdiction` (audited, with the counties that were asked for), rather than silently dropping it, so probing shows up in the audit trail and the alarm.
  - Source: the per-H3-cell totals the Streams aggregator writes (`CheckinTotalsTable`, `byAlert` index; `backend/src/scale/checkins/totals.ts`). The function can query only that index; it has no access to the check-ins table or Aurora.
  - Cells are placed in counties by their center point (`backend/src/agency/areas.ts`), using the NWS county zones (`CACnnn`) from the zone pack ingest already uses, with NWS as fallback. A cell on a county line counts for one county only. If a requested county has no boundary the request fails closed (503 `area_data_unavailable`).
  - `complete: false` means the alert had more than 50,000 cells with check-ins and only the first 100 pages were read.
  - Response: `{ alertId, asOf, minCellCount, complete, areas: [{ countyFips, checkedIn, suppressed, cells: [{ h3Cell, checkedIn, suppressed }] }] }`.

The OpenAPI contract replaces the earlier placeholder paths `/agency/alerts` (list) and `/agency/alerts/{alertId}/response` (never implemented; it promised `recipients` counts that Relay does not store). Like every contract change it needs all-three approval. An agency alert list is left for the owner (it needs read access to Aurora alerts filtered by the jurisdiction's SAME codes).

## Small-cell suppression

**Default pending the team's privacy-floor decision** (D-1). `backend/src/agency/suppression.ts`:

1. Every count below the threshold is withheld (`checkedIn: null`, `suppressed: true`). Default **10**, set with `-c agencyMinCellCount` (env `AGENCY_MIN_CELL_COUNT`); the response states the threshold in force.
2. Complementary suppression: while the withheld cells of a county add up to less than the threshold, the smallest visible cell is withheld too, so "county total minus visible cells" never reveals a small count (a test checks this exhaustively over small cases).
3. A county whose total is below the threshold is withheld whole.

Known limit: two responses taken minutes apart can still show how a small cell changed (differencing over time). Rounding, noise or a delay would close that and are part of D-1. The earlier contract said 20; 10 is the default the task set, and the threshold is a single setting.

## Audit trail (AU-2, AU-3, AU-9, AU-11, AU-12)

### What is recorded

One JSON object per agency API call that reaches the function (`backend/src/agency/audit.ts`, schema `relay.agency-audit/1`):

| Field                         | Meaning                                                                                                                                                                                                                  |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `time`, `requestId`           | When, and the API Gateway request id (joins the access log)                                                                                                                                                              |
| `action`                      | `agency.me.read`, `agency.alert.summary.read`, or `agency.unknown`                                                                                                                                                       |
| `outcome`, `reason`, `status` | `allow` / `deny` / `error`, a stable reason code, the HTTP status                                                                                                                                                        |
| `actor`                       | `sub`, `agencyId`, `roles` as the token states them (null when there was no valid token)                                                                                                                                 |
| `resource`                    | route, `alertId`, the counties asked for or given by default                                                                                                                                                             |
| `source`                      | API Gateway peer IP (a CloudFront address through the edge), `X-Forwarded-For` as received (CloudFront appends the viewer address last; entries left of it come from the client), user agent (clipped to 512 characters) |

It never contains check-in counts, cells, tokens or anything about members of the public; a test asserts the exact field list and that no count leaks. The function's own log line per request (`agency_request`) has the action, outcome and status only, not the actor or IP.

Requests the JWT authorizer refuses are in the agency API access log instead (same request id format). Together they cover every agency call.

### Fail closed

The audit write happens before the response is returned. If it fails, the request gets 503 `audit_unavailable` and no data, whatever the decision was (`AgencyAuditWriteFailedAlarm` fires on the first failure). The reasoning: agency reads are not on the life-safety path (alerts and check-ins do not depend on them), so refusing a read is always safer than serving one nobody can account for. If the configuration needed to write records is missing, every call returns 500.

### Storage: direct PutObject, not Firehose

Each record is one `PutObject` to `AgencyAuditBucket` under `agency-audit/v1/YYYY/MM/DD/HH/<time>-<requestId>.json`.

- **Reliable and simple:** `PutObject` returns only once the object is durably stored, which is exactly what fail-closed needs. With Firehose, `PutRecord` succeeds when Firehose has buffered the record; delivery to S3 happens later and can fail separately, so it needs its own error bucket and alarm and the "stored before served" guarantee becomes "accepted before served".
- **Volume is tiny:** a few dozen users. Assume 50 users x 200 calls a day = 10,000 records a day, 300,000 a month.
- **Cost:** PutObject at $0.005 per 1,000 requests is about **$1.50 a month**; storage of a few hundred MB a year is cents; SSE-KMS with an S3 Bucket Key keeps KMS calls low. Firehose would be about $0.029 per GB ingested with each record rounded up to 5 KB, so 300,000 x 5 KB = 1.5 GB, about **$0.05 a month** plus its own S3 PUTs (one per buffered batch). Firehose becomes the better choice if agency volume grows by orders of magnitude or records must stream to a SIEM. (Prices are us-west-2 list prices as we understand them; check the S3 and Firehose pricing pages.)

### Protection (AU-9)

The bucket follows the trail bucket's pattern (`monitoring-stack.ts`, `b2-hardening`):

- **S3 Object Lock, governance mode**, default retention `-c agencyAuditObjectLockDays` (prod 365, dev 1). Object Lock can only be enabled when the bucket is created and never turned off.
- Versioned, private (all public access blocked, bucket-owner-enforced), TLS only (TLS 1.2+), **SSE-KMS with the logs key** (`encryption.ts` `LOG_BUCKET_IDS`), server access logs to the stack's access log bucket, retained on stack deletion on every stage.
- Bucket policy (`DenyAuditTamperExceptBreakGlass`): nobody may `DeleteObject`, `DeleteObjectVersion`, `PutObjectRetention`, `PutObjectLegalHold` or `BypassGovernanceRetention` unless the caller is the role `relay-<stage>-audit-break-glass`. **That role is not created here** (S-06 creates accounts and roles); until it exists, nobody can delete records inside the lock window short of an account root user rewriting the bucket policy.
- Bucket policy (`DenyAuditWriteExceptAgencyFunction`): only the agency function's role may `PutObject`, so nobody else can plant a record. The function itself has `s3:PutObject` on the prefix and nothing else (no read, no delete).
- Configuration changes to the bucket (policy, lifecycle, versioning, Object Lock, logging, deletion) are reported to the alerts topic (`AgencyAuditBucketChangedRule`), since an administrator could still rewrite the policy.

Gap, as for the trail: governance mode and a workload-account bucket. Compliance mode or a separate log-archive account (AU-9(2)) is the stronger option ([Decisions](#decisions) D-6).

### Retention (AU-11)

Lifecycle expiry `-c agencyAuditRetentionDays` (prod 365, dev 30), the same for non-current versions; the lock may not be longer than the retention (synth refuses). The value the State requires is open (Q-21).

### Cognito admin actions

Not duplicated. Cognito user pool management calls (`AdminCreateUser`, `AdminDisableUser`, `AdminDeleteUser`, `AdminAddUserToGroup`, `AdminRemoveUserFromGroup`, `AdminUpdateUserAttributes`, `AdminResetUserPassword`, `UpdateUserPool`, ...) are CloudTrail **management events**, so the existing multi-region trail (`monitoring-stack.ts` `Trail`, management events, Object Lock, file validation) already records who did it, when and from where; AWS redacts passwords in them. What this branch adds is a report: `AgencyUserPoolChangedRule` sends the alerts topic any change that weakens sign-in (MFA configuration, pool or client updates, identity providers, threat protection). Group membership changes are routine and are not paged; they stay in the trail. Verify once deployed that these events appear in the trail (S-06).

### Alarms

| Alarm                         | Fires when                                                           | Source                                                                                    |
| ----------------------------- | -------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `AgencyDeniedRequestsAlarm`   | 20+ (`-c agencyDeniedAlarmThreshold`) 401/403 responses in 5 minutes | metric filter on the agency API access log, so authorizer and function denials both count |
| `AgencyAuditWriteFailedAlarm` | any audit write failure in a minute                                  | metric filter on the function log (`agency_audit_write_failed`)                           |

Both notify the existing alerts topic (`AlarmActionsAspect`).

## Cost

| Item                                       | Estimate (us-west-2 list prices as we understand them; verify)                                                                                                                                                                                |
| ------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Cognito Plus                               | about $0.02 per monthly active user, no free tier on Plus: 50 users is about **$1/month**. Essentials (about $0.015/MAU after a 10,000 MAU free tier, so $0 at this size) keeps pre-token V2 and password history but loses threat protection |
| S3 audit records                           | about $1.50/month PUTs at 300,000 records, storage cents                                                                                                                                                                                      |
| HTTP API                                   | $1.00 per million requests: cents                                                                                                                                                                                                             |
| Lambda                                     | cents                                                                                                                                                                                                                                         |
| CloudWatch Logs (access log, function log) | cents at this volume                                                                                                                                                                                                                          |
| KMS                                        | no new key (the logs key)                                                                                                                                                                                                                     |

## Decisions

For the agency-side owner, the State and counsel. Each is a default, not a decision.

| #    | Decision                                                                                                                                                                                                                                                | Default here                                                | Who                              |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- | -------------------------------- |
| D-1  | Privacy floor for check-in aggregates: threshold, complementary suppression, rounding or noise, delay                                                                                                                                                   | Threshold 10, primary + complementary suppression, no noise | Team, counsel (PRIV-3, PRIV-4)   |
| D-2  | Role matrix: is `relay-admin` kept out of check-in data? Does `agency-operator` differ from viewer before the statewide phase? Is there a statewide role or wildcard?                                                                                   | Admin: no data; operator = viewer; no wildcard              | Owner, State                     |
| D-3  | Session lifetimes and revocation: 15 min access token, 8 h refresh. A disabled user's access token works for up to 15 min at API Gateway. Shorter (5 min minimum), or a per-request user lookup (Lambda authorizer or AdminGetUser with a short cache)? | 15 min / 8 h, no per-request lookup                         | Owner, State ISO (AC-12, AC-2)   |
| D-4  | Cognito vs a state/county IdP (SAML/OIDC), and where MFA and lockout are enforced                                                                                                                                                                       | Cognito with required TOTP; federation hook documented      | State (Q-14)                     |
| D-5  | Threat protection actions (block high-risk sign-ins, compromised credentials) and the Plus plan cost                                                                                                                                                    | Plus, enforced on prod, Cognito default risk actions        | Owner                            |
| D-6  | Audit store: governance vs compliance mode, log-archive account, retention period                                                                                                                                                                       | Governance, workload account, 365 days prod                 | State ISO (AU-9(2), AU-11, Q-21) |
| D-7  | Fail closed on audit failure (agency reads stop if S3 is unavailable)                                                                                                                                                                                   | Fail closed                                                 | Owner, State                     |
| D-8  | Cell-to-county rule (cell center) and boundary source (NWS county zones) vs Census TIGER or a precomputed table                                                                                                                                         | Cell center, NWS county zones                               | Owner                            |
| D-9  | Cross-county requests: refuse the whole request vs silently trim to the jurisdiction                                                                                                                                                                    | Refuse and audit                                            | Owner                            |
| D-10 | Contract: replacing `/agency/alerts` and `/agency/alerts/{alertId}/response` with `/agency/me` and `/agency/alerts/{alertId}/summary`                                                                                                                   | Replaced                                                    | All three teammates              |
| D-11 | Agency API custom origin domain (disables its execute-api endpoint, like `-c apiOriginDomainName` for the public API)                                                                                                                                   | Not built; prod synth warns                                 | Owner                            |
| D-12 | Email for invitations and recovery: Cognito's default sender (low daily limit) vs SES with a state domain                                                                                                                                               | Cognito default                                             | Owner                            |
| D-13 | Break-glass role for the audit bucket: who holds it, how it is approved and reviewed                                                                                                                                                                    | Name reserved, role not created                             | State ISO (S-06)                 |

## Known gaps

- No front end, so no session inactivity timeout, sign-out UI, system-use notice (AC-8) or TOTP enrollment screens yet.
- The agency API keeps its default `execute-api` endpoint (D-11): a caller can skip CloudFront and the WAF, though the JWT authorizer and (on prod) the origin-verify header still refuse them.
- No AWS accounts (S-06), so none of this has run; the Cognito lockout numbers and CloudTrail coverage of Cognito events are from AWS documentation, not observed.
- Differencing over time is not mitigated (D-1).
- `X-Forwarded-For` is recorded raw; the viewer address is its last entry only when the request came through CloudFront.
