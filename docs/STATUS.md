# Relay engine status

Last updated: 2026-11-09 (overnight build session). Branch: `s-05-restructure-monorepo`.

Everything below is built and tested **offline**. Nothing has been deployed to AWS yet, because the AWS accounts (S-06) don't exist yet.

Checks: `npm run lint && npm run typecheck && npm test` gives 457 tests passing. `cd infra && npx cdk synth -c stage=dev` (and `stage=prod`) succeeds and produces 5 stacks.

The engine went through three independent safety reviews. Every confirmed finding was fixed and has a test.

## Tickets

| ID    | Ticket                          | Status                                            | Where                                                                              |
| ----- | ------------------------------- | ------------------------------------------------- | ---------------------------------------------------------------------------------- |
| S-02  | Alert format                    | Done, needs all-three approval                    | `packages/shared/src/alert.ts`                                                     |
| S-03  | Check-in format                 | Done, needs all-three approval                    | `packages/shared/src/checkin.ts`                                                   |
| S-04  | API contract (OpenAPI)          | Done, needs all-three approval                    | `packages/shared/openapi/relay.yaml`                                               |
| S-05  | Monorepo and CI                 | Done                                              | root config, `.github/workflows/ci.yml`                                            |
| S-07  | Database                        | Done in code, not deployed                        | `infra/lib/data-stack.ts`, `backend/migrations/`, `backend/src/adapters/postgres/` |
| S-09  | Fake data generators            | Done                                              | `backend/src/dev/`, `backend/dev-data/`                                            |
| AE-01 | Poll the NWS feed               | Done                                              | `backend/src/ingest/poll.ts`, `nws-client.ts`                                      |
| AE-02 | Archive raw responses           | Done                                              | `backend/src/adapters/aws/s3-raw-archive.ts`                                       |
| AE-03 | Validate incoming alerts        | Done                                              | `backend/src/ingest/normalize.ts`                                                  |
| AE-04 | Updates, cancels, expirations   | Done                                              | `backend/src/ingest/lifecycle.ts`                                                  |
| AE-05 | Alert area to H3 cells          | Done; PostGIS cross-check still open              | `backend/src/geo/`                                                                 |
| AE-06 | Match alerts to saved places    | Done (100k places in about 1.7 s)                 | `backend/src/delivery/plan-delivery.ts`                                            |
| AE-07 | Push through Expo               | Done                                              | `backend/src/delivery/`, `backend/src/adapters/expo/`                              |
| AE-08 | Signed check-in tokens          | Done                                              | `backend/src/tokens/`                                                              |
| AE-09 | Sign alerts inside Relay        | Done (KMS ECDSA P-256; verifiers public key only) | `backend/src/signing/`, `backend/src/adapters/signing-alert-store.ts`              |
| AE-10 | Device and push-token endpoints | Done                                              | `backend/src/api/`                                                                 |
| AE-11 | Saved places endpoints          | Done                                              | `backend/src/api/`                                                                 |
| AE-12 | Alert endpoints for the app     | Done                                              | `backend/src/api/`                                                                 |
| AE-13 | EN/ES plain-language summaries  | Done; Spanish should get a native-speaker read    | `backend/src/summaries/`                                                           |
| AG-09 | Check-in endpoint (agency area) | Done; one check-in per device per alert           | `backend/src/api/`, `backend/src/adapters/aws/dynamo-checkin-store.ts`             |
| AG-10 | Kill switch (agency area)       | Engine side done                                  | SSM `/relay/<stage>/delivery-kill-switch`                                          |

## Compliance foundation

Added 2026-11-10 on branch `compliance-foundation`, reviewed 2026-11-11 ([review](./compliance/review-2026-11.md)). Drafts for the agency ISO, not a compliance claim; nothing is deployed.

- **NIST 800-53 Rev. 5 moderate gap map:** [control-map.md](./compliance/control-map.md) and its [summary](./compliance/control-map-summary.md): 14 Built, 73 Partial, 46 Missing engineering rows; `node docs/compliance/check-evidence.mjs` checks every citation.
- **cdk-nag NIST 800-53 R5** on every stack. An unsuppressed finding fails `cdk synth` and `npm test`; dev and prod have none. Every exception is per resource: [cdk-nag-exceptions.md](./compliance/cdk-nag-exceptions.md).
- **Customer-managed KMS keys** (data, messaging, logs; edge in us-east-1), yearly rotation, on every table, cluster, data bucket, secret, queue, topic, log group and the trail; key-state changes alert the alerts topic. TLS 1.2+ on S3, SQS and CloudFront-to-API; viewer TLS 1.2+ needs `-c edgeDomainName -c edgeCertificateArn`. FIPS endpoints on prod (`-c fips`), except the API Lambda (Amazon Location).
- **Logs added:** WAF, S3 access, VPC flow, Postgres. Buckets versioned.
- **CI scanning, warn-only:** npm audit, CycloneDX SBOM, gitleaks, CodeQL (`.github/workflows/security.yml`, `codeql.yml`; [ci-security-scanning.md](./compliance/ci-security-scanning.md)).
- **Runbooks:** [key rotation](./runbooks/key-rotation.md), [push credential rotation](./runbooks/push-credential-rotation.md).
- **Open:** ISO acceptance of the exceptions, an edge domain and certificate, AWS Config / Security Hub after S-06, and the owner items in the [review](./compliance/review-2026-11.md#left-for-the-owner).

## How an alert flows

1. Every minute, EventBridge triggers the ingest Lambda, which polls NWS twice per run, 30 seconds apart (the NWS minimum; the poller keeps its own spacing, honors `Retry-After` and backs off on 429/503). Features of one response are processed concurrently, but one alert chain stays in order.
2. The raw response is archived to S3.
3. Each feature is normalized and validated. Only `Actual` alerts are kept.
4. The alert area becomes H3 cells (resolution 7).
5. The lifecycle step merges updates and cancels.
6. The alert gets EN/ES summaries, is signed, and is saved to Aurora.
7. Delivery is planned: matched devices get jobs on SQS. Statements are in-app only, with no push.
8. The delivery worker re-checks everything (signed, Actual, active, not ended, kill switch). It then takes a 45-second lease on `alertId:severity:deviceId`, sends through Expo (8-second timeout), and marks the key sent. Retryable failures release the lease and SQS retries after 30 seconds.
9. An alert is marked "seen" only after planning succeeds. A failure, an outage, or a paused kill switch leaves it unseen, so the next poll retries it.
10. The phone's "I'm okay" calls `POST /alerts/{id}/checkins` with the signed token. Each device counts once per alert in the per-H3-cell totals.

Push policy: emergencies and warnings are loud and high-priority, watches and advisories are quiet, and statements get no push (`backend/src/delivery/policy.ts`).

## Before the first deploy (in order)

1. **S-06:** create the dev/prod AWS accounts and the GitHub OIDC deploy role.
2. `cd infra && npx cdk deploy --all -c stage=dev`.
3. `npm run seed-keys -- dev` (in `infra/`) to replace the placeholder token and device-credential keys (alert signing uses a KMS key, nothing to seed).
4. Run the migrations against Aurora (see `infra/README.md`).
5. Put the Expo access token in its secret, or leave it empty.
6. Capture real NWS responses from the S3 archive and replace the constructed fixtures in `backend/test/fixtures/nws/`.

## Known gaps and decisions

- **NWS fixtures are constructed**, not recorded, because api.weather.gov was unreachable from the build environment.
- **Cancellations aren't pushed yet.** A cancelled alert stops further pushes and shows as cancelled in the app.
- **NWS Updates re-notify only on a severity increase** or for newly covered devices.
- **A Lambda killed after Expo accepted a push but before it was marked sent** re-sends that one push once, after the 45-second lease. This is an accepted trade-off: a rare duplicate rather than a missed alert.
- **Push-token reassignment:** a device that knows another phone's Expo token could take it over. Tokens are never logged or returned, so the risk is low, but it should be revisited before the Pilot.
- **The OpenAPI spec changed tonight** (check-in 403 responses, and the once-per-device rule). It needs all-three approval like the other shared formats.
- **The California boundary** used for coordinate checks is a rough outline (demo only).
- **SAME codes** for Red Flag, Fire Weather and heat products don't exist. Those summaries match on event name.
- **Data API instead of a VPC** for Lambdas: this avoids about $32/month of NAT Gateway cost.
