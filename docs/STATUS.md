# Relay engine status

Last updated: 2026-11-09 (overnight build session). Branch: `s-05-restructure-monorepo`.

Everything below is built and tested **offline**. Nothing has been deployed to AWS yet, because the AWS accounts (S-06) don't exist yet.

Checks: `npm run lint && npm run typecheck && npm test` gives 355 tests passing. `cd infra && npx cdk synth -c stage=dev` (and `stage=prod`) succeeds and produces 5 stacks.

## Tickets

| ID    | Ticket                          | Status                                         | Where                                                                              |
| ----- | ------------------------------- | ---------------------------------------------- | ---------------------------------------------------------------------------------- |
| S-02  | Alert format                    | Done, needs all-three approval                 | `packages/shared/src/alert.ts`                                                     |
| S-03  | Check-in format                 | Done, needs all-three approval                 | `packages/shared/src/checkin.ts`                                                   |
| S-04  | API contract (OpenAPI)          | Done, needs all-three approval                 | `packages/shared/openapi/relay.yaml`                                               |
| S-05  | Monorepo and CI                 | Done                                           | root config, `.github/workflows/ci.yml`                                            |
| S-07  | Database                        | Done in code, not deployed                     | `infra/lib/data-stack.ts`, `backend/migrations/`, `backend/src/adapters/postgres/` |
| S-09  | Fake data generators            | Done                                           | `backend/src/dev/`, `backend/dev-data/`                                            |
| AE-01 | Poll the NWS feed               | Done                                           | `backend/src/ingest/poll.ts`, `nws-client.ts`                                      |
| AE-02 | Archive raw responses           | Done                                           | `backend/src/adapters/aws/s3-raw-archive.ts`                                       |
| AE-03 | Validate incoming alerts        | Done                                           | `backend/src/ingest/normalize.ts`                                                  |
| AE-04 | Updates, cancels, expirations   | Done                                           | `backend/src/ingest/lifecycle.ts`                                                  |
| AE-05 | Alert area to H3 cells          | Done; PostGIS cross-check still open           | `backend/src/geo/`                                                                 |
| AE-06 | Match alerts to saved places    | Done (100k places in about 1.7 s)              | `backend/src/delivery/plan-delivery.ts`                                            |
| AE-07 | Push through Expo               | Done                                           | `backend/src/delivery/`, `backend/src/adapters/expo/`                              |
| AE-08 | Signed check-in tokens          | Done                                           | `backend/src/tokens/`                                                              |
| AE-09 | Sign alerts inside Relay        | Done                                           | `backend/src/signing/`, `backend/src/adapters/signing-alert-store.ts`              |
| AE-10 | Device and push-token endpoints | Done                                           | `backend/src/api/`                                                                 |
| AE-11 | Saved places endpoints          | Done                                           | `backend/src/api/`                                                                 |
| AE-12 | Alert endpoints for the app     | Done                                           | `backend/src/api/`                                                                 |
| AE-13 | EN/ES plain-language summaries  | Done; Spanish should get a native-speaker read | `backend/src/summaries/`                                                           |
| AG-10 | Kill switch (agency area)       | Engine side done                               | SSM `/relay/<stage>/delivery-kill-switch`                                          |

## How an alert flows

1. Every minute, EventBridge triggers the ingest Lambda, which polls NWS about every 12 seconds within one run.
2. The raw response is archived to S3.
3. Each feature is normalized and validated. Only `Actual` alerts are kept.
4. The alert area becomes H3 cells (resolution 7).
5. The lifecycle step merges updates and cancels.
6. The alert gets EN/ES summaries, is signed, and is saved to Aurora.
7. Delivery is planned: matched devices get jobs on SQS.
8. The delivery worker re-checks everything (signed, Actual, active, not ended, kill switch), then claims `alertId:severity:deviceId`, sends through Expo, and releases the claim on a retryable failure.
9. An alert is marked "seen" only after planning succeeds, so a failure is retried on the next poll.

## Before the first deploy (in order)

1. **S-06:** create the dev/prod AWS accounts and the GitHub OIDC deploy role.
2. `cd infra && npx cdk deploy --all -c stage=dev`.
3. `npm run seed-keys -- dev` (in `infra/`) to replace the placeholder token and alert-signing keys.
4. Run the migrations against Aurora (see `infra/README.md`).
5. Put the Expo access token in its secret, or leave it empty.
6. Capture real NWS responses from the S3 archive and replace the constructed fixtures in `backend/test/fixtures/nws/`.

## Known gaps and decisions

- **NWS fixtures are constructed**, not recorded, because api.weather.gov was unreachable from the build environment.
- **Cancellations aren't pushed yet.** A cancelled alert stops further pushes and shows as cancelled in the app.
- **NWS Updates re-notify only on a severity increase** or for newly covered devices.
- **A Lambda killed after a claim but before the send** strands that one push. The claim lasts until the alert ends plus one day. A short in-flight claim that is extended after a successful send would close this gap.
- **The California boundary** used for coordinate checks is a rough outline (demo only).
- **SAME codes** for Red Flag, Fire Weather and heat products don't exist. Those summaries match on event name.
- **Data API instead of a VPC** for Lambdas: this avoids about $32/month of NAT Gateway cost.
