# Statewide scale: status

Branch `scale-statewide`, stacked on PR #5. Plan: `docs/scale-plan.md`. Quotas: `docs/scale-quotas.md`. Measured results: `loadtest/results/`.

Everything here is built and tested **offline**. Nothing is deployed (the AWS accounts, S-06, don't exist yet). The test suites pass: about 1,070 tests across backend, infra, shared and loadtest. `cdk synth` passes for dev and prod.

## What was built

| Piece              | Where                                                            | What it does                                                                                                                                                                                                                                                                                                                        |
| ------------------ | ---------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Cell index         | `backend/src/scale/cell-index/`                                  | A DynamoDB lookup of devices by H3 cell, written through from the places API, with a repair outbox and a backfill script. Each entry carries the device's cells, so a device is pushed from exactly one cell per alert.                                                                                                             |
| Parallel fan-out   | `backend/src/scale/fanout/`                                      | Ingest publishes `AlertReady`. The planner splits the area into 16-cell shards and invokes shard Lambdas asynchronously. Shards build deterministic (cell, bucket) batches, can resume when near their time limit, and pre-scale the sender fleet for urgent alerts.                                                                |
| Direct push        | `backend/src/adapters/push/`                                     | APNs over pooled HTTP/2 with ES256 provider tokens, and FCM HTTP v1 with cached OAuth. Expo is for development only.                                                                                                                                                                                                                |
| Sender service     | `backend/src/scale/sender/`, `backend/Dockerfile.sender`         | Fargate containers that drain the urgent queue first. Claims are leases that renew while work runs. Retries back off and honor Retry-After, within a time budget. A breaker on mass "dead token" results stops wrong-config wipes. Dead-token cleanup runs asynchronously. A pause stops pulling work, so nothing lands in the DLQ. |
| Check-ins at scale | `backend/src/scale/checkins/`, `backend/src/tokens/`             | v2 tokens bind the alert, device and cell, and device credentials are stateless, so check-ins need zero database reads. Writes are spread by `alertId#deviceId`. A DynamoDB Streams aggregator writes per-cell totals with no double counting.                                                                                      |
| CDN and edge       | `backend/src/scale/publish/`, `infra/lib/edge-stack.ts`          | Signed alert JSON and per-shard index files go to S3 behind CloudFront. WAF has managed rules and per-IP rate limits. An origin header blocks direct execute-api access.                                                                                                                                                            |
| Zone pack          | `backend/src/geo/s3-zone-provider.ts`                            | California NWS zone geometries are preloaded to S3, with NWS as the fallback.                                                                                                                                                                                                                                                       |
| Late joiners       | `backend/src/api/`                                               | A device that adds a place during an active alert gets it, once.                                                                                                                                                                                                                                                                    |
| Load-test harness  | `loadtest/`                                                      | Rate-limited mock APNs/FCM servers, a 1M-device Santa Clara County seed, the full-path simulation, and exactly-once reports.                                                                                                                                                                                                        |
| Infra              | `infra/lib/{fanout,sender,edge,api,ingest,data,config}-stack.ts` | Every new piece has least-privilege IAM. The env contracts are derived from handler source, and a test enforces them.                                                                                                                                                                                                               |

## Measured (local, 2 CPUs, one Node process; a floor, not an AWS prediction)

|                                                   | v1               | v2 (after fixes) |
| ------------------------------------------------- | ---------------- | ---------------- |
| Devices                                           | 1,000,000        | 1,000,000        |
| Time to last push                                 | 212.7 s          | 217.0 s          |
| Mean / best-window sends per second               | 5,500 / 7,398    | 4,636 / 6,631    |
| Missed devices                                    | 0                | 0                |
| Cross-cell duplicate pushes                       | 99,085           | **0**            |
| Re-plan new pushes                                | 0                | 0                |
| Check-ins through the API / aggregator per second | 50,825 / 594,862 | 42,826 / 473,340 |

On this machine, the mock servers and senders share one CPU thread. On AWS, an independent review estimated each sender container at about 3,000 to 5,000 APNs pushes per second, so 6 to 8 warm containers cover 1M in about 60 s. Prod keeps 10 warm, and urgent alerts pre-scale up to 100.

## Reviews

One independent review covered the scale branch. Every confirmed finding was fixed and tested. The biggest were:

- no retry backoff
- the sender fleet ramp taking 3 to 5 minutes
- pause draining queues into the DLQ
- oversized shards
- the dead-token wipe risk
- serial publishing in front of fan-out
- the WAF bypass

## Before the AWS load test (P-01)

1. Accounts and quotas: S-06, plus the requests in `docs/scale-quotas.md`. The most urgent are the FCM project quota, Fargate vCPUs, Lambda concurrency, the API Gateway RPS and DynamoDB warm throughput.
2. `cdk deploy --all -c stage=dev`, then `npm run seed-keys -- dev`, then migrations 0006 and 0007, then the cell-index backfill.
3. Store the APNs .p8 key and the FCM service account in their secrets, then deploy with `-c senderEnabled=true`.
4. Build and upload the zone pack: `backend/scripts/build-zone-pack.ts --upload`.
5. Deploy the mock APNs/FCM in a separate stack, point the sender at it, seed 1M devices into the cell index, and run the fan-out and a distributed check-in burst.

## Known gaps

- **The app must register native APNs/FCM tokens** (`getDevicePushTokenAsync`) and collapse notifications by alert id. That's app-team work.
- **Agency totals:** `DynamoCheckInTotals.listTotals` exists, but the agency route (AG-04) needs agency auth (AG-02) first.
- **Cancellation pushes** are still not sent.
- **A concurrent place add and remove** on one device can, in a narrow window, cause one extra push (never a miss).
- **Multi-region failover** and SMS fallback are not built.
