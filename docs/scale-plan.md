# Relay statewide scale plan

Goal: one alert reaches **1,000,000 phones within 60 to 120 seconds** of NWS publishing it, and the system absorbs **1,000,000 check-ins within about 5 minutes** (peaks of 20,000 to 50,000 requests per second), with **zero lost alerts**. The design must hold for one county (about 500,000 devices) and for the whole state (plan for 10,000,000).

Shared shapes are in `backend/src/scale/contracts.ts`.

## Where the demo design breaks, and the fix

| Step       | Demo design                          | Limit                            | Scale design                                                                                                                     |
| ---------- | ------------------------------------ | -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| Send       | Expo push service                    | About 600 per second per project | Direct APNs (HTTP/2) and FCM HTTP v1 from long-lived Fargate senders                                                             |
| Plan       | One ingest Lambda plans every device | 70 s timeout, serial             | Ingest publishes `AlertReady`. The planner shards the area by H3 res-4 parent, and shard workers build `SendBatch`es in parallel |
| Match      | Aurora via the Data API              | 1 MB responses, 2 ACU            | DynamoDB cell index (pk = H3 cell), written through from the places API                                                          |
| Dedupe     | One claim per device                 | About 2M writes per alert        | One claim per deterministic batch (cell plus a stable bucket)                                                                    |
| API        | HTTP API throttled to 50 rps         |                                  | CloudFront + WAF in front, higher throttles, per-device/IP rate limits                                                           |
| Check-in   | Totals on one partition per alert    | About 1,000 writes/s             | pk = `alertId#deviceId`; a Streams aggregator writes totals per `alertId#cell`                                                   |
| Alert read | Lambda + Aurora per request          |                                  | Signed alert JSON published to S3 behind CloudFront. The headline and summary ride in the push                                   |
| Zones      | Fetched from NWS during an event     | NWS latency                      | California zone geometries preloaded to S3                                                                                       |

## Data flow at scale

1. Ingest validates, signs and saves the alert (unchanged), then publishes `AlertReady` to the plan queue instead of planning in-line. An alert counts as seen only after the publish succeeds.
2. The **planner** re-reads and re-verifies the alert, groups its cells by res-4 parent into `ShardJob`s, and enqueues them.
3. Each **shard worker** pages the cell index for its cells. Per cell, it splits devices into `CELL_BUCKETS` stable buckets, chunks each bucket to `SEND_BATCH_SIZE`, creates the check-in tokens (v2, with h3Cell bound in), and enqueues `SendBatch`es on the `urgent` or `standard` queue.
4. The **sender service** (Fargate, scaled on queue depth, urgent first):
   - claim `batchKey` (45 s lease)
   - re-check the alert's eligibility and the kill switch
   - send each recipient through APNs or FCM over pooled HTTP/2 connections
   - markSent
   - clear permanent failures from the cell index and device store
   - re-queue retryable failures as a new batch (`attempt + 1`)
5. **Phones** show the push with the headline and the "I'm okay" button. Opening the alert reads it from CloudFront.
6. **Check-in**:
   - The API verifies the device credential statelessly (HMAC-derived) and the token (binds alert, device and cell), so it needs no database reads.
   - The check-in is written conditionally to the check-ins table.
   - Streams feed the aggregator, which writes totals per alert cell for the agency map.

## Proof

- **Local simulation (CI-sized and 1M-sized):** 1,000,000 fake devices across a county-sized alert, run through the planner, shard workers and senders, against mock APNs/FCM servers that enforce realistic limits. It measures time-to-last-push and throughput, and verifies exactly-once per device.
- **AWS load test (P-01), once accounts exist:** the same flow, deployed, with the mocks hosted in a separate stack. A distributed k6 run provides the check-in burst.
- **Quotas (P-02):** every AWS, Apple and Google limit to raise, with requested values (`docs/scale-quotas.md`).
