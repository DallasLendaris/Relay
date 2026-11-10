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
3. Each **shard worker** pages the cell index for its cells. Per cell, it splits devices into `CELL_BUCKETS` stable buckets (one `SendBatch` per bucket, so batch keys never shift), creates the check-in tokens (v2, with h3Cell bound in), and enqueues `SendBatch`es on the `urgent` or `standard` queue.
   - **Dense cells.** A metro res-7 cell can hold 75,000-150,000 devices, which would make a (cell, bucket) batch about 0.5 MB, over the 256 KB SQS message limit. A cell with more than `DENSE_CELL_RECIPIENTS` (20,000) recipients, or with any bucket over `BATCH_BYTE_BUDGET` (200 KB of JSON, tokens included), is sent in 16 fixed parts per bucket by the first hex digit of the device id: key `<alert>:<severity>:<cell>:<bucket>.<part>/16`. Cells below that keep their old keys. The cell index returns a cell in device-id order, so the worker streams a dense cell part by part (memory is about one part, not the whole cell; the 1.9M-device shard that OOM-killed a 730 MB worker held every cell whole). That relies on ids being spread over their first hex digit, which server-issued random v4 UUIDs are. Ids that are not (the scale suite's sequential UUIDs, `dev-<n>`) put a whole cell in one part; then, when the open part holds more than 20,000 recipients, the worker sets whole buckets aside and re-reads the part for them in later passes. Part keys stay the same, memory stays at about 20,000 recipients, and each pass is one more read of the part (`rereadPasses` in the shard log, `fanout.dense_part_reread` warning).
   - **Exactly-once across a tier change.** Before a dense cell's parts go out, the shard worker claims each bucket's normal key in the sender's idempotency table, marks `<key>:dense`, then marks the normal key sent (a tombstone). A normal batch from an earlier plan that is still queued is then a duplicate to the sender; one already sent makes the bucket "covered" and its parts are not sent; a later normal re-plan's keys are tombstoned. Before any bucket tombstone, a cell marker `<alert>:<severity>:<cell>:dense` is written, and every later plan of the same alert version reads it (one read per cell) and keeps the cell dense even if it has shrunk: a dense attempt that dies after tombstoning but before enqueuing its parts is always finished by the next run of that cell, never replaced by normal batches the sender would drop. A bucket whose normal batch is mid-send parks just those cells on the paused queue (about 60 s) instead of failing the job. Dense parts depend only on (bucket, first character), so a dense re-plan repeats its keys. A crash between marks can only cause a duplicate, never a miss. Late join treats a tombstoned bucket key as a pointer to the device's part key and asks whether that was sent.
   - **Size guards.** Messages are packed by measured bytes (at most 10 batches and 240 KB). A batch that still does not fit (about 800+ recipients in one part, or the sender re-queueing a big retry) is halved into `<key>:s<path>` pieces; a body over 256 KB is refused with a clear error rather than sent to fail at SQS.
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
