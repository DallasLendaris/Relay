# Local fan-out simulation: 1,000,000 devices

One alert, 1,000,000 seeded devices, the real planner, shard workers, `processBatch`, `ApnsSender` (h2c) and `FcmSender` against the mock APNs and FCM (default error mix, FCM quota 10,000/s). All figures below were measured by this run; none are estimates.

## How it was run

```sh
cd loadtest && node --max-old-space-size=3120 --import tsx src/sim/run-fanout.ts --devices 1000000 --senders 2 --out results/fanout-1m-local-2senders.md
```

Machine: 2 CPUs (Intel(R) Xeon(R) Processor @ 2.30GHz), 8,023 MB RAM (4,306 MB free at start), Node v22.23.4, linux 6.18.44-fc-v310, V8 heap limit 3,120 MB. The mock servers, the senders, the shard workers and the check-in code all share one Node process (one JS thread), so the mocks and the senders compete for CPU. This is a local measurement, not an AWS prediction.

## Result

| Measure | Value |
| --- | --- |
| Devices seeded / entries / cells used | 1,000,000 / 1,149,982 / 1,130 |
| Alert cells (shards) | 1,136 (8 shards) |
| Devices in the alert area / with a push target | 1,000,000 / 1,000,000 |
| Batches planned (recipients) | 70,042 (1,148,582) |
| Time to plan (`planShards` returns) | 4 ms |
| Time to first successful push | 4.9 s |
| Time to last successful push | 212.7 s |
| Sends per second, mean (first to last push) | 5,500 |
| Sends per second, best 5 s window | 7,398 |
| Push latency p50 / p99 / max (per push, includes in-sender queueing) | 299.0 ms / 1851.4 ms / 6074.7 ms over 1,160,864 attempts |
| Batches processed (of which retries) | 74,442 (4,400) |
| Recipients retried after a retryable failure | 12,282 |
| Permanent failures (dead token, cleared) | 5,630 |
| Gave up (still failing after 5 attempts) | 0 |
| Message redeliveries (batch not acked) | 0 |
| FCM requests rate-limited by the quota bucket (429) | 0 |
| Peak heap / peak RSS | 1996.1 MB / 2731.3 MB |

## Reading these numbers

- The run is CPU-bound on one JS thread (load average at start: 1.42 on 2 CPUs). The mock servers do about as much work per push as the senders, so 5,500 pushes per second is what this machine did, not what the design allows.
- The FCM quota (10,000/s) never bound: FCM accepted 513,616 pushes at about 2,472/s on average, and the quota bucket refused 0 requests. The 429s in the error table are the injected ones from the error mix.
- Push latency is measured around each push, so it includes waiting for an in-flight slot and for the event loop; the mock's own latency model is p50 20 ms and p99 150 ms.
- The senders' request timeout is 10 s. No push timed out in this run, so no retry followed an accepted-but-unanswered push.

## Exactly once

```
exactly-once (two-cells excluded): PASS (expected 1000000, once 896473, excused 4442, missing 0, known two-cells 99085, other duplicates 0 (after a lost response 0, unexplained 0), unexpected 0)
raw checkExactlyOnce: exactly-once: FAIL (expected 1000000, once 896473, excused 4442, missing 0, duplicated 99085, unexpected 0)
```

- Expected devices (distinct push targets in the area): 1,000,000; delivered exactly once: 896,473; excused as permanent failures (mock answered unregistered): 4,442; missing: 0.
- Known two-cells duplicates (a device with places in two alert cells gets one push per cell, documented gap in `shard-worker.ts`): 99,085 of 99,579 such devices.
- Any other duplicate: 0, of which 0 followed a timeout or connection error on the first push (the mock had accepted it, the sender never saw the answer and retried) and 0 are unexplained. Unexpected tokens: 0.
- Devices the mock accepted but whose success the sender never saw: 0 (they get no check-in token in the burst below).
- Verdict: **PASS** (known two-cells duplicates are not counted as failures; any other duplicate is).

## Re-plan of the same alert

Planner and shard workers ran again on the same signed alert: 8 shards in 3 ms, 70,042 batches re-enqueued, outcomes {"duplicate":70042}. New push requests that reached the mocks: **0**; new accepted pushes: **0**; new messages given to the push senders: 0.

## Sends per second, per 5 s window (successful pushes)

| Window start | Successful pushes | per second | Retryable failures | Permanent failures |
| ---: | ---: | ---: | ---: | ---: |
| 0 s | 760 | 152 | 2 | 7 |
| 5 s | 23,327 | 4,665 | 174 | 98 |
| 10 s | 27,708 | 5,542 | 267 | 163 |
| 15 s | 29,178 | 5,836 | 355 | 136 |
| 20 s | 33,478 | 6,696 | 185 | 121 |
| 25 s | 19,233 | 3,847 | 297 | 75 |
| 30 s | 25,400 | 5,080 | 263 | 113 |
| 35 s | 20,911 | 4,182 | 124 | 108 |
| 40 s | 31,943 | 6,389 | 242 | 164 |
| 45 s | 24,715 | 4,943 | 263 | 116 |
| 50 s | 25,646 | 5,129 | 299 | 122 |
| 55 s | 22,529 | 4,506 | 328 | 112 |
| 60 s | 24,559 | 4,912 | 301 | 132 |
| 65 s | 32,587 | 6,517 | 242 | 158 |
| 70 s | 23,758 | 4,752 | 289 | 125 |
| 75 s | 36,135 | 7,227 | 386 | 182 |
| 80 s | 26,951 | 5,390 | 106 | 144 |
| 85 s | 30,636 | 6,127 | 377 | 167 |
| 90 s | 35,785 | 7,157 | 296 | 180 |
| 95 s | 24,227 | 4,845 | 356 | 125 |
| 100 s | 28,385 | 5,677 | 246 | 150 |
| 105 s | 34,817 | 6,963 | 421 | 195 |
| 110 s | 27,987 | 5,597 | 410 | 141 |
| 115 s | 22,095 | 4,419 | 239 | 120 |
| 120 s | 36,992 | 7,398 | 307 | 190 |
| 125 s | 22,961 | 4,592 | 177 | 135 |
| 130 s | 26,029 | 5,206 | 490 | 132 |
| 135 s | 32,862 | 6,572 | 253 | 163 |
| 140 s | 21,280 | 4,256 | 164 | 107 |
| 145 s | 30,863 | 6,173 | 485 | 160 |
| 150 s | 23,024 | 4,605 | 213 | 115 |
| 155 s | 28,415 | 5,683 | 325 | 124 |
| 160 s | 16,808 | 3,362 | 147 | 66 |
| 165 s | 27,498 | 5,500 | 251 | 136 |
| 170 s | 26,390 | 5,278 | 237 | 114 |
| 175 s | 29,718 | 5,944 | 422 | 157 |
| 180 s | 31,776 | 6,355 | 501 | 156 |
| 185 s | 23,308 | 4,662 | 455 | 108 |
| 190 s | 28,184 | 5,637 | 231 | 113 |
| 195 s | 34,991 | 6,998 | 326 | 152 |
| 200 s | 26,059 | 5,212 | 299 | 146 |
| 205 s | 32,816 | 6,563 | 435 | 152 |
| 210 s | 10,228 | 2,046 | 96 | 50 |

## Check-in burst

One v2 check-in per delivered device (995,558), tokens taken from this run's sends, through the API's stateless `POST /alerts/{id}/checkins` route with an in-memory check-in store (the device, place, alert and cell-index stores are traps that fail the run if touched), then every stored check-in as a stream INSERT through the real aggregator in batches of 1,000 against an in-memory totals table. Single JS thread, no network, so this is a code-path throughput, not an API Gateway figure.

| Measure | Value |
| --- | --- |
| API check-ins | 995,558 in 19.6 s = **50,825/s** (200 OK: 995,558, errors: 0) |
| API handler time p50 / p99 (200 in flight) | 3 ms / 12.69 ms |
| Aggregator | 996 batches of 1,000 in 1.7 s = **594,862/s** (failed items: 0) |
| Counter rows / marker rows written | 1,130 / 995,558 |
| Per-cell totals sum | 995,558 |
| Expected (devices in area - permanent failures - delivered without an ack) | 995,558 = 1,000,000 - 4,442 - 0 |
| Check-in store total | 995,558 |
| Per-cell mismatches (1,130 cells) | 0 |
| Verdict | **PASS** |

## Results JSON

```json
{
  "machine": {
    "nproc": 2,
    "cpuModel": "Intel(R) Xeon(R) Processor @ 2.30GHz",
    "memoryTotalMb": 8023,
    "loadAverage1mAtStart": 1.42,
    "memoryFreeAtStartMb": 4306,
    "node": "v22.23.4",
    "platform": "linux 6.18.44-fc-v310"
  },
  "heapLimitMb": 3120,
  "command": "cd loadtest && node --max-old-space-size=3120 --import tsx src/sim/run-fanout.ts --devices 1000000 --senders 2 --out results/fanout-1m-local-2senders.md",
  "fanout": {
    "options": {
      "devices": 1000000,
      "seed": 1,
      "fcmRate": 10000,
      "senders": 2,
      "apnsSessions": 4,
      "messagesPerSender": 12,
      "shardConcurrency": 32
    },
    "seedMs": 6065,
    "heapAfterSeedMb": 630.1,
    "peakHeapMb": 1996.1,
    "peakRssMb": 2731.3,
    "counts": {
      "devices": 1000000,
      "entries": 1149982,
      "cellsUsed": 1130,
      "alertCells": 1136,
      "devicesInArea": 1000000,
      "devicesWithTarget": 1000000,
      "devicesInTwoOrMoreCells": 99579,
      "shards": 8,
      "plannedBatches": 70042,
      "plannedRecipients": 1148582,
      "skippedNoTarget": 0,
      "sameCellDuplicates": 1400
    },
    "timing": {
      "planMs": 4,
      "firstPushMs": 4859,
      "lastPushMs": 212670,
      "wallMs": 212671
    },
    "throughput": {
      "meanOkPerSec": 5500,
      "peakWindowOkPerSec": 7398,
      "windows": [
        {
          "startSec": 0,
          "ok": 760,
          "okPerSec": 152,
          "retryable": 2,
          "permanent": 7
        },
        {
          "startSec": 5,
          "ok": 23327,
          "okPerSec": 4665,
          "retryable": 174,
          "permanent": 98
        },
        {
          "startSec": 10,
          "ok": 27708,
          "okPerSec": 5542,
          "retryable": 267,
          "permanent": 163
        },
        {
          "startSec": 15,
          "ok": 29178,
          "okPerSec": 5836,
          "retryable": 355,
          "permanent": 136
        },
        {
          "startSec": 20,
          "ok": 33478,
          "okPerSec": 6696,
          "retryable": 185,
          "permanent": 121
        },
        {
          "startSec": 25,
          "ok": 19233,
          "okPerSec": 3847,
          "retryable": 297,
          "permanent": 75
        },
        {
          "startSec": 30,
          "ok": 25400,
          "okPerSec": 5080,
          "retryable": 263,
          "permanent": 113
        },
        {
          "startSec": 35,
          "ok": 20911,
          "okPerSec": 4182,
          "retryable": 124,
          "permanent": 108
        },
        {
          "startSec": 40,
          "ok": 31943,
          "okPerSec": 6389,
          "retryable": 242,
          "permanent": 164
        },
        {
          "startSec": 45,
          "ok": 24715,
          "okPerSec": 4943,
          "retryable": 263,
          "permanent": 116
        },
        {
          "startSec": 50,
          "ok": 25646,
          "okPerSec": 5129,
          "retryable": 299,
          "permanent": 122
        },
        {
          "startSec": 55,
          "ok": 22529,
          "okPerSec": 4506,
          "retryable": 328,
          "permanent": 112
        },
        {
          "startSec": 60,
          "ok": 24559,
          "okPerSec": 4912,
          "retryable": 301,
          "permanent": 132
        },
        {
          "startSec": 65,
          "ok": 32587,
          "okPerSec": 6517,
          "retryable": 242,
          "permanent": 158
        },
        {
          "startSec": 70,
          "ok": 23758,
          "okPerSec": 4752,
          "retryable": 289,
          "permanent": 125
        },
        {
          "startSec": 75,
          "ok": 36135,
          "okPerSec": 7227,
          "retryable": 386,
          "permanent": 182
        },
        {
          "startSec": 80,
          "ok": 26951,
          "okPerSec": 5390,
          "retryable": 106,
          "permanent": 144
        },
        {
          "startSec": 85,
          "ok": 30636,
          "okPerSec": 6127,
          "retryable": 377,
          "permanent": 167
        },
        {
          "startSec": 90,
          "ok": 35785,
          "okPerSec": 7157,
          "retryable": 296,
          "permanent": 180
        },
        {
          "startSec": 95,
          "ok": 24227,
          "okPerSec": 4845,
          "retryable": 356,
          "permanent": 125
        },
        {
          "startSec": 100,
          "ok": 28385,
          "okPerSec": 5677,
          "retryable": 246,
          "permanent": 150
        },
        {
          "startSec": 105,
          "ok": 34817,
          "okPerSec": 6963,
          "retryable": 421,
          "permanent": 195
        },
        {
          "startSec": 110,
          "ok": 27987,
          "okPerSec": 5597,
          "retryable": 410,
          "permanent": 141
        },
        {
          "startSec": 115,
          "ok": 22095,
          "okPerSec": 4419,
          "retryable": 239,
          "permanent": 120
        },
        {
          "startSec": 120,
          "ok": 36992,
          "okPerSec": 7398,
          "retryable": 307,
          "permanent": 190
        },
        {
          "startSec": 125,
          "ok": 22961,
          "okPerSec": 4592,
          "retryable": 177,
          "permanent": 135
        },
        {
          "startSec": 130,
          "ok": 26029,
          "okPerSec": 5206,
          "retryable": 490,
          "permanent": 132
        },
        {
          "startSec": 135,
          "ok": 32862,
          "okPerSec": 6572,
          "retryable": 253,
          "permanent": 163
        },
        {
          "startSec": 140,
          "ok": 21280,
          "okPerSec": 4256,
          "retryable": 164,
          "permanent": 107
        },
        {
          "startSec": 145,
          "ok": 30863,
          "okPerSec": 6173,
          "retryable": 485,
          "permanent": 160
        },
        {
          "startSec": 150,
          "ok": 23024,
          "okPerSec": 4605,
          "retryable": 213,
          "permanent": 115
        },
        {
          "startSec": 155,
          "ok": 28415,
          "okPerSec": 5683,
          "retryable": 325,
          "permanent": 124
        },
        {
          "startSec": 160,
          "ok": 16808,
          "okPerSec": 3362,
          "retryable": 147,
          "permanent": 66
        },
        {
          "startSec": 165,
          "ok": 27498,
          "okPerSec": 5500,
          "retryable": 251,
          "permanent": 136
        },
        {
          "startSec": 170,
          "ok": 26390,
          "okPerSec": 5278,
          "retryable": 237,
          "permanent": 114
        },
        {
          "startSec": 175,
          "ok": 29718,
          "okPerSec": 5944,
          "retryable": 422,
          "permanent": 157
        },
        {
          "startSec": 180,
          "ok": 31776,
          "okPerSec": 6355,
          "retryable": 501,
          "permanent": 156
        },
        {
          "startSec": 185,
          "ok": 23308,
          "okPerSec": 4662,
          "retryable": 455,
          "permanent": 108
        },
        {
          "startSec": 190,
          "ok": 28184,
          "okPerSec": 5637,
          "retryable": 231,
          "permanent": 113
        },
        {
          "startSec": 195,
          "ok": 34991,
          "okPerSec": 6998,
          "retryable": 326,
          "permanent": 152
        },
        {
          "startSec": 200,
          "ok": 26059,
          "okPerSec": 5212,
          "retryable": 299,
          "permanent": 146
        },
        {
          "startSec": 205,
          "ok": 32816,
          "okPerSec": 6563,
          "retryable": 435,
          "permanent": 152
        },
        {
          "startSec": 210,
          "ok": 10228,
          "okPerSec": 2046,
          "retryable": 96,
          "permanent": 50
        }
      ]
    },
    "pushLatency": {
      "count": 1160864,
      "minMs": 1.0852380000033008,
      "meanMs": 386.1635825299975,
      "p50Ms": 299.0193370000052,
      "p90Ms": 716.0024709999998,
      "p95Ms": 963.0310939999908,
      "p99Ms": 1851.419984,
      "maxMs": 6074.716053
    },
    "pushErrors": {
      "Apns410:Unregistered": 3107,
      "Fcm404:UNREGISTERED": 2523,
      "Apns429:TooManyRequests": 1277,
      "Apns500:InternalServerError": 629,
      "Fcm500:INTERNAL": 512,
      "Fcm429:QUOTA_EXCEEDED": 1029,
      "ApnsConnectionError: ERR_HTTP2_STREAM_ERROR": 8835
    },
    "pass1": {
      "messagesProcessed": 12883,
      "batchesProcessed": 74442,
      "retryBatchesProcessed": 4400,
      "byReason": {
        "sent": 74442
      },
      "sent": 1142952,
      "permanentFailed": 5630,
      "retried": 12282,
      "gaveUp": 0,
      "redeliveries": 0
    },
    "deliveries": {
      "totalOkPushes": 1142952,
      "apnsAccepted": 629336,
      "fcmAccepted": 513616,
      "apnsUnregistered": 3104,
      "fcmUnregistered": 2523,
      "apnsRequests": 634349,
      "fcmRequests": 517680,
      "fcmRateLimited": 0,
      "injected429or500": 3447
    },
    "exactlyOnce": {
      "ok": true,
      "expectedDevices": 1000000,
      "deliveredOnce": 896473,
      "excusedPermanent": 4442,
      "missing": 0,
      "knownTwoCellDuplicates": 99085,
      "unexpectedDuplicates": 0,
      "duplicatesAfterLostResponse": 0,
      "duplicatesUnexplained": 0,
      "deliveredWithoutAck": 0,
      "unexpectedTokens": 0,
      "summary": "exactly-once (two-cells excluded): PASS (expected 1000000, once 896473, excused 4442, missing 0, known two-cells 99085, other duplicates 0 (after a lost response 0, unexplained 0), unexpected 0)\nraw checkExactlyOnce: exactly-once: FAIL (expected 1000000, once 896473, excused 4442, missing 0, duplicated 99085, unexpected 0)"
    },
    "replan": {
      "planMs": 2.685682999988785,
      "shards": 8,
      "batchesProcessed": 70042,
      "byReason": {
        "duplicate": 70042
      },
      "newPushRequests": 0,
      "newAcceptedPushes": 0,
      "newPushAttemptsInSender": 0
    },
    "permanentFailureDevices": 4442,
    "deliveredWithoutAck": 0
  },
  "checkinBurst": {
    "checkIns": 995558,
    "api": {
      "ms": 19588,
      "perSec": 50825,
      "ok200": 995558,
      "errors": 0,
      "p50Ms": 3,
      "p99Ms": 12.69
    },
    "aggregator": {
      "batches": 996,
      "batchSize": 1000,
      "ms": 1674,
      "perSec": 594862,
      "countersWritten": 1130,
      "markers": 995558,
      "failedItems": 0
    },
    "verification": {
      "ok": true,
      "totalsSum": 995558,
      "expectedSum": 995558,
      "storeSum": 995558,
      "perCellMismatches": 0,
      "cells": 1130,
      "expectedFormula": "devices in area - permanent failures - delivered without an ack"
    }
  }
}
```
