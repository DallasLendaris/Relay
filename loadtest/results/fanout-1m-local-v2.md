# Local fan-out simulation: 1,000,000 devices

One alert, 1,000,000 seeded devices, the real planner, shard workers, `processBatch`, `ApnsSender` (h2c) and `FcmSender` against the mock APNs and FCM (default error mix, FCM quota 10,000/s). All figures below were measured by this run; none are estimates.

## How it was run

```sh
cd loadtest && node --max-old-space-size=3120 --import tsx src/sim/run-fanout.ts --devices 1000000 --senders 2 --out results/fanout-1m-local-v2.md
```

Machine: 2 CPUs (Intel(R) Xeon(R) Processor @ 2.30GHz), 8,023 MB RAM (4,133 MB free at start), Node v22.23.4, linux 6.18.44-fc-v310, V8 heap limit 3,120 MB. The mock servers, the senders, the shard workers and the check-in code all share one Node process (one JS thread), so the mocks and the senders compete for CPU. This is a local measurement, not an AWS prediction.

## Result

| Measure | Value |
| --- | --- |
| Devices seeded / entries / cells used | 1,000,000 / 1,149,982 / 1,130 |
| Alert cells (shards) | 1,136 (75 shards) |
| Devices in the alert area / with a push target | 1,000,000 / 1,000,000 |
| Batches planned (recipients) | 69,431 (1,000,000) |
| Time to plan (`planShards` returns) | 8 ms |
| Time to first successful push | 2.3 s |
| Time to last successful push | 217.0 s |
| Sends per second, mean (first to last push) | 4,636 |
| Sends per second, best 5 s window | 6,631 |
| Push latency p50 / p99 / max (per push, includes in-sender queueing) | 384.3 ms / 2501.5 ms / 7076.9 ms over 1,009,355 attempts |
| Batches processed (of which retries) | 73,417 (3,986) |
| Recipients retried after a retryable failure | 9,355 |
| Permanent failures (dead token, cleared) | 4,914 |
| Gave up (still failing after 5 attempts) | 0 |
| Message redeliveries (batch not acked) | 0 |
| Retry messages delayed (longest delay) | 3,986 (4.0 s) |
| Permanent-failure breaker opened | YES (sender log events: {"delivery.permanent_breaker_open":6}; first events: [{"reason":"batch_rate","permanent":6,"total":156,"batchRate":0.0385,"rollingRate":0.01,"errors":{"Apns410:Unregistered":3,"Fcm404:UNREGISTERED":3}},{"reason":"batch_rate","permanent":5,"total":121,"batchRate":0.0413,"rollingRate":0.011,"errors":{"Apns410:Unregistered":3,"Fcm404:UNREGISTERED":2}},{"reason":"batch_rate","permanent":5,"total":238,"batchRate":0.021,"rollingRate":0.009,"errors":{"Apns410:Unregistered":3,"Fcm404:UNREGISTERED":2}},{"reason":"batch_rate","permanent":5,"total":206,"batchRate":0.0243,"rollingRate":0.007,"errors":{"Apns410:Unregistered":3,"Fcm404:UNREGISTERED":2,"Apns500:InternalServerError":1}},{"reason":"batch_rate","permanent":5,"total":222,"batchRate":0.0225,"rollingRate":0.007,"errors":{"ApnsConnectionError: ERR_HTTP2_STREAM_ERROR":15,"Apns410:Unregistered":3,"Fcm404:UNREGISTERED":2}}]) |
| FCM requests rate-limited by the quota bucket (429) | 0 |
| Peak heap / peak RSS | 2027.7 MB / 2791 MB |

## Reading these numbers

- The run is CPU-bound on one JS thread (load average at start: 1.52 on 2 CPUs). The mock servers do about as much work per push as the senders, so 4,636 pushes per second is what this machine did, not what the design allows.
- The FCM quota (10,000/s) never bound: FCM accepted 447,295 pushes at about 2,084/s on average, and the quota bucket refused 0 requests. The 429s in the error table are the injected ones from the error mix.
- Push latency is measured around each push, so it includes waiting for an in-flight slot and for the event loop; the mock's own latency model is p50 20 ms and p99 150 ms.
- The senders' request timeout is 10 s. No push timed out in this run, so no retry followed an accepted-but-unanswered push.

## Exactly once

```
exactly-once: PASS (expected 1000000, once 995086, excused 4914, missing 0, cross-cell 0, other duplicates 0 (after a lost response 0, unexplained 0), unexpected 0)
raw checkExactlyOnce: exactly-once: PASS (expected 1000000, once 995086, excused 4914, missing 0, duplicated 0, unexpected 0)
```

- Expected devices (distinct push targets in the area): 1,000,000; delivered exactly once: 995,086; excused as permanent failures (mock answered unregistered): 4,914; missing: 0.
- Cross-cell duplicates (a device with places in several alert cells pushed more than once; the primary-cell rule should make this 0): 0 of 99,579 multi-cell devices (149,076 non-primary entries skipped by the planner).
- Any other duplicate: 0, of which 0 followed a timeout or connection error on the first push (the mock had accepted it, the sender never saw the answer and retried) and 0 are unexplained. Unexpected tokens: 0.
- Devices the mock accepted but whose success the sender never saw: 0 (they get no check-in token in the burst below).
- Verdict: **PASS** (any duplicate, cross-cell or after a lost response, is a failure).

## Re-plan of the same alert

Planner and shard workers ran again on the same signed alert: 75 shards in 8 ms, 69,431 batches re-enqueued, outcomes {"duplicate":69431}. New push requests that reached the mocks: **0**; new accepted pushes: **0**; new messages given to the push senders: 0.

## Sends per second, per 5 s window (successful pushes)

| Window start | Successful pushes | per second | Retryable failures | Permanent failures |
| ---: | ---: | ---: | ---: | ---: |
| 0 s | 13,801 | 2,760 | 76 | 62 |
| 5 s | 22,951 | 4,590 | 201 | 121 |
| 10 s | 24,873 | 4,975 | 245 | 132 |
| 15 s | 24,495 | 4,899 | 167 | 110 |
| 20 s | 21,175 | 4,235 | 84 | 66 |
| 25 s | 28,115 | 5,623 | 247 | 122 |
| 30 s | 20,786 | 4,157 | 191 | 86 |
| 35 s | 25,928 | 5,186 | 224 | 130 |
| 40 s | 10,735 | 2,147 | 98 | 53 |
| 45 s | 0 | 0 | 0 | 0 |
| 50 s | 19,012 | 3,802 | 247 | 97 |
| 55 s | 32,096 | 6,419 | 278 | 154 |
| 60 s | 19,925 | 3,985 | 92 | 98 |
| 65 s | 29,382 | 5,876 | 324 | 152 |
| 70 s | 17,746 | 3,549 | 94 | 91 |
| 75 s | 30,801 | 6,160 | 252 | 150 |
| 80 s | 18,789 | 3,758 | 152 | 99 |
| 85 s | 28,701 | 5,740 | 225 | 151 |
| 90 s | 22,481 | 4,496 | 283 | 109 |
| 95 s | 24,455 | 4,891 | 177 | 124 |
| 100 s | 17,433 | 3,487 | 114 | 100 |
| 105 s | 28,626 | 5,725 | 332 | 155 |
| 110 s | 16,428 | 3,286 | 152 | 82 |
| 115 s | 26,291 | 5,258 | 299 | 128 |
| 120 s | 24,065 | 4,813 | 197 | 130 |
| 125 s | 21,755 | 4,351 | 289 | 121 |
| 130 s | 25,668 | 5,134 | 305 | 141 |
| 135 s | 21,679 | 4,336 | 156 | 114 |
| 140 s | 28,801 | 5,760 | 301 | 142 |
| 145 s | 19,226 | 3,845 | 207 | 106 |
| 150 s | 29,113 | 5,823 | 327 | 162 |
| 155 s | 22,652 | 4,530 | 136 | 119 |
| 160 s | 30,035 | 6,007 | 246 | 151 |
| 165 s | 21,116 | 4,223 | 214 | 103 |
| 170 s | 23,969 | 4,794 | 146 | 127 |
| 175 s | 19,630 | 3,926 | 177 | 91 |
| 180 s | 24,914 | 4,983 | 214 | 128 |
| 185 s | 26,224 | 5,245 | 180 | 106 |
| 190 s | 24,198 | 4,840 | 222 | 110 |
| 195 s | 33,153 | 6,631 | 321 | 155 |
| 200 s | 21,412 | 4,282 | 226 | 104 |
| 205 s | 28,030 | 5,606 | 394 | 148 |
| 210 s | 24,080 | 4,816 | 511 | 116 |
| 215 s | 341 | 68 | 0 | 0 |

## Check-in burst

One v2 check-in per delivered device (995,086), tokens taken from this run's sends, through the API's stateless `POST /alerts/{id}/checkins` route with an in-memory check-in store (the device, place, alert and cell-index stores are traps that fail the run if touched), then every stored check-in as a stream INSERT through the real aggregator in batches of 1,000 against an in-memory totals table. Single JS thread, no network, so this is a code-path throughput, not an API Gateway figure.

| Measure | Value |
| --- | --- |
| API check-ins | 995,086 in 23.2 s = **42,826/s** (200 OK: 995,086, errors: 0) |
| API handler time p50 / p99 (200 in flight) | 3.19 ms / 15.96 ms |
| Aggregator | 996 batches of 1,000 in 2.1 s = **473,340/s** (failed items: 0) |
| Counter rows / marker rows written | 1,130 / 995,086 |
| Per-cell totals sum | 995,086 |
| Expected (devices in area - permanent failures - delivered without an ack) | 995,086 = 1,000,000 - 4,914 - 0 |
| Check-in store total | 995,086 |
| Per-cell mismatches (1,130 cells) | 0 |
| Verdict | **PASS** |

## Results JSON

```json
{
  "machine": {
    "nproc": 2,
    "cpuModel": "Intel(R) Xeon(R) Processor @ 2.30GHz",
    "memoryTotalMb": 8023,
    "loadAverage1mAtStart": 1.52,
    "memoryFreeAtStartMb": 4133,
    "node": "v22.23.4",
    "platform": "linux 6.18.44-fc-v310"
  },
  "heapLimitMb": 3120,
  "command": "cd loadtest && node --max-old-space-size=3120 --import tsx src/sim/run-fanout.ts --devices 1000000 --senders 2 --out results/fanout-1m-local-v2.md",
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
    "seedMs": 6672,
    "heapAfterSeedMb": 690.9,
    "peakHeapMb": 2027.7,
    "peakRssMb": 2791,
    "counts": {
      "devices": 1000000,
      "entries": 1149982,
      "cellsUsed": 1130,
      "alertCells": 1136,
      "devicesInArea": 1000000,
      "devicesWithTarget": 1000000,
      "devicesInTwoOrMoreCells": 99579,
      "shards": 75,
      "plannedBatches": 69431,
      "plannedRecipients": 1000000,
      "skippedNoTarget": 0,
      "sameCellDuplicates": 906,
      "skippedNonPrimary": 149076
    },
    "timing": {
      "planMs": 8,
      "firstPushMs": 2336,
      "lastPushMs": 216985,
      "wallMs": 216986
    },
    "throughput": {
      "meanOkPerSec": 4636,
      "peakWindowOkPerSec": 6631,
      "windows": [
        {
          "startSec": 0,
          "ok": 13801,
          "okPerSec": 2760,
          "retryable": 76,
          "permanent": 62
        },
        {
          "startSec": 5,
          "ok": 22951,
          "okPerSec": 4590,
          "retryable": 201,
          "permanent": 121
        },
        {
          "startSec": 10,
          "ok": 24873,
          "okPerSec": 4975,
          "retryable": 245,
          "permanent": 132
        },
        {
          "startSec": 15,
          "ok": 24495,
          "okPerSec": 4899,
          "retryable": 167,
          "permanent": 110
        },
        {
          "startSec": 20,
          "ok": 21175,
          "okPerSec": 4235,
          "retryable": 84,
          "permanent": 66
        },
        {
          "startSec": 25,
          "ok": 28115,
          "okPerSec": 5623,
          "retryable": 247,
          "permanent": 122
        },
        {
          "startSec": 30,
          "ok": 20786,
          "okPerSec": 4157,
          "retryable": 191,
          "permanent": 86
        },
        {
          "startSec": 35,
          "ok": 25928,
          "okPerSec": 5186,
          "retryable": 224,
          "permanent": 130
        },
        {
          "startSec": 40,
          "ok": 10735,
          "okPerSec": 2147,
          "retryable": 98,
          "permanent": 53
        },
        {
          "startSec": 45,
          "ok": 0,
          "okPerSec": 0,
          "retryable": 0,
          "permanent": 0
        },
        {
          "startSec": 50,
          "ok": 19012,
          "okPerSec": 3802,
          "retryable": 247,
          "permanent": 97
        },
        {
          "startSec": 55,
          "ok": 32096,
          "okPerSec": 6419,
          "retryable": 278,
          "permanent": 154
        },
        {
          "startSec": 60,
          "ok": 19925,
          "okPerSec": 3985,
          "retryable": 92,
          "permanent": 98
        },
        {
          "startSec": 65,
          "ok": 29382,
          "okPerSec": 5876,
          "retryable": 324,
          "permanent": 152
        },
        {
          "startSec": 70,
          "ok": 17746,
          "okPerSec": 3549,
          "retryable": 94,
          "permanent": 91
        },
        {
          "startSec": 75,
          "ok": 30801,
          "okPerSec": 6160,
          "retryable": 252,
          "permanent": 150
        },
        {
          "startSec": 80,
          "ok": 18789,
          "okPerSec": 3758,
          "retryable": 152,
          "permanent": 99
        },
        {
          "startSec": 85,
          "ok": 28701,
          "okPerSec": 5740,
          "retryable": 225,
          "permanent": 151
        },
        {
          "startSec": 90,
          "ok": 22481,
          "okPerSec": 4496,
          "retryable": 283,
          "permanent": 109
        },
        {
          "startSec": 95,
          "ok": 24455,
          "okPerSec": 4891,
          "retryable": 177,
          "permanent": 124
        },
        {
          "startSec": 100,
          "ok": 17433,
          "okPerSec": 3487,
          "retryable": 114,
          "permanent": 100
        },
        {
          "startSec": 105,
          "ok": 28626,
          "okPerSec": 5725,
          "retryable": 332,
          "permanent": 155
        },
        {
          "startSec": 110,
          "ok": 16428,
          "okPerSec": 3286,
          "retryable": 152,
          "permanent": 82
        },
        {
          "startSec": 115,
          "ok": 26291,
          "okPerSec": 5258,
          "retryable": 299,
          "permanent": 128
        },
        {
          "startSec": 120,
          "ok": 24065,
          "okPerSec": 4813,
          "retryable": 197,
          "permanent": 130
        },
        {
          "startSec": 125,
          "ok": 21755,
          "okPerSec": 4351,
          "retryable": 289,
          "permanent": 121
        },
        {
          "startSec": 130,
          "ok": 25668,
          "okPerSec": 5134,
          "retryable": 305,
          "permanent": 141
        },
        {
          "startSec": 135,
          "ok": 21679,
          "okPerSec": 4336,
          "retryable": 156,
          "permanent": 114
        },
        {
          "startSec": 140,
          "ok": 28801,
          "okPerSec": 5760,
          "retryable": 301,
          "permanent": 142
        },
        {
          "startSec": 145,
          "ok": 19226,
          "okPerSec": 3845,
          "retryable": 207,
          "permanent": 106
        },
        {
          "startSec": 150,
          "ok": 29113,
          "okPerSec": 5823,
          "retryable": 327,
          "permanent": 162
        },
        {
          "startSec": 155,
          "ok": 22652,
          "okPerSec": 4530,
          "retryable": 136,
          "permanent": 119
        },
        {
          "startSec": 160,
          "ok": 30035,
          "okPerSec": 6007,
          "retryable": 246,
          "permanent": 151
        },
        {
          "startSec": 165,
          "ok": 21116,
          "okPerSec": 4223,
          "retryable": 214,
          "permanent": 103
        },
        {
          "startSec": 170,
          "ok": 23969,
          "okPerSec": 4794,
          "retryable": 146,
          "permanent": 127
        },
        {
          "startSec": 175,
          "ok": 19630,
          "okPerSec": 3926,
          "retryable": 177,
          "permanent": 91
        },
        {
          "startSec": 180,
          "ok": 24914,
          "okPerSec": 4983,
          "retryable": 214,
          "permanent": 128
        },
        {
          "startSec": 185,
          "ok": 26224,
          "okPerSec": 5245,
          "retryable": 180,
          "permanent": 106
        },
        {
          "startSec": 190,
          "ok": 24198,
          "okPerSec": 4840,
          "retryable": 222,
          "permanent": 110
        },
        {
          "startSec": 195,
          "ok": 33153,
          "okPerSec": 6631,
          "retryable": 321,
          "permanent": 155
        },
        {
          "startSec": 200,
          "ok": 21412,
          "okPerSec": 4282,
          "retryable": 226,
          "permanent": 104
        },
        {
          "startSec": 205,
          "ok": 28030,
          "okPerSec": 5606,
          "retryable": 394,
          "permanent": 148
        },
        {
          "startSec": 210,
          "ok": 24080,
          "okPerSec": 4816,
          "retryable": 511,
          "permanent": 116
        },
        {
          "startSec": 215,
          "ok": 341,
          "okPerSec": 68,
          "retryable": 0,
          "permanent": 0
        }
      ]
    },
    "pushLatency": {
      "count": 1009355,
      "minMs": 0.5646460000134539,
      "meanMs": 556.0035219674663,
      "p50Ms": 384.2690639999928,
      "p90Ms": 1192.9182020000007,
      "p95Ms": 1791.6418160000176,
      "p99Ms": 2501.546346999996,
      "maxMs": 7076.880669999999
    },
    "pushErrors": {
      "Apns410:Unregistered": 2704,
      "Fcm404:UNREGISTERED": 2242,
      "Apns429:TooManyRequests": 1102,
      "Apns500:InternalServerError": 545,
      "Fcm500:INTERNAL": 451,
      "Fcm429:QUOTA_EXCEEDED": 891,
      "ApnsConnectionError: ERR_HTTP2_STREAM_ERROR": 6230,
      "FcmConnectionError: ECONNRESET": 104
    },
    "pass1": {
      "messagesProcessed": 12126,
      "batchesProcessed": 73417,
      "retryBatchesProcessed": 3986,
      "byReason": {
        "sent": 73417
      },
      "sent": 995086,
      "permanentFailed": 4914,
      "retried": 9355,
      "gaveUp": 0,
      "redeliveries": 0
    },
    "retryDelays": {
      "delayedMessages": 3986,
      "maxDelaySeconds": 4
    },
    "senderLogEvents": {
      "delivery.permanent_breaker_open": 6
    },
    "breakerEvents": [
      {
        "reason": "batch_rate",
        "permanent": 6,
        "total": 156,
        "batchRate": 0.0385,
        "rollingRate": 0.01,
        "errors": {
          "Apns410:Unregistered": 3,
          "Fcm404:UNREGISTERED": 3
        }
      },
      {
        "reason": "batch_rate",
        "permanent": 5,
        "total": 121,
        "batchRate": 0.0413,
        "rollingRate": 0.011,
        "errors": {
          "Apns410:Unregistered": 3,
          "Fcm404:UNREGISTERED": 2
        }
      },
      {
        "reason": "batch_rate",
        "permanent": 5,
        "total": 238,
        "batchRate": 0.021,
        "rollingRate": 0.009,
        "errors": {
          "Apns410:Unregistered": 3,
          "Fcm404:UNREGISTERED": 2
        }
      },
      {
        "reason": "batch_rate",
        "permanent": 5,
        "total": 206,
        "batchRate": 0.0243,
        "rollingRate": 0.007,
        "errors": {
          "Apns410:Unregistered": 3,
          "Fcm404:UNREGISTERED": 2,
          "Apns500:InternalServerError": 1
        }
      },
      {
        "reason": "batch_rate",
        "permanent": 5,
        "total": 222,
        "batchRate": 0.0225,
        "rollingRate": 0.007,
        "errors": {
          "ApnsConnectionError: ERR_HTTP2_STREAM_ERROR": 15,
          "Apns410:Unregistered": 3,
          "Fcm404:UNREGISTERED": 2
        }
      },
      {
        "reason": "batch_rate",
        "permanent": 6,
        "total": 262,
        "batchRate": 0.0229,
        "rollingRate": 0.008,
        "errors": {
          "Fcm404:UNREGISTERED": 5,
          "Apns429:TooManyRequests": 1,
          "Fcm429:QUOTA_EXCEEDED retry-after=1": 1
        }
      }
    ],
    "breakerOpened": true,
    "deliveries": {
      "totalOkPushes": 995086,
      "apnsAccepted": 547791,
      "fcmAccepted": 447295,
      "apnsUnregistered": 2704,
      "fcmUnregistered": 2241,
      "apnsRequests": 552142,
      "fcmRequests": 450879,
      "fcmRateLimited": 0,
      "injected429or500": 2989
    },
    "exactlyOnce": {
      "ok": true,
      "expectedDevices": 1000000,
      "deliveredOnce": 995086,
      "excusedPermanent": 4914,
      "missing": 0,
      "knownTwoCellDuplicates": 0,
      "unexpectedDuplicates": 0,
      "duplicatesAfterLostResponse": 0,
      "duplicatesUnexplained": 0,
      "deliveredWithoutAck": 0,
      "unexpectedTokens": 0,
      "summary": "exactly-once: PASS (expected 1000000, once 995086, excused 4914, missing 0, cross-cell 0, other duplicates 0 (after a lost response 0, unexplained 0), unexpected 0)\nraw checkExactlyOnce: exactly-once: PASS (expected 1000000, once 995086, excused 4914, missing 0, duplicated 0, unexpected 0)"
    },
    "replan": {
      "planMs": 8.407203999988269,
      "shards": 75,
      "batchesProcessed": 69431,
      "byReason": {
        "duplicate": 69431
      },
      "newPushRequests": 0,
      "newAcceptedPushes": 0,
      "newPushAttemptsInSender": 0
    },
    "permanentFailureDevices": 4914,
    "deliveredWithoutAck": 0
  },
  "checkinBurst": {
    "checkIns": 995086,
    "api": {
      "ms": 23235,
      "perSec": 42826,
      "ok200": 995086,
      "errors": 0,
      "p50Ms": 3.19,
      "p99Ms": 15.96
    },
    "aggregator": {
      "batches": 996,
      "batchSize": 1000,
      "ms": 2102,
      "perSec": 473340,
      "countersWritten": 1130,
      "markers": 995086,
      "failedItems": 0
    },
    "verification": {
      "ok": true,
      "totalsSum": 995086,
      "expectedSum": 995086,
      "storeSum": 995086,
      "perCellMismatches": 0,
      "cells": 1130,
      "expectedFormula": "devices in area - permanent failures - delivered without an ack"
    }
  }
}
```
