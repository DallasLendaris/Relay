# Local fan-out simulation: 1,000,000 devices

One alert, 1,000,000 seeded devices, the real planner, shard workers, `processBatch`, `ApnsSender` (h2c) and `FcmSender` against the mock APNs and FCM (default error mix, FCM quota 10,000/s). All figures below were measured by this run; none are estimates.

## How it was run

```sh
cd loadtest && node --max-old-space-size=3120 --import tsx src/sim/run-fanout.ts --devices 1000000
```

Machine: 2 CPUs (Intel(R) Xeon(R) Processor @ 2.30GHz), 8,023 MB RAM (4,088 MB free at start), Node v22.23.4, linux 6.18.44-fc-v310, V8 heap limit 3,120 MB. The mock servers, the senders, the shard workers and the check-in code all share one Node process (one JS thread), so the mocks and the senders compete for CPU. This is a local measurement, not an AWS prediction.

## Result

| Measure                                                              | Value                                                     |
| -------------------------------------------------------------------- | --------------------------------------------------------- |
| Devices seeded / entries / cells used                                | 1,000,000 / 1,149,982 / 1,130                             |
| Alert cells (shards)                                                 | 1,136 (8 shards)                                          |
| Devices in the alert area / with a push target                       | 1,000,000 / 1,000,000                                     |
| Batches planned (recipients)                                         | 70,042 (1,148,582)                                        |
| Time to plan (`planShards` returns)                                  | 3 ms                                                      |
| Time to first successful push                                        | 5.5 s                                                     |
| Time to last successful push                                         | 235.2 s                                                   |
| Sends per second, mean (first to last push)                          | 4,976                                                     |
| Sends per second, best 5 s window                                    | 6,908                                                     |
| Push latency p50 / p99 / max (per push, includes in-sender queueing) | 694.5 ms / 3221.5 ms / 14736.5 ms over 1,158,586 attempts |
| Batches processed (of which retries)                                 | 74,467 (4,425)                                            |
| Recipients retried after a retryable failure                         | 10,004                                                    |
| Permanent failures (dead token, cleared)                             | 5,633                                                     |
| Gave up (still failing after 5 attempts)                             | 0                                                         |
| Message redeliveries (batch not acked)                               | 0                                                         |
| FCM requests rate-limited by the quota bucket (429)                  | 0                                                         |
| Peak heap / peak RSS                                                 | 2017.1 MB / 2810 MB                                       |

## Reading these numbers

- The run is CPU-bound on one JS thread (load average at start: 2.8 on 2 CPUs). The mock servers do about as much work per push as the senders, so 4,976 pushes per second is what this machine did, not what the design allows.
- The FCM quota (10,000/s) never bound: FCM accepted 514,014 pushes at about 2,238/s on average, and the quota bucket refused 0 requests. The 429s in the error table are the injected ones from the error mix.
- Push latency is measured around each push, so it includes waiting for an in-flight slot and for the event loop; the mock's own latency model is p50 20 ms and p99 150 ms.
- The senders' request timeout is 10 s. 403 pushes timed out because the loop was saturated; the mock had already accepted some of them, so their retries are the duplicates listed below. That is the at-least-once edge of "retry after a timeout", not a planner or idempotency bug: batch claims held (the re-plan sent nothing).

## Exactly once

```
exactly-once (two-cells excluded): FAIL (expected 1000000, once 896238, excused 4393, missing 0, known two-cells 98971, other duplicates 398 (after a lost response 398, unexplained 0), unexpected 0)
raw checkExactlyOnce: exactly-once: FAIL (expected 1000000, once 896238, excused 4393, missing 0, duplicated 99369, unexpected 0)
```

- Expected devices (distinct push targets in the area): 1,000,000; delivered exactly once: 896,238; excused as permanent failures (mock answered unregistered): 4,393; missing: 0.
- Known two-cells duplicates (a device with places in two alert cells gets one push per cell, documented gap in `shard-worker.ts`): 98,971 of 99,579 such devices.
- Any other duplicate: 398, of which 398 followed a timeout or connection error on the first push (the mock had accepted it, the sender never saw the answer and retried) and 0 are unexplained. Unexpected tokens: 0.
- Devices the mock accepted but whose success the sender never saw: 1 (they get no check-in token in the burst below).
- Verdict: **FAIL** (known two-cells duplicates are not counted as failures; any other duplicate is).

## Re-plan of the same alert

Planner and shard workers ran again on the same signed alert: 8 shards in 6 ms, 70,042 batches re-enqueued, outcomes {"duplicate":70042}. New push requests that reached the mocks: **0**; new accepted pushes: **0**; new messages given to the push senders: 0.

## Sends per second, per 5 s window (successful pushes)

| Window start | Successful pushes | per second | Retryable failures | Permanent failures |
| -----------: | ----------------: | ---------: | -----------------: | -----------------: |
|          0 s |                 0 |          0 |                  0 |                  0 |
|          5 s |            23,780 |      4,756 |                154 |                104 |
|         10 s |            25,440 |      5,088 |                451 |                147 |
|         15 s |            22,369 |      4,474 |                477 |                117 |
|         20 s |            27,058 |      5,412 |                113 |                 97 |
|         25 s |            18,719 |      3,744 |                381 |                 71 |
|         30 s |            30,082 |      6,016 |                187 |                122 |
|         35 s |            21,518 |      4,304 |                287 |                111 |
|         40 s |            28,140 |      5,628 |                254 |                138 |
|         45 s |            25,995 |      5,199 |                334 |                126 |
|         50 s |            28,700 |      5,740 |                109 |                149 |
|         55 s |            22,560 |      4,512 |                106 |                100 |
|         60 s |            25,801 |      5,160 |                337 |                136 |
|         65 s |            34,539 |      6,908 |                344 |                165 |
|         70 s |            23,674 |      4,735 |                148 |                130 |
|         75 s |            27,064 |      5,413 |                372 |                144 |
|         80 s |            29,248 |      5,850 |                114 |                144 |
|         85 s |            27,253 |      5,451 |                163 |                138 |
|         90 s |            28,590 |      5,718 |                204 |                162 |
|         95 s |            22,194 |      4,439 |                 69 |                115 |
|        100 s |            27,406 |      5,481 |                318 |                137 |
|        105 s |            28,959 |      5,792 |                305 |                147 |
|        110 s |            17,787 |      3,557 |                226 |                103 |
|        115 s |            33,073 |      6,615 |                230 |                177 |
|        120 s |            21,839 |      4,368 |                 69 |                122 |
|        125 s |            27,069 |      5,414 |                240 |                131 |
|        130 s |            31,681 |      6,336 |                292 |                176 |
|        135 s |            20,974 |      4,195 |                148 |                109 |
|        140 s |            30,930 |      6,186 |                211 |                157 |
|        145 s |            18,650 |      3,730 |                 68 |                 97 |
|        150 s |            27,420 |      5,484 |                 95 |                143 |
|        155 s |            19,649 |      3,930 |                402 |                101 |
|        160 s |            24,393 |      4,879 |                207 |                119 |
|        165 s |            31,367 |      6,273 |                234 |                138 |
|        170 s |            19,943 |      3,989 |                211 |                 85 |
|        175 s |            29,272 |      5,854 |                110 |                136 |
|        180 s |            19,753 |      3,951 |                135 |                 92 |
|        185 s |            27,308 |      5,462 |                116 |                146 |
|        190 s |            18,664 |      3,733 |                151 |                 92 |
|        195 s |            25,224 |      5,045 |                363 |                118 |
|        200 s |            15,408 |      3,082 |                218 |                 72 |
|        205 s |            23,718 |      4,744 |                117 |                 95 |
|        210 s |            12,959 |      2,592 |                 38 |                 47 |
|        215 s |            23,509 |      4,702 |                117 |                104 |
|        220 s |            20,213 |      4,043 |                168 |                121 |
|        225 s |            27,318 |      5,464 |                353 |                117 |
|        230 s |            25,507 |      5,101 |                257 |                134 |
|        235 s |               232 |         46 |                  1 |                  1 |

## Check-in burst

One v2 check-in per delivered device (995,606), tokens taken from this run's sends, through the API's stateless `POST /alerts/{id}/checkins` route with an in-memory check-in store (the device, place, alert and cell-index stores are traps that fail the run if touched), then every stored check-in as a stream INSERT through the real aggregator in batches of 1,000 against an in-memory totals table. Single JS thread, no network, so this is a code-path throughput, not an API Gateway figure.

| Measure                                                                    | Value                                                           |
| -------------------------------------------------------------------------- | --------------------------------------------------------------- |
| API check-ins                                                              | 995,606 in 24.0 s = **41,419/s** (200 OK: 995,606, errors: 0)   |
| API handler time p50 / p99 (200 in flight)                                 | 3.42 ms / 17.95 ms                                              |
| Aggregator                                                                 | 996 batches of 1,000 in 2.0 s = **487,844/s** (failed items: 0) |
| Counter rows / marker rows written                                         | 1,130 / 995,606                                                 |
| Per-cell totals sum                                                        | 995,606                                                         |
| Expected (devices in area - permanent failures - delivered without an ack) | 995,606 = 1,000,000 - 4,393 - 1                                 |
| Check-in store total                                                       | 995,606                                                         |
| Per-cell mismatches (1,130 cells)                                          | 0                                                               |
| Verdict                                                                    | **PASS**                                                        |

## Second full run: 2 senders

The same 1,000,000-device run with `--senders 2` (command in `fanout-1m-local-2senders.md`, which has the full tables and JSON) was
less starved and had no timeouts: time to last successful push 212.7 s, mean 5,500 pushes per second (best 5 s window 7,398), push latency
p50 299 ms / p99 1,851 ms / max 6,075 ms, **0 duplicates other than the known two-cells case (99,085 of 99,579 such devices)**, 0 missing,
exactly-once PASS, re-plan sent nothing (0 new requests), check-in burst 995,558 check-ins at 50,825/s through the API route and 594,862/s through
the aggregator, totals sum 995,558 = 1,000,000 - 4,442, 0 per-cell mismatches. With this machine's CPU as the limit, fewer in-flight pushes
gave lower latency, a higher rate and no lost-response duplicates; the 4-sender run above shows what happens when the loop is oversubscribed.

## Results JSON

```json
{
  "machine": {
    "nproc": 2,
    "cpuModel": "Intel(R) Xeon(R) Processor @ 2.30GHz",
    "memoryTotalMb": 8023,
    "loadAverage1mAtStart": 2.8,
    "memoryFreeAtStartMb": 4088,
    "node": "v22.23.4",
    "platform": "linux 6.18.44-fc-v310"
  },
  "heapLimitMb": 3120,
  "command": "cd loadtest && node --max-old-space-size=3120 --import tsx src/sim/run-fanout.ts --devices 1000000",
  "fanout": {
    "options": {
      "devices": 1000000,
      "seed": 1,
      "fcmRate": 10000,
      "senders": 4,
      "apnsSessions": 4,
      "messagesPerSender": 12,
      "shardConcurrency": 32
    },
    "seedMs": 8037,
    "heapAfterSeedMb": 633.2,
    "peakHeapMb": 2017.1,
    "peakRssMb": 2810,
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
      "planMs": 3,
      "firstPushMs": 5483,
      "lastPushMs": 235187,
      "wallMs": 235188
    },
    "throughput": {
      "meanOkPerSec": 4976,
      "peakWindowOkPerSec": 6908,
      "windows": [
        {
          "startSec": 0,
          "ok": 0,
          "okPerSec": 0,
          "retryable": 0,
          "permanent": 0
        },
        {
          "startSec": 5,
          "ok": 23780,
          "okPerSec": 4756,
          "retryable": 154,
          "permanent": 104
        },
        {
          "startSec": 10,
          "ok": 25440,
          "okPerSec": 5088,
          "retryable": 451,
          "permanent": 147
        },
        {
          "startSec": 15,
          "ok": 22369,
          "okPerSec": 4474,
          "retryable": 477,
          "permanent": 117
        },
        {
          "startSec": 20,
          "ok": 27058,
          "okPerSec": 5412,
          "retryable": 113,
          "permanent": 97
        },
        {
          "startSec": 25,
          "ok": 18719,
          "okPerSec": 3744,
          "retryable": 381,
          "permanent": 71
        },
        {
          "startSec": 30,
          "ok": 30082,
          "okPerSec": 6016,
          "retryable": 187,
          "permanent": 122
        },
        {
          "startSec": 35,
          "ok": 21518,
          "okPerSec": 4304,
          "retryable": 287,
          "permanent": 111
        },
        {
          "startSec": 40,
          "ok": 28140,
          "okPerSec": 5628,
          "retryable": 254,
          "permanent": 138
        },
        {
          "startSec": 45,
          "ok": 25995,
          "okPerSec": 5199,
          "retryable": 334,
          "permanent": 126
        },
        {
          "startSec": 50,
          "ok": 28700,
          "okPerSec": 5740,
          "retryable": 109,
          "permanent": 149
        },
        {
          "startSec": 55,
          "ok": 22560,
          "okPerSec": 4512,
          "retryable": 106,
          "permanent": 100
        },
        {
          "startSec": 60,
          "ok": 25801,
          "okPerSec": 5160,
          "retryable": 337,
          "permanent": 136
        },
        {
          "startSec": 65,
          "ok": 34539,
          "okPerSec": 6908,
          "retryable": 344,
          "permanent": 165
        },
        {
          "startSec": 70,
          "ok": 23674,
          "okPerSec": 4735,
          "retryable": 148,
          "permanent": 130
        },
        {
          "startSec": 75,
          "ok": 27064,
          "okPerSec": 5413,
          "retryable": 372,
          "permanent": 144
        },
        {
          "startSec": 80,
          "ok": 29248,
          "okPerSec": 5850,
          "retryable": 114,
          "permanent": 144
        },
        {
          "startSec": 85,
          "ok": 27253,
          "okPerSec": 5451,
          "retryable": 163,
          "permanent": 138
        },
        {
          "startSec": 90,
          "ok": 28590,
          "okPerSec": 5718,
          "retryable": 204,
          "permanent": 162
        },
        {
          "startSec": 95,
          "ok": 22194,
          "okPerSec": 4439,
          "retryable": 69,
          "permanent": 115
        },
        {
          "startSec": 100,
          "ok": 27406,
          "okPerSec": 5481,
          "retryable": 318,
          "permanent": 137
        },
        {
          "startSec": 105,
          "ok": 28959,
          "okPerSec": 5792,
          "retryable": 305,
          "permanent": 147
        },
        {
          "startSec": 110,
          "ok": 17787,
          "okPerSec": 3557,
          "retryable": 226,
          "permanent": 103
        },
        {
          "startSec": 115,
          "ok": 33073,
          "okPerSec": 6615,
          "retryable": 230,
          "permanent": 177
        },
        {
          "startSec": 120,
          "ok": 21839,
          "okPerSec": 4368,
          "retryable": 69,
          "permanent": 122
        },
        {
          "startSec": 125,
          "ok": 27069,
          "okPerSec": 5414,
          "retryable": 240,
          "permanent": 131
        },
        {
          "startSec": 130,
          "ok": 31681,
          "okPerSec": 6336,
          "retryable": 292,
          "permanent": 176
        },
        {
          "startSec": 135,
          "ok": 20974,
          "okPerSec": 4195,
          "retryable": 148,
          "permanent": 109
        },
        {
          "startSec": 140,
          "ok": 30930,
          "okPerSec": 6186,
          "retryable": 211,
          "permanent": 157
        },
        {
          "startSec": 145,
          "ok": 18650,
          "okPerSec": 3730,
          "retryable": 68,
          "permanent": 97
        },
        {
          "startSec": 150,
          "ok": 27420,
          "okPerSec": 5484,
          "retryable": 95,
          "permanent": 143
        },
        {
          "startSec": 155,
          "ok": 19649,
          "okPerSec": 3930,
          "retryable": 402,
          "permanent": 101
        },
        {
          "startSec": 160,
          "ok": 24393,
          "okPerSec": 4879,
          "retryable": 207,
          "permanent": 119
        },
        {
          "startSec": 165,
          "ok": 31367,
          "okPerSec": 6273,
          "retryable": 234,
          "permanent": 138
        },
        {
          "startSec": 170,
          "ok": 19943,
          "okPerSec": 3989,
          "retryable": 211,
          "permanent": 85
        },
        {
          "startSec": 175,
          "ok": 29272,
          "okPerSec": 5854,
          "retryable": 110,
          "permanent": 136
        },
        {
          "startSec": 180,
          "ok": 19753,
          "okPerSec": 3951,
          "retryable": 135,
          "permanent": 92
        },
        {
          "startSec": 185,
          "ok": 27308,
          "okPerSec": 5462,
          "retryable": 116,
          "permanent": 146
        },
        {
          "startSec": 190,
          "ok": 18664,
          "okPerSec": 3733,
          "retryable": 151,
          "permanent": 92
        },
        {
          "startSec": 195,
          "ok": 25224,
          "okPerSec": 5045,
          "retryable": 363,
          "permanent": 118
        },
        {
          "startSec": 200,
          "ok": 15408,
          "okPerSec": 3082,
          "retryable": 218,
          "permanent": 72
        },
        {
          "startSec": 205,
          "ok": 23718,
          "okPerSec": 4744,
          "retryable": 117,
          "permanent": 95
        },
        {
          "startSec": 210,
          "ok": 12959,
          "okPerSec": 2592,
          "retryable": 38,
          "permanent": 47
        },
        {
          "startSec": 215,
          "ok": 23509,
          "okPerSec": 4702,
          "retryable": 117,
          "permanent": 104
        },
        {
          "startSec": 220,
          "ok": 20213,
          "okPerSec": 4043,
          "retryable": 168,
          "permanent": 121
        },
        {
          "startSec": 225,
          "ok": 27318,
          "okPerSec": 5464,
          "retryable": 353,
          "permanent": 117
        },
        {
          "startSec": 230,
          "ok": 25507,
          "okPerSec": 5101,
          "retryable": 257,
          "permanent": 134
        },
        {
          "startSec": 235,
          "ok": 232,
          "okPerSec": 46,
          "retryable": 1,
          "permanent": 1
        }
      ]
    },
    "pushLatency": {
      "count": 1158586,
      "minMs": 2.1284670000022743,
      "meanMs": 887.772468714418,
      "p50Ms": 694.4518430000026,
      "p90Ms": 1764.174125000005,
      "p95Ms": 2258.3859539999976,
      "p99Ms": 3221.4572209999897,
      "maxMs": 14736.452535
    },
    "pushErrors": {
      "Apns410:Unregistered": 3107,
      "Fcm404:UNREGISTERED": 2526,
      "Apns500:InternalServerError": 629,
      "Apns429:TooManyRequests": 1277,
      "Fcm500:INTERNAL": 512,
      "Fcm429:QUOTA_EXCEEDED": 1029,
      "ApnsConnectionError: ERR_HTTP2_STREAM_ERROR": 6154,
      "Timeout": 403
    },
    "pass1": {
      "messagesProcessed": 12908,
      "batchesProcessed": 74467,
      "retryBatchesProcessed": 4425,
      "byReason": {
        "sent": 74467
      },
      "sent": 1142949,
      "permanentFailed": 5633,
      "retried": 10004,
      "gaveUp": 0,
      "redeliveries": 0
    },
    "deliveries": {
      "totalOkPushes": 1142949,
      "apnsAccepted": 629336,
      "fcmAccepted": 514014,
      "apnsUnregistered": 3102,
      "fcmUnregistered": 2525,
      "apnsRequests": 634349,
      "fcmRequests": 518083,
      "fcmRateLimited": 0,
      "injected429or500": 3448
    },
    "exactlyOnce": {
      "ok": false,
      "expectedDevices": 1000000,
      "deliveredOnce": 896238,
      "excusedPermanent": 4393,
      "missing": 0,
      "knownTwoCellDuplicates": 98971,
      "unexpectedDuplicates": 398,
      "duplicatesAfterLostResponse": 398,
      "duplicatesUnexplained": 0,
      "deliveredWithoutAck": 1,
      "unexpectedTokens": 0,
      "summary": "exactly-once (two-cells excluded): FAIL (expected 1000000, once 896238, excused 4393, missing 0, known two-cells 98971, other duplicates 398 (after a lost response 398, unexplained 0), unexpected 0)\nraw checkExactlyOnce: exactly-once: FAIL (expected 1000000, once 896238, excused 4393, missing 0, duplicated 99369, unexpected 0)"
    },
    "replan": {
      "planMs": 5.507268999994267,
      "shards": 8,
      "batchesProcessed": 70042,
      "byReason": {
        "duplicate": 70042
      },
      "newPushRequests": 0,
      "newAcceptedPushes": 0,
      "newPushAttemptsInSender": 0
    },
    "permanentFailureDevices": 4393,
    "deliveredWithoutAck": 1
  },
  "checkinBurst": {
    "checkIns": 995606,
    "api": {
      "ms": 24037,
      "perSec": 41419,
      "ok200": 995606,
      "errors": 0,
      "p50Ms": 3.42,
      "p99Ms": 17.95
    },
    "aggregator": {
      "batches": 996,
      "batchSize": 1000,
      "ms": 2041,
      "perSec": 487844,
      "countersWritten": 1130,
      "markers": 995606,
      "failedItems": 0
    },
    "verification": {
      "ok": true,
      "totalsSum": 995606,
      "expectedSum": 995606,
      "storeSum": 995606,
      "perCellMismatches": 0,
      "cells": 1130,
      "expectedFormula": "devices in area - permanent failures - delivered without an ack"
    }
  }
}
```
