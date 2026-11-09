# @relay/loadtest

Mock push services, seed data and report helpers for proving the scale targets in
`docs/scale-plan.md` (1,000,000 recipients per alert, exactly once).

| File                 | What it is                                                                              |
| -------------------- | --------------------------------------------------------------------------------------- |
| `src/mock-apns.ts`   | HTTP/2 mock of APNs `POST /3/device/{token}`                                            |
| `src/mock-fcm.ts`    | HTTP/1.1 mock of FCM HTTP v1 `POST /v1/projects/{id}/messages:send` plus OAuth `/token` |
| `src/seed.ts`        | Streams 1,000,000 devices as `CellIndexEntry` objects over Santa Clara County           |
| `src/report.ts`      | Latency percentiles, throughput, and the exactly-once check                             |
| `src/mock-common.ts` | Seeded RNG, latency model, error mix, token bucket, per-token counters                  |

## How they are used

**Local 1M simulation.** A test or script starts both mocks in-process, streams `generateEntries()` into the cell
index (or into the planner directly), runs the planner, shard workers and senders pointed at `mockApns.url` and
`mockFcm.url`, then asserts:

```ts
const apns = await startMockApns({ seed: 1 });
const fcm = await startMockFcm({ seed: 1 });
// ... run the fan-out against apns.url / fcm.url ...
const delivered = new Map([...apns.counters.delivered, ...fcm.counters.delivered]);
const excused = new Set([
  ...apns.counters.unregistered.keys(),
  ...fcm.counters.unregistered.keys(),
]);
const result = checkExactlyOnce(expectedTokens, delivered, { excused });
formatExactlyOnce(result); // PASS or FAIL, with missing and duplicate tokens (redacted)
```

`delivered` counts only accepted (200) messages, so a sender that retries after a 429 or 500 is still delivered once.
Tokens the mock answered 410 or 404 UNREGISTERED are terminal and are passed as `excused`. A token delivered twice is
always a failure. Use `LatencyRecorder` around each send and `throughput()` for time-to-last-push.

**AWS load test (P-01).** The same mock servers run on Fargate in a separate stack (standalone mode below, behind
an NLB; use `--tls` for APNs), and the senders are pointed at them with an endpoint override. The seed data is
written with `npm run seed ... --out cells.ndjson` and loaded into the DynamoDB cell index with `readNdjson()`.
The exactly-once check reads the mocks' counters (expose `stats()` and `counters` over a small admin endpoint,
not yet built) after the run.

## Run the mocks standalone

```sh
npm run mock:apns -w @relay/loadtest -- --port 8443 --tls --p50 20 --p99 150 --max-streams 1000 --goaway 0.0005
npm run mock:fcm  -w @relay/loadtest -- --port 8080 --rate 10000
npm run seed      -w @relay/loadtest -- --devices 1000000 --seed 1 --out /tmp/cells.ndjson
```

Without `--tls` APNs speaks h2c (HTTP/2 over cleartext), which is what local runs use. With `--tls` it generates
a self-signed certificate on start; clients must trust it (in-process, use `mockApns.cert` as `ca`). The
certificate is never written to disk. Both mocks log JSON lines and never log tokens.

Defaults: latency p50 20 ms and p99 150 ms (log-normal), errors 0.5% 410 Unregistered, 0.2% 429 and 0.1% 500,
APNs 1,000 streams per connection, FCM 10,000 messages per second. FCM answers an unregistered token with
404 `UNREGISTERED` (as the real service does), a quota hit with 429 `QUOTA_EXCEEDED` and `Retry-After`, and requires a
bearer token from `POST /token` (valid 3600 s).

Notes on behaviour that clients meet in real life:

- APNs advertises `MAX_CONCURRENT_STREAMS` in SETTINGS. A client that sends before it has seen SETTINGS can get
  `REFUSED_STREAM`; it must retry. A graceful GOAWAY lets in-flight streams finish; the client must reconnect.
- The seed generator uses Santa Clara County's bounding box (about 36.89 to 37.48 N, 122.20 to 121.21 W), so
  a few uniform points fall outside the county border. 70% of places are drawn from clusters around San Jose,
  Santa Clara, Sunnyvale and Mountain View, which makes the densest H3 res-7 cell hold about 21,000 devices at 1M
  (p50 about 330, p95 about 5,600 per cell, about 1,100 cells used).

## The mock limits are assumptions

The numbers above (FCM 10,000 messages per second from the 600,000 per minute project quota, APNs about 1,000
streams per connection, the latency distribution, the error mix) are assumptions, not measured service limits.
Check each against `docs/scale-quotas.md` (P-02) and adjust the options when the real values are known; a load
test passing against a mock that is more generous than the real service proves nothing.

## Tests

`npm test -w @relay/loadtest`. The tests start the mocks on random local ports and need no network. The 1M seed test
streams to `/dev/null` and asserts the time (under 30 s) and heap growth (under 200 MB).
