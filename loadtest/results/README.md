# Local 1,000,000-device fan-out results

All runs: 2 CPUs, 8 GB, one Node process holding the mocks, senders, shard workers and check-in code (CPU-bound, not an AWS prediction). Full tables and JSON are in each file.

| Run                                                                  | File                          | Senders | Last push | Mean sends/s | Push p50 / p99 | Cross-cell duplicates | Lost-response duplicates | Missing | Re-plan new pushes | Check-in API / aggregator per s |
| -------------------------------------------------------------------- | ----------------------------- | ------: | --------: | -----------: | -------------- | --------------------: | -----------------------: | ------: | -----------------: | ------------------------------- |
| v1 (before primary-cell, backoff)                                    | `fanout-1m-local.md`          |       4 |   235.2 s |        4,976 | 695 / 3,222 ms |                98,971 |                      398 |       0 |                  0 | 41,419 / 487,844                |
| v1                                                                   | `fanout-1m-local-2senders.md` |       2 |   212.7 s |        5,500 | 299 / 1,851 ms |                99,085 |                        0 |       0 |                  0 | 50,825 / 594,862                |
| v2 (primary-cell, retry backoff, breaker, async dead-target cleanup) | `fanout-1m-local-v2.md`       |       2 |   217.0 s |        4,636 | 384 / 2,502 ms |                     0 |                        0 |       0 |                  0 | 42,826 / 473,340                |

v2 also: 75 shards (16 cells each), 3,986 retry messages delayed by backoff (longest about 4 s), and the permanent-failure breaker opened 6 times at a 0.5% dead-token rate. Every opening was the per-batch rule (at least 5 permanent and more than 2% in one batch of 121 to 238 recipients), not the rolling rate (about 1%). The effect here was only that those few dead tokens were retried instead of cleared; the batch rule is sensitive to ordinary variation in batches of a couple hundred.

The v1 rate differences between runs are within the noise of a shared 2-CPU machine.
