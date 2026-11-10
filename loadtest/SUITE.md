# Scalability suite

Seven tests that measure the real Relay code (planner, shard workers, senders, check-in API,
aggregator, idempotency, cell index) against mock APNs/FCM and, for t4, real databases. Built to run
natively on a Windows 11 PC (12 cores, 32 GB RAM) with Node 22 and Docker Desktop; it also runs on
Linux and macOS.

## Run it (Windows, one command)

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run-scale-suite.ps1
```

The script checks Node >= 22, Git and Docker Desktop (if Docker is not running it warns that t4 will
be skipped and asks whether to continue), keeps the PC awake while it runs, uses an npm cache on the
repo's drive (this session only), runs `npm ci` if `node_modules` is missing, runs the suite, writes a
log, and prints where the results are and the total time. Data goes to `<drive>:\relay-suite-data`
and results to `<repo parent>\results\suite-<timestamp>` (for `D:\relay-audit\Relay` that is
`D:\relay-audit\results\suite-<timestamp>`). Nothing is written to `C:`.

Expected total at full scale: 4 to 5 hours. Leave the PC alone while it runs: the tests compete for
the CPU, so other load skews the numbers. Linux/macOS/WSL: `scripts/run-scale-suite.sh`.

## Tests

| Id  | Title                   | What it does                                                                                                                                                                                                                                                                                                                          | Est.     |
| --- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| t1  | Single-process baseline | The existing local simulation as the recorded 1M run: 1,000,000 devices, one alert, real planner/workers/senders vs mock APNs/FCM in one Node process (6 GB heap), then a check-in burst. Reports time to last push, sends/s, missed, duplicates, check-ins/s, and `speedup_vs_2cpu_baseline` against the recorded 217.0 s on 2 CPUs. | 5 min    |
| t2  | Multi-process fan-out   | Same fan-out with senders and mocks spread over several processes (placeholder title; see the test's report).                                                                                                                                                                                                                         | see plan |
| t3  | Check-in burst          | Check-in API and aggregator under a burst (placeholder).                                                                                                                                                                                                                                                                              | see plan |
| t4  | Real databases          | DynamoDB Local and PostGIS in Docker (requires Docker; skipped if unavailable).                                                                                                                                                                                                                                                       | see plan |
| t5  | Failure under load      | Faults injected while a fan-out runs (placeholder).                                                                                                                                                                                                                                                                                   | see plan |
| t6  | Ingest storm            | Device and location ingest at high rate (placeholder).                                                                                                                                                                                                                                                                                | see plan |
| t7  | Soak                    | Long run to find leaks and drift (placeholder).                                                                                                                                                                                                                                                                                       | see plan |

The runner prints the plan (ids, titles, estimated minutes, total) before it starts.
Correctness is part of every result: missed = 0, duplicates = 0 and equal totals are checks, and any
failed check makes the test's verdict `fail`.

## Results

Per run, in the output folder:

- `SUMMARY.md`: machine info and a table of tests (verdict, duration, headline numbers, links). It is
  rewritten after every test, so a crash leaves the partial results.
- `<id>.md`: human report for the test; `<id>.json`: the raw result.
- `suite-log.txt`: the full console log (Windows script; the shell script writes the same file).

A test that throws is recorded as `fail` and the suite continues. A test that needs Docker when Docker
is missing or fails to start is `skipped` with the reason. Exit code is 1 if any test failed.

## Subsets, smoke mode, options

```powershell
# only t1 and t3
powershell -ExecutionPolicy Bypass -File scripts\run-scale-suite.ps1 -Tests t1,t3
# smoke: tiny sizes, whole suite in a few minutes, to check the setup (and what CI does)
powershell -ExecutionPolicy Bypass -File scripts\run-scale-suite.ps1 -Scale smoke
```

Script parameters: `-Tests`, `-Scale full|smoke`, `-Workers N` (default logical CPUs - 2),
`-OutDir`, `-DataDir`. Directly (any OS), from the repo root:

```sh
npm run suite --workspace loadtest -- --tests t1,t2 --scale full --workers 10 --out <dir> --data <dir> --keep-services
npm run suite:smoke --workspace loadtest -- --tests t1
```

`--keep-services` leaves the Docker containers running afterwards (default: `docker compose down -v`).
Docker services are defined in `loadtest/suite/docker-compose.yml` (DynamoDB Local on 8000,
PostGIS 16 on 55432, password from `RELAY_PG_PASSWORD`, default `relay`).

## For test authors

- `src/suite/types.ts` is the contract; register a test in `src/suite/tests/index.ts` (one import and
  one array entry per line).
- `src/suite/lib/child.ts`: `spawnWorker(file, args, { heapMb })` with `.result`, `.rssPeak`, `.kill()`;
  the child calls `runWorkerMain(fn)` (or `reportRss()` and `sendResult(obj)`). No signals or shell, so
  it works on Windows; children are killed on exit and Ctrl+C.
- `src/suite/lib/rss.ts`: `RssSampler` (peak RSS of this process plus child sources).
- `src/suite/lib/report.ts`: optional helpers for the `<id>.md` report.
