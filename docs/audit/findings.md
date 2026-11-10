# Audit findings (full list)

Audit of `audit-baseline` (74acc5c), 2026-11-09. Report: "Relay System Audit" (Claude Docs). Severities are after independent verification; see the VERIFY sections.

## DEP (Haiku) - no critical/high

- DEP-1 med: zod 3 advisory GHSA-mrhx-c7xc-m3w5, unbounded z.array in AlertSchema on ingest path -> add .max() bounds + body size cap (or zod 4).
- DEP-2 low-med: sender image ships source maps with sources -> drop .map from runtime stage.
- DEP-3 low-med: node:22-slim not digest pinned.
- DEP-4 low: actions not SHA-pinned; no permissions block.
- DEP-5/6 low: vitest/tinypool, aws-cdk-lib, minimatch dev-only advisories.
- DEP-7 low: no .dockerignore.
- Clean: licenses, no git deps, lockfile committed, npm ci, non-root image, no pull_request_target.

## GOV (Sonnet)

- GOV-1 high (doc): no CCSA package (SSP, FIPS199, PTA/PIA, BIA/RTO, arch diagram). Fix = docs. https://cdt.ca.gov/services/ccsa ; SAM 4983.1
- GOV-2 high CODE: check-ins table no TTL, PITR+RETAIN -> forever device-in-cell-at-time records (CPRA exposure). Fix: TTL (e.g., 30-90d, counsel). api-stack.ts:121-129
- GOV-3 high CODE: no DELETE /devices/me (erase device, places cascade, cell index, check-ins).
- GOV-4 high CODE: es push body uses Relay summary text with no "Relay summary" label or source; NWS disclaimer forbids presenting modified as official. send-jobs.ts:30-39 (+ sender policy text). Fix: label + senderName in push; never replace official first line.
- GOV-5 high (decision/doc): IPAWS role undecided (redistributor AHIF MOA vs originator COG). Doc positioning: supplements WEA/EAS.
- GOV-6 high (known gap): no agency auth (Cognito) exists. AG-02.
- GOV-7 med: EN/ES hard-coded (CHECK constraint + enums); CA Gov Code 8594.16, 7299.7 (AB1638). Loosen to BCP-47 list.
- GOV-8 med: cancellations not pushed (Cal OES principles 7/8 retraction within 10 min).
- GOV-9 med: no terms/disclaimer/privacy notice.
- GOV-10 med: no CloudTrail/WAF logs/audit trail for kill switch; 1-month retention.
- GOV-11 med: no SLA/IR/VDP/SECURITY.md.
- GOV-12 low: no exercise/test mode for drills.
  Counsel Qs: SaaS vs state-operated; CPRA on raw check-ins + retention; language statutes bind vendor?; liability/disclaimer; Cal OES letter; county COG.

## PRIV (Sonnet)

- PRIV-1 high = GOV-3: no DELETE /devices/me; Gov Code 11015.5(c) (CSU covered) discard on request. Needs cell index, places, device, checkins (GSI by deviceId or key scan), tombstone.
- PRIV-2 high = GOV-2: checkins no TTL.
- PRIV-3 high: agency aggregate protections absent; no recipients-per-cell count recorded; listTotals raw. (AG-04 not built - fix: record recipients per cell at fan-out; suppression+deterministic noise in future agency route.)
- PRIV-4 med: Sybil + differencing defeats k=20; deterministic per (alert,cell) noise.
- PRIV-5 high: exact lat/lng + address + label stored forever, unused (match uses h3 only). Fix: store cell only (or centroid); label/address device-side.
- PRIV-6 high: all compute roles use relay_admin DB secret (rds-data on whole cluster); ingest needs alerts only. Fix: per-function DB roles/secrets; CMK.
- PRIV-7 med: cell index deviceCells fingerprint; tokens in SQS 14d incl DLQ; check-in token cleartext deviceId+cell.
- PRIV-8 med: no notice/consent/export (Gov Code 11015.5, Civ 1798.17, 1798.34).
- PRIV-9 low: stateless credential irrevocable -> tombstone deny-list.
- PRIV-10 low: logs: worker.ts:150 deviceId; String(err) at sender.ts:318,468, fanout handlers.ts:61, planner.ts:85, main.ts:168; redactPushTokens misses APNs/FCM.
- PRIV-11 low: no inactivity expiry; snapshots; cell index no TTL.
  Counsel: operator (SJSU/CSU/aux/state/county) decides IPA vs CCPA; minors; threshold/noise policy; backup purge SLA.

## SEC-C (Sonnet)

- SEC-C-1 high: compromised API Lambda can enqueue unsigned SendBatch for any active alert to any devices (has sqs:SendMessage urgent/standard + all tokens via DB + token keys). Sender doesn't check recipients in area. Fix: separate late-join queue with {alertId, deviceId} consumed by component that derives targets/tokens; or MAC SendBatch with key API lacks.
- SEC-C-2 high: execute-api bypasses WAF; Lambda runs even for 403; no reserved concurrency for API/planner/ingest/aggregator; throttle 20k > 10k quota -> flood starves planner. Fix: reserved concurrency, API cap; REST API+regional WAF or custom domain + disable default endpoint.
- SEC-C-3 high: no alarm actions anywhere, no SNS topic. Fix: SNS topic + subscription + attach to all alarms.
- SEC-C-4 high(pitch): no CloudTrail/GuardDuty/Config/SecHub; no CF/WAF/API/S3 access logs; no alarm on kill-switch/secret changes; 30d retention.
- SEC-C-5 med: seed-keys --rotate kid=k{count+1} overwrites live key after deletion (probe). Fix monotonic kid + refuse overwrite.
- SEC-C-6 med = PRIV-6: shared relay_admin, no rotation.
- SEC-C-7 med: symmetric alert-signing key held by 5 identities incl Fargate -> any can forge. Fix: KMS asymmetric sign, verifiers public key.
- SEC-C-8 med: Aurora backup retention 1 day default, single writer, no AWS Backup.
- SEC-C-9 med: default cloudfront cert TLSv1, no security headers, redirect-to-https.
- SEC-C-10 med: CI no permissions, tag-pinned actions; deploy pipeline/OIDC absent; bootstrap admin role.
- SEC-C-11 med: zone pack unsigned; buckets no versioning.
- SEC-C-12 low: WAF path transforms NONE; no IP reputation.
- SEC-C-13 low: origin secret rotation outage -> accept prev+current.
- SEC-C-14 low: sender readonlyRootFilesystem, image pin.
- SEC-C-15 low: Expo placeholder not validated; legacy delivery stack in prod; no termination protection.

## SEC-A (Sonnet) probes at scratchpad/probes/sec-a/

- SEC-A-1 high CONFIRMED(probe2): one phone mints unlimited check-in identities for any CA cell: POST /devices unauth + place anywhere + assignPushTarget moves token + late-join mints v2 token -> 25 check-ins counted. Fix: per-push-token device cooldown/limit, per-token late-join rate limit, attestation (App Attest/Play Integrity) later; flag cells.
- SEC-A-2 med: cred key rotation/revocation no fallback.
- SEC-A-3 med = SEC-C-5 seed-keys kid overwrite.
- SEC-A-4 med = PRIV-3 agency API not built; listTotals no county filter, client cursor; no recipients.
- SEC-A-5 med: publishPublic, GET /alerts, late-join don't verify signature (probe: unsigned row served + 25 late-join enqueued).
- SEC-A-6 med: origin guard / keyrings throw on Secrets Manager error after cache expiry -> all API 500 (no stale-if-error); rotation 403 until redeploy.
- SEC-A-7 low: guard off outside prod; default endpoint enabled.
- SEC-A-8 med = SEC-C-7 symmetric alert key.
- SEC-A-9 low: base64url signature malleable (lenient decode) -> exact-string compare.
- SEC-A-10 low: keyring accepts all-zero key; Expo placeholder unvalidated.
- SEC-A-11 low = PRIV-10 String(err) logs.
- SEC-A-12 low: **proto** key dropped in canonicalization -> Object.create(null).
  Clean: token forging, credential cross-use, signature coverage, APNs JWT, FCM OAuth, origin compare.

## SAFE (Sonnet) probe scratchpad/probes/safety/p1.ts

- SAFE-1 high: emergency tier never set for real NWS (Tornado/Flash Flood Emergency = TOR/FFW + parameters.tornadoDamageThreat/flashFloodDamageThreat CONSIDERABLE|CATASTROPHIC); normalize ignores parameters & capSeverity; upgrade doesn't re-notify. Fix: read damage threat + CAP Extreme -> emergency; key includes tier (already relaySeverity in key -> upgrade will re-push).
- SAFE-2 high: summaries use expires not ends; ignore onset ("in effect until 3 AM" wrong). Fix ends ?? expires; "starting {onset}".
- SAFE-3 high: Spanish empty for Tornado, Tsunami, Extreme Wind, High Wind, Winter Storm, Dust, Severe T-storm -> English headline with Spanish severity title. Fix: templates top ~25 CA events + bilingual titles.
- SAFE-4 med: no cancel push; cancelled alert summary regenerates "in effect". Fix: quiet "Cancelled by NWS" push w/ collapseKey; skip summaries if cancelled.
- SAFE-5 med: same-tier update silent (ends/instruction change).
- SAFE-6 med: Statement w/ Immediate+Severe never pushed; Air Quality Alert no push; IPAWS Evacuation Immediate/Blue Alert = statement. Fix promote by urgency/severity; allowlist EVI/CEM/CAE/LAE/SPW/LEW.
- SAFE-7 med = GOV-4: push text: no action/place/source; Spanish unlabeled; RELAY_SUMMARY_LABEL unused; title redundant "Warning: Tornado Warning".
- SAFE-8 med: raw description fallback with "* WHAT..." bullets; cut mid-word.
- SAFE-9 med: Spanish: 'Declaración'->'Comunicado'; 'inundación repentina'->'inundaciones repentinas'; 'Fíjese'->'Revise'; 12 a. m./p. m. -> medianoche/mediodía. Native review.
- SAFE-10 low: time ambiguity; add date if >24h, PT.
- SAFE-11 med: Freeze/Gale/Special Marine loud; "Practice/Demo Warning" loud if Actual; primary cell lexicographic.
- SAFE-12 med PLAUSIBLE: /alerts/active may drop cancelled instead of emitting Cancel.
- SAFE-14 low: summary advice beyond official ("Be ready to leave fast" for Red Flag).
- SAFE-15 low: eventCode SAME[0]="NWS" for non-SAME products; NationalWeatherService codes ignored.

## OPS (Sonnet) probes/ops

- OPS-1 critical = SEC-C-3: no alarm actions/SNS/dashboards/budgets.
- OPS-2 high: missing alarms: sender RunningTaskCount<desired, PermanentBreakerOpen, GaveUp, DeadTargetDropped, APNs 403/fcm auth, aggregator IteratorAge, planner/shard Errors, plan/standard/delivery queue age, Aurora resume, WAF blocks, cost; NWS 200-but-stale; planner/shard emit no metrics; planned-vs-sent.
- OPS-3 high: no operator recall/cancel/correction; kill switch unpause re-plans.
- OPS-4 high: prod deploy without -c senderEnabled=true -> DesiredCount 0 kills fleet. Persist in cdk.json; ECS deployment alarms disabled.
- OPS-5 high = SEC-C-8: Aurora 1-day backup, single writer, no cross-region; restore new ARN, no runbook.
- OPS-6 high: keyrings (token, alert, device-credential) not replicated; losing device cred keyring => mass re-register.
- OPS-7 high: kill switch runbook wrong: paused shard throws -> async retries exhaust -> shard DLQ within minutes; no confirm step; any value != 'paused' fails open (typo).
- OPS-8 med = SEC-C-5 seed-keys; APNs/FCM rotation needs force-new-deployment (undocumented).
- OPS-9 med: migrations autocommit per statement, half-applied, no lock; non-concurrent index.
- OPS-10 med: prod min ACU 0 resume vs 17s retry budget.
- OPS-11 med: runbooks missing (DLQ commands, NWS outage, region failure, zone pack rebuild).
- OPS-12 med = SEC-C-11 bucket versioning; 1-month logs.
- OPS-13 med: single region; recommended pilot vs statewide posture.
- OPS-14 high(budget): idle prod ~$410/mo (Fargate 10 tasks $288), pilot ~$450-560, statewide ~$3.5-4k; per 1M pushes ~$1-3; no budget alarm. Savings: senderMinTasks 3-4 pilot.
- OPS-15 low: idempotency warm throughput claimed in README but absent.

## SEC-B (Sonnet) probes/sec-b p1-p6

- SEC-B-1 critical(rated) = SEC-A-1 (rated high): forged check-ins at scale via device mint + late-join token. Consolidate: HIGH-CRIT. Fix: push token can't move to a new device within N hours (cooldown); per-token device cap; late-join rate limit per token; attestation later (decision).
- SEC-B-2 med: geocoder cost amp; address unbounded -> cap 200 chars, per-device geocode budget.
- SEC-B-3 med: fake devices flood urgent queue + trip breaker; no device retention.
- SEC-B-4 med CONFIRMED: 5-place cap race (10 stored on PGlite). Fix in SQL single statement w/ lock.
- SEC-B-5 med: degenerate geometry (collinear, <4 positions, Infinity) -> generic h3 error -> retried forever, never sent, ETag never commits (blocks whole feed!). Fix validate geometry in normalize; fallback to UGC; classify permanent.
- SEC-B-6 low-med: NUL in label -> 500; long tokens -> 500 (btree 2.7KB); label unbounded. Fix bounds -> 400.
- SEC-B-7 low: CA polygon rejects Needles/Blythe; geocoder (0,0) accepted; NaN -> 500.
- SEC-B-8 low: log route raw path -> log route pattern.
- SEC-B-10 low: unbounded ingest reads; hole rings not size-estimated.
- SEC-B-11 low: OpenAPI lacks bounds; drift.

## REL (Sonnet) probes/reliability p1,p2

- REL-1 high PLAUSIBLE: kill-switch read throws on SSM throttle (40 TPS std); 500 cold shards stampede SSM+Aurora; throws -> async retry 1m,2m -> DLQ; sender treats error as paused. Fix: SSM higher throughput, last-known-value with bounded staleness, jitter.
- REL-2 high: pause mid-fan-out -> shards to DLQ, no auto recovery after unpause (manual redrive); + no alarm actions. Fix auto-redrive on unpause or planner re-publish.
- REL-3 high: no transactional outbox: crash between Aurora commit and Dynamo write = no repair msg -> device never indexed (missed alert). Fix: outbox row in same Aurora transaction + drainer; scheduled reconcile with drift alarm.
- REL-4 med CONFIRMED: Cancel before original -> ignored+seen; original later created & pushed. Fix: cancelled tombstone by references.
- REL-5 med: batch key lacks sourceId -> material updates never re-pushed; area-grow moving primary cell duplicates.
- REL-6 med = REL token exp fixed at fan-out end; NWS extends expires -> 403 check-in. Fix: grace or check current end.
- REL-7 med CONFIRMED: unknown UGC zones silently dropped (partial area).
- REL-8 med = OPS-2 GaveUp/breaker no alarm.
- REL-9 med = OPS-10 Aurora min 0.
- REL-10 med: ingest serial; inline planDelivery no budget (don't use inline in prod).
- REL-11 med: no priority at shard stage (urgent vs standard share pool).
- REL-12 low: late-join duplicate (different key).
- REL-13 low: removal race eventual consistency.
- REL-14 med: tests don't prove exactly-once vs real DynamoDB.

## RED (Sonnet)

Threat model: troll, disinfo actor, state actor, insider, compromised dep/CI, confused operator.

- RED-1 crit: compromised API -> SendBatch -> real active alert to arbitrary devices (correction by orchestrator: SendBatch carries no text; text comes from the stored signed alert, so it's a real alert to the wrong people, not attacker text, unless combined with RED-2). Breaker: ID-only late-join queue / MAC.
- RED-2 crit: symmetric alert key held by 5 identities + seed-keys overwrite + no secret alarms -> durable signed forgeries. Breaker: KMS asymmetric.
- RED-3 high CONFIRMED: forged okay data (SEC-A-1). Breaker: token cooldown/caps.
- RED-4 crit(avail): execute-api flood + no reserved concurrency -> planner starved. Breaker: reserved conc + disable default endpoint.
- RED-5 high: kill switch typo fail-open / SSM throttle fail-closed / pause->DLQ. Breaker: strict enum + last-known value + CloudTrail alarm.
- RED-6 high: shared DB creds + exact coords forever + no delete.
- RED-7 high: supply chain (unpinned actions/image, no egress control/GuardDuty).
- RED-8 med-high: deploy without senderEnabled -> silent non-delivery.
  Cheapest chain-breakers: (1) alarms+SNS+CloudTrail+GuardDuty (2) ID-only late-join + KMS signing (3) per-token caps/cooldown (4) reserved concurrency + default endpoint off (5) least-priv DB roles + cell-only storage + TTL. Runners-up: strict kill-switch enum + LKV; SHA pins.

## VERIFY rel

- REL-1 PARTLY -> MEDIUM (SDK retries; ~1 min delay). Fix: stale-on-error cache; planner reads once & passes; SSM higher throughput.
- REL-2 CONFIRMED HIGH. Fix: paused shard re-enqueues with delay instead of throwing.
- REL-3 CONFIRMED HIGH. Fix: outbox/repair before commit or scheduled reconciler.
- REL-4 CONFIRMED MEDIUM (narrow trigger).
- OPS-4 CONFIRMED HIGH. Fix: prod default senderEnabled true; never derive DesiredCount to 0 on prod deploy.
- OPS-7 PARTLY -> MEDIUM (typo fail-open confirmed; README partly covers). Fix: strict 'running'|'paused' validation.
- OPS-6 PARTLY -> MEDIUM.

## VERIFY sec

- SEC-C-1 CONFIRMED HIGH (needs API RCE; replays real alert only). Fix: route late-join via scoped enqueue fn; remove API SQS send; or sender verifies recipients vs alert area.
- SEC-C-2 PARTLY -> MEDIUM. Fix: reserve concurrency planner/aggregator, cap API, disable default endpoint/resource policy.
- SEC-A-5 PARTLY MEDIUM (sender re-verifies; display only).
- SEC-B-5 PARTLY MEDIUM (Infinity refuted; collinear/<4 positions/TypeError real; per-feature catch, not blocking others).
- SEC-A-6 CONFIRMED MEDIUM -> stale-on-error up to 1h.
- SEC-A-1/B-1 CONFIRMED HIGH (critical once AG-04 ships; hard gate). Fix: one check-in token per (alertId, push target hash); rate-limit assignPushTarget per token; attestation later.

## VERIFY safe

- SAFE-1 CONFIRMED HIGH. parameters.tornadoDamageThreat / flashFloodDamageThreat arrays; CATASTROPHIC=emergency, CONSIDERABLE=PDS. Sources vlab CAP doc, CAP_v12 guide, Tornado-IBW.pdf.
- SAFE-2 CONFIRMED HIGH. ends ?? expires; "starting onset".
- SAFE-3 PARTLY HIGH (fallback is fully English, title too); both paths.
- SAFE-6 CONFIRMED MEDIUM -> promote statement to advisory when Immediate + Severe/Extreme.
- SAFE-12 PARTLY HIGH: no feed-membership reconciliation; CAP Cancel only for errors; routine end may just drop from /active. Fix: on 200, stored active CA alerts absent from feed (and not expired) -> mark ended + notify; read expiredReferences. Not on 304/error.
- GOV-4/SAFE-7 CONFIRMED HIGH: prefix "Resumen de Relay:" + "Fuente: NWS".

## VERIFY config: all 10 CONFIRMED (GOV-2, PRIV-1, PRIV-5, PRIV-6, OPS-1 (12 alarms no actions; README falsely says "pages"), OPS-5, OPS-6, SEC-C-9, OPS-4 (+SENDER_MAX_TASKS 0), OPS-15).

## PERF (Sonnet) probes/perf

Verdict: county 1.0M ~35-45 s warm w/ quotas raised; 55-60 s with default FCM; 1.9M 80-95 s. Statewide 10M: 130-150 s w/o FCM limit; 7-8 min with one FCM project.
Measured per task: APNs 7.4k/s, FCM 3.2k/s; shard 73-100k recipients/s.

- PERF-1 high: single FCM project caps 600k/min; quota raises hard. Fix: shard devices across N Firebase projects + per-project token bucket.
- PERF-2 high: sender re-verifies HMAC over whole alert per batch (6.4ms at 82k cells) -> memoize per (alertId, sourceId, signature).
- PERF-3 high: alert opens hit API+Aurora not CDN; statewide alert JSON 1.5MB (all cells). Fix: CDN URL in push data + contract; cell-free summary JSON.
- PERF-4 high: SSM stampede; cell index & idempotency tables lack warm throughput; claim() throttle throws outside try.
- PERF-5 high = OPS-10: prod min ACU 0 -> 15-30 s resume > 17.5 s retry. Fix 0.5 ACU (~$44/mo).
- PERF-6 high: ingest serial; publishPublic in finally; NWS asks <=1 req/30 s per URL but Relay polls every 12 s (ban risk). Fix: concurrent alerts, poll >=30 s (or NWWS).
- PERF-7 med: dense shard memory (2GB OOM ~1.2M/shard), sequential sendBodies; density assumption 20k/cell unsupported (SF/LA 75-150k).
- PERF-8 med: check-in tier below peak; separate check-in fn + provisioned concurrency.
- PERF-9 med: aggregator 2 WRU/record; totals no warm throughput.
- PERF-10 med: sender ramp 10->100 60-75 s; prescale urgent-only.
- PERF-11 med: statewide publish 1,700 Data API calls.
- PERF-12 low: docs overclaim (sim single process; 217 s).
  Cost per alert: county 1M ~$11-17 incl Fargate burst; state 10M ~$15-30; fixed ~$320/mo warm tasks.
