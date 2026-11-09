# Business impact analysis and contingency plan (draft)

> **Draft, pending review by SJSU counsel / the agency ISO.** Targets below are what the system achieves **as built**, not commitments. The CCSA says the Technology Recovery Plan is validated during the assessment ([CDT CCSA page](https://cdt.ca.gov/services/ccsa), fetched 2026-11-09), and the State's technology recovery policy is SAM 5325 ([CDT policy page](https://www.cdt.ca.gov/security/policy/), fetched 2026-11-09, link only; SAM 5325 itself not retrieved). This document is the engineering input to that plan.

## 1. Business impact

Relay's job is time-critical. The impact of an outage depends on when it happens.

| Scenario                        | Impact                                                                                                                                             |
| ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Outage with no active hazard    | Low. No alerts are missed because none exist. Saved places can be re-added.                                                                        |
| Outage during an active hazard  | High. People who rely on Relay for a faster or more specific NWS summary do not get it. WEA and EAS continue, because Relay does not replace them. |
| Data loss of devices and places | High for the affected people: they receive nothing until they re-register, and they may not know.                                                  |
| Data loss of check-ins          | Medium. Agencies and families lose "I'm okay" data for recent alerts. Safety is not directly affected.                                             |
| Wrong alert sent (integrity)    | High. Handled by the [incident response playbooks](./incident-response.md), not by recovery.                                                       |

Proposed targets once an operator exists (to be agreed with the agency; none is met today):

| Objective                              | Pilot               | Statewide                                                                                                           |
| -------------------------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------- |
| Alert delivery during an active event  | Best effort, no SLA | 99.9% of alerts sent within 120 s of NWS publish (the design goal in `docs/scale-plan.md`; not yet measured on AWS) |
| Recovery time objective, whole service | 8 hours             | 1 hour                                                                                                              |
| Recovery point objective, device data  | 1 hour              | 5 minutes                                                                                                           |

## 2. Components as built

RPO is how much data can be lost. RTO is how long recovery takes. "As built" values are read from `infra/lib/*.ts` at commit `74acc5c` and from the audit. Cloud service guarantees marked unverified were not checked against AWS documentation in this draft.

| Component                                          | Backup as built                                                                                                                                                               | RPO as built                                                                                                     | RTO as built                                                                                                                                                                     | Finding        |
| -------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- |
| Aurora Postgres (alerts, devices, places)          | Default automated backups with **1-day** retention. Fix planned: 14 days with PITR. No cross-region copy. Prod keeps a final snapshot on removal and has deletion protection. | Up to the PITR granularity (about 5 minutes, unverified) while inside the window. Nothing older than the window. | Hours. A restore creates a **new cluster with a new ARN**, which the stacks and secrets must be pointed at. No runbook or drill exists.                                          | OPS-5, SEC-C-8 |
| Aurora single writer, no replica                   | None                                                                                                                                                                          | n/a                                                                                                              | An instance or AZ failure means a cluster recovery of minutes to hours (unverified).                                                                                             | SEC-C-8        |
| DynamoDB cell index                                | PITR on in prod                                                                                                                                                               | Seconds to minutes (unverified)                                                                                  | Rebuildable from Aurora with the backfill script (`backend/src/scale/cell-index/backfill.ts`). Duration scales with device count, not measured on AWS.                           | REL-3          |
| DynamoDB check-ins                                 | PITR on in prod; no TTL as built                                                                                                                                              | Seconds to minutes                                                                                               | Restore creates a new table. Check-ins are recent data; loss is acceptable for hours.                                                                                            | GOV-2          |
| DynamoDB check-in totals                           | PITR on in prod; TTL set by aggregator                                                                                                                                        | Seconds to minutes                                                                                               | Rebuilt from check-ins only if the check-ins survive; the stream retains records for a short period (unverified).                                                                | PRIV-3         |
| DynamoDB ingest state                              | PITR on in prod; rows expire with the alert                                                                                                                                   | Seconds to minutes                                                                                               | If lost, ingest re-processes the live NWS feed. Duplicate sends are blocked by the sender's batch claims, which live in the sender's dedupe store (not reviewed for this draft). | -              |
| S3 raw NWS archive                                 | None (no versioning). Moves to infrequent access after 30 days.                                                                                                               | Unbounded                                                                                                        | Not needed to serve alerts. Audit and replay value only.                                                                                                                         | SEC-C-11       |
| S3 zone pack                                       | None (no versioning)                                                                                                                                                          | Rebuildable                                                                                                      | Rebuild with `backend/scripts/build-zone-pack.ts --upload`. Ingest falls back to NWS for zones.                                                                                  | SEC-C-11       |
| S3 public alerts (CloudFront origin)               | None (no versioning)                                                                                                                                                          | Rebuildable from Aurora                                                                                          | Republish active alerts from Aurora. Region is `us-east-1`.                                                                                                                      | SEC-C-11       |
| Secrets (token, alert, device-credential keyrings) | Not replicated, no scheduled backup. Only the origin-verify secret is replicated to `us-east-1`.                                                                              | Total loss possible                                                                                              | Losing the **device-credential keyring** invalidates every installed device: mass re-registration. Losing the alert keyring invalidates signatures on stored alerts.             | OPS-6          |
| Push credentials (APNs key, FCM service account)   | Held in Apple and Google accounts                                                                                                                                             | None                                                                                                             | Reissue from Apple or Google, then force a new sender deployment (undocumented).                                                                                                 | OPS-8          |
| Lambdas, Fargate sender, SQS, WAF, CloudFront      | Defined in CDK                                                                                                                                                                | n/a (stateless)                                                                                                  | Redeploy from source. Deploys are manual, and a prod deploy without `-c senderEnabled=true` sets the fleet to zero.                                                              | OPS-4          |
| NWS api.weather.gov (external)                     | n/a                                                                                                                                                                           | n/a                                                                                                              | Outside Relay's control. Ingest keeps polling. A stale-but-200 feed is not alarmed.                                                                                              | OPS-2          |

Queues: SQS retains messages for 14 days, with dead-letter queues. A paused kill switch or a failure moves work to the DLQ and recovery is manual (REL-2).

## 3. Single-region posture (OPS-13)

As built, Relay runs in `us-west-2`, with CloudFront, WAF and the public alert bucket in `us-east-1` because WAF for CloudFront must live there (comment in `infra/lib/edge-stack.ts`). There is no second region for compute or data.

What a regional outage in `us-west-2` does:

- Ingest, planner, shard workers, the sender, the API for registration and check-in, Aurora and the cell index all stop.
- Alerts already published to the `us-east-1` bucket keep being served through CloudFront, but nothing new is published.
- Users already holding a push notification are unaffected.

AWS regions have rarely failed entirely (unverified), and AZ-level failures are more likely and are partly covered by Aurora and DynamoDB multi-AZ behavior (unverified).

## 4. Pilot versus statewide posture

| Topic                                          | Pilot (county, up to about 500,000 devices)                                                                              | Statewide (up to 10 million devices)                                                                                                  |
| ---------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------- |
| Regions                                        | One region. Accept a regional outage as a known risk and say so in the pilot agreement.                                  | A warm standby in a second region, chosen by the operator. Candidates are `us-west-1` or `us-east-2`; not evaluated.                  |
| Relay's role                                   | One of several channels. WEA and EAS remain primary.                                                                     | Same. A statewide claim of resilience needs a tested failover.                                                                        |
| Database                                       | Aurora PITR at 14 days, snapshot copy to a second region on a schedule (AWS Backup).                                     | Cross-region replica (for example Aurora Global Database) or tested snapshot-copy restore (unverified feasibility with the Data API). |
| DynamoDB                                       | PITR.                                                                                                                    | Global tables for the cell index and check-ins (unverified: check cost and consistency).                                              |
| Secrets                                        | Replicate keyrings to the second region. Store an offline copy of the device-credential keyring with the ISO.            | Replicated and tested.                                                                                                                |
| Test                                           | One restore drill before go-live. Written report.                                                                        | Annual failover exercise, quarterly restore drill.                                                                                    |
| Cost (audit estimate, OPS-14, not re-verified) | About $450 to $560 per month for a pilot; the idle prod fleet is about $410 per month (Fargate about $288 for 10 tasks). | About $3,500 to $4,000 per month for statewide, before a second region.                                                               |

Recommended: ship the pilot single-region with the specific mitigations below, and state plainly that regional failover is not built. Do not claim high availability to a state reviewer before a restore drill has been run.

Pilot mitigations, in order: Aurora backup retention to 14 days; alarm actions and a notification topic (OPS-1); a restore drill and runbook; keyrings backed up; bucket versioning; per-component restore tested; `senderEnabled` default true for prod.

## 5. Restore runbook outline

This is an outline. It must be turned into a tested runbook (OPS-11) and the commands filled in once an AWS account exists. Nothing below has been executed.

**Preconditions:** the AWS accounts exist; alarms notify a human; the kill switch parameter is `/relay/<stage>/delivery-kill-switch` (`running` or `paused`).

1. **Declare and pause.** Declare an incident ([incident response](./incident-response.md)). If recovery will take more than a few minutes and alerts may be stale or wrong, set the kill switch to `paused`. Note that, as built, a pause makes in-flight shards fail into the DLQ (REL-2, OPS-7), so plan to redrive after unpausing.
2. **Assess.** Which store is damaged: Aurora, a DynamoDB table, an S3 bucket, a secret? Is it corruption, deletion or a regional outage? Record the time of the last good state.
3. **Aurora:**
   - Restore to a point in time before the damage into a **new** cluster. Do not overwrite the damaged cluster until the new one is verified.
   - Capture the new cluster ARN and secret ARN.
   - Point the stacks at the new ARN (a `data-stack` change and redeploy of dependents), or use a mechanism that keeps the ARN stable once one is designed.
   - Check that migrations through `0007` are applied (`backend/migrations/`).
   - Confirm that Data API access is enabled and the roles work.
4. **Cell index.** After any Aurora restore, rebuild the cell index from Aurora with the backfill script, then let the repair queue catch late changes. Compare counts of indexed devices with Aurora.
5. **Check-ins and totals.** Restore the check-ins table with PITR to a new table if needed and repoint. Totals are rebuilt from check-ins; document that agency totals may be undercounted until then.
6. **Secrets.** Restore keyrings from the offline copy. Never regenerate the device-credential keyring unless every device is meant to re-register. See the key compromise playbook in the incident response document.
7. **Buckets.** Rebuild the zone pack, republish active alerts to the public bucket.
8. **Verify.** Run a synthetic alert in the dev stage (or the planned exercise mode, GOV-12) before sending real traffic. Check the sender fleet is running at the expected count (`senderEnabled=true`).
9. **Unpause and redrive.** Set the kill switch to `running`. Redrive the plan, shard and send DLQs (REL-2). Watch the alarms.
10. **Record.** Write the timeline, data lost and follow-up actions. If personal data was lost or exposed, follow the data exposure playbook.

## 6. Open items

| #   | Item                                                                | Owner               |
| --- | ------------------------------------------------------------------- | ------------------- |
| B-1 | Agree RTO and RPO targets with the agency.                          | Client              |
| B-2 | Decide pilot single-region versus second region.                    | Client              |
| B-3 | Run and record one restore drill.                                   | Engineering         |
| B-4 | Document key backup custody (who holds the offline keyring copy).   | Engineering and ISO |
| B-5 | Write the State Technology Recovery Plan using the agency template. | ISO                 |
