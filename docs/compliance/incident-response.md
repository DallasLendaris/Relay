# Incident response plan (draft)

> **Draft, pending review by SJSU counsel / the agency ISO.** This plan describes how the Relay team intends to respond. It is not yet adopted, has never been exercised, and several steps depend on tooling that does not exist at the audited commit (notably alarm notifications, OPS-1). The operator's ISO owns the final version.

## 1. Scope and honest starting point

Covered: security incidents (compromise, data exposure, abuse) and operational incidents (wrong alert, missed alert, outage) for Relay.

What is missing today, and affects every playbook:

- No alarm sends a notification. The 12 CloudWatch alarms have no actions and no notification topic exists (OPS-1, SEC-C-3). Detection currently means a human looking.
- No CloudTrail or GuardDuty, so there is no record of who changed the kill switch or a secret (SEC-C-4, GOV-10).
- No operator recall or cancel tool exists yet (OPS-3). A recall document is being written at [alert-recall.md](../runbooks/alert-recall.md) by another track; link target may not exist until that work merges.
- No on-call rota, no pager, no 24x7 coverage. A pilot cannot promise any response time until these exist.

## 2. Roles

| Role                      | Responsibility                                                                              | Today                                                  |
| ------------------------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------------ |
| Incident commander (IC)   | Runs the response, declares severity, decides pause and recall, owns the timeline.          | To be named. Capstone team lead until a client exists. |
| Engineering lead          | Diagnoses, fixes, executes playbook steps in AWS.                                           | Capstone engineers                                     |
| Communications lead       | Writes user and agency messages, talks to NWS, Cal OES, press.                              | To be named by the operator                            |
| Agency ISO / ISO delegate | Receives the report, files the Cal-CSIRS report if a state entity, approves breach notices. | The operator's ISO, not yet identified                 |
| Privacy officer           | Decides whether personal data is affected and what notice is required.                      | To be named                                            |
| Counsel                   | Advises on notification duties and liability.                                               | SJSU counsel, not yet engaged                          |
| Scribe                    | Writes down times, actions and decisions as they happen.                                    | Anyone not the IC                                      |

One person can hold several roles in a pilot. The IC must not also be the only engineer.

## 3. Severity levels

| Level | Meaning                                                                              | Examples                                                                                                                                                          | Target first response (once on-call exists) |
| ----- | ------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------- |
| SEV-1 | Risk to life or a public-facing safety failure, or confirmed personal data exposure. | Wrong or forged alert sent; evacuation sent to the wrong area; real alert not delivered during an event; signing key compromise; device or location data exposed. | 15 minutes, 24x7                            |
| SEV-2 | Degraded delivery or a security weakness being exploited, without confirmed harm.    | Sender fleet below desired count; DLQ growing; abuse flood against the API; NWS outage during active hazards.                                                     | 1 hour                                      |
| SEV-3 | Defect or weakness with no current impact on delivery or data.                       | Reported vulnerability, single failing batch, log leak.                                                                                                           | 1 business day                              |
| SEV-4 | Question or minor issue.                                                             | Documentation gap.                                                                                                                                                | 5 business days                             |

The first-response targets are proposals, not commitments, and no staffing exists to meet them yet.

## 4. Reporting path for a state entity

If the operator is a California state entity (whether SJSU or the CSU counts as one for this purpose is a counsel question), the CDT security policy page says the entity must **immediately** report the incident through Cal-CSIRS. Cal-CSIRS notifies the Office of Information Security (OIS) and the California Highway Patrol Computer Crimes Investigation Unit (CCIU). Reporting should not wait for complete information, and reporting outside Cal-CSIRS "is NOT an acceptable substitute". An Information Security Incident Report is due within ten business days of the Cal-CSIRS submission ([CDT security policy](https://www.cdt.ca.gov/security/policy/), fetched 2026-11-09).

| Step | Action                                                                                                                                                                                                                                                                                                                      |
| ---- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1    | The IC tells the operator's ISO as soon as a SEV-1 or a suspected security incident is declared.                                                                                                                                                                                                                            |
| 2    | The ISO (or delegate) files in Cal-CSIRS immediately, with whatever is known.                                                                                                                                                                                                                                               |
| 3    | If Cal-CSIRS is offline during business hours, email OIS at security@state.ca.gov. If it is offline outside business hours and immediate law-enforcement help is needed, call the CHP Emergency Notification and Tactical Alert Center (ENTAC), which the page lists at 916-843-4199, and notify OIS the next business day. |
| 4    | Breach notifications to affected people must be attached to the Cal-CSIRS incident for OIS review and approval before release. If more than 500 California residents are affected, a sample copy goes to the Attorney General (per the same page, citing Civil Code 1798.29 and 1798.82).                                   |
| 5    | Within ten business days of the submission, send the Information Security Incident Report (SIMM 5340-A gives instructions; not retrieved).                                                                                                                                                                                  |

The page does not mention CHP-ESU, and it does not give a Cal OES notification rule. A wrong alert is also a public-safety matter: tell Cal OES and the NWS forecast office (see the playbook). If the operator is not a state entity (for example a vendor running Relay for SJSU), the contract must say who files. That is a counsel question.

## 5. General procedure

1. **Detect and declare.** Anyone can declare. Write the time.
2. **Stabilize.** If the system may be sending harm, pause delivery first (playbook B), ask questions later.
3. **Scope.** What was affected: alerts, people, data, keys?
4. **Contain and fix.** Follow the playbook.
5. **Communicate.** Operator ISO, agencies, users, NWS if relevant.
6. **Recover.** See the [restore runbook outline](./business-impact-and-contingency.md#5-restore-runbook-outline).
7. **Review.** Blameless review within five business days. Record the cause, impact, timeline and follow-ups. If a control gap is found, add it to the [SSP gaps](./system-security-plan.md#5-known-gaps).

## 6. Playbooks

AWS commands below use `<stage>` and the region `us-west-2`. Most of them are quoted from `infra/README.md`. None has been run against a deployed system, because nothing is deployed.

### A. A bad alert was sent

Examples: a wrong or misleading summary, an alert to the wrong area, an alert sent for an NWS product that NWS already cancelled, a forged alert (also run the key compromise playbook).

1. **Pause** delivery (playbook B) if more pushes may still go out. Check how many were sent so far.
2. **Follow the recall procedure** in [docs/runbooks/alert-recall.md](../runbooks/alert-recall.md). (It is being written by another track. At the audited commit no operator recall exists, and cancellations are not pushed to phones, OPS-3, GOV-8. If the recall doc is not merged, the only tools are pausing delivery and updating or marking the alert in the database.)
3. **Decide the correction.** The user-facing message must be accurate, timely, and not impersonate NWS. Relay summaries are labelled "Relay summary" and cite NWS as the source (see [positioning](../positioning.md)).
4. **Notify.** The operator's ISO, the local NWS forecast office if the error is in how an NWS product was handled, and Cal OES if people may have been told to take the wrong protective action. For a state entity, file Cal-CSIRS if the cause is a security incident.
5. **Find the cause.** Was it an NWS error, a Relay summary or translation error (SAFE-1 to SAFE-9), a routing error, or a forgery?
6. **Do not delete evidence.** Keep the raw NWS archive, the Aurora row, and logs.
7. **Review.** Add a test that reproduces it.

### B. Kill switch (stop all delivery)

Stop all push delivery:

    aws ssm put-parameter --region us-west-2 --name /relay/<stage>/delivery-kill-switch \
      --type String --value paused --overwrite

Facts from the code and README, as built:

- Workers and senders pick the change up in about 30 seconds.
- Only the exact value `paused` stops delivery. **Any other value, including a typo, means running** (OPS-7). After the write, read the parameter back and confirm it says `paused`.
- A paused sender stops pulling new work. Shard workers that run while paused fail, and after their retries they land in the shard DLQ, and a reading failure on the parameter is treated as paused by the sender (REL-1, REL-2). Plan to redrive the DLQs after resuming (see `infra/README.md`, "Runbook: shard DLQ not empty" and the send DLQ runbook).
- Alerts that were not planned stay unseen, so the next ingest poll replans them after you resume. Think before resuming: a resume can send an alert that has since been cancelled or corrected.

To resume, write `running` the same way, then redrive DLQs, then watch the sender task count and the DLQ alarms. Record who paused, when, and why. There is no audit trail for the change at the audited commit (no CloudTrail), so write it down yourself.

### C. Key compromise and rotation

Keys and secrets and what each protects:

| Secret                                          | Compromise means                                                                                             |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `relay/<stage>/alert-keys` (HMAC)               | Someone can sign a forged alert. Five identities can read it, including the Fargate sender (SEC-C-7, RED-2). |
| `relay/<stage>/token-keys`                      | Someone can mint check-in tokens.                                                                            |
| `relay/<stage>/device-credential-keys`          | Someone can derive device credentials and impersonate devices.                                               |
| `relay/<stage>/apns-key`, `fcm-service-account` | Someone can send pushes to your app as Relay.                                                                |
| Aurora admin secret (`relay_admin`)             | Full read and write on all personal data (PRIV-6).                                                           |
| `relay/<stage>/origin-verify`                   | The CloudFront origin guard can be bypassed.                                                                 |

Steps:

1. Declare SEV-1. Pause delivery (playbook B) if alert or push keys are involved.
2. Preserve evidence: secret version history, any CloudTrail data if it exists (it does not by default), logs.
3. **Rotate the HMAC keyrings:** `npm run seed-keys -- <stage> --rotate` in `infra/` adds a new key id and makes it current. Old keys stay in the keyring so existing tokens still verify, and loaders cache for about 5 minutes (`infra/README.md`). For a real compromise you must then **remove the old key id** from the keyring so forgeries stop verifying. That invalidates tokens and alerts signed by it, so plan the user impact: lost check-in links for open alerts, re-signing of active alerts.
4. **Known defect:** `--rotate` names the new key `k{count+1}`, so after a key has been deleted it can overwrite a live key (SEC-C-5, OPS-8). Check the keyring contents with `aws secretsmanager get-secret-value` before and after, and do not delete a key id and then rotate again without checking.
5. **Device-credential keyring:** rotating without keeping the old key forces every device to re-register (OPS-6, SEC-A-2). Keep the old key during a normal rotation. Remove it only for a confirmed compromise, and tell users.
6. **APNs and FCM:** create a new key in the Apple or Google console, write it to the secret, and **force a new sender deployment** because the sender caches it (undocumented, OPS-8). Revoke the old key.
7. **Aurora:** rotate the admin secret in Secrets Manager. There is no rotation schedule today. Check that all consumers re-read it, because they cache.
8. **Origin-verify secret:** change the primary, redeploy `edge`, expect up to five minutes of 403s (`infra/README.md`).
9. Check for forged alerts or check-ins in the window. Re-run the audit probes in the capstone scratch area if available.
10. Report per section 4 if a state entity.

### D. Personal data exposure

Examples: a database snapshot or table made public, a log leak, an API bug returning other people's places, a stolen credential used to read tables.

1. Declare SEV-1 and tell the privacy officer and ISO at once.
2. **Contain:** remove the exposure path (revoke access, make a bucket private, disable the endpoint, rotate credentials per playbook C). If the API is leaking, pause the affected route or deploy a block.
3. **Scope:** what data and whose? The data inventory in the [SSP](./system-security-plan.md#3-data-inventory) lists what could be involved: push tokens, saved places (exact coordinates and address as built), check-ins with device and cell, device ids. No names or contact details are stored. Without CloudTrail or access logs (SEC-C-4) scoping may not be possible, which should be stated truthfully.
4. **Notify:** the operator's ISO and, for a state entity, Cal-CSIRS immediately (section 4). Counsel decides whether the breach notification statutes (Civil Code 1798.29 and 1798.82 for state agencies and others, per the CDT page) are triggered. Whether location data plus a device id counts as "personal information" under those statutes is a counsel question; do not assume either way. Notices to users need OIS review first for a state entity.
5. **Notify users** through the app if counsel agrees. There is no email or phone contact for users; reach them by an in-app notice.
6. **Review:** fix the cause, shorten retention (see the [privacy analysis](./privacy-threshold-analysis.md)), add a test.

### E. NWS outage or stale feed

Relay depends on one upstream source for alerts. Three different failures look different:

| Failure                               | Symptom                                     | Detection today                                                                                 |
| ------------------------------------- | ------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| NWS returns errors or times out       | Ingest errors; alerts not seen as processed | An ingest error alarm exists with no action (OPS-1).                                            |
| NWS returns 200 but the feed is stale | No new alerts, no error                     | Not detected (OPS-2). This is the dangerous one.                                                |
| Malformed or malicious content        | Validation rejects features                 | Logged; one bad feature should not block the others, but SEC-B-5 found some geometry that does. |

Steps:

1. Confirm independently: check api.weather.gov in a browser and the NWS alerts page. Compare with local TV or radio and Cal OES channels.
2. Decide a public statement. Relay must not imply it is checking when it is not. If the outage is longer than a few minutes during an active hazard, post a notice in the app (once a status mechanism exists; not built) and tell agencies.
3. Do not work around the outage by inventing alerts or sourcing from unofficial feeds. NWS content must not be modified and presented as official ([NWS disclaimer](https://www.weather.gov/disclaimer), fetched 2026-11-09).
4. The ingest design retries: an alert is marked seen only after planning succeeds, so the next poll picks up what was missed when NWS returns. Check the ingest logs after recovery for backlog and check the cancelled-before-original case (REL-4).
5. Record the duration. When IPAWS access exists (see [positioning](../positioning.md)) it can be a second source; it is not built.
6. Tell users to rely on WEA, EAS, local authorities and NOAA Weather Radio when Relay cannot be trusted to be current.

## 7. Exercises and records

- Hold one tabletop of each playbook before a pilot. Record gaps in the SSP.
- Keep an incident log: date, severity, summary, cause, actions, time to detect, time to recover, follow-ups.
- No test or exercise mode exists for drills (GOV-12). A drill against production would send real pushes. Use the dev stage.

## 8. Contact placeholders

| Who                       | Contact                              |
| ------------------------- | ------------------------------------ |
| Incident commander        | To be filled in                      |
| Operator ISO              | To be filled in                      |
| Privacy officer           | To be filled in                      |
| Counsel                   | To be filled in                      |
| Cal OES contact           | To be filled in                      |
| Local NWS forecast office | To be filled in                      |
| Vulnerability reports     | See [SECURITY.md](../../SECURITY.md) |
