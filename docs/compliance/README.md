# Compliance documents (drafts)

> **Draft, pending review by SJSU counsel / the agency ISO.** Everything in this folder was written by the engineering team from the code at commit `74acc5c`, the audit findings and public sources. None of it claims compliance, certification or approval. Nothing is deployed.

These documents prepare the package for the California Cloud Services Assessment (CCSA). The CCSA's required uploads, as listed by CDT on 2026-11-09 ([CDT CCSA page](https://cdt.ca.gov/services/ccsa)), and where each stands:

| CCSA artifact                                       | Status                                                                                                                                                                           |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Cloud Services Assessment Questionnaire             | Not started. Needs the operator.                                                                                                                                                 |
| FIPS 199 classification form                        | Draft: [fips199-categorization.md](./fips199-categorization.md)                                                                                                                  |
| Cloud Alternative Analysis                          | Not started. Needs the operator.                                                                                                                                                 |
| Cloud Architecture and Network Diagram              | Draft diagram inside the [SSP](./system-security-plan.md#2-architecture-and-data-flow). The network detail (no VPC for Lambdas, isolated DB subnets) is described but not drawn. |
| Cloud System Security Plan (CSSP)                   | Draft outline: [system-security-plan.md](./system-security-plan.md)                                                                                                              |
| Organizational chart of cloud-related staffing      | Not started. Needs the operator.                                                                                                                                                 |
| Privacy Threshold and Impact Analysis (SIMM 5310-C) | Draft, not on the SIMM form: [privacy-threshold-analysis.md](./privacy-threshold-analysis.md)                                                                                    |
| Well-Architected assessment                         | Not possible until an AWS account exists.                                                                                                                                        |
| Technology Recovery Plan (validated during CCSA)    | Input draft: [business-impact-and-contingency.md](./business-impact-and-contingency.md)                                                                                          |

## Index

| Document                                                                   | What it is                                                                           |
| -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| [system-security-plan.md](./system-security-plan.md)                       | Boundary, diagram, data inventory, controls by family, every known gap.              |
| [fips199-categorization.md](./fips199-categorization.md)                   | Proposed categorization: C moderate, I high, A high.                                 |
| [privacy-threshold-analysis.md](./privacy-threshold-analysis.md)           | What is collected, retention, sharing, user rights, privacy questions.               |
| [business-impact-and-contingency.md](./business-impact-and-contingency.md) | RPO and RTO as built, pilot versus statewide posture, restore runbook outline.       |
| [incident-response.md](./incident-response.md)                             | Roles, severities, Cal-CSIRS path, five playbooks.                                   |
| [control-map.md](./control-map.md)                                         | NIST 800-53 Rev. 5 moderate gap map ([summary](./control-map-summary.md)).           |
| [cdk-nag-exceptions.md](./cdk-nag-exceptions.md)                           | cdk-nag NIST 800-53 R5 exceptions register, keys, TLS and FIPS endpoints.            |
| [ci-security-scanning.md](./ci-security-scanning.md)                       | CI scanning (npm audit, SBOM, gitleaks, CodeQL) and first findings.                  |
| [review-2026-11.md](./review-2026-11.md)                                   | Independent review of the compliance-foundation work.                                |
| [../positioning.md](../positioning.md)                                     | Pitch position, IPAWS role, NWS attribution, disclaimer wording, earthquake framing. |
| [../../SECURITY.md](../../SECURITY.md)                                     | Vulnerability disclosure policy (draft).                                             |
| [../runbooks/alert-recall.md](../runbooks/alert-recall.md)                 | Alert recall runbook, written by another track (may not exist until it merges).      |

## How to read the status words

- **Built**: in the code at the audited commit.
- **Fix planned**: an audit finding with a fix assigned in the audit-fix round. Re-check against `main`.
- **Not built**: absent.

## Verified sources and what is unverified

Fetched on 2026-11-09 and used: the CDT CCSA page, the CDT information security policy page, the NWS disclaimer, FEMA's AHIF page, FEMA's IPAWS sign-up page, the FEMA IPAWS Process Map Playbook, NIST FIPS 199, ShakeAlert, and a third-party copy of Gov Code 11015.5.

Not retrieved or not confirmed (treat as unverified wherever they appear): the text of SAM 5300, SIMM 5315-B, SIMM 140, SIMM 5340-A, SAM 4983.1 and SIMM 5310-C itself; the official leginfo text of Gov Code 11015.5 (the leginfo page returned an error); the California language statutes (Gov Code 8594.16 and 7299.7), the Information Practices Act and CPRA provisions cited in the audit; Cal OES's role in IPAWS approval; AWS service limits for PITR, backups and failover; ShakeAlert delivery by WEA.

The audit ledger itself is not in the repository. Finding IDs in these documents refer to it.

## Decisions and questions

Consolidated from every auditor's questions in the ledger (GOV, PRIV, SEC, OPS, REL, SAFE, RED, DEP) plus the questions raised while writing these drafts. "Who" is the party who has to answer.

| #    | Question or decision                                                                                                                                                                                                      | Who                 | Source                 | Needed before |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------- | ---------------------- | ------------- |
| Q-1  | Who operates Relay: SJSU or CSU, an auxiliary, a state agency, a county, or a vendor? This decides IPA or CCPA, who the ISO is, and who files incident reports.                                                           | Client, counsel     | GOV counsel, PRIV      | Anything      |
| Q-2  | Is the service SaaS provided to a state entity or operated by one?                                                                                                                                                        | Client, counsel     | GOV                    | CCSA          |
| Q-3  | IPA versus CCPA/CPRA for check-ins, saved places and push tokens. Does Gov Code 11015.5 apply (it names the CSU)?                                                                                                         | Counsel             | PRIV-8, GOV            | Pilot         |
| Q-4  | Is precise geolocation under the CPRA triggered by exact coordinates, by an H3 cell of about 5 km2, or by a check-in record?                                                                                              | Counsel             | PRIV (CPRA)            | Pilot         |
| Q-5  | Minors: age floor, COPPA or CCPA minors rules, parental notice.                                                                                                                                                           | Counsel             | PRIV                   | Pilot         |
| Q-6  | Retention: is 30 days after the alert right for check-ins? Inactivity expiry length? Records-retention rules that conflict with deletion?                                                                                 | Counsel, client     | GOV-2, PRIV-2, PRIV-11 | Pilot         |
| Q-7  | Backup purge window after a deletion request (Aurora snapshots, DynamoDB PITR).                                                                                                                                           | Counsel, ISO        | PRIV                   | Pilot         |
| Q-8  | Public records and law-enforcement requests: what can be produced, and what is exempt?                                                                                                                                    | Counsel             | Drafting               | Pilot         |
| Q-9  | Small-count threshold and noise policy for agency aggregates, and who approves it.                                                                                                                                        | Client, ISO         | PRIV-3, PRIV-4         | Agency access |
| Q-10 | Do the language statutes (Gov Code 8594.16 and 7299.7, AB 1638, as cited in the audit) bind a vendor or a pilot, and which languages are required?                                                                        | Counsel, Cal OES    | GOV-7, SAFE-3          | Pilot         |
| Q-11 | Liability and disclaimer wording, warranty, terms of service, who indemnifies whom.                                                                                                                                       | Counsel             | GOV-9                  | Pilot         |
| Q-12 | Is a letter from or approval by Cal OES needed or wanted for a pilot? Which county Alerting Authorities (COGs) should be consulted?                                                                                       | Client, Cal OES     | GOV counsel            | Pilot         |
| Q-13 | IPAWS: confirm Role 1 (redistributor via AHIF with an MOA), and ask FEMA whether Relay is eligible. Who is the Alerting Authority if Role 2 is ever wanted?                                                               | Client, counsel     | GOV-5                  | Statewide     |
| Q-14 | Agency authentication: Cognito, state SSO, or agency-provided identity? Which agencies, and who approves access?                                                                                                          | Client              | GOV-6, SEC-A-4         | Agency access |
| Q-15 | Attestation (App Attest, Play Integrity) to stop forged check-ins: accept the cost and device exclusions, or rely on per-token limits only?                                                                               | Client, engineering | SEC-A-1, SEC-B-1       | Agency access |
| Q-16 | Is "Relay summary" labelling with an NWS source line acceptable to NWS and the clients? Who reviews the Spanish text (native speaker)?                                                                                    | Client, counsel     | GOV-4, SAFE-9          | Pilot         |
| Q-17 | RTO and RPO targets, and pilot single-region versus a second region.                                                                                                                                                      | Client              | OPS-13, OPS-5          | Pilot         |
| Q-18 | Who owns the offline copy of the device-credential and alert keyrings, and where is it kept?                                                                                                                              | ISO, engineering    | OPS-6                  | Pilot         |
| Q-19 | Who is on call, and what response time can the operator promise? Is there a paging service, and who pays?                                                                                                                 | Client              | OPS-1, GOV-11          | Pilot         |
| Q-20 | Budget: idle prod is about $410 per month, a pilot about $450 to $560, statewide about $3,500 to $4,000 (audit estimates). Who funds it, and what is the budget alarm threshold? Is `senderMinTasks` reduced for a pilot? | Client              | OPS-14                 | Pilot         |
| Q-21 | Audit logging: CloudTrail, GuardDuty, access logs, and the log retention period the ISO requires.                                                                                                                         | ISO                 | SEC-C-4, GOV-10        | CCSA          |
| Q-22 | Is FIPS 199 confidentiality moderate or high (keys and secrets as a separate asset class, or raise the whole system)?                                                                                                     | ISO                 | This folder            | CCSA          |
| Q-23 | Which security standard set applies (SIMM 5315-B, SIMM 140, SAM 5300 series), and does CSU follow it or its own CSU policy?                                                                                               | ISO, counsel        | Drafting               | CCSA          |
| Q-24 | Third-party processors (AWS, Apple, Google, geocoder): acceptable, and what contract terms are needed? Which geocoder, and is its retention acceptable?                                                                   | Counsel, ISO        | SSP                    | Pilot         |
| Q-25 | Cancellations: push a "cancelled" notice quietly, and within what time? (Cal OES principles referenced in the audit cite retraction within 10 minutes; unverified.)                                                       | Client, Cal OES     | GOV-8, SAFE-4          | Pilot         |
| Q-26 | Operator recall policy: who may recall or correct a sent alert, and how fast must it happen?                                                                                                                              | Client              | OPS-3                  | Pilot         |
| Q-27 | An exercise or test mode for drills: when and how, and how are users told a drill is not real?                                                                                                                            | Client, Cal OES     | GOV-12                 | Pilot         |
| Q-28 | Earthquake: is a post-event check-in feature wanted, who triggers it, and how is it kept from reading as early warning?                                                                                                   | Client              | Positioning            | Later         |
| Q-29 | Vulnerability disclosure: who owns `security@` and the safe-harbor wording, and does SJSU or the operator adopt it?                                                                                                       | Counsel, ISO        | GOV-11                 | Pilot         |
| Q-30 | Supply chain: accept the CI and image-pinning work (DEP-3, DEP-4, SEC-C-10) as pilot gates? Where does the deploy pipeline and OIDC role live (S-06)?                                                                     | Engineering, ISO    | DEP, SEC-C-10          | Pilot         |
| Q-31 | Account structure: separate AWS accounts per stage, break-glass, SSO for staff, termination protection.                                                                                                                   | Engineering, ISO    | SEC-C-10, SEC-C-15     | Pilot         |
| Q-32 | Where may data be stored (US regions only, California residency)? Is AWS GovCloud or a state-approved region needed?                                                                                                      | ISO, counsel        | Drafting               | CCSA          |
