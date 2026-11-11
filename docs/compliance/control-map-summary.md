# Control gap map: one-page summary

> **Draft, pending review by SJSU counsel / the agency ISO.** This is a plain-language summary of [control-map.md](./control-map.md). It measures Relay against the NIST SP 800-53 Rev. 5 **moderate** baseline (release 5.2.0, 287 controls and enhancements) as of commit `be6bc32`, with the rows the compliance-foundation work changed updated after the 2026-11 review ([review-2026-11.md](./review-2026-11.md)). Nothing is deployed, so "built" means "in the code", not "proven in AWS".

## Where Relay stands

| Category                                                      | Count | Status                                       |
| ------------------------------------------------------------- | ----: | -------------------------------------------- |
| Engineering: code or AWS configuration Relay builds           |   133 | 15 Built, 72 Partial, 46 Missing             |
| Organizational: policy, people, process                       |   108 | 17 Drafted, 91 Not started                   |
| Inherited: AWS provides it (Relay still owns a customer part) |    46 | Inherited (mostly physical, media, hardware) |
| **Total in the moderate baseline**                            |   287 |                                              |

The map also lists 50 rows outside the moderate baseline so nothing is missed: Program Management (PM, which no baseline includes), the privacy baseline's PT controls, and SR-4.

What this means:

- **The engineering is further along than the paperwork.** The code has least-privilege IAM, per-function database roles, KMS-signed alerts, CloudTrail with alerts on kill-switch, secret and key changes, GuardDuty, input bounds and redacted logs. Most of the 71 Partial rows are in that state because the human side is missing. No one has an account yet, and there is no deploy pipeline.
- **Almost no organizational work exists.** Apart from the drafts in this folder (SSP, FIPS 199, PTA, BIA, incident response, SECURITY.md), there are no policies, no training and no personnel rules. None of that can be done in code.
- **The compliance-foundation work has merged** (cdk-nag, customer-managed KMS, TLS 1.2+, FIPS endpoints, rotation runbooks, CI scanning and an SBOM). Of the 13 rows it targeted, SC-28 and SC-28(1) are now Built, SR-4 moved from Missing to Partial, and the other ten stay Partial with smaller gaps; they are marked "Updated (compliance-foundation)" in the map. The review also downgraded two overstated Built rows (AU-9, SI-11) to Partial, so the engineering counts were unchanged from `be6bc32` at that review. On `b2-hardening` SI-11 became Built again (the delivery worker no longer logs error text), which gives the counts above.

## Top 10 engineering gaps for a summer 2027 single-county pilot

Ranked by how much each one blocks or endangers a real pilot. "Planned" means this branch already starts it.

1. **Human access foundation** (AC-2, IA-2, IA-2(1), AC-17, MA-4, AU-9(4)). Create the AWS accounts (S-06): a separate prod account, IAM Identity Center with MFA, a break-glass role and a log-archive account. Every other control assumes this exists.
2. **Deploy pipeline with approvals** (CM-3, CM-5, AC-17(4), SA-10). Built, not yet run: `.github/workflows/deploy.yml` (OIDC roles in `infra/bootstrap/`, no long-lived keys, `cdk diff` in the run summary, prod behind required reviewers; [deploy runbook](../runbooks/deploy.md)). Still needed: the AWS accounts (S-06), the GitHub environments, reviewers and branch protection on `main`, a narrower CloudFormation execution role than the bootstrap default, and a migration runner with a lock (OPS-9). CM-5 and AC-17(4) moved from Missing to Partial.
3. **Close the WAF bypass** (SC-7, SC-7(3), SC-5, CM-7). Disable the default `execute-api` endpoint so that nothing reaches the API Lambda without passing CloudFront (SEC-C-2, SEC-A-7). The code is ready behind `-c apiOriginDomainName` / `-c apiOriginCertificateArn` (b2-hardening); it needs the API origin domain and a us-west-2 ACM certificate.
4. **Edge TLS and domain** (SC-8, SC-8(1), SC-17, SC-20). The TLS 1.2+ policy is now in code behind `-c edgeDomainName` / `-c edgeCertificateArn`. Still needed: the Relay domain and ACM certificate themselves, security headers and DNSSEC in Route 53. Until then the default CloudFront certificate allows TLS 1.0.
5. **Missing logs and log-failure alarms** (AU-2, AU-5, AU-12, SI-4(4)). WAF, S3 access, VPC flow and Postgres logs are now on. Turn on CloudFront and API Gateway access logs. Alarm when CloudTrail stops delivering. Consider data events on the check-in and device tables.
6. **Backups you can actually restore** (CP-9, CP-9(1), CP-6, CP-10). Back up the keyrings offline (OPS-6). (The zone-pack and public buckets are now versioned.) Set up an AWS Backup plan with a cross-region copy. Write the restore runbook and run one restore test.
7. **Agency sign-in and safe aggregates** (IA-2(2), IA-8, AC-21, AC-7, AC-8, AC-12). Needed only if the county sees check-in data during the pilot. Choose Cognito or state SSO (Q-14), with MFA, small-cell suppression and noise (PRIV-3, PRIV-4).
8. **Privacy notice, consent and expiry** (PT-4, PT-5, SI-12, AC-2(3)). Show a privacy notice and get consent in the app. Expire inactive devices (PRIV-11). Set retention for the raw NWS archive. The text comes from counsel. The mechanism is engineering.
9. **Workload scanning and configuration compliance** (RA-5, CM-6, CM-8(3), SI-3, SR-10). CI scanning (warn-only) and cdk-nag are now in place. Still needed: AWS Config and Security Hub with the NIST 800-53 standard, ECR or Inspector image scanning, and the sender base image pinned by digest (DEP-3).
10. **Integrity on every read path** (SI-7, SI-7(1)). Verify alert signatures on `GET /alerts` and the public publish path, not only before sending (SEC-A-5). Sign the zone pack (SEC-C-11).

Next after these: device attestation against forged check-ins (IA-3, Q-15), an egress proxy for Lambdas (SC-7(8)), and a cancellation push (IR-4(1), GOV-8).

## What the team must do (not code)

These cannot be solved by engineering. Most need the operator (Q-1), whoever that turns out to be.

- **Name the people.** Pick the operator, the ISO, the authorizing official, a privacy officer, an incident commander and an on-call rota (PM-2, PM-19, CA-6, IR-7). Until these people exist, most organizational rows cannot move.
- **Write the policy set.** One short policy and procedure per family: the 18 "-1" controls (AC-1, AT-1, ...). A combined handbook is acceptable if the ISO agrees.
- **People rules.** Rules of behavior and access agreements, screening for anyone with prod access, and an access-removal checklist for when students graduate (PL-4, PS-1 to PS-9, AC-2(13)).
- **Training and exercises.** Security awareness training, role training for on-call staff and key custodians, one tabletop for the incident plan and one restore test (AT-2, AT-3, IR-2, IR-3, CP-3, CP-4).
- **Supplier paperwork.** Review the AWS, Apple, Google and geocoder terms and get DPAs. Collect AWS's FedRAMP or SOC reports for the inherited controls. Agree a notice duty with each supplier (SA-4, SA-9, SR-6, SR-8, CA-3).
- **Risk and authorization.** A SAM 5305.7 risk assessment, a POA&M with owners and dates (built from this map), tailoring decisions (PIV-only controls, SI-8 spam, SC-15, PT-6, PT-8) and the decision whether the high baseline applies (Q-22) (RA-3, CA-5, PL-11).
- **Privacy decisions.** Decide the authority to process, the notice wording, retention periods and minors (PT-2, PT-5, Q-3 to Q-7). These are counsel's calls.

To re-check the evidence after code changes, run `node docs/compliance/check-evidence.mjs`.
