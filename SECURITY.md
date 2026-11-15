# Security policy

> **Draft, pending review by SJSU counsel / the agency ISO.** Relay is a student capstone project. It is not deployed, has no 24x7 staff and no bug bounty. The safe harbor wording below has not been approved by counsel and does not yet bind any organization. Do not treat it as a legal promise until it is adopted.

## Reporting a vulnerability

Please report security problems privately. Do not open a public issue.

- **Preferred:** use GitHub's private vulnerability reporting on this repository (Security tab, "Report a vulnerability"), if it is enabled.
- **Email:** `security@TO-BE-ASSIGNED`. A monitored address does not exist yet. Until one does, use the GitHub route above. This placeholder must be replaced before the policy is published.

Include, if you can:

- what you found and where (endpoint, file or component)
- steps to reproduce, or a proof of concept
- what an attacker could do with it
- whether you saw any personal data
- how you would like to be credited, or if you prefer not to be

Do not include real people's personal data in a report. A redacted sample is enough.

## Scope

In scope:

- the code in this repository: backend, API, ingest, sender, infrastructure definitions (`infra/`), the shared formats and OpenAPI contract
- the Relay mobile app, once published
- any deployed Relay environment, once one exists and the operator has confirmed it is in scope

Out of scope:

- third-party services Relay depends on: NWS, Apple APNs, Google FCM, AWS, Expo, the geocoder. Report those to their owners.
- social engineering, phishing, or physical attacks against team members or the operator
- denial-of-service or load testing against any deployed environment. The load-test tools in `loadtest/` are for a separate test stack only.
- findings that need a rooted or jailbroken device with no further impact, missing best-practice headers without a demonstrated impact, and reports from automated scanners with no proof
- vulnerabilities in dependencies with no demonstrated path to Relay. A report that shows a path is welcome.

Some weaknesses are already known and tracked in the [system security plan](docs/compliance/system-security-plan.md#5-known-gaps). You are still welcome to report them, but they may be closed as known.

## What not to do

- Do not access, modify or delete data that is not yours. Stop when you have shown the problem.
- Do not send alerts or pushes to real people.
- Do not try to forge, change or delete real alerts, and do not interfere with delivery during an active hazard. People may depend on it.
- Do not disclose the issue publicly before it is fixed or before the response window below has passed without a reply.

## Safe harbor (draft wording)

If you act in good faith, follow this policy, avoid harm to people and to the availability of the service, and report promptly, the maintainers intend to:

- treat your research as authorized
- not pursue or support legal action against you for it
- work with you to understand and fix the issue

This is a statement of intent by the maintainers. It does not bind SJSU, the CSU, the State of California or any agency, and it cannot override a law or a third party's rights. Counsel must decide the final wording and who it binds.

## What to expect

These are targets for a student project with no on-call staff. They are not guarantees.

| Step                                   | Target                                                                         |
| -------------------------------------- | ------------------------------------------------------------------------------ |
| Acknowledge your report                | 5 business days                                                                |
| First assessment (valid, severity)     | 10 business days                                                               |
| Fix or mitigation for high or critical | 30 days, sooner if people are at risk                                          |
| Fix for medium and low                 | Best effort, 90 days                                                           |
| Disclosure                             | Coordinated with you. Default 90 days after the report, or on release of a fix |

Severity follows the incident levels in the [incident response plan](docs/compliance/incident-response.md#3-severity-levels). If the operator becomes a California state entity, a confirmed security incident is also reported through the State's incident process (Cal-CSIRS) by the operator's ISO, which can affect what we can say and when.

## Supported versions

Only the latest commit on `main` is supported. Nothing has been released.

## Related documents

- [System security plan (draft)](docs/compliance/system-security-plan.md)
- [Incident response plan (draft)](docs/compliance/incident-response.md)
- [Compliance index](docs/compliance/README.md)
