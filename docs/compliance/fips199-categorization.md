# FIPS 199 security categorization (draft)

> **Draft, pending review by SJSU counsel / the agency ISO.** This is a proposed categorization written by the engineering team. The agency that operates Relay must make the final determination.

The CCSA asks for a "Classification Categorization Form – System Classification (FIPS 199)" ([CDT CCSA page](https://cdt.ca.gov/services/ccsa), fetched 2026-11-09). FIPS 199 defines low, moderate and high impact for each of confidentiality, integrity and availability ([NIST FIPS 199](https://nvlpubs.nist.gov/nistpubs/FIPS/NIST.FIPS.199.pdf), fetched 2026-11-09):

- **Low**: minor harm; the organization can still perform its functions, less effectively.
- **Moderate**: significant harm that does not involve loss of life or serious life-threatening injury.
- **High**: severe or catastrophic harm, including loss of life or serious life-threatening injury.

The California form may use different wording. The State's own data classification assessment is SAM 5305.5, listed on the CCSA page and not retrieved for this draft.

## Proposed result

| Objective       | Proposed impact | Short reason                                                                            |
| --------------- | --------------- | --------------------------------------------------------------------------------------- |
| Confidentiality | **Moderate**    | Location of people at the time of an emergency, tied to a persistent device identifier. |
| Integrity       | **High**        | A false, altered or wrongly targeted alert can cause harm to life.                      |
| Availability    | **High**        | A missed or late alert during a tornado, flash flood or fire can cause loss of life.    |

Overall: **High** (the high-water mark across the three objectives). Moderate for confidentiality is a proposal; see "What would change this".

## Information types

| Information type                                      | C        | I        | A        |
| ----------------------------------------------------- | -------- | -------- | -------- |
| Public alerts (NWS text, Relay summaries, signatures) | Low      | High     | High     |
| Device and push tokens                                | Moderate | Moderate | High     |
| Saved places (address, coordinates, cell)             | Moderate | Low      | Moderate |
| Check-ins (device, cell, time)                        | Moderate | Moderate | Moderate |
| Aggregated check-in totals for agencies               | Low      | Moderate | Moderate |
| Signing keys, push credentials, DB secrets            | High     | High     | High     |

The system takes the highest value of each column, which gives C moderate (secrets are covered by the high handling in the SSP, see below), I high, A high.

Note on keys: the keys score high for confidentiality, which would raise the system's confidentiality rating by the strict high-water-mark rule. The proposal keeps C at moderate for the data about people and handles the keys as a separate, tightly controlled asset class (secrets manager, least privilege, rotation). The agency ISO should confirm that approach or raise C to high. This is the most debatable cell in the table.

## Reasoning

**Confidentiality: moderate.**

- The data is not a name, email or phone number. Users are anonymous installs.
- But it holds saved places (often a home address and exact coordinates, today) and, for check-ins, a record that a given device was in a given cell during a given emergency, tied to a push token that persists. In combination that is sensitive location data about private individuals.
- Exposure could cause significant harm (stalking, profiling, burglary targeting during evacuations) but is not expected to cause loss of life by itself. That fits moderate under FIPS 199.
- The audited system keeps exact coordinates and check-in rows indefinitely and uses one shared admin database credential (PRIV-5, GOV-2, PRIV-6). Those are reasons the controls are weaker than moderate requires today, not reasons to lower the category.

**Integrity: high.**

- Relay alerts tell people to take protective action. An altered, forged or misdirected alert (for example an evacuation order sent to the wrong area, or a cancel that never reaches people) can lead to injury or death.
- Relay summaries are derived from official NWS text; a wrong summary is also an integrity failure (SAFE-1 to SAFE-9).
- Forged check-ins would mislead responders about who is safe (SEC-A-1). Responders could divert resources, so check-in integrity is at least moderate and becomes high once agencies rely on it.

**Availability: high.**

- The entire value of the system is delivery within about 60 to 120 seconds of NWS publishing, during the event. Downtime or delay during an event could contribute to loss of life.
- Relay supplements WEA and EAS, it does not replace them (see [positioning](../positioning.md)). That lowers the marginal harm of an outage but does not remove it, because some people receive Relay and not other channels (and a Relay summary is often earlier or more specific for NWS products that do not trigger WEA).
- The current design is single region with one-day database backups at the audited commit (OPS-5, OPS-13), so the high availability rating is aspirational. See the [BIA](./business-impact-and-contingency.md).

## What would change this

| If                                                                                       | Then                                                                                                    |
| ---------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Relay stores only H3 cells and never exact coordinates, address or label (PRIV-5)        | Confidentiality stays moderate, with lower impact inside the category.                                  |
| Relay is positioned as one of several channels and never as the sole warning             | Argues for availability moderate for a pilot. Do not rely on it for a statewide deployment.             |
| The operator is a state entity and agencies receive check-in data with small cell counts | Raise confidentiality or add suppression rules before release (PRIV-3, PRIV-4).                         |
| Relay becomes an alert originator through IPAWS                                          | Integrity and availability stay high; add the IPAWS COG requirements and state approval.                |
| Children's data is collected knowingly                                                   | Counsel to review; may raise confidentiality (see [privacy analysis](./privacy-threshold-analysis.md)). |

## Review record

| Item                     | Value                             |
| ------------------------ | --------------------------------- |
| Prepared by              | Relay capstone team (engineering) |
| Date                     | 2026-11-09                        |
| Reviewed by              | Not yet reviewed                  |
| Approved by (agency ISO) | Not yet approved                  |
