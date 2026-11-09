# Privacy threshold analysis and impact analysis (draft)

> **Draft, pending review by SJSU counsel / the agency ISO.** Written in the spirit of SIMM 5310-C, the Privacy Threshold and Impact Analysis that the CCSA asks for ([CDT CCSA page](https://cdt.ca.gov/services/ccsa), fetched 2026-11-09). The SIMM 5310-C form itself was not retrieved, so this is **not** the form. The agency privacy officer must transfer it. Nothing here is legal advice.

## 1. Summary

Relay stores pseudonymous device identifiers and the places people choose to be alerted about, and records an "I'm okay" check-in per device per alert. It does not ask for a name, email address or phone number. The location data is nonetheless sensitive, and the audited system keeps more of it, for longer, than it needs. Section 6 lists the fixes and the questions that need counsel.

## 2. What is collected, and why

| Item                                                        | Collected from                     | Purpose                                                  | Needed?                                               |
| ----------------------------------------------------------- | ---------------------------------- | -------------------------------------------------------- | ----------------------------------------------------- |
| Random device id, credential hash                           | App on first launch                | Anonymous identity for places and check-ins              | Yes                                                   |
| Language                                                    | App                                | Pick the summary language                                | Yes                                                   |
| Push token (APNs, FCM)                                      | OS push service, via the app       | Deliver the alert                                        | Yes                                                   |
| Place label and short address                               | User typing, geocoder              | Display in the app                                       | Not for matching. Should stay on the device (PRIV-5). |
| Exact latitude and longitude of the place                   | Geocoder or user                   | Compute the H3 cell                                      | Only transiently. Store the cell only (PRIV-5).       |
| H3 resolution 7 cell of each place (hexagon of about 5 km2) | Derived                            | Match NWS alert areas to people                          | Yes                                                   |
| Check-in: alert id, device id, cell, time                   | App, when the user taps "I'm okay" | Tell the user's circle and agencies that people are safe | Yes while the alert is recent; not after              |

Not collected: name, email, phone number, contacts, continuous device location, advertising ids, analytics. The user is not asked for background location. If the app team adds any of these, this analysis must be redone.

## 3. Retention

| Data                  | As built (audited commit)                                                          | Target (fix planned)                                                                       |
| --------------------- | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Check-in rows         | Never expire. PITR on and table retained in prod.                                  | Expire 30 days after the alert ends (DynamoDB TTL), and on device deletion. GOV-2, PRIV-2. |
| Check-in totals       | TTL set by the aggregator.                                                         | Keep. Aggregates only.                                                                     |
| Device, places        | Until manually deleted. No endpoint exists.                                        | Deleted on `DELETE /devices/me` (cascade to places, cell index, check-ins). GOV-3, PRIV-1. |
| Inactive devices      | Never expire.                                                                      | Expiry after a period of inactivity. Length is a counsel question. PRIV-11.                |
| Cell index entries    | No TTL.                                                                            | Deleted with the device.                                                                   |
| Push tokens in queues | Up to 14 days, including dead-letter queues.                                       | Shorter retention for queues that carry tokens. PRIV-7.                                    |
| Backups and snapshots | Aurora snapshots and DynamoDB PITR windows apply.                                  | Document the purge window after a deletion request. Counsel question.                      |
| Application logs      | One month; contain no tokens or coordinates by policy, with known leaks (PRIV-10). | Fix the leaks. Set an audit-log retention separately.                                      |

DynamoDB PITR keeps up to 35 days of history and Aurora backup retention is configurable. These windows mean a deleted row can still exist in a backup. This is unverified against AWS documentation in this draft and must be confirmed before it appears in a user-facing notice.

## 4. Sharing

- **Agencies:** aggregate counts per alert and H3 cell only, never device ids or push tokens. At the audited commit the agency route is not built, and no small-count suppression exists (PRIV-3). A cell with few residents could reveal an individual's check-in, and an attacker who creates fake devices can defeat a simple threshold (PRIV-4). Both must be fixed before any agency sees data.
- **Apple and Google:** receive the push token and alert text in order to deliver it. They do not receive saved places.
- **Geocoder:** receives the address a user types. Provider, terms and retention are to be confirmed.
- **AWS:** hosts all data as a processor.
- **Sale or advertising:** none, by design. Gov Code 11015.5(b) forbids a state agency from distributing or selling this kind of information without written permission, with exceptions ([text](https://california.public.law/codes/ca_gov't_code_section_11015.5), fetched 2026-11-09, a third-party copy of the statute).
- **Law enforcement, subpoenas, public records requests:** no policy exists. Counsel question.

## 5. User rights

| Right                       | Status                                                                                                                                                                                                                                                                                                                                              |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Notice at collection        | Not built. A notice is needed (GOV-9, PRIV-8). Gov Code 11015.5(a) lists what a state agency's notice must contain (the tool in use, what and why, discard on request, IPA, no sale, public records, contact), if that statute applies.                                                                                                             |
| Delete my data              | Fix planned: `DELETE /devices/me`, covering device, places, cell index entries and check-ins. Gov Code 11015.5(c) requires a state agency (the statute names the CSU) to discard electronically collected personal information on request. A deletion path that works only from the app does not help someone who lost the phone. Counsel question. |
| Access, export              | Not built (PRIV-8).                                                                                                                                                                                                                                                                                                                                 |
| Revoke the credential       | Not possible today (stateless credential, PRIV-9). A tombstone deny-list is proposed.                                                                                                                                                                                                                                                               |
| Opt out of check-in sharing | Check-ins are voluntary: nothing is sent unless the user taps "I'm okay".                                                                                                                                                                                                                                                                           |

The user can also remove a place, and delete the app (which stops pushes once the OS reports the token dead; the server does not learn of the uninstall until then).

## 6. Risks and open questions for counsel

| #    | Question                                                                                                                                                                                                                                                                                                                                                  |
| ---- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| P-1  | **IPA or CCPA/CPRA?** The operator decides. A state agency or the CSU is under the Information Practices Act and Gov Code 11015.5. An auxiliary or a private vendor is more likely under the CCPA as amended by the CPRA. If SJSU operates it, which regime applies? Does a vendor run for a state entity inherit the IPA?                                |
| P-2  | **CPRA treatment of precise geolocation.** The CPRA treats precise geolocation as sensitive personal information. Is an H3 cell of about 5 km2 "precise"? Is an exact coordinate plus an address, which the audited system stores? Is a check-in (device in a cell at a time) sensitive? (unverified: counsel to confirm the CPRA definition and radius.) |
| P-3  | **Minors.** The app has no age gate. A child can install it. Does COPPA or the CCPA's minors rules apply, and what notice or consent follows? Should the app set an age floor?                                                                                                                                                                            |
| P-4  | **Retention lengths.** Is 30 days after the alert right for check-ins? How long for inactive devices? Is there a records-retention schedule that overrides deletion for a state operator?                                                                                                                                                                 |
| P-5  | **Backups.** What purge window after a deletion request is acceptable? Is "gone from live systems, expires from backups within N days" enough?                                                                                                                                                                                                            |
| P-6  | **Public records.** Would check-ins or place data be subject to California Public Records Act requests if a state entity operates Relay? What exemption applies, if any?                                                                                                                                                                                  |
| P-7  | **Law enforcement requests.** What is the response policy, and does an aggregate-only design limit what can be produced?                                                                                                                                                                                                                                  |
| P-8  | **Small-count threshold.** What minimum count and noise policy should agencies see (PRIV-3, PRIV-4)? Who approves?                                                                                                                                                                                                                                        |
| P-9  | **Processors.** Are AWS, Apple, Google and the geocoder acceptable processors, and what contract terms are needed?                                                                                                                                                                                                                                        |
| P-10 | **Notice wording** and who signs it. See the disclaimer drafts in [positioning](../positioning.md).                                                                                                                                                                                                                                                       |

## 7. Review record

| Item                     | Value                             |
| ------------------------ | --------------------------------- |
| Prepared by              | Relay capstone team (engineering) |
| Date                     | 2026-11-09                        |
| Reviewed by              | Not yet reviewed                  |
| Privacy officer sign-off | Not yet reviewed                  |
