# Relay positioning (one page)

> **Draft, pending review by SJSU counsel / the agency ISO.** This is the position the capstone team proposes to take in the pitch. Counsel and the agency decide the final wording. Nothing here claims an agreement, approval or endorsement that does not exist.

## The position in four sentences

Relay **supplements** Wireless Emergency Alerts (WEA), the Emergency Alert System (EAS) and ShakeAlert. It does not replace them. Relay **redistributes** public National Weather Service (NWS) alerts, and later possibly the IPAWS public alert feed, as plain-language summaries for places people saved, and it lets them say "I'm okay". Relay is **not an alert originator**: it does not create official warnings and has no authority to. Earthquakes are a **post-event check-in** use, not early warning.

## What Relay adds

- A place-based notice: people choose the places they care about (home, a parent's house, a school), and Relay alerts them when an NWS alert covers one of those places, even if they are somewhere else.
- A plain-language summary in English and Spanish next to the official text (more languages are an open item, GOV-7).
- A one-tap "I'm okay" for family, and per-area response totals for agencies, with no individual check-ins shown to agencies (not built yet: agency access needs authentication, GOV-6).

## What Relay is not

| Not this                              | Why                                                                                |
| ------------------------------------- | ---------------------------------------------------------------------------------- |
| A replacement for WEA, EAS, or sirens | Those are the official, authority-originated channels. Relay is an extra channel.  |
| An earthquake early warning system    | ShakeAlert is. Relay has no seismic input and no seconds-scale latency. See below. |
| An alert originator                   | See "IPAWS role". Relay does not send official alerts under its own authority.     |
| Endorsed by NWS, NOAA, FEMA, Cal OES  | No such endorsement exists. The app and pitch must never suggest one.              |
| Guaranteed to deliver                 | No SLA exists. The disclaimer says so.                                             |

## IPAWS role (decision GOV-5)

Two roles are possible. The proposal is the first.

**Role 1, redistributor (proposed).** Relay reads public alerts from NWS today. A later second source is the IPAWS All-Hazards Information Feed (AHIF). FEMA describes it as a free feed of public alerts posted by IPAWS (from EAS and WEA), and says access requires registering on the IPAWS User Portal, approval by an IPAWS team member, and a **Memorandum of Agreement (MOA)** with FEMA. The MOA lets providers receive public alerts in CAP format and pass them to their audiences, including through apps. FEMA also says the feed is "not a tool to originate alerts", that alerts cannot be used for advertising, and that excessive requests are prohibited ([FEMA, IPAWS All-Hazards Information Feed](https://fema.gov/emergency-managers/practitioners/integrated-public-alert-warning-system/technology-developers/all-hazards-information-feed), fetched 2026-11-09). The page does not list who is eligible. Ask `ipaws@fema.dhs.gov`, as the page suggests. Nothing has been applied for. This is not built.

**Role 2, originator (not proposed).** If the State or a county later decides that Relay should send official alerts, the operator, not Relay, would need to become an Alerting Authority. FEMA's steps are: consult the State IPAWS representative, complete the IS-247 course, choose IPAWS-compatible software tested in the IPAWS-OPEN environment, apply for an MOA and receive a COG (Collaborative Operating Group) ID, then apply for public alerting permissions that a designated state official signs ([FEMA, IPAWS sign-up](https://www.fema.gov/emergency-managers/practitioners/integrated-public-alert-warning-system/public-safety-officials/sign-up), fetched 2026-11-09). In California that state review would involve Cal OES (the FEMA page names only "a designated state official"; the Cal OES specifics are unverified). That decision belongs to the State and counsel, and it changes the liability, the system categorization and the required controls. Relay should not take it by default.

Open question for the clients: who is the Alerting Authority if Role 2 is ever wanted?

## NWS attribution and non-endorsement

NWS information is in the public domain, but the NWS disclaimer sets conditions ([NWS disclaimer](https://www.weather.gov/disclaimer), fetched 2026-11-09). In summary:

- Do not claim NWS information as your own, and do not claim copyright in it.
- Do not use it in a way that implies endorsement by or affiliation with NOAA or NWS.
- Do not modify NWS content and present it as official government material, and do not present your own information so it appears official.
- Pay attention to the date and time of the data displayed.
- The NWS name and logo are trademark-protected. Using them to identify unaltered NWS content or to link to NWS is allowed. Other uses need permission.
- The information is provided "as is" and the user assumes the risk of its use.

What this means for Relay:

1. A Relay summary is Relay's own text. It must be labelled as a Relay summary and name NWS as the source. It must never replace or alter the official NWS headline or text (audit finding GOV-4, SAFE-7).
2. The official NWS wording must remain reachable from the alert screen, with the issue time and the NWS office.
3. Do not show the NWS logo as if it were Relay's or as an endorsement. Do not use NOAA, NWS, FEMA or Cal OES names in marketing as sponsors.
4. Show the issued, effective and ending times in Pacific time and state when the data was last checked.

## User-facing disclaimer wording (draft)

These are proposals for counsel to edit. They are not yet in the app.

**Push notification (short):**

- English: `Relay summary: <Event> for <place>. Source: NWS.`
- Spanish: `Resumen de Relay: <Evento> para <lugar>. Fuente: NWS.`

The audit confirmed that the prefix must be present in both languages (GOV-4). The official NWS first line stays separate and unchanged.

**Alert screen:**

> This is a summary written by Relay from the National Weather Service alert below. It is not an official government message. Read the official alert and follow instructions from local officials. Relay is not affiliated with or endorsed by the National Weather Service, NOAA or any government agency.

**About / terms screen (starting text):**

> Relay is an additional way to see weather alerts for places you save. It does not replace Wireless Emergency Alerts, the Emergency Alert System, sirens, NOAA Weather Radio, or instructions from local officials. Alerts may be late, incomplete or not delivered, for example when your phone has no signal or notifications are off. Do not rely on Relay as your only source of warnings. Call 911 in an emergency.

**Check-in screen:**

> Tapping "I'm okay" tells Relay that this phone is safe. Agencies see only totals for your approximate area (about 5 square kilometers), never your name, address or exact location. [Who else sees the check-in, such as family circles, depends on app features not covered by this draft.] Data is deleted after 30 days. [Retention and deletion wording is subject to the [privacy analysis](compliance/privacy-threshold-analysis.md) and counsel. The 30-day target is a fix in progress, not yet merged.]

Liability and warranty language (a real terms of service) and the privacy notice are not drafted here. They need counsel and depend on the operator (GOV-9, PRIV-8).

## Earthquake framing

ShakeAlert is the West Coast earthquake early warning system, covering California, Oregon and Washington ([ShakeAlert](https://www.shakealert.org/), fetched 2026-11-09). The ShakeAlert site says the USGS works with licensed Alert Delivery Partners. Whether and how phone alerts reach the public (for example through WEA) was not confirmed from that page and should be checked with USGS or Cal OES before it is stated in the pitch.

Relay's position:

- Relay does **not** give earthquake early warning. Its input is NWS alerts, with possible IPAWS later. It cannot warn seconds before shaking and must never say it can.
- After a significant earthquake, Relay can offer a **post-event check-in**: "Are you okay?" and an "I'm okay" response. This is a possible later feature, and it needs an operator-initiated, authority-approved trigger. It is not built.
- Do not use language like "early warning", "seconds of warning" or "earthquake alert" in the app or pitch. Say: "After an earthquake, let your family know you're okay."
- Tsunami warnings are issued as NWS products and could flow through Relay like other NWS alerts (unverified: check the NWS tsunami product path, and note that the Spanish templates for Tsunami were missing at the audited commit, SAFE-3).

## For the pitch

Say: "Relay makes official public warnings easier to understand and lets people tell their families they are safe. It works alongside WEA, EAS and ShakeAlert."

Do not say: "Relay replaces", "official", "endorsed", "early warning", "guaranteed", "statewide ready" (nothing is deployed; see [scale status](scale-status.md)).

## Sources

- FEMA IPAWS All-Hazards Information Feed, fetched 2026-11-09: <https://fema.gov/emergency-managers/practitioners/integrated-public-alert-warning-system/technology-developers/all-hazards-information-feed>
- FEMA IPAWS sign-up (MOA, COG, IS-247, state signature), fetched 2026-11-09: <https://www.fema.gov/emergency-managers/practitioners/integrated-public-alert-warning-system/public-safety-officials/sign-up>
- NWS disclaimer, fetched 2026-11-09: <https://www.weather.gov/disclaimer>
- ShakeAlert, fetched 2026-11-09: <https://www.shakealert.org/>
- Unverified: Cal OES approval step for IPAWS; ShakeAlert delivery by WEA; NWS tsunami product path; the California language statutes in GOV-7.
