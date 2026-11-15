# Cancellation and update pushes

Status: built on branch `b2-cancel` (2026-11-11). Offline only, like everything else.

Before this change, ingest already understood CAP `Cancel` and `Update` (`backend/src/ingest/lifecycle.ts`), and feed reconciliation ended alerts that NWS stopped listing (`backend/src/ingest/poll.ts`). A phone that received alert X was never told that X was cancelled or changed. This note is the design for the follow-up push, written before the code.

## What gets a follow-up

| Event                                                     | Push?               | Loudness                                                           | Text (EN / ES)                                |
| --------------------------------------------------------- | ------------------- | ------------------------------------------------------------------ | --------------------------------------------- |
| CAP `Cancel` (NWS cancels the alert)                      | yes, kind `cancel`  | quiet: normal priority, no sound, never critical / time-sensitive  | "Cancelled: Flood Warning" / "Cancelado: ..." |
| CAP `Update`, the device's cell is still in the area      | yes, kind `update`  | the new severity's policy (`delivery/policy.ts`); quiet if no push | "Updated: Flood Warning" + new headline       |
| CAP `Update`, the device's cell was dropped from the area | yes, kind `removed` | quiet                                                              | "Update: ... no longer includes your area"    |
| Feed absence (`no_longer_in_feed`) or `expiredReferences` | **no push**         | in-app only: the CDN pointer and document say `cancelled`          | none                                          |
| Plain expiry                                              | no push             | in-app only (computed `expired`)                                   | none                                          |

Feed absence is not a cancellation: NWS guidance is that alerts may simply expire or drop out of `/alerts/active`, and edge caches flap (that is why reconciliation waits 3 polls and 5 minutes, and can restore). A loud or even a visible push for it would be wrong if the alert comes back. The phone sees the end on its next read of the pointer (at most 75 s stale) or when it opens the alert. A later real CAP `Cancel` for an alert that reconciliation already ended is now applied (it was ignored before), and that one does get the quiet `cancel` push.

An Update whose severity changed already re-notifies every device in the new area through the normal fan-out (new batch keys, `alertId:newSeverity:...`). So for a severity change the follow-up covers only the dropped cells (`removed`). The exception is an Update down to `statement` (normal fan-out sends nothing): then the kept cells get a quiet `update` too, so a loud warning on the lock screen is not left standing.

Spanish: the follow-up titles and bodies are fixed templates in `backend/src/delivery/follow-up.ts`, using the existing Spanish event names (`summaries/templates.ts` `EVENT_NAME_ES`). No machine translation. They need the same native-speaker read as the rest (`docs/summaries-spanish-review.md`).

Collapse key: a follow-up uses the alert id as its collapse / thread id, like the original, so on the lock screen it replaces the original notification.

## Who is targeted: devices that were sent the original

There is no per-device delivery log at scale: the sender claims one key per batch, `alertId:severity:cell:bucket` (dense parts `...:bucket.part/16`, split pieces `...:s<path>`). But each planned batch's `markSent` already stores the member set of its recipients (8-byte hashes, `scale/fanout/member-set.ts`, G5). That is enough to answer "was device D in the batch that went out" with one read per batch key.

So a follow-up is a second, cell-based fan-out over the **previous version's area**, gated per device by the previous version's sent records:

1. The planner takes the cells to visit: for `cancel`, the cancelled alert's own cells (lifecycle now keeps the cancelled version's area, see below); for `update`, the new cells it should cover plus the cells this Update dropped (`alert_dropped_cells`, below).
2. The follow-up shard worker pages the cell index for each cell, exactly like the normal shard worker. For each device it finds the key that carried it under the previous severity (`fanout/keys.ts` `plannedKeyFor`: normal key, or dense part key) and inspects it once (memoized per key):
   - `sent`, with a member set and a version tag: the device is targeted only if it is in the member set **and** the batch was sent for an older version of the alert (tag differs from the current `sourceId`'s tag). A batch sent for the current version (an Update's own normal fan-out reusing the same key) already carried the new text: no follow-up.
   - `sent` without a member set (written before this change, over the size cap, a dense tombstone, a split marker): fail toward a quiet follow-up, with the primary-cell rule over the visited cells so a multi-cell device still gets one.
   - `leased` (the original batch is being sent right now): targeted, and the follow-up batch is delayed (below) so it lands after.
   - `absent` (the original never went out to this bucket): not targeted. For a `cancel` the worker first **tombstones** the key (claim it and mark it sent, as the dense-cell code does), so a still-queued or cached original batch for that bucket can never be sent after the cancellation. If the claim finds the key `leased` or `sent`, it is handled as above.
3. Because membership is checked per cell, a multi-cell device is in exactly one cell's member set and so gets exactly one follow-up, without the primary-cell rule.

Not covered (documented, small): a device that removed its place after the original was sent is not in the cell index any more and gets no follow-up (its app still sees the pointer). Late-join batches (`:late:` keys) store no member set; a late-joined device is found through its bucket's key only when that bucket's batch went out (then it fails open, as above), otherwise it is missed.

The inline (dev-only, `FANOUT_MODE=inline`) path has a per-device key `alertId:severity:deviceId`, so there the worker simply inspects that key (same version rule, same tombstone for a cancel).

## Exactly once

Follow-up batch keys carry the kind and the version of the message that caused them, so they are never deduped against the original and two follow-ups of one alert never collide:

```
<alertId>:<severity>:fu.<kind>.<ver>:<cell>:<bucket>      ver = first 12 hex of sha256(new sourceId)
<alertId>:fu.<kind>.<ver>:<deviceId>                       inline worker
```

`kind` is `cancel`, `update` or `removed`. Keys are deterministic per (version, cell, bucket), so a re-plan, a replayed Cancel (an earlier poll saved it but failed before marking it seen) or a repeated Cancel in successive polls produces the same keys and the sender's claim drops the copies. Retry and re-check keys derived by the sender (`:r<n>`, `:v<n>`, `:h<n>`, `:u<n>`) keep the prefix, so a retried follow-up is still a follow-up (the sender reads the kind from the key, not from a field a re-queue could drop).

The sender now also stores a version tag (`sentVersion`, 12 hex of the batch's `sourceId`) on every key it marks sent. That is the "older version" test in step 2.

## Ordering

- **Last word is the latest version.** The sender sends a follow-up only while its version is the alert's current version (`alert.sourceId` tag equals the key's `ver`) and the state matches (`cancel` needs `cancelled`; `update` / `removed` need `active` or `updated`). An Update follow-up still queued when a Cancel lands is dropped (`stale_version`). Lifecycle treats `cancelled` as final (a later Update is ignored), so once a Cancel is stored nothing can follow it; the only way back is restoration after feed absence, which never pushed anything.
- **In-flight sends.** A cancel follow-up is enqueued with a 75 s delay (`FOLLOW_UP_DELAY_SECONDS`): longer than the 45 s sender lease plus the 5 s sender alert cache and 10 s fan-out alert cache (60 s). An original or Update batch already being sent when the Cancel committed finishes first. An update follow-up gets the same delay only when it found a `leased` key. This is best effort (a heartbeat can extend a lease past 75 s); APNs and FCM do not guarantee order anyway, and the shared collapse id means the phone keeps the newest arrival.
- **Cancel mid-fan-out.** Remaining original batches stop by themselves: the sender re-checks eligibility per batch and refuses a `cancelled` alert (`inactive`, acked). The window where a cached read still says active (up to 5 s) is closed by the tombstones in step 2: a bucket the follow-up worker found `absent` is marked sent before any follow-up goes out, so the late original batch is a duplicate to the sender. Shard jobs of the original still running are not stopped (they re-check eligibility at start only); their batches are refused at the sender.

## Lifecycle changes

- A `Cancel` keeps the stored version's area, cells, severity and PDS flag; it takes the Cancel message's text, times and ids. The cancelled record describes the alert it ends. (Before, a Cancel without a polygon or zones stored zero cells, so the CDN shard index never dropped the alert.)
- Every Update or Cancel records `supersedes: { sourceId, relaySeverity }`, the version it replaced. It is part of the signed alert and of the public document. Follow-ups read the previous severity from it, so a replayed message still knows which keys to inspect.
- A `Cancel` for an alert that reconciliation ended for feed absence (stored state `cancelled`, last message not a Cancel) is applied, and the feed-absence marker is cleared, so the alert can never be restored after a real Cancel.
- `alert_dropped_cells` (migration 0015): when a save replaces an alert's cells with a new version, the cells the new version no longer covers are recorded under the new `sourceId`, in the same transaction. Only ingest writes it; the planner reads it (grants in 0011 and 0015).

## Public alert document

The document already carries `state` and `messageType`, and its URL hash covers `state`, so a cancelled version is a new immutable document (signed alert, `cellsSha256` over the kept cells) and the pointer `alerts/v1/latest/<id>.json` names it with `state: "cancelled"`. New: `supersedes` names the version it replaced. A phone that opens an old push's `alertUrl` gets the old immutable document, so the app must read the pointer (derived from `alertId`, `PUBLIC_ALERT_PATH`) whenever it shows an alert; the follow-up push's own `alertUrl` points at the new document. Old documents cannot be marked in place: they are cached for a year, immutable by design.

Push `data` gains `followUp` (`cancel` | `update` | `removed`) on follow-ups only. The app should drop the "I'm okay" action for `cancel` and `removed` (the category stays `alert-checkin`, because an unknown Android `click_action` can make the notification untappable).

## Check-ins on a cancelled alert

They still count. A v2 check-in token is valid until the alert's original end time whatever its state, and the check-in route does not read the alert. "I'm okay" after a cancellation is still useful to the agency (who is safe after a false alarm or an early end). Follow-up pushes carry a fresh v2 token for the same alert, device and cell, so a phone that only has the follow-up can still check in.

## Kill switch

A paused kill switch holds follow-ups like originals: ingest keeps the Cancel or Update unseen, the planner and shard worker park the job, the sender retries.

## Not done

- Partial cancellation by zone inside one CAP message is modelled by NWS as an Update with a smaller area; that is the `removed` case. A CAP Cancel is always for the whole alert.
- No silent / background push for feed absence. If the owner wants one, it is a new kind with `content-available` only; the key scheme already allows it.
- The inline (dev) path does the per-device inspect and the version test, but no delay.
- The operator recall tool (`backend/scripts/alert-recall.ts`) still only marks the alert cancelled and republishes; it sends no follow-up (it stores the record like a feed-absence end, so a later real CAP Cancel is applied and pushed). Making recall push is a small change (publish a `followUp: 'cancel'` AlertReady after a recall that stores `messageType: 'Cancel'`), left for the owner because recall is an operator decision.
