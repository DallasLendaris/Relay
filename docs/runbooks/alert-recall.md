# Runbook: recall or correct an alert

Use this when an alert that is stored and showing in the app must come down by hand: it was
issued by mistake, it is wrong, or the agency has withdrawn it and Relay did not notice.

## What the recall does

`backend/scripts/alert-recall.ts` takes a Relay alert id (the UUID, not the NWS id) and a reason.

1. Loads the stored alert and sets its `state` to `cancelled`.
2. Drops the alert's summaries (a cancelled alert never reads "in effect").
3. Re-signs it with the current alert signing key (the signing store does this on every save).
4. Republishes the public JSON: `alerts/v1/<id>.json` now says `cancelled`, and the alert leaves
   the shard indexes (`alerts/v1/active/<shard>.json`) that phones read.
5. Writes one structured log line, `alert.recalled`, with the alert id, NWS id, event, previous
   state and your reason. That line is the audit trail. The reason is not stored on the alert.

It is safe to run again on the same alert. A second run re-saves and republishes it, which repairs
a publish that failed the first time.

## What it does NOT do

- **It does not send a push notification.** Phones that already got the alert are not told it was
  cancelled, and they are not told what was wrong. A "cancelled" push (and a correction push) is
  out of scope for this tool and is tracked as GOV-8 / SAFE-4. If people were told to take
  protective action, tell them through the agency or WEA/EAS, not through this script.
- It does not stop a fan-out that is already running. Pause delivery first (kill switch) if
  pushes are still going out. See the kill-switch runbook.
- It does not edit the official NWS text. To correct text you cannot change the alert; recall it
  and let the agency's corrected message arrive through the normal feed.
- A recalled alert stays cancelled. If NWS later re-lists the same message id, the feed poll
  ignores it (the id is already applied).

## Run it

You need AWS credentials for the account, and these environment variables (the same values as the
ingest Lambda):

```
DB_CLUSTER_ARN=...  DB_SECRET_ARN=...  DB_NAME=...
ALERT_KEYS_SECRET_ARN=...
PUBLIC_ALERTS_BUCKET=...  PUBLIC_ALERTS_REGION=...
```

1. Find the alert id. In the app/API it is `id` in `GET /alerts`; in the database it is
   `alerts.id`.
2. Dry run first. It prints what it would do and changes nothing:
   `npx tsx backend/scripts/alert-recall.ts <alert-id> --reason "Issued for wrong county" --dry-run`
3. Run it for real, without `--dry-run`. It prints a JSON result with `ok`, `previousState`
   and `republished`.
4. Check the public copy: `alerts/v1/<alert-id>.json` in the public bucket has `"state":"cancelled"`
   (CloudFront may serve the old copy until its cache expires; the cache time is short).

If `PUBLIC_ALERTS_BUCKET` is not set, the stored alert is cancelled but nothing is republished;
the script warns about this.

The reason must be at least 5 characters. Say what was wrong and who asked.

## Automatic version of the same thing

The ingest poll also cancels alerts that NWS silently drops from `/alerts/active` (reason
`no_longer_in_feed`, after 2 consecutive successful polls) or lists in `expiredReferences`. Those
show up as `alert.ended_by_reconciliation` log lines. They have the same limits: no push.
