# Fake dev data

Made-up alerts, check-ins and saved places for Santa Clara County. Front-end teammates can use these files directly, without running anything or calling the backend. Nothing here is real: names, addresses, tokens and device ids are all invented.

## Files

Generated with seed `42` and the default counts (6 alerts, 500 check-ins, 1000 places).

- `alerts.json`: an array of `Alert` objects (from `@relay/shared`). Alerts cycle through Flash Flood Warning, Red Flag Warning and Heat Advisory. Every polygon sits inside Santa Clara County. All alerts are active from `2026-12-10T18:00:00Z` until six hours later. `area.h3Cells` lists the H3 cells (resolution 7) the alert covers.
- `checkins.json`: an array of `CheckIn` objects. The check-ins are shared across the alerts. Each check-in's `h3Cell` is one of its alert's `h3Cells`, and its `checkedInAt` falls between the alert's `effective` and `expires`. Tokens are fake strings and will not verify against the real signing keys.
- `places.json`: an array of saved places, matching the `Place` interface in `backend/src/ports.ts`. Places are spread across the county bounding box (latitude 37.10 to 37.45, longitude -122.15 to -121.60). Several places share each device id.

## Regenerate

Run from the repository root. The defaults match the committed files:

```sh
npx tsx backend/scripts/generate-fake-data.ts --seed 42 --alerts 6 --checkins 500 --places 1000
```

Or from the `backend` folder:

```sh
npm run fake-data -- --seed 42
```

Every option is optional. The same seed and counts always write the same files, byte for byte. Use a different `--seed` to get a different set. Check-ins are shared out across the alerts as evenly as possible.

The generator code is in `backend/src/dev/fake-data.ts`. If you change how the data is made, regenerate these files and commit them together.
