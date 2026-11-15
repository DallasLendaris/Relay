# Relay app (PoC client)

The smallest phone client that runs the Relay alert path end to end against a Relay server in local mode. It is **not** the designed Relay UI (that lives in Figma and is 195B work). Expo SDK 58, one screen, `App.tsx`.

What it does:

1. **Connect** registers the phone (`POST /devices`) and its Expo push token (`PUT /devices/me/push-token`).
2. **Save my location** or **Save demo point** saves one place (`POST /places`, by coordinates). The demo point is in San José, inside the sample Flash Flood Warning.
3. When an alert covers the place, the server pushes it. The push carries a signed check-in token.
4. **I'm okay** sends that token back (`POST /alerts/{id}/checkins`). The server counts it once in the place's H3 cell.

This app is not an npm workspace of the repo, so the root `npm ci` does not install it.

## Run it on a phone

You need a real phone (push does not work in simulators), on the same Wi-Fi as the laptop running the server, and a free Expo account.

1. Start the server on the laptop (repo root): `RELAY_FEED=live RELAY_PUSH=expo npm run local -w @relay/backend`. Note the laptop's LAN address, for example `192.168.1.20`.
2. In this folder: `npm ci`, then once, `npx eas-cli init` to create the Expo project id that push tokens need (it is written into `app.json`).
3. `npx expo start`, then open the app:
   - **iPhone:** scan the QR code with the Camera app to open it in Expo Go.
   - **Android:** Expo Go on Android cannot receive push notifications (since SDK 53). Make a development build instead: `npx expo install expo-dev-client`, `npx eas-cli build:configure`, `npx eas-cli build --profile development --platform android`, install the build, then `npx expo start --dev-client`. An iPhone is the faster route for the demo.
4. In the app, set the server to `http://<laptop LAN address>:8787`, tap **Connect**, then **Save demo point** (or **Save my location** if a live alert covers you).
5. With a covering alert active, the push arrives. Tap **I'm okay**. The laptop's status page (`http://localhost:8787/`) shows the count.

If no live California alert covers you, run the server with `RELAY_FEED=replay` (the default) and save the demo point: the replayed alert is pushed through Expo's real service in the same way.

## Check it builds

```
npm ci
npm run typecheck
npx expo export --platform android
```
