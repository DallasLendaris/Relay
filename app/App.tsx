/**
 * Relay PoC app: the smallest phone client that exercises the alert engine end to end.
 *
 * 1. Connect: registers this phone (POST /devices) and its Expo push token
 *    (PUT /devices/me/push-token) with a Relay server running in local mode on a laptop.
 * 2. Save a place: this phone's location, or a fixed San José point (POST /places).
 * 3. When an alert covers the place, the server pushes it. The push carries a signed
 *    check-in token; tapping "I'm okay" sends it back (POST /alerts/{id}/checkins).
 *
 * This is not the designed Relay UI (that lives in Figma); it is the PoC client for 195A.
 */
import Constants from 'expo-constants';
import * as Device from 'expo-device';
import * as Location from 'expo-location';
import * as Notifications from 'expo-notifications';
import { StatusBar } from 'expo-status-bar';
import { useCallback, useEffect, useRef, useState } from 'react';
import {
  Platform,
  Pressable,
  SafeAreaView,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  View,
} from 'react-native';

Notifications.setNotificationHandler({
  handleNotification: async () => ({
    shouldShowBanner: true,
    shouldShowList: true,
    shouldPlaySound: true,
    shouldSetBadge: false,
  }),
});

/** Inside the sample Flash Flood Warning area, for demos without a live alert nearby. */
const DEMO_POINT = { latitude: 37.35, longitude: -121.9 };
const DEFAULT_SERVER = process.env.EXPO_PUBLIC_RELAY_URL ?? 'http://192.168.1.10:8787';

interface AlertSummary {
  id: string;
  event: string;
  relaySeverity: string;
  headline?: string;
  expires: string;
}

type Status = { kind: 'idle' | 'busy' | 'ok' | 'error'; text: string };

function newKey() {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

async function expoPushToken(): Promise<string> {
  if (!Device.isDevice) throw new Error('Push needs a real phone, not a simulator');
  if (Platform.OS === 'android') {
    await Notifications.setNotificationChannelAsync('default', {
      name: 'Alerts',
      importance: Notifications.AndroidImportance.MAX,
    });
  }
  const current = await Notifications.getPermissionsAsync();
  const granted = current.granted || (await Notifications.requestPermissionsAsync()).granted;
  if (!granted) throw new Error('Notifications are turned off for this app');
  const projectId =
    (Constants.expoConfig?.extra as { eas?: { projectId?: string } } | undefined)?.eas?.projectId ??
    Constants.easConfig?.projectId;
  if (!projectId) throw new Error('No EAS project id: run "npx eas init" in app/ first');
  return (await Notifications.getExpoPushTokenAsync({ projectId })).data;
}

export default function App() {
  const [server, setServer] = useState(DEFAULT_SERVER);
  const [credential, setCredential] = useState<string | null>(null);
  const [placeCell, setPlaceCell] = useState<string | null>(null);
  const [alerts, setAlerts] = useState<AlertSummary[]>([]);
  const [checkedIn, setCheckedIn] = useState<Record<string, string>>({});
  const [status, setStatus] = useState<Status>({ kind: 'idle', text: 'Not connected' });
  const [log, setLog] = useState<string[]>([]);
  /** alertId -> signed check-in token, taken from the push. */
  const tokens = useRef(new Map<string, string>());

  const note = useCallback((line: string) => {
    const time = new Date().toLocaleTimeString();
    setLog((l) => [`${time}  ${line}`, ...l].slice(0, 30));
  }, []);

  const api = useCallback(
    async (method: string, path: string, body?: unknown) => {
      const res = await fetch(`${server.replace(/\/$/, '')}${path}`, {
        method,
        headers: {
          'content-type': 'application/json',
          ...(credential ? { authorization: `Bearer ${credential}` } : {}),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      });
      const text = await res.text();
      const json = text ? (JSON.parse(text) as Record<string, unknown>) : {};
      if (!res.ok) throw new Error(`${res.status} ${String(json['message'] ?? text)}`);
      return json;
    },
    [server, credential],
  );

  const refresh = useCallback(async () => {
    if (!credential) return;
    const res = (await api('GET', '/alerts')) as { alerts: AlertSummary[] };
    setAlerts(res.alerts);
  }, [api, credential]);

  // Keep the check-in token from every push, whether it arrives in the foreground or is tapped.
  useEffect(() => {
    const take = (n: Notifications.Notification) => {
      const data = n.request.content.data as { alertId?: string; checkInToken?: string };
      if (data.alertId && data.checkInToken) tokens.current.set(data.alertId, data.checkInToken);
      note(`Push received: ${n.request.content.title ?? 'alert'}`);
      void refresh().catch(() => {});
    };
    const a = Notifications.addNotificationReceivedListener(take);
    const b = Notifications.addNotificationResponseReceivedListener((r) => take(r.notification));
    const last = Notifications.getLastNotificationResponse();
    if (last) take(last.notification);
    return () => {
      a.remove();
      b.remove();
    };
  }, [note, refresh]);

  const run = (label: string, fn: () => Promise<void>) => async () => {
    setStatus({ kind: 'busy', text: `${label}…` });
    try {
      await fn();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setStatus({ kind: 'error', text: `${label} failed: ${message}` });
      note(`${label} failed: ${message}`);
    }
  };

  const connect = run('Connecting', async () => {
    const token = await expoPushToken();
    const reg = await fetch(`${server.replace(/\/$/, '')}/devices`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{}',
    });
    if (!reg.ok) throw new Error(`register returned ${reg.status}`);
    const device = (await reg.json()) as { credential: string };
    const put = await fetch(`${server.replace(/\/$/, '')}/devices/me/push-token`, {
      method: 'PUT',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${device.credential}` },
      body: JSON.stringify({ expoPushToken: token, language: 'en' }),
    });
    if (!put.ok) throw new Error(`push token returned ${put.status}`);
    setCredential(device.credential);
    setStatus({ kind: 'ok', text: 'Connected. Now save a place.' });
    note('Registered this phone and its push token');
  });

  const savePlace = (useGps: boolean) =>
    run('Saving place', async () => {
      let point = DEMO_POINT;
      if (useGps) {
        const perm = await Location.requestForegroundPermissionsAsync();
        if (!perm.granted) throw new Error('Location permission denied');
        const pos = await Location.getCurrentPositionAsync({});
        point = { latitude: pos.coords.latitude, longitude: pos.coords.longitude };
      }
      const place = (await api('POST', '/places', { label: 'Home', ...point })) as {
        h3Cell: string;
      };
      setPlaceCell(place.h3Cell);
      setStatus({ kind: 'ok', text: 'Place saved. Waiting for alerts.' });
      note(`Saved place in H3 cell ${place.h3Cell}`);
      await refresh();
    });

  const checkIn = (alertId: string) =>
    run("Sending I'm okay", async () => {
      const token = tokens.current.get(alertId);
      if (!token) throw new Error('No check-in token yet: it arrives with the push');
      const res = (await api('POST', `/alerts/${alertId}/checkins`, {
        status: 'ok',
        token,
        idempotencyKey: newKey(),
      })) as { checkedInAt: string };
      setCheckedIn((c) => ({ ...c, [alertId]: res.checkedInAt }));
      setStatus({ kind: 'ok', text: "Sent: you're marked okay." });
      note(`"I'm okay" sent for ${alertId.slice(0, 8)}`);
    });

  return (
    <SafeAreaView style={styles.safe}>
      <StatusBar style="dark" />
      <ScrollView contentContainerStyle={styles.body}>
        <Text style={styles.title}>Relay</Text>
        <Text style={styles.sub}>Proof-of-concept client</Text>

        <Text style={styles.label}>Relay server</Text>
        <TextInput
          style={styles.input}
          value={server}
          onChangeText={setServer}
          autoCapitalize="none"
          autoCorrect={false}
          editable={!credential}
          accessibilityLabel="Relay server address"
        />
        <Button
          label={credential ? 'Connected' : 'Connect'}
          onPress={connect}
          disabled={!!credential}
        />

        <View style={styles.row}>
          <Button label="Save my location" onPress={savePlace(true)} disabled={!credential} flex />
          <Button label="Save demo point" onPress={savePlace(false)} disabled={!credential} flex />
        </View>
        {placeCell ? <Text style={styles.small}>Place cell: {placeCell}</Text> : null}

        <Text style={[styles.status, status.kind === 'error' && styles.error]}>{status.text}</Text>

        <View style={styles.headerRow}>
          <Text style={styles.section}>Alerts for your place</Text>
          <Pressable onPress={run('Refreshing', refresh)} disabled={!credential} hitSlop={12}>
            <Text style={styles.link}>Refresh</Text>
          </Pressable>
        </View>
        {alerts.length === 0 ? <Text style={styles.small}>None right now.</Text> : null}
        {alerts.map((a) => (
          <View key={a.id} style={styles.card}>
            <Text style={styles.severity}>{a.relaySeverity.toUpperCase()}</Text>
            <Text style={styles.event}>{a.event}</Text>
            {a.headline ? <Text style={styles.headline}>{a.headline}</Text> : null}
            <Text style={styles.small}>Until {new Date(a.expires).toLocaleString()}</Text>
            {checkedIn[a.id] ? (
              <Text style={styles.done}>Marked okay</Text>
            ) : (
              <Button label="I'm okay" onPress={checkIn(a.id)} />
            )}
          </View>
        ))}

        <Text style={styles.section}>Activity</Text>
        {log.map((l, i) => (
          <Text key={i} style={styles.small}>
            {l}
          </Text>
        ))}
      </ScrollView>
    </SafeAreaView>
  );
}

function Button(props: { label: string; onPress: () => void; disabled?: boolean; flex?: boolean }) {
  return (
    <Pressable
      accessibilityRole="button"
      onPress={props.onPress}
      disabled={props.disabled}
      style={({ pressed }) => [
        styles.button,
        props.flex && styles.flex,
        props.disabled && styles.buttonDisabled,
        pressed && styles.pressed,
      ]}
    >
      <Text style={styles.buttonText}>{props.label}</Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  safe: { flex: 1, backgroundColor: '#ffffff' },
  body: { padding: 20, gap: 10 },
  title: { fontSize: 28, fontWeight: '700', color: '#14213d' },
  sub: { fontSize: 14, color: '#555', marginBottom: 8 },
  label: { fontSize: 14, fontWeight: '600', color: '#14213d' },
  input: { borderWidth: 1, borderColor: '#bbb', borderRadius: 8, padding: 12, fontSize: 16 },
  row: { flexDirection: 'row', gap: 10 },
  flex: { flex: 1 },
  button: {
    backgroundColor: '#14213d',
    borderRadius: 10,
    minHeight: 48,
    justifyContent: 'center',
    alignItems: 'center',
    paddingHorizontal: 16,
  },
  buttonDisabled: { backgroundColor: '#8a93a6' },
  pressed: { opacity: 0.8 },
  buttonText: { color: '#fff', fontSize: 16, fontWeight: '600' },
  status: { fontSize: 15, color: '#1b5e20', marginTop: 4 },
  error: { color: '#b71c1c' },
  headerRow: { flexDirection: 'row', justifyContent: 'space-between', marginTop: 16 },
  section: { fontSize: 18, fontWeight: '700', color: '#14213d', marginTop: 16 },
  link: { fontSize: 16, color: '#0b57d0', marginTop: 16 },
  card: { borderWidth: 1, borderColor: '#ddd', borderRadius: 12, padding: 14, gap: 6 },
  severity: { fontSize: 12, fontWeight: '700', color: '#b71c1c', letterSpacing: 1 },
  event: { fontSize: 20, fontWeight: '700', color: '#14213d' },
  headline: { fontSize: 15, color: '#333' },
  done: { fontSize: 16, fontWeight: '600', color: '#1b5e20' },
  small: { fontSize: 13, color: '#555' },
});
