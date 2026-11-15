-- One Expo push token belongs to at most one device. A reinstalled app registers a new
-- device with the same token; assignPushToken moves it, this index makes sure it can't
-- be held twice. Nulls are allowed many times.

-- Existing duplicates (if any) would block the index: keep the newest device's token.
UPDATE devices SET expo_push_token = NULL
 WHERE expo_push_token IS NOT NULL
   AND id NOT IN (
     SELECT DISTINCT ON (expo_push_token) id FROM devices
      WHERE expo_push_token IS NOT NULL
      ORDER BY expo_push_token, created_at DESC, id
   );

CREATE UNIQUE INDEX devices_expo_push_token_key ON devices (expo_push_token)
  WHERE expo_push_token IS NOT NULL;
