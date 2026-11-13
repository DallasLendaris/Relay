-- Native push targets (APNs, FCM) next to the legacy Expo token. Expo stays for development
-- builds; for platform 'expo', push_token and expo_push_token hold the same value.
ALTER TABLE devices ADD COLUMN push_platform text;
ALTER TABLE devices ADD COLUMN push_token text;

UPDATE devices SET push_platform = 'expo', push_token = expo_push_token
 WHERE expo_push_token IS NOT NULL;

ALTER TABLE devices ADD CONSTRAINT devices_push_platform_check
  CHECK (push_platform IN ('ios', 'android', 'expo'));
ALTER TABLE devices ADD CONSTRAINT devices_push_target_pair_check
  CHECK ((push_platform IS NULL) = (push_token IS NULL));
ALTER TABLE devices ADD CONSTRAINT devices_push_expo_match_check
  CHECK (push_platform IS DISTINCT FROM 'expo' OR push_token = expo_push_token);

-- One push target belongs to at most one device. Nulls are allowed many times.
CREATE UNIQUE INDEX devices_push_target_key ON devices (push_platform, push_token)
  WHERE push_token IS NOT NULL;
