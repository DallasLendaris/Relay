-- clearPushToken (a dead token reported by APNs/FCM) looks devices up by push_token alone.
-- The only index containing push_token is devices_push_target_key (push_platform, push_token),
-- which leads with push_platform and so cannot serve a lookup by token: it was a full scan of
-- the devices table for every dead token, thousands of times in a statewide send.
-- expo_push_token already has devices_expo_push_token_key (0004, partial on IS NOT NULL), which
-- serves `expo_push_token = $1`; a second index on it would only add write cost.
CREATE INDEX devices_push_token_idx ON devices (push_token) WHERE push_token IS NOT NULL;
