-- Push-target move rules (SEC-A-1 / SEC-B-1): one phone must not mint unlimited device identities
-- by moving one push token. See backend/src/scale/latejoin/push-target-rules.ts.
--
-- push_target_updated_at: last assignPushTarget call for the device (the inactivity rule).
-- push_target_changed_at: last time the target VALUE changed (none -> some, or a new token). The
--   late-join Lambda uses it to tell a device that was in the planned fan-out from one that got
--   its target afterwards.
ALTER TABLE devices ADD COLUMN push_target_updated_at timestamptz;
ALTER TABLE devices ADD COLUMN push_target_changed_at timestamptz;
UPDATE devices SET push_target_updated_at = created_at, push_target_changed_at = created_at;

-- One row per move of a token between devices. token_hash = sha256(platform || token): the token
-- itself is never stored here. Rows older than the window are pruned by the move statement.
CREATE TABLE push_token_moves (
  token_hash text NOT NULL,
  device_id uuid NOT NULL REFERENCES devices (id) ON DELETE CASCADE,
  moved_at timestamptz NOT NULL
);
CREATE INDEX push_token_moves_token_idx ON push_token_moves (token_hash, moved_at DESC);
