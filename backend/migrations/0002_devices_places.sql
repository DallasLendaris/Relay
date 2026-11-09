-- A device is an anonymous install. Only the hash of its credential is stored.
CREATE TABLE devices (
  id uuid PRIMARY KEY,
  credential_hash text UNIQUE NOT NULL,
  expo_push_token text,
  language text NOT NULL CHECK (language IN ('en', 'es')),
  created_at timestamptz NOT NULL
);

-- Saved places. h3_cell is what alerts are matched against.
CREATE TABLE places (
  id uuid PRIMARY KEY,
  device_id uuid NOT NULL REFERENCES devices (id) ON DELETE CASCADE,
  label text NOT NULL,
  short_address text NOT NULL,
  latitude double precision NOT NULL CHECK (latitude BETWEEN -90 AND 90),
  longitude double precision NOT NULL CHECK (longitude BETWEEN -180 AND 180),
  h3_cell text NOT NULL,
  created_at timestamptz NOT NULL
);

CREATE INDEX places_h3_cell_idx ON places (h3_cell);
CREATE INDEX places_device_id_idx ON places (device_id);
