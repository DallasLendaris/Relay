-- The full Alert lives in record (jsonb). The other columns exist for lookups.
CREATE TABLE alerts (
  id uuid PRIMARY KEY,
  source_id text UNIQUE,
  state text NOT NULL,
  expires timestamptz NOT NULL,
  h3_cells text[] NOT NULL,
  record jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- GIN index makes "h3_cells && {cells}" (overlap) fast.
CREATE INDEX alerts_h3_cells_idx ON alerts USING GIN (h3_cells);

-- Every NWS message id in an alert's chain (its own and its references),
-- so an update or cancel can find the alert it belongs to.
CREATE TABLE alert_source_ids (
  source_id text PRIMARY KEY,
  alert_id uuid NOT NULL REFERENCES alerts (id) ON DELETE CASCADE
);

CREATE INDEX alert_source_ids_alert_id_idx ON alert_source_ids (alert_id);
