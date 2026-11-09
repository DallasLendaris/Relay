-- Data minimization (audit PRIV-5) and erasure (PRIV-1).
--
-- 1. Exact coordinates are no longer stored. Alert matching uses h3_cell only. The cell's CENTER
--    (a cell is about 5 km2, so the center says nothing finer than h3_cell already does)
--    replaces the exact point. Pre-deploy: no data needs preserving, so the old columns are
--    dropped without a backfill. A row left over in a dev database gets 0,0 (never read).
ALTER TABLE places ADD COLUMN cell_center_lat double precision NOT NULL DEFAULT 0;
ALTER TABLE places ADD COLUMN cell_center_lng double precision NOT NULL DEFAULT 0;
ALTER TABLE places ALTER COLUMN cell_center_lat DROP DEFAULT;
ALTER TABLE places ALTER COLUMN cell_center_lng DROP DEFAULT;
ALTER TABLE places DROP COLUMN latitude;
ALTER TABLE places DROP COLUMN longitude;

-- The street text is kept only for places added by address; a dropped pin has none.
ALTER TABLE places ALTER COLUMN short_address DROP NOT NULL;
-- Labels are generic ("Home", "Work"), so they are short.
ALTER TABLE places ADD CONSTRAINT places_label_length CHECK (char_length(label) BETWEEN 1 AND 60);

-- 2. Tombstones. DELETE /devices/me removes the device row and records the id here, so a stateless
--    (signed, database-free) device credential for it is refused at check-in. One row per erasure
--    request; it holds nothing but the random device id and a time.
CREATE TABLE deleted_devices (
  device_id uuid PRIMARY KEY,
  deleted_at timestamptz NOT NULL
);
