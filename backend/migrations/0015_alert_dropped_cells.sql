-- Follow-up pushes (docs/cancellation.md). When a new version of an alert (an NWS Update) replaces
-- the alert's cells, the cells it no longer covers are recorded here under the new version's
-- source id, in the same transaction as the save. The fan-out planner reads them to send the
-- quiet "no longer includes your area" follow-up to devices that were sent the earlier version.
CREATE TABLE IF NOT EXISTS alert_dropped_cells (
  alert_id uuid NOT NULL REFERENCES alerts (id) ON DELETE CASCADE,
  source_id text NOT NULL,
  h3_cell text NOT NULL,
  PRIMARY KEY (alert_id, source_id, h3_cell)
);

-- Grants (0011 lists the table too, by name when present, so a db-roles.ts re-run keeps them).
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'relay_ingest') THEN
    GRANT SELECT, INSERT, DELETE ON TABLE alert_dropped_cells TO relay_ingest;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'relay_fanout') THEN
    GRANT SELECT ON TABLE alert_dropped_cells TO relay_fanout;
  END IF;
END
$$;
