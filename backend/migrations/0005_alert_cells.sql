-- Alert areas can cover tens of thousands of H3 cells, far more than fits in one Data API
-- request (64 KB) or response (1 MB). Cells move to their own table, written and read in
-- chunks. The alert record is stored WITHOUT area.h3Cells; the store puts them back on read.
CREATE TABLE alert_cells (
  alert_id uuid NOT NULL REFERENCES alerts (id) ON DELETE CASCADE,
  h3_cell text NOT NULL,
  PRIMARY KEY (alert_id, h3_cell)
);

CREATE INDEX alert_cells_h3_cell_idx ON alert_cells (h3_cell);

-- Move existing alerts over.
INSERT INTO alert_cells (alert_id, h3_cell)
SELECT id, unnest(h3_cells) FROM alerts WHERE h3_cells IS NOT NULL
ON CONFLICT DO NOTHING;

-- The old array column is no longer used. 0003 is already merged, so keep the column but
-- make it optional, empty it, and drop its GIN index.
ALTER TABLE alerts ALTER COLUMN h3_cells DROP NOT NULL;

UPDATE alerts SET h3_cells = NULL, record = jsonb_set(record, '{area,h3Cells}', '[]'::jsonb);

DROP INDEX alerts_h3_cells_idx;
