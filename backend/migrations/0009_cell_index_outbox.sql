-- Transactional outbox for the DynamoDB cell index. The API writes one row here in the SAME
-- transaction as every place insert/delete and every push-target change, so a crash between the
-- Aurora commit and the cell-index write can no longer leave a device unindexed (a missed alert).
-- The API deletes the row after its write-through succeeds; a scheduled drainer redoes the rows
-- that are left (older than 30 s) from Aurora's truth and alarms when attempts reach 10.
--
-- No foreign key to devices: the row must survive (and be drained) after a device is deleted.
CREATE TABLE cell_index_outbox (
  id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  device_id  uuid NOT NULL,
  op         text NOT NULL CHECK (op IN ('put', 'remove', 'update')),
  -- {"h3Cell": "...", "placeId": "..."}; both '' for an update of all the device's places.
  payload    jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  attempts   integer NOT NULL DEFAULT 0
);

CREATE INDEX cell_index_outbox_created_at_idx ON cell_index_outbox (created_at);
