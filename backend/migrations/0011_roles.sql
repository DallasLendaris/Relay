-- Least-privilege database roles (audit PRIV-6 / SEC-C-6). Until now every compute role used the
-- Aurora master secret. Each role below is what one kind of function really needs, derived from the
-- SQL in backend/src/adapters/postgres. The roles are created NOLOGIN: migrations cannot know the
-- passwords. After deploying, run infra/scripts/db-roles.ts with the admin secret; it sets each
-- role's password from its Secrets Manager secret and enables LOGIN. It also re-runs this file, so
-- grants on tables added by later migrations are picked up. Everything here is safe to run twice.
--
--   relay_api            devices and places read/write, alerts read, push-target bookkeeping
--   relay_ingest         alerts, alert_source_ids, alert_cells read/write; no devices or places
--   relay_ingest_inline  relay_ingest plus read on devices and places (dev, FANOUT_MODE=inline,
--                        where ingest plans deliveries in-process)
--   relay_fanout         alerts read (planner and shard workers)
--   relay_sender         alerts read; devices read and push-token clearing; places read
--                        (the dead-target cleanup lists a device's places)
--   relay_ops            devices and places read, cell-index outbox (repair, drainer, backfill)
--
-- Tables created later get no grants until they are added here: there are no default privileges.

DO $$
DECLARE
  r text;
BEGIN
  FOREACH r IN ARRAY ARRAY['relay_api', 'relay_ingest', 'relay_ingest_inline', 'relay_fanout', 'relay_sender', 'relay_ops'] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
      EXECUTE format('CREATE ROLE %I NOLOGIN', r);
    END IF;
  END LOOP;
END
$$;

GRANT relay_ingest TO relay_ingest_inline;

REVOKE ALL ON ALL TABLES IN SCHEMA public FROM relay_api, relay_ingest, relay_ingest_inline, relay_fanout, relay_sender, relay_ops;

GRANT USAGE ON SCHEMA public TO relay_api, relay_ingest, relay_ingest_inline, relay_fanout, relay_sender, relay_ops;

DO $$
DECLARE
  g record;
  col record;
  seq text;
BEGIN
  FOR g IN
    SELECT * FROM (VALUES
      (false, 'devices', 'relay_api', 'SELECT, INSERT, UPDATE, DELETE'),
      (false, 'devices', 'relay_ingest_inline', 'SELECT'),
      (false, 'devices', 'relay_sender', 'SELECT'),
      (false, 'devices', 'relay_ops', 'SELECT'),
      (false, 'places', 'relay_api', 'SELECT, INSERT, UPDATE, DELETE'),
      (false, 'places', 'relay_ingest_inline', 'SELECT'),
      (false, 'places', 'relay_sender', 'SELECT'),
      (false, 'places', 'relay_ops', 'SELECT'),
      (false, 'alerts', 'relay_api', 'SELECT'),
      (false, 'alerts', 'relay_ingest', 'SELECT, INSERT, UPDATE'),
      (false, 'alerts', 'relay_fanout', 'SELECT'),
      (false, 'alerts', 'relay_sender', 'SELECT'),
      (false, 'alert_source_ids', 'relay_api', 'SELECT'),
      (false, 'alert_source_ids', 'relay_ingest', 'SELECT, INSERT, UPDATE'),
      (false, 'alert_source_ids', 'relay_fanout', 'SELECT'),
      (false, 'alert_source_ids', 'relay_sender', 'SELECT'),
      (false, 'alert_cells', 'relay_api', 'SELECT'),
      (false, 'alert_cells', 'relay_ingest', 'SELECT, INSERT, DELETE'),
      (false, 'alert_cells', 'relay_fanout', 'SELECT'),
      (false, 'alert_cells', 'relay_sender', 'SELECT'),
      (true, 'push_token_moves', 'relay_api', 'SELECT, INSERT, DELETE'),
      (true, 'cell_index_outbox', 'relay_api', 'SELECT, INSERT, UPDATE, DELETE'),
      (true, 'cell_index_outbox', 'relay_ops', 'SELECT, INSERT, UPDATE, DELETE'),
      (true, 'deleted_devices', 'relay_api', 'SELECT, INSERT'),
      (true, 'deleted_devices', 'relay_ops', 'SELECT')
    ) AS v(optional, tbl, grantee, privs)
  LOOP
    IF to_regclass(format('public.%I', g.tbl)) IS NULL THEN
      IF g.optional THEN
        CONTINUE;
      END IF;
      RAISE EXCEPTION 'table % is missing; run the earlier migrations first', g.tbl;
    END IF;
    EXECUTE format('GRANT %s ON TABLE public.%I TO %I', g.privs, g.tbl, g.grantee);
    IF g.privs LIKE '%INSERT%' THEN
      FOR col IN
        SELECT attname FROM pg_attribute
         WHERE attrelid = to_regclass(format('public.%I', g.tbl)) AND attnum > 0 AND NOT attisdropped
      LOOP
        seq := pg_get_serial_sequence(format('public.%I', g.tbl), col.attname);
        IF seq IS NOT NULL THEN
          EXECUTE format('GRANT USAGE, SELECT ON SEQUENCE %s TO %I', seq, g.grantee);
        END IF;
      END LOOP;
    END IF;
  END LOOP;
END
$$;

GRANT UPDATE (expo_push_token, push_platform, push_token) ON TABLE devices TO relay_sender;
