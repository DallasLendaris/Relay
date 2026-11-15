-- Lease for the cell-index outbox drainer. The drainer now loops claim -> apply within one run,
-- and two runs can overlap (a slow run, or a retry of the scheduled invocation). `claimed_until`
-- is set by the claim (now + lease); other claims skip rows whose lease is still live (and rows
-- locked by a claim in flight, FOR UPDATE SKIP LOCKED), so no row is applied twice at once. A run
-- that dies leaves its rows claimed only until the lease ends.
--
-- Grants: relay_ops already has table-level SELECT, INSERT, UPDATE, DELETE on cell_index_outbox
-- (0011_roles.sql), which covers the new column; relay_api's table-level grants do too.
ALTER TABLE cell_index_outbox ADD COLUMN claimed_until timestamptz;
