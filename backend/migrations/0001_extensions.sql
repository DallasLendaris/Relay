-- PostGIS is not needed for H3 matching; it is enabled for the later PostGIS cross-check.
-- Kept alone in this file so tests can skip it when the test database lacks PostGIS.
CREATE EXTENSION IF NOT EXISTS postgis;
