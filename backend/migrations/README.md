# Database migrations

Every schema change is a SQL migration file in this folder. Migrations run from CI against Aurora Serverless v2 (PostgreSQL + PostGIS). Never change tables by hand in the AWS console, and never edit a migration that has already been merged; add a new one instead.
