-- Local dev only. Mounted at /etc/postgresql.schema.sql, which supabase/postgres runs as
-- supabase_admin at the end of its bootstrap, explicitly "to update role passwords".
--
-- Why this is needed. supabase/postgres ships `postgres` pre-created in the image build, so
-- the role already exists when the entrypoint runs:
--
--     if not exists (select from pg_roles where rolname = 'postgres') then
--       create role postgres superuser login password '$PGPASSWORD';
--
-- That branch never fires, so POSTGRES_PASSWORD is silently ignored for `postgres`, while
-- pg_hba demands scram-sha-256 for every non-loopback host. Result: the committed
-- DATABASE_URL cannot authenticate, and every database-backed test skips green. Defect D10.
--
-- Do NOT instead mount over /docker-entrypoint-initdb.d. That directory holds the image's
-- own init-scripts, migrate.sh, and migrations; a bind mount shadows all of them and the
-- container dies before `postgres` is ever created.
--
-- Note what the image's own migrations/10000000000000_demote-postgres.sql does:
--     ALTER ROLE postgres NOSUPERUSER CREATEDB CREATEROLE LOGIN REPLICATION BYPASSRLS;
-- `postgres` is NOT a superuser here, but it does hold BYPASSRLS. That is defect D1 in one
-- line: the role the application connects as bypasses row-level security by image design,
-- so no isolation claim is meaningful until section 3.3 lands mytheus_app / mytheus_relay /
-- mytheus_admin and the application stops using `postgres`.

ALTER USER postgres WITH PASSWORD 'postgres';

-- The entrypoint creates POSTGRES_DB as supabase_admin, so `public` in the mytheus database
-- is owned by supabase_admin and `postgres` (non-superuser) cannot even create a table in
-- it: "permission denied for schema public". Alembic therefore cannot create
-- alembic_version, and every database test errors out before it reaches an assertion.
-- This script is run by the entrypoint as supabase_admin against POSTGRES_DB, which is
-- exactly the privilege and the database needed to hand ownership over.
--
-- Dev convenience only. Section 3.3 replaces this: mytheus_admin owns the schema and runs
-- migrations, mytheus_app gets explicit least-privilege grants, and neither holds BYPASSRLS.
ALTER SCHEMA public OWNER TO postgres;
GRANT ALL ON SCHEMA public TO postgres;
