-- COPY of mytheus-platform deploy/postgres-init/harden-roles.sql (the canonical file). Keep the
-- two identical below this header: CI (.github/workflows/ci.yml) and the local compose stack apply
-- this copy, production applies the platform one.
-- Mytheus hardening of the supabase/postgres:15.8.1.060 image's roles. IDEMPOTENT.
--
-- Run as a SUPERUSER (supabase_admin). Two ways it runs:
--   * New volume: `zz-mytheus-harden-roles.sh` (mounted into /docker-entrypoint-initdb.d by
--     compose.prod.yml) runs this file once, after the image's own bootstrap.
--   * Existing database: `harden-existing-db.sh` (operator script, same directory) runs it by
--     hand. Safe to re-run any number of times; every statement converges to the same state.
--
-- Why (journal plan Part 3, fixes 1, 3 and 7). Every tenant guarantee here is a row-level-
-- security policy, and a policy is inert for a login that is a superuser, holds BYPASSRLS,
-- belongs to a predefined role that bypasses table ACLs, or can `SET ROLE` to one of those.
-- The image ships several such logins that Mytheus never uses:
--
--   supabase_read_only_user  LOGIN, BYPASSRLS, member of pg_read_all_data: reads every row of
--                            every tenant. Neutralised: NOLOGIN, NOBYPASSRLS, membership revoked.
--   authenticator            LOGIN, member of service_role (BYPASSRLS): PostgREST's login, one
--                            `SET ROLE service_role` from every row. Mytheus runs no PostgREST.
--                            NOLOGIN (re-enable only together with a PostgREST deployment).
--   supabase_storage_admin   LOGIN, member of service_role. Mytheus runs no Supabase Storage
--                            (objects are on Cloudflare R2). NOLOGIN.
--
-- `postgres` (BYPASSRLS + pg_read_all_data on this image) and `supabase_admin` (superuser) stay:
-- they are the schema owner and the image's own superuser. Neither may ever be a service's
-- DATABASE_URL; every service's pool guard refuses both at connect time.
--
-- It also marks `ts_match_vq` / `ts_match_qv` (the `tsvector @@ tsquery` operator and its
-- commutator) LEAKPROOF. Under RLS only a leakproof qual may use an index, so without this no
-- full-text search can use its GIN index for any tenant. Their only errors depend on the query
-- constant (stack depth, interrupts), never on a row's contents. Needs a superuser, which the
-- backend's migration role on this image is not; hence it lives here too.

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'supabase_read_only_user') THEN
    ALTER ROLE supabase_read_only_user NOLOGIN NOBYPASSRLS;
    IF pg_has_role('supabase_read_only_user', 'pg_read_all_data', 'MEMBER') THEN
      REVOKE pg_read_all_data FROM supabase_read_only_user;
    END IF;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticator') THEN
    ALTER ROLE authenticator NOLOGIN;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'supabase_storage_admin') THEN
    ALTER ROLE supabase_storage_admin NOLOGIN;
  END IF;
END $$;

ALTER FUNCTION ts_match_vq(tsvector, tsquery) LEAKPROOF;
ALTER FUNCTION ts_match_qv(tsquery, tsvector) LEAKPROOF;

-- Verification: must return zero rows (postgres and supabase_admin excepted, see above).
SELECT r.rolname AS still_bypasses_isolation
FROM pg_roles r
WHERE r.rolcanlogin
  AND r.rolname NOT IN ('postgres', 'supabase_admin')
  AND (r.rolsuper OR r.rolbypassrls
       OR pg_has_role(r.oid, 'pg_read_all_data', 'MEMBER')
       OR pg_has_role(r.oid, 'pg_write_all_data', 'MEMBER')
       OR pg_has_role(r.oid, 'pg_execute_server_program', 'MEMBER')
       OR pg_has_role(r.oid, 'pg_read_server_files', 'MEMBER')
       OR pg_has_role(r.oid, 'pg_write_server_files', 'MEMBER')
       OR EXISTS (SELECT 1 FROM pg_roles b
                  WHERE b.oid <> r.oid AND (b.rolsuper OR b.rolbypassrls)
                    AND pg_has_role(r.oid, b.oid, 'MEMBER')));
