"""0005 retire the schema-wide default privileges, and revoke the image's blanket grants

Plan 3-4 open gap 5, the "single highest-leverage fix outstanding". Two arms of one defect.

ARM 1, the one that was recorded. Migration `a1c4e7f20b18` (0002) ends with

    ALTER DEFAULT PRIVILEGES IN SCHEMA public
      GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO mytheus_app, mytheus_admin;

so that a later migration's table is usable without anyone remembering a GRANT. A partition
created by `CREATE TABLE ... PARTITION OF` is a newly created table, so it inherited that
grant too - and privileges are checked against the relation NAMED in a query while the
parent's RLS policy is only enforced on access THROUGH the parent. `DELETE FROM
audit_logs_2026_07` therefore succeeded for `mytheus_app` while `DELETE FROM audit_logs` was
refused, and `transcript_chunks` hit the same thing for real in Plan 3-4: a live cross-tenant
read, write and delete, reproduced empirically. Both were closed one table at a time
(`c3e8b1d47f92` here, `_partition_privileges.lock_down_partition` in `mytheus-realtime`), and
the schema-wide cause was left open to recur on the next partitioned table from any repo.

ARM 2, found while fixing arm 1 and materially worse. `supabase/postgres` ships its own
`ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON TABLES TO anon,
authenticated, service_role` at bootstrap, so EVERY table this program has ever created -
partition or not - also grants the full DML set to those three. `service_role` holds
`rolbypassrls`, and `authenticator` is a LOGIN role that is a member of all three. Verified
against the running database before this migration was written, with no `app.current_org`
set at any point:

    SET ROLE service_role;
    SELECT count(*) FROM audit_logs;            -> 352   (every tenant)
    SELECT count(*) FROM audit_logs_2026_08;    -> 346   (every tenant, via the partition)
    DELETE FROM audit_logs_2026_08 WHERE ...;   -> DELETE 1

That is defect D1's exact shape on a different role: the append-only audit trail is erasable,
the `organizations` column-level carve-out is bypassed, and every `tenant_isolation` policy is
inert for anything that can reach `service_role`. It is live on the deployed VPS, which runs
the same image.

WHAT THIS MIGRATION DOES

1. Retires the TABLES default privileges in `public` entirely, for the application roles and
   the image's three alike. Nothing is inherited any more, so a future partition from ANY
   repo comes out with an empty ACL by construction. This is the durable fix for arm 1.
2. Removes the image's three roles from the SEQUENCES and FUNCTIONS default privileges, and
   revokes what they already hold on every table, sequence and function in `public`.
3. Sweeps every existing partition clean of any non-owner grantee.

WHY NOT THE `ddl_command_end` EVENT TRIGGER

Plan 3-4 gap 5 names an auto-revoking event trigger as the other candidate. It is not
available to this migration: PostgreSQL requires SUPERUSER to `CREATE EVENT TRIGGER`, and in
`supabase/postgres` the migration runner `postgres` is NOT a superuser (the image's own
`10000000000000_demote-postgres.sql` demotes it; `supabase_admin` is the only superuser, and
`postgres` is not a member of it). Retiring the default privileges reaches the same place
without needing a privilege we do not have, and it is the stronger of the two anyway: a
trigger revokes a grant after the fact, whereas this means the grant is never made.

WHAT THIS COSTS, AND WHAT PAYS FOR IT

0002's comment defends the blanket grant on the grounds that "without it every new table is
silently unreadable by the application until someone remembers a GRANT, and the symptom is a
production permission error rather than a failing test." That objection is correct and is
answered rather than ignored: `migrations/_grants.py` makes the grant one call, and
`tests/test_roles_and_policies.py::test_every_org_id_table_grants_read_to_the_request_role`
turns the forgotten GRANT into exactly the failing test 0002 wished for.

EXISTING GRANTS ARE NOT DISTURBED. A default ACL only applies to objects created after it, so
retiring it cannot remove a privilege any current table already holds. Step 2's revokes are
the only thing here that takes a privilege away, and they take it only from `anon`,
`authenticated` and `service_role`.

ONE CROSS-REPO CONSEQUENCE, DISCLOSED RATHER THAN LEFT TO BE FOUND

`mytheus-realtime`'s `transcript_chunks` migration (`be66698dbefb`) creates its table in this
same `public` schema and contains no GRANT at all - it relies entirely on the default
privilege this migration retires. Nothing breaks on any database where that table already
exists, because its grants were made before this ran and are untouched. What breaks is a
rebuild from an EMPTY CLUSTER in which that migration runs after this one: the parent table
comes out with an empty ACL and `mytheus_app` gets "permission denied for table
transcript_chunks". That path is disaster recovery and a new developer's first `make dev`, so
it is real.

The fix is one line in that repo, immediately after its `CREATE POLICY tenant_isolation`:

    op.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON transcript_chunks "
               "TO mytheus_app, mytheus_admin;")

It is deliberately not applied from here. `transcript_chunks` is owned by
`transcription-service`, and reaching across a repo boundary to grant on another service's
table is the same shape of mistake that `_canon` ownership rules exist to prevent - it would
also be invisible to anyone reading that repo. Recorded as an open gap instead. A loud,
one-line, immediately diagnosable failure on a rebuild path was judged the better trade
against leaving a proven cross-tenant read/write/delete open for longer.

TWO RESIDUALS, RECORDED RATHER THAN CLAIMED CLOSED

1. `service_role` still exists and still holds `rolbypassrls`. `ALTER ROLE ... NOBYPASSRLS`
   needs superuser, which the migration runner does not have. What this removes is its reach:
   it now holds no privilege on anything in `public`. `supabase_admin` could re-grant, which
   is why `test_no_bypassrls_role_holds_privileges_on_a_public_table` runs in CI rather than
   this being a one-time cleanup.
2. The image's `ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin IN SCHEMA public GRANT ALL
   ON TABLES TO anon, authenticated, service_role` survives, for the same missing-membership
   reason. It is inert as long as `supabase_admin` creates no table in `public`, because a
   default ACL applies only to objects created by its own role, and nothing in this program
   runs as `supabase_admin`. The hazard is an operator running DDL from inside the container,
   where `psql -U supabase_admin` is the path of least resistance:  any table created that way
   is born granting the full DML set to a BYPASSRLS role.
   `test_public_schema_grants_nothing_by_default` is scoped to roles that actually own a table
   here, so it begins failing the moment that happens rather than asserting a property nobody
   can currently restore.

Revision ID: 9b2c4d1e6f30
Revises: 56f72ac73fa9
Create Date: 2026-08-28

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9b2c4d1e6f30"
down_revision: Union[str, None] = "56f72ac73fa9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


APP_ROLE = "mytheus_app"
RELAY_ROLE = "mytheus_relay"
ADMIN_ROLE = "mytheus_admin"

# The three roles `supabase/postgres` creates and grants everything to. None is a LOGIN role,
# but `authenticator` is, and it is a member of all three - so "cannot log in" is not the
# control it looks like. This program runs no PostgREST and no Supabase API, so nothing here
# needs them; on a plain Postgres image they would not exist at all.
IMAGE_ROLES = ("anon", "authenticated", "service_role")


def _for_each_existing_role(roles: Sequence[str], statement_template: str) -> None:
    """Run `statement_template` once per role in `roles` that exists, with %(role)s filled in.

    Wrapped in a DO block per role rather than one statement over a role list, because a
    single `REVOKE ... FROM a, b, c` fails outright if any one of the three is missing.
    `anon`/`authenticated`/`service_role` are `supabase/postgres` artifacts, not ours, so a
    plain `postgres` image has none of them and every statement naming one would error. This
    keeps the migration portable across both base images.
    """
    for role in roles:
        stmt = statement_template.replace("%(role)s", role)
        op.execute(
            f"""
            DO $$ BEGIN
              IF EXISTS (SELECT FROM pg_roles WHERE rolname = '{role}') THEN
                {stmt}
              END IF;
            END $$;
            """
        )


# Revoke every privilege held by anyone who is not the relation's owner, on every partition in
# `public`. Written against `relacl` rather than a hardcoded role list so it cleans up whatever
# is actually there: the recorded defect was about `mytheus_app`/`mytheus_admin`, and the
# grants that were really present also included the image's three. A list would have missed
# exactly the arm nobody had noticed.
_SWEEP_PARTITIONS = """
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT DISTINCT c.oid::regclass AS rel, a.grantee AS grantee_oid
        FROM pg_class c
        CROSS JOIN LATERAL aclexplode(c.relacl) a
        WHERE c.relnamespace = 'public'::regnamespace
          AND c.relispartition
          AND a.grantee <> c.relowner
    LOOP
        IF r.grantee_oid = 0 THEN
            EXECUTE format('REVOKE ALL ON %s FROM PUBLIC', r.rel);
        ELSE
            EXECUTE format('REVOKE ALL ON %s FROM %I', r.rel,
                           pg_get_userbyid(r.grantee_oid));
        END IF;
    END LOOP;
END $$;
"""


def upgrade() -> None:
    # --- 1. Retire the TABLES default privileges in `public` -----------------------------
    #
    # For the application roles AND the image's three. After this, a table created in this
    # schema - including a partition, from this repo or any sibling repo sharing the database
    # - starts with an empty ACL. That is arm 1 closed at the cause instead of one partition
    # at a time.
    #
    # `FOR ROLE postgres` is implicit (the default is the role running the statement), and it
    # is the right one: a default ACL applies to objects created BY that role, and every
    # migration in this program runs as `postgres`. The image also installs an identical
    # entry `FOR ROLE supabase_admin`, which this cannot touch and does not need to - it
    # applies only to objects `supabase_admin` itself creates, and it creates none of ours.
    _for_each_existing_role(
        (APP_ROLE, ADMIN_ROLE, *IMAGE_ROLES),
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON TABLES FROM %(role)s;",
    )

    # --- 2. Sequences and functions: drop the image's three, keep the application roles ---
    #
    # The partition hazard is specific to tables: a sequence carries no rows, no RLS and no
    # partitions, so inheritance there costs nothing and removing it would break the next
    # BIGSERIAL insert for no gain. The image's blanket grant is removed from both anyway,
    # because nothing in this program should reach either through those roles.
    _for_each_existing_role(
        IMAGE_ROLES,
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON SEQUENCES FROM %(role)s;",
    )
    _for_each_existing_role(
        IMAGE_ROLES,
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON FUNCTIONS FROM %(role)s;",
    )

    # --- 3. Revoke what the image's roles already hold ------------------------------------
    #
    # Retiring a default ACL changes nothing about objects that already exist, so this is the
    # step that actually closes arm 2 on the tables holding data today. `ALL TABLES` includes
    # partitions.
    #
    # Schema USAGE is deliberately left in place: it grants nothing readable on its own once
    # the object privileges are gone, and revoking it is the kind of change to an image-owned
    # role that breaks something unrelated months later.
    _for_each_existing_role(
        IMAGE_ROLES,
        "REVOKE ALL ON ALL TABLES IN SCHEMA public FROM %(role)s;",
    )
    _for_each_existing_role(
        IMAGE_ROLES,
        "REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM %(role)s;",
    )
    _for_each_existing_role(
        IMAGE_ROLES,
        "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM %(role)s;",
    )

    # --- 4. Sweep every existing partition clean -----------------------------------------
    #
    # Step 3 covers the image's three. This covers anything else a partition picked up before
    # step 1 landed, and makes the migration idempotent against a database where a partition
    # was created between `c3e8b1d47f92` and here without its `REVOKE`.
    op.execute(_SWEEP_PARTITIONS)

    op.execute(
        """
        COMMENT ON SCHEMA public IS
        'Tables here are granted explicitly, one migration at a time. The schema-wide '
        'ALTER DEFAULT PRIVILEGES was retired in 9b2c4d1e6f30 because a partition is a newly '
        'created table and inherited the grant while the parent RLS policy did not follow, '
        'which produced a live cross-tenant read/write/delete on transcript_chunks. Use '
        'migrations/_grants.py; tests/test_roles_and_policies.py fails a table that has no '
        'grant and a default privilege that reintroduces one.';
        """
    )


def downgrade() -> None:
    # Restores 0002's schema-wide grant and the image's bootstrap grants, so a downgrade
    # lands on the prior privilege state exactly - including the defect. That is the point of
    # a downgrade: it is not the place to keep a fix the upgrade is being rolled back from.
    op.execute("COMMENT ON SCHEMA public IS NULL;")

    _for_each_existing_role(
        (APP_ROLE, ADMIN_ROLE),
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %(role)s;",
    )
    _for_each_existing_role(
        IMAGE_ROLES,
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO %(role)s;",
    )
    _for_each_existing_role(
        IMAGE_ROLES,
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO %(role)s;",
    )
    _for_each_existing_role(
        IMAGE_ROLES,
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS TO %(role)s;",
    )
    _for_each_existing_role(
        IMAGE_ROLES,
        "GRANT ALL ON ALL TABLES IN SCHEMA public TO %(role)s;",
    )
    _for_each_existing_role(
        IMAGE_ROLES,
        "GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO %(role)s;",
    )
    _for_each_existing_role(
        IMAGE_ROLES,
        "GRANT ALL ON ALL FUNCTIONS IN SCHEMA public TO %(role)s;",
    )

    # The partitions swept in upgrade step 4 are NOT re-granted. Restoring a cross-tenant
    # read/write/delete on `audit_logs` and `transcript_chunks` partitions by name is not
    # something a rollback should do silently, and every one of them was created with an
    # explicit REVOKE in its own migration anyway, so this leaves them where those
    # migrations put them.
