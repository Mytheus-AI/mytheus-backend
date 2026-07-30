"""0002 least-privilege roles, tenant-root carve-out, tenant FKs, relay drain policy

Plan 1-2 section 3.3. This migration is what makes every isolation claim in the program
mean something. Before it, the application connected as `postgres`, which holds `rolbypassrls`
in `supabase/postgres` (granted by the image's own `10000000000000_demote-postgres.sql`), so
row-level security did not apply to the request path at all and the eleven `tenant_isolation`
policies from `82d44163c933` were decorative.

Four things land together because they are one change; splitting them leaves the database in
a state where the application cannot start:

1. Three login roles, none holding SUPERUSER or BYPASSRLS: `mytheus_app` (request path),
   `mytheus_relay` (outbox drain, nothing else), `mytheus_admin` (operator seam).
2. The `organizations` `tenant_isolation` policy is REMOVED, and column-level grants plus a
   table comment take its place. Reasoning in `_canon/02-data-model.md` section 7 rule 7,
   ADR-0002 D3, and at length in `work-done-till-now/02-plan-1-2-foundation-and-tenancy.md`
   section 4.
3. `organizations.clerk_org_id` and `organizations.status`, required by the resolved
   tenant-claim decision (ADR-0010 D2).
4. The missing `org_id` foreign keys to the tenant root on the seven tables that lacked one
   (defect D6), and the `outbox_relay_drain` permissive policy that lets the relay drain
   across tenants without BYPASSRLS (defect D4).

Passwords are deliberately absent. The roles are created without one, so they cannot
authenticate until an operator sets it: `scripts/bootstrap_roles.py` does that from the
environment. A password in a migration is a password in git history.

Revision ID: a1c4e7f20b18
Revises: 82d44163c933
Create Date: 2026-07-30

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a1c4e7f20b18"
down_revision: Union[str, None] = "82d44163c933"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


APP_ROLE = "mytheus_app"
RELAY_ROLE = "mytheus_relay"
ADMIN_ROLE = "mytheus_admin"
ALL_ROLES = (APP_ROLE, RELAY_ROLE, ADMIN_ROLE)

# The seven tables carrying org_id with no foreign key to the tenant root (defect D6).
# `users`, `tenant_settings` and `data_residency` already had one.
_TABLES_MISSING_ORG_FK = [
    "outbox",
    "inbox",
    "teams",
    "team_members",
    "departments",
    "meetings",
    "meeting_participants",
]

# Every tenant-scoped table the request path touches.
_TENANT_TABLES = [
    "users",
    "teams",
    "team_members",
    "departments",
    "tenant_settings",
    "data_residency",
    "meetings",
    "meeting_participants",
    "outbox",
    "inbox",
]

# The only columns of the tenant root the request path may read. A blanket SELECT is refused,
# which bounds the accepted risk of dropping the policy: a forgotten WHERE cannot dump the
# customer directory, and the failure is loud instead of silent.
_ORG_READABLE_COLUMNS = "id, clerk_org_id, name, slug, status, deleted_at"


def _create_role(name: str) -> None:
    """Create a login role idempotently, with no password and no elevated attributes.

    NOSUPERUSER and NOBYPASSRLS are stated explicitly rather than relied on as defaults:
    they are the entire point of this migration, and being explicit means a later `ALTER
    ROLE` that grants them shows up as a diff against a stated intent.
    """
    op.execute(
        f"""
        DO $$ BEGIN
          IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{name}') THEN
            CREATE ROLE {name} LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE;
          ELSE
            ALTER ROLE {name} NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE;
          END IF;
        END $$;
        """
    )


def upgrade() -> None:
    # --- 1. Roles -----------------------------------------------------------------------
    for role in ALL_ROLES:
        _create_role(role)

    for role in ALL_ROLES:
        op.execute(f"GRANT USAGE ON SCHEMA public TO {role};")

    # --- 2. Tenant root: drop the policy, replace it with column grants ------------------
    #
    # The policy was keyed on `organizations.id`, so with app.current_org unset the row was
    # invisible. The edge must resolve clerk_org_id -> org_id BEFORE any tenant is known, so
    # under that policy the authorization checkpoint had no executable path at all. A
    # SECURITY DEFINER resolver does not rescue it either: FORCE ROW LEVEL SECURITY removes
    # the owner's exemption, so the function is subject to the policy it exists to get past.
    op.execute("DROP POLICY IF EXISTS tenant_isolation ON organizations;")
    op.execute("ALTER TABLE organizations NO FORCE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE organizations DISABLE ROW LEVEL SECURITY;")
    op.execute(
        """
        COMMENT ON TABLE organizations IS
        'Tenant root: id IS org_id. Deliberately NOT tenant_isolation-policied - the table '
        'has no org_id column, so the canonical policy (canon 02-data-model section 7) is '
        'not expressible on it, and the edge must resolve clerk_org_id -> id before any '
        'tenant is known. Reads are bounded by column-level grants and the single sanctioned '
        'query shape in libs/shared/org_resolver.py. Never add a query shape that reads this '
        'table without an equality predicate on id or clerk_org_id. See ADR-0002 D3.';
        """
    )

    # --- 3. clerk_org_id + status -------------------------------------------------------
    op.execute("ALTER TABLE organizations ADD COLUMN IF NOT EXISTS clerk_org_id text;")
    op.execute(
        "ALTER TABLE organizations "
        "ADD CONSTRAINT uq_organizations_clerk_org_id UNIQUE (clerk_org_id);"
    )
    op.execute(
        "ALTER TABLE organizations ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'active';"
    )
    op.execute(
        "ALTER TABLE organizations ADD CONSTRAINT ck_organizations_status_valid "
        "CHECK (status IN ('active','suspended'));"
    )

    # --- 4. The missing tenant foreign keys (defect D6) ----------------------------------
    #
    # RTBF works by deleting everything carrying a given org_id. A row whose org_id points at
    # a tenant root that never existed is precisely the row that sweep fails to find, so the
    # absence of these constraints made right-to-be-forgotten unprovable. RESTRICT rather
    # than CASCADE: tenant deletion is the RTBF saga's job, ordered and audited, not a side
    # effect of one DELETE.
    # Orphans must go first, or the constraint cannot be added. They are safe to delete and
    # impossible to keep: a row whose org_id names a tenant root that does not exist is
    # unreachable by every code path (app.current_org is only ever set from a token resolved
    # through `organizations`, so no request can name that value), undeliverable, and
    # invisible to an RTBF sweep. That is the defect, not a consequence of fixing it.
    #
    # This is safe to run now because no environment holds real tenant data yet
    # (mytheus-platform/env/prod.env.example carries a blocking DO-NOT-DEPLOY warning until
    # this migration lands). If it is ever re-run against a database that does, the count in
    # the NOTICE is the number of rows that were already unrecoverable, and it should be
    # investigated before the deploy proceeds rather than after.
    for table in _TABLES_MISSING_ORG_FK:
        op.execute(
            f"""
            DO $$ DECLARE n bigint; BEGIN
              DELETE FROM {table} t
               WHERE NOT EXISTS (SELECT 1 FROM organizations o WHERE o.id = t.org_id);
              GET DIAGNOSTICS n = ROW_COUNT;
              IF n > 0 THEN
                RAISE NOTICE 'D6 cleanup: deleted % orphan row(s) from {table}', n;
              END IF;
            END $$;
            """
        )
        op.execute(
            f"ALTER TABLE {table} ADD CONSTRAINT fk_{table}_org_id_organizations "
            f"FOREIGN KEY (org_id) REFERENCES organizations(id) ON DELETE RESTRICT;"
        )

    # --- 5. Grants: mytheus_app (request path) ------------------------------------------
    for table in _TENANT_TABLES:
        op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {table} TO {APP_ROLE};")
    op.execute(f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {APP_ROLE};")
    # Column-level only on the tenant root, and read-only: a tenant is born through the
    # operator seam, never through a request handler.
    op.execute(f"REVOKE ALL ON organizations FROM {APP_ROLE};")
    op.execute(f"GRANT SELECT ({_ORG_READABLE_COLUMNS}) ON organizations TO {APP_ROLE};")

    # --- 6. Grants + drain policy: mytheus_relay ----------------------------------------
    #
    # Defect D4. The relay drains every tenant's rows, which the fail-closed policy forbids by
    # default: with no app.current_org set it matched zero rows and relay_once read that as an
    # empty queue, losing every event with no error and no metric separating it from idle.
    # The fix is NOT a BYPASSRLS role. One permissive policy on one table keeps the
    # cross-tenant reach inspectable in pg_policies, and keeps it off every other table.
    op.execute(f"GRANT SELECT, UPDATE ON outbox TO {RELAY_ROLE};")
    op.execute(f"GRANT USAGE, SELECT ON SEQUENCE outbox_id_seq TO {RELAY_ROLE};")
    op.execute(
        f"CREATE POLICY outbox_relay_drain ON outbox TO {RELAY_ROLE} "
        f"USING (true) WITH CHECK (true);"
    )
    op.execute(
        """
        COMMENT ON POLICY outbox_relay_drain ON outbox IS
        'The relay must read every tenant to drain the shared outbox. Scoped to this one '
        'table and this one role so the cross-tenant reach is visible in pg_policies rather '
        'than hidden in a role attribute. mytheus_relay has no grant on any other table.';
        """
    )

    # --- 7. Grants: mytheus_admin (operator seam) ---------------------------------------
    #
    # Elevated in GRANTS, not in isolation: FORCE ROW LEVEL SECURITY still applies, so an
    # RTBF purge has to name the tenant it is purging instead of reaching across all of them.
    for table in _TENANT_TABLES:
        op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {table} TO {ADMIN_ROLE};")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON organizations TO {ADMIN_ROLE};")
    op.execute(f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {ADMIN_ROLE};")

    # Tables created by later migrations inherit these grants. Without this every new table
    # is silently unreadable by the application until someone remembers a GRANT, and the
    # symptom is a permission error in production rather than a failing test.
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {APP_ROLE}, {ADMIN_ROLE};"
    )
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        f"GRANT USAGE, SELECT ON SEQUENCES TO {APP_ROLE}, {ADMIN_ROLE};"
    )


def downgrade() -> None:
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        f"REVOKE USAGE, SELECT ON SEQUENCES FROM {APP_ROLE}, {ADMIN_ROLE};"
    )
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        f"REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM {APP_ROLE}, {ADMIN_ROLE};"
    )

    op.execute("DROP POLICY IF EXISTS outbox_relay_drain ON outbox;")

    for table in _TABLES_MISSING_ORG_FK:
        op.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS fk_{table}_org_id_organizations;")

    op.execute("ALTER TABLE organizations DROP CONSTRAINT IF EXISTS ck_organizations_status_valid;")
    op.execute("ALTER TABLE organizations DROP COLUMN IF EXISTS status;")
    op.execute("ALTER TABLE organizations DROP CONSTRAINT IF EXISTS uq_organizations_clerk_org_id;")
    op.execute("ALTER TABLE organizations DROP COLUMN IF EXISTS clerk_org_id;")

    # Restore 82d44163c933's policy exactly, so downgrade lands on the prior schema even
    # though that policy is the thing this migration exists to remove.
    op.execute("COMMENT ON TABLE organizations IS NULL;")
    op.execute("ALTER TABLE organizations ENABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE organizations FORCE ROW LEVEL SECURITY;")
    op.execute(
        "CREATE POLICY tenant_isolation ON organizations "
        "USING (id = NULLIF(current_setting('app.current_org', true), '')::uuid) "
        "WITH CHECK (id = NULLIF(current_setting('app.current_org', true), '')::uuid);"
    )

    # Roles are dropped last: a role still holding grants cannot be dropped.
    for role in ALL_ROLES:
        op.execute(
            f"""
            DO $$ BEGIN
              IF EXISTS (SELECT FROM pg_roles WHERE rolname = '{role}') THEN
                REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {role};
                REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM {role};
                REVOKE ALL ON SCHEMA public FROM {role};
                DROP ROLE {role};
              END IF;
            END $$;
            """
        )
