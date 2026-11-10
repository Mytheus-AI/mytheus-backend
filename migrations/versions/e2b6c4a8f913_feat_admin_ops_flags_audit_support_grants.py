"""feat(admin-ops): add feature_flags, ops_audit_log, support_grants (P12, plan 3.13)

The three admin-ops-service stores, plus two narrow identity seams the ops console needs.

- `feature_flags`: `org_id` NULL is a global flag. Three policies, so each reach is stated:
  `tenant_isolation` (the canonical one: a tenant session reads and writes only its own rows),
  `global_flags_read` (a tenant session may READ global rows, never write them), and
  `ops_console` (full access, only inside a transaction that set `app.ops_console = 'on'`
  transaction-locally; admin-ops-service's staff-only flag routes are the only code that does).
  `UNIQUE NULLS NOT DISTINCT (key, org_id)` makes "one global row per key" a constraint.
- `ops_audit_log`: staff activity. Not tenant-scoped (no `org_id`; `target_org_id` names the
  tenant a staff action was about), so no tenant policy; append-only by GRANT (SELECT, INSERT),
  the same mechanism as `audit_logs`. No foreign key on `target_org_id`: a staff call naming an
  unknown tenant must still be logged.
- `support_grants`: a tenant owner/admin's time-boxed consent for one staff user. Canonical
  tenant policy.
- `organizations.created_at` becomes readable by `mytheus_app` (column grant) for the ops
  tenant directory. `SELECT *` stays refused: `updated_at` and `plan_id` remain ungranted.
- `set_organization_status(org, status)`: SECURITY DEFINER, so identity-tenant-service (the
  sole `tenant-events` producer) can flip `organizations.status` for suspend/reinstate while
  `mytheus_app` still holds no UPDATE on any `organizations` column (the column-grant carve-out
  `test_app_role_insert_on_organizations_is_exactly_the_provisioning_columns` pins). EXECUTE is
  revoked from PUBLIC and granted to `mytheus_app` only. It works because `organizations` is NO
  FORCE ROW LEVEL SECURITY (migration a1c4e7f20b18), so the owner is not policied there.

Revision ID: e2b6c4a8f913
Revises: d4b8e2f61a37
Create Date: 2026-11-10
"""

from typing import Sequence, Union

from alembic import op

from migrations._grants import APP_ROLE, grant_append_only_table, grant_tenant_table

revision: str = "e2b6c4a8f913"
down_revision: Union[str, None] = "d4b8e2f61a37"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_CURRENT_ORG = "NULLIF(current_setting('app.current_org', true), '')::uuid"
_RLS_PREDICATE = f"org_id = {_CURRENT_ORG}"
_OPS_CONSOLE = "current_setting('app.ops_console', true) = 'on'"


def _tenant_policy(table: str) -> None:
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON {table} "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    grant_tenant_table(op, table)


def upgrade() -> None:
    op.execute("""
        CREATE TABLE feature_flags (
          id          uuid        NOT NULL DEFAULT gen_random_uuid(),
          key         text        NOT NULL,
          org_id      uuid        REFERENCES organizations(id) ON DELETE RESTRICT,
          enabled     boolean     NOT NULL DEFAULT false,
          rules       jsonb       NOT NULL DEFAULT '{}'::jsonb,
          updated_by  uuid,
          created_at  timestamptz NOT NULL DEFAULT now(),
          updated_at  timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_feature_flags PRIMARY KEY (id),
          CONSTRAINT uq_feature_flags_key_org UNIQUE NULLS NOT DISTINCT (key, org_id),
          CONSTRAINT ck_feature_flags_key_format CHECK (key ~ '^[a-z0-9][a-z0-9_.-]{0,99}$')
        );
        """)
    _tenant_policy("feature_flags")
    op.execute(
        "CREATE POLICY global_flags_read ON feature_flags FOR SELECT USING (org_id IS NULL);"
    )
    op.execute(
        f"CREATE POLICY ops_console ON feature_flags USING ({_OPS_CONSOLE}) "
        f"WITH CHECK ({_OPS_CONSOLE});"
    )
    op.execute(
        "COMMENT ON TABLE feature_flags IS "
        "'Feature flags (P12). org_id NULL = global. Written only by admin-ops-service staff "
        "routes (app.ops_console); tenants read their own rows and the global ones.';"
    )

    op.execute("""
        CREATE TABLE ops_audit_log (
          id             uuid        NOT NULL DEFAULT gen_random_uuid(),
          staff_user_id  uuid,
          action         text        NOT NULL,
          target_org_id  uuid,
          details        jsonb       NOT NULL DEFAULT '{}'::jsonb,
          created_at     timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_ops_audit_log PRIMARY KEY (id)
        );
        """)
    op.execute("CREATE INDEX ix_ops_audit_log_created ON ops_audit_log (created_at);")
    op.execute(
        "CREATE INDEX ix_ops_audit_log_target_created ON ops_audit_log (target_org_id, created_at);"
    )
    grant_append_only_table(op, "ops_audit_log")
    op.execute(
        "COMMENT ON TABLE ops_audit_log IS "
        "'Every /admin/ops/* call, reads and denials included (P12). Append-only by GRANT.';"
    )

    op.execute("""
        CREATE TABLE support_grants (
          id             uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id         uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          granted_by     uuid        NOT NULL,
          staff_user_id  uuid        NOT NULL,
          scope          text        NOT NULL,
          expires_at     timestamptz NOT NULL,
          revoked_at     timestamptz,
          created_at     timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_support_grants PRIMARY KEY (id),
          CONSTRAINT ck_support_grants_scope CHECK (scope IN ('read_only', 'impersonate')),
          CONSTRAINT ck_support_grants_window CHECK (expires_at > created_at)
        );
        """)
    op.execute(
        "CREATE INDEX ix_support_grants_org_staff ON support_grants (org_id, staff_user_id);"
    )
    _tenant_policy("support_grants")
    op.execute(
        "COMMENT ON TABLE support_grants IS "
        "'Tenant-consented, time-boxed staff access (P12). Written only by admin-ops-service.';"
    )

    op.execute(f"GRANT SELECT (created_at) ON organizations TO {APP_ROLE};")

    op.execute("""
        CREATE FUNCTION set_organization_status(p_org uuid, p_status text)
        RETURNS text
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
          previous text;
        BEGIN
          IF p_status NOT IN ('active', 'suspended') THEN
            RAISE EXCEPTION 'invalid organization status %', p_status USING ERRCODE = '22023';
          END IF;
          SELECT status INTO previous FROM organizations
            WHERE id = p_org AND deleted_at IS NULL FOR UPDATE;
          IF NOT FOUND THEN
            RETURN NULL;
          END IF;
          IF previous IS DISTINCT FROM p_status THEN
            UPDATE organizations SET status = p_status, updated_at = now() WHERE id = p_org;
          END IF;
          RETURN previous;
        END;
        $$;
        """)
    op.execute("REVOKE ALL ON FUNCTION set_organization_status(uuid, text) FROM PUBLIC;")
    op.execute(f"GRANT EXECUTE ON FUNCTION set_organization_status(uuid, text) TO {APP_ROLE};")


def downgrade() -> None:
    op.execute("DROP FUNCTION IF EXISTS set_organization_status(uuid, text);")
    op.execute(f"REVOKE SELECT (created_at) ON organizations FROM {APP_ROLE};")
    op.execute("DROP TABLE IF EXISTS support_grants CASCADE;")
    op.execute("DROP TABLE IF EXISTS ops_audit_log CASCADE;")
    op.execute("DROP TABLE IF EXISTS feature_flags CASCADE;")
