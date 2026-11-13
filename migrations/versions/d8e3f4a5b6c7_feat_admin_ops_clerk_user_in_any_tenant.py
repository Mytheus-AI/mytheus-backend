"""feat(admin-ops): clerk_user_in_any_tenant, so a workspace member can never be staff (ADR-0046 D1)

Superadmins are dedicated Clerk accounts that belong to no workspace. admin-ops-service refuses
an allowlisted account as staff when this answers true: the account has a live `users` row in
some workspace. SECURITY INVOKER (the default), so it runs as the caller (`mytheus_app`): it
binds `app.current_org` to each live workspace in turn, transaction-locally, so RLS checks every
`users` row it looks at, and it restores the caller's binding before returning. It returns one
boolean and nothing else, so no workspace's data leaves it.

Revision ID: d8e3f4a5b6c7
Revises: c7d2e3f4a5b6
Create Date: 2026-11-13
"""

from typing import Sequence, Union

from alembic import op

from migrations._grants import ADMIN_ROLE, APP_ROLE

revision: str = "d8e3f4a5b6c7"
down_revision: Union[str, None] = "c7d2e3f4a5b6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE FUNCTION clerk_user_in_any_tenant(p_clerk_user_id text)
        RETURNS boolean
        LANGUAGE plpgsql
        SECURITY INVOKER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
          o record;
          found_row boolean := false;
          previous text := current_setting('app.current_org', true);
        BEGIN
          IF p_clerk_user_id IS NULL OR p_clerk_user_id = '' THEN
            RETURN false;
          END IF;
          FOR o IN SELECT g.id FROM organizations g WHERE g.deleted_at IS NULL LOOP
            PERFORM set_config('app.current_org', o.id::text, true);
            IF EXISTS (
              SELECT 1 FROM users u
              WHERE u.clerk_user_id = p_clerk_user_id AND u.deleted_at IS NULL
            ) THEN
              found_row := true;
              EXIT;
            END IF;
          END LOOP;
          PERFORM set_config('app.current_org', coalesce(previous, ''), true);
          RETURN found_row;
        END;
        $$;
        """)
    op.execute("REVOKE ALL ON FUNCTION clerk_user_in_any_tenant(text) FROM PUBLIC;")
    op.execute(
        f"GRANT EXECUTE ON FUNCTION clerk_user_in_any_tenant(text) TO {APP_ROLE}, {ADMIN_ROLE};"
    )


def downgrade() -> None:
    op.execute("DROP FUNCTION IF EXISTS clerk_user_in_any_tenant(text);")
