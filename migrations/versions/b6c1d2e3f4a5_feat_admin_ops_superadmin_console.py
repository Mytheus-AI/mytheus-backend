"""feat(admin-ops): support_sessions, tenant_migrations, ops_tenant_rollup (ADR-0046)

The superadmin console's two new stores and the one function its cross-workspace views read.

- `support_sessions`: one staff member acting as one member of a workspace, under that
  workspace's active `impersonate` support grant. `tenant_context` refuses a token that carries
  an `act` claim unless a matching row here is open, so the time box is enforced server-side.
  Canonical tenant policy.
- `tenant_migrations`: a staff request to move a workspace to another region or database cell.
  At most one open (`requested`) row per workspace, by partial unique index. Canonical tenant
  policy. `data_residency` is not touched: identity-tenant-service stays its only writer.
- `ops_tenant_rollup(weeks, period)`: per-workspace NUMBERS for the overview, health and costs
  views. SECURITY INVOKER (the default), so it runs as the caller (`mytheus_app`) and every
  row it counts is still checked against the RLS policy: it binds `app.current_org` to each
  live workspace in turn, transaction-locally, and restores the caller's value at the end. It
  never returns a row of tenant content, only counts, sums, the plan code and the placement.
  This is the per-tenant loop the directory already does in Python, moved into one round trip.

Revision ID: b6c1d2e3f4a5
Revises: 3a87a7db84a0
Create Date: 2026-11-13
"""

from collections.abc import Sequence

from alembic import op

from migrations._grants import APP_ROLE, grant_tenant_table

revision: str = "b6c1d2e3f4a5"
down_revision: str | None = "3a87a7db84a0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"


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
        CREATE TABLE support_sessions (
          id                   uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id               uuid        NOT NULL REFERENCES organizations(id)
                                    ON DELETE RESTRICT,
          grant_id             uuid        NOT NULL REFERENCES support_grants(id)
                                    ON DELETE RESTRICT,
          staff_user_id        uuid        NOT NULL,
          staff_clerk_user_id  text        NOT NULL,
          user_id              uuid        NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
          reason               text        NOT NULL,
          clerk_actor_token_id text,
          started_at           timestamptz NOT NULL DEFAULT now(),
          expires_at           timestamptz NOT NULL,
          ended_at             timestamptz,
          end_reason           text,
          CONSTRAINT pk_support_sessions PRIMARY KEY (id),
          CONSTRAINT ck_support_sessions_window CHECK (expires_at > started_at),
          CONSTRAINT ck_support_sessions_end_reason
            CHECK (end_reason IS NULL OR end_reason IN ('staff', 'failed'))
        );
        """)
    op.execute(
        "CREATE INDEX ix_support_sessions_open ON support_sessions "
        "(org_id, staff_clerk_user_id, user_id) WHERE ended_at IS NULL;"
    )
    _tenant_policy("support_sessions")
    op.execute(
        "COMMENT ON TABLE support_sessions IS "
        "'Staff acting as a member under an impersonate grant (ADR-0046). admin-ops-service.';"
    )

    op.execute("""
        CREATE TABLE tenant_migrations (
          id           uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id       uuid        NOT NULL REFERENCES organizations(id)
                                    ON DELETE RESTRICT,
          from_region  text        NOT NULL,
          from_cell    text        NOT NULL,
          to_region    text        NOT NULL,
          to_cell      text        NOT NULL,
          status       text        NOT NULL DEFAULT 'requested',
          reason       text        NOT NULL,
          requested_by uuid        NOT NULL,
          created_at   timestamptz NOT NULL DEFAULT now(),
          updated_at   timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_tenant_migrations PRIMARY KEY (id),
          CONSTRAINT ck_tenant_migrations_status
            CHECK (status IN ('requested', 'cancelled', 'completed')),
          CONSTRAINT ck_tenant_migrations_region CHECK (to_region IN ('us', 'eu', 'ap'))
        );
        """)
    op.execute(
        "CREATE UNIQUE INDEX uq_tenant_migrations_one_open ON tenant_migrations (org_id) "
        "WHERE status = 'requested';"
    )
    _tenant_policy("tenant_migrations")
    op.execute(
        "COMMENT ON TABLE tenant_migrations IS "
        "'Staff requests to move a workspace between regions or cells (ADR-0046).';"
    )

    op.execute("""
        CREATE FUNCTION ops_tenant_rollup(p_weeks integer, p_period text)
        RETURNS TABLE (
          org_id          uuid,
          name            text,
          status          text,
          created_at      timestamptz,
          plan            text,
          region          text,
          db_cell         text,
          members          bigint,
          meetings_total   bigint,
          meetings_7d      bigint,
          meetings_weekly  bigint[],
          tokens           bigint,
          token_quota      bigint,
          outbox_pending   bigint,
          outbox_oldest   timestamptz,
          outbox_attempts  integer
        )
        LANGUAGE plpgsql
        SECURITY INVOKER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
          o record;
          previoustext := current_setting('app.current_org', true);
          week0 timestamptz := date_trunc('week', now()) - make_interval(weeks => p_weeks - 1);
        BEGIN
          IF p_weeks < 1 OR p_weeks > 52 THEN
            RAISE EXCEPTION 'p_weeks must be 1..52' USING ERRCODE = '22023';
          END IF;
          FOR o IN
            SELECT g.id, g.name, g.status, g.created_at FROM organizations g
            WHERE g.deleted_at IS NULL ORDER BY g.created_at DESC, g.id DESC
          LOOP
            PERFORM set_config('app.current_org', o.id::text, true);
            org_id := o.id;
            name := o.name;
            status := o.status;
            created_at := o.created_at;
            SELECT p.code INTO plan FROM subscriptions s JOIN plans p ON p.id = s.plan_id LIMIT 1;
            SELECT d.region, d.db_cell INTO region, db_cell FROM data_residency d LIMIT 1;
            SELECT count(*) INTO members FROM users u WHERE u.deleted_at IS NULL;
            SELECT count(*), count(*) FILTER (WHERE m.created_at >= now() - interval '7 days')
              INTO meetings_total, meetings_7d FROM meetings m;
            SELECT array_agg(coalesce(c.n, 0) ORDER BY w.wk) INTO meetings_weekly
              FROM generate_series(week0, date_trunc('week', now()), interval '1 week') AS w(wk)
              LEFT JOIN (
                SELECT date_trunc('week', m.created_at) AS wk, count(*) AS n FROM meetings m
                WHERE m.created_at >= week0 GROUP BY 1
              ) c ON c.wk = w.wk;
            SELECT coalesce(sum(a.quantity), 0) INTO tokens FROM usage_aggregates a
              WHERE a.dimension = 'agent_token' AND a.billing_period = p_period;
            SELECT b.token_quota INTO token_quota FROM agent_budgets b
              WHERE b.period = p_period LIMIT 1;
            SELECT count(*), min(x.created_at), coalesce(max(x.attempts), 0)
              INTO outbox_pending, outbox_oldest, outbox_attempts
              FROM outbox x WHERE x.published_at IS NULL;
            RETURN NEXT;
            plan := NULL; region := NULL; db_cell := NULL; token_quota := NULL;
          END LOOP;
          PERFORM set_config('app.current_org', coalesce(previous, ''), true);
        END;
        $$;
        """)
    op.execute("REVOKE ALL ON FUNCTION ops_tenant_rollup(integer, text) FROM PUBLIC;")
    op.execute(f"GRANT EXECUTE ON FUNCTION ops_tenant_rollup(integer, text) TO {APP_ROLE};")


def downgrade() -> None:
    op.execute("DROP FUNCTION IF EXISTS ops_tenant_rollup(integer, text);")
    op.execute("DROP TABLE IF EXISTS tenant_migrations CASCADE;")
    op.execute("DROP TABLE IF EXISTS support_sessions CASCADE;")
