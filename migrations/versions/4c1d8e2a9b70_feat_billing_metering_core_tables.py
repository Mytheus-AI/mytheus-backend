"""feat(billing-metering-service): plans, subscriptions, invoices, usage_events, usage_aggregates, agent_budgets, quotas

Plan 7-9 section 3.1 step 2 (build first: the agent tier cannot dispatch an LLM call until a
budget exists to check). Every table here is owned and written only by
`billing-metering-service` (`_canon/02-data-model.md` section 2.2).

Decisions this migration applies, all in mytheus-docs ADR-0029:

- D1, per-tenant allowances. `plans.included_agent_tokens` / `included_meeting_minutes` are
  per-tenant monthly pools. The seed below is the plan text's catalog; the semantics live in
  the provisioning code (`services/billing_metering_service/budgets.py`), not here.
- D4, `usage_events.model_tier`. Nullable, CHECK-constrained to the tier names.
- D6, `usage_events` is partitioned by MONTH with a DEFAULT catch-all, like `chat_messages`
  and `audit_logs` (`9817d41e0603`), and is registered in `scripts/ensure_partitions.py` so
  `tests/test_partition_coverage.py` watches it. The dedupe backstop is UNIQUE
  `(org_id, idempotency_key, occurred_at)`: Postgres requires the partition key in a unique
  index on a partitioned table, and `occurred_at` is already a component of the key
  (`usage:{org_id}:{event_type}:{occurred_at}:{source_id}`), so it is equivalent to the
  canon's `(org_id, idempotency_key)`.

Grants (ADR-0021, `9b2c4d1e6f30`): nothing is inherited, so every table is granted
explicitly. `plans` is the one global, non-tenant table: readable by the request role, and
writable only by the schema owner (catalog changes are migrations). `usage_events` is
append-only for the application roles (SELECT + INSERT): a metered fact is never edited, and
retention is by dropping a partition, which is DDL reserved to the owner. Partitions get an
owner-only ACL and are reachable only through the parent, where the RLS policy lives.

Revision ID: 4c1d8e2a9b70
Revises: 9817d41e0603
Create Date: 2026-11-08
"""

from __future__ import annotations

from datetime import date
from typing import Sequence, Union

from alembic import op

from migrations._grants import ADMIN_ROLE, APP_ROLE, grant_append_only_table, grant_tenant_table

# revision identifiers, used by Alembic.
revision: str = "4c1d8e2a9b70"
down_revision: Union[str, None] = "9817d41e0603"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"

# Fixed constants, not date.today(): a migration must replay identically (9817d41e0603).
_FIRST = (2026, 11)
_LAST = (2027, 12)
_USAGE_INDEXES = ("CREATE INDEX ix_{p}_org_occurred ON {p} (org_id, occurred_at)",)
_LOCKDOWN_ROLES = "mytheus_app, mytheus_admin, mytheus_relay, PUBLIC"
_SWEEP_ONE = """
DO $$
DECLARE r record;
BEGIN
    FOR r IN
        SELECT DISTINCT a.grantee AS g
        FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a
        WHERE c.oid = '{p}'::regclass AND a.grantee <> c.relowner AND a.grantee <> 0
    LOOP
        EXECUTE format('REVOKE ALL ON {p} FROM %I', pg_get_userbyid(r.g));
    END LOOP;
END $$;
"""


def _force_rls(table: str) -> None:
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON {table} "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )


def _lock_down(p: str) -> None:
    op.execute(f"REVOKE ALL ON {p} FROM {_LOCKDOWN_ROLES};")
    op.execute(_SWEEP_ONE.format(p=p))


def _months() -> list[tuple[int, int]]:
    y, m = _FIRST
    out = []
    while (y, m) <= _LAST:
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def _bounds(y: int, m: int) -> tuple[str, str]:
    end = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
    return date(y, m, 1).isoformat(), end.isoformat()


def upgrade() -> None:
    # --- plans: global catalog, versioned, never hard-deleted (invoices must reproduce) -----
    op.execute(
        """
        CREATE TABLE plans (
          id                       uuid        NOT NULL DEFAULT gen_random_uuid(),
          code                     text        NOT NULL,
          name                     text        NOT NULL,
          seat_price_cents         integer     NOT NULL DEFAULT 0,
          included_meeting_minutes bigint      NOT NULL,
          included_agent_tokens    bigint      NOT NULL,
          features                 jsonb       NOT NULL DEFAULT '{}'::jsonb,
          version                  integer     NOT NULL DEFAULT 1,
          created_at               timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_plans PRIMARY KEY (id),
          CONSTRAINT uq_plans_code_version UNIQUE (code, version),
          CONSTRAINT ck_plans_code_valid CHECK (code IN ('free','pro','enterprise'))
        );
        """
    )
    op.execute(f"GRANT SELECT ON plans TO {APP_ROLE}, {ADMIN_ROLE};")
    # A space after every JSON colon: op.execute() wraps the string in text(), which would
    # read `"key":false` as a bind parameter named `false`.
    op.execute(
        """
        INSERT INTO plans (code, name, seat_price_cents, included_meeting_minutes,
                           included_agent_tokens, features) VALUES
          ('free', 'Free', 0, 300, 200000,
           '{"context_gap_agent": false,"proactive_agent": false,"max_concurrent_meetings": 20,"retention_days_max": 90,"support_tier": "community"}'),
          ('pro', 'Pro', 2500, 3000, 4000000,
           '{"context_gap_agent": true,"proactive_agent": false,"integrations": ["google_calendar","slack"]}'),
          ('enterprise', 'Enterprise', 0, 0, 0,
           '{"sso_saml": true,"scim_provisioning": true,"cell_isolation": true,"proactive_agent": true}');
        """
    )

    # --- subscriptions: one per tenant -------------------------------------------------------
    op.execute(
        """
        CREATE TABLE subscriptions (
          id                     uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id                 uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          plan_id                uuid        NOT NULL REFERENCES plans(id),
          stripe_subscription_id text,
          status                 text        NOT NULL DEFAULT 'active',
          seats                  integer     NOT NULL DEFAULT 1,
          trial_ends_at          timestamptz,
          current_period_start   timestamptz,
          current_period_end     timestamptz,
          created_at             timestamptz NOT NULL DEFAULT now(),
          updated_at             timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_subscriptions PRIMARY KEY (id),
          CONSTRAINT uq_subscriptions_org_id UNIQUE (org_id),
          CONSTRAINT ck_subscriptions_status_valid
            CHECK (status IN ('trialing','active','past_due','canceled')),
          CONSTRAINT ck_subscriptions_seats_positive CHECK (seats >= 1)
        );
        """
    )
    _force_rls("subscriptions")
    grant_tenant_table(op, "subscriptions")

    # --- invoices: immutable once finalized; Stripe webhook + dunning land in their own band --
    op.execute(
        """
        CREATE TABLE invoices (
          id                uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id            uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          subscription_id   uuid        NOT NULL REFERENCES subscriptions(id),
          stripe_invoice_id text,
          period_start      timestamptz,
          period_end        timestamptz,
          status            text        NOT NULL,
          amount_due_cents  integer     NOT NULL,
          tax_cents         integer     NOT NULL DEFAULT 0,
          pdf_url           text,
          created_at        timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_invoices PRIMARY KEY (id),
          CONSTRAINT ck_invoices_status_valid
            CHECK (status IN ('draft','open','paid','void','uncollectible'))
        );
        """
    )
    op.execute("CREATE INDEX ix_invoices_org_id ON invoices (org_id);")
    _force_rls("invoices")
    grant_tenant_table(op, "invoices")

    # --- usage_events: append-only metering feed, monthly partitions + DEFAULT (D6) ---------
    op.execute(
        """
        CREATE TABLE usage_events (
          id              uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id          uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id         uuid,
          event_type      text        NOT NULL,
          quantity        numeric     NOT NULL,
          model_tier      text,
          idempotency_key text        NOT NULL,
          occurred_at     timestamptz NOT NULL,
          CONSTRAINT pk_usage_events PRIMARY KEY (id, occurred_at),
          CONSTRAINT uq_usage_events_org_key UNIQUE (org_id, idempotency_key, occurred_at),
          CONSTRAINT ck_usage_events_event_type_valid
            CHECK (event_type IN ('meeting_minute','agent_token','transcription_second','storage_gb')),
          CONSTRAINT ck_usage_events_model_tier_valid
            CHECK (model_tier IS NULL OR model_tier IN ('haiku','sonnet','opus')),
          CONSTRAINT ck_usage_events_quantity_nonneg CHECK (quantity >= 0)
        ) PARTITION BY RANGE (occurred_at);
        """
    )
    op.execute("CREATE TABLE usage_events_default PARTITION OF usage_events DEFAULT;")
    for tmpl in _USAGE_INDEXES:
        op.execute(tmpl.format(p="usage_events_default") + ";")
    _lock_down("usage_events_default")
    for y, m in _months():
        start, end = _bounds(y, m)
        p = f"usage_events_{y:04d}_{m:02d}"
        op.execute(
            f"CREATE TABLE {p} PARTITION OF usage_events "
            f"FOR VALUES FROM ('{start}') TO ('{end}');"
        )
        for tmpl in _USAGE_INDEXES:
            op.execute(tmpl.format(p=p) + ";")
        _lock_down(p)
    _force_rls("usage_events")
    grant_append_only_table(op, "usage_events")
    op.execute(
        "COMMENT ON TABLE usage_events IS "
        "'Append-only metering feed, monthly range-partitioned on occurred_at with a DEFAULT "
        "catch-all (ADR-0029 D6). agent_token rows come from the usage-events topic; the other "
        "three dimensions are derived inside billing-metering-service (ADR-0029 D3). "
        "scripts/ensure_partitions.py keeps months ahead of the calendar.';"
    )

    # --- usage_aggregates: nightly rollup, idempotent upsert key -----------------------------
    op.execute(
        """
        CREATE TABLE usage_aggregates (
          id             uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id         uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          dimension      text        NOT NULL,
          billing_period text        NOT NULL,
          quantity       numeric     NOT NULL,
          tier_breakdown jsonb       NOT NULL DEFAULT '{}'::jsonb,
          updated_at     timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_usage_aggregates PRIMARY KEY (id),
          CONSTRAINT uq_usage_aggregates_org_dim_period UNIQUE (org_id, dimension, billing_period),
          CONSTRAINT ck_usage_aggregates_dimension_valid
            CHECK (dimension IN ('meeting_minute','agent_token','transcription_second','storage_gb')),
          CONSTRAINT ck_usage_aggregates_period_format CHECK (billing_period ~ '^[0-9]{4}-[0-9]{2}$')
        );
        """
    )
    _force_rls("usage_aggregates")
    grant_tenant_table(op, "usage_aggregates")

    # --- agent_budgets: per tenant per month; the ok -> throttled -> blocked state machine ---
    op.execute(
        """
        CREATE TABLE agent_budgets (
          id              uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id          uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          period          text        NOT NULL,
          token_quota     bigint      NOT NULL,
          tokens_consumed bigint      NOT NULL DEFAULT 0,
          hard_cap        bigint      NOT NULL,
          alert_80_sent   boolean     NOT NULL DEFAULT false,
          alert_95_sent   boolean     NOT NULL DEFAULT false,
          state           text        NOT NULL DEFAULT 'ok',
          created_at      timestamptz NOT NULL DEFAULT now(),
          updated_at      timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_agent_budgets PRIMARY KEY (id),
          CONSTRAINT uq_agent_budgets_org_period UNIQUE (org_id, period),
          CONSTRAINT ck_agent_budgets_state_valid CHECK (state IN ('ok','throttled','blocked')),
          CONSTRAINT ck_agent_budgets_period_format CHECK (period ~ '^[0-9]{4}-[0-9]{2}$'),
          CONSTRAINT ck_agent_budgets_cap_at_least_quota CHECK (hard_cap >= token_quota)
        );
        """
    )
    _force_rls("agent_budgets")
    grant_tenant_table(op, "agent_budgets")

    # --- quotas: the non-token dimensions --------------------------------------------------
    op.execute(
        """
        CREATE TABLE quotas (
          id        uuid    NOT NULL DEFAULT gen_random_uuid(),
          org_id    uuid    NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          dimension text    NOT NULL,
          period    text    NOT NULL,
          quota     numeric NOT NULL,
          consumed  numeric NOT NULL DEFAULT 0,
          state     text    NOT NULL DEFAULT 'ok',
          CONSTRAINT pk_quotas PRIMARY KEY (id),
          CONSTRAINT uq_quotas_org_dim_period UNIQUE (org_id, dimension, period),
          CONSTRAINT ck_quotas_dimension_valid
            CHECK (dimension IN ('meeting_minute','transcription_second','storage_gb')),
          CONSTRAINT ck_quotas_state_valid CHECK (state IN ('ok','throttled','blocked'))
        );
        """
    )
    _force_rls("quotas")
    grant_tenant_table(op, "quotas")


def downgrade() -> None:
    for table in (
        "quotas",
        "agent_budgets",
        "usage_aggregates",
        "usage_events",
        "invoices",
        "subscriptions",
        "plans",
    ):
        op.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
