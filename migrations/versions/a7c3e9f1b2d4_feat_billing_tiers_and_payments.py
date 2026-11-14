"""feat(billing): Free / Pro / Ultra / Pay as you go / Unlimited, credit top-ups, Razorpay

The catalog (plans are versioned, never edited in place):

| code        | name           | price                         | agent tokens               |
| ----------- | -------------- | ----------------------------- | -------------------------- |
| `free`      | Free           | $0                            | 10,000 a month             |
| `pro`       | Pro            | $25 a month                   | 200,000 a month            |
| `ultra`     | Ultra          | $200 a month                  | 2,000,000 a month          |
| `payg`      | Pay as you go  | $500 deposit, settled monthly | no cap, metered            |
| `unlimited` | Unlimited      | staff only, for testing       | no cap (10^15)             |

`seat_price_cents` is the workspace's price: the monthly price for Pro and Ultra, the deposit for
Pay as you go. Pay as you go is metered at `PAYG_CENTS_PER_MILLION` (billing code); at the end of
its 30-day window the usage is priced, and the difference with the deposit is either refunded
(through Razorpay) or billed (a balance the admin pays from Usage and billing). That is recorded in
`payg_settlements`, one row per deposit. "Unlimited" is set only by Mytheus staff from the ops
console; it and Pay as you go carry a cap too large to reach, so the agent tier's `hard_cap` check
needs no special case.

Every subscription moves to the newest version of its plan, and this month's `agent_budgets`
rows take the new allowance (`hard_cap = token_quota`: the allowance is a hard stop, with no
throttle band). Recomputing `state` here means a workspace already past 10,000 tokens on Free is
blocked from the moment this runs, which is the intent.

`agent_budgets` gains `alert_75_sent` / `alert_90_sent` (admins are warned at 75%, 90% and 95%;
`alert_80_sent` stays for old rows and is no longer set) and `credit_tokens`, the tokens bought on
top of the plan this month (credit packs), so a plan change mid-month keeps what was paid for.

`payment_orders` records each Razorpay checkout (tenant-scoped, RLS): a plan month, a Pay as you go
deposit, a credit pack or a settlement balance; the amount and currency charged; whether it was
paid. `subscriptions.current_period_end` is set for a bought plan (30 days from payment) and NULL
for Free and for a plan set by staff, which do not lapse. Both new tables are
billing-metering-service's alone.

Revision ID: a7c3e9f1b2d4
Revises: f1a2b3c4d5e6
Create Date: 2026-11-14
"""

from collections.abc import Sequence

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "a7c3e9f1b2d4"
down_revision: str | None = "f1a2b3c4d5e6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"
NO_CAP_TOKENS = 10**15


def _tenant_table(table: str) -> None:
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON {table} "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    grant_tenant_table(op, table)


def upgrade() -> None:
    op.execute("ALTER TABLE plans DROP CONSTRAINT ck_plans_code_valid;")
    op.execute(
        "ALTER TABLE plans ADD CONSTRAINT ck_plans_code_valid "
        "CHECK (code IN ('free','pro','ultra','payg','unlimited','enterprise'));"
    )
    # A space after every JSON colon: op.execute() wraps the string in text(), which would read
    # `"key":false` as a bind parameter named `false`.
    op.execute(f"""
        INSERT INTO plans (code, name, seat_price_cents, included_meeting_minutes,
                           included_agent_tokens, features, version) VALUES
          ('free', 'Free', 0, 300, 10000,
           '{{"context_gap_agent": false, "proactive_agent": false, "support_tier": "community"}}',
           2),
          ('pro', 'Pro', 2500, 3000, 200000,
           '{{"context_gap_agent": true, "proactive_agent": false, "support_tier": "email"}}',
           2),
          ('ultra', 'Ultra', 20000, 20000, 2000000,
           '{{"context_gap_agent": true, "proactive_agent": true, "support_tier": "priority"}}',
           1),
          ('payg', 'Pay as you go', 50000, 0, {NO_CAP_TOKENS},
           '{{"context_gap_agent": true, "proactive_agent": true, "support_tier": "priority"}}',
           1),
          ('unlimited', 'Unlimited', 0, 0, {NO_CAP_TOKENS},
           '{{"context_gap_agent": true, "proactive_agent": true, "support_tier": "priority"}}',
           1);
        """)
    # Every subscription onto the newest version of its plan.
    op.execute("""
        UPDATE subscriptions s SET plan_id = latest.id, updated_at = now()
        FROM plans cur,
             LATERAL (SELECT id FROM plans p WHERE p.code = cur.code
                      ORDER BY p.version DESC LIMIT 1) latest
        WHERE cur.id = s.plan_id AND s.plan_id <> latest.id;
        """)

    op.execute("""
        ALTER TABLE agent_budgets
          ADD COLUMN alert_75_sent boolean NOT NULL DEFAULT false,
          ADD COLUMN alert_90_sent boolean NOT NULL DEFAULT false,
          ADD COLUMN credit_tokens bigint NOT NULL DEFAULT 0,
          ADD CONSTRAINT ck_agent_budgets_credit_tokens_nonneg CHECK (credit_tokens >= 0);
        """)
    # This month's budgets take the new allowance and a hard stop at it.
    op.execute("""
        UPDATE agent_budgets b
        SET token_quota = p.included_agent_tokens + b.credit_tokens,
            hard_cap    = p.included_agent_tokens + b.credit_tokens,
            state = CASE WHEN b.tokens_consumed >= p.included_agent_tokens + b.credit_tokens
                         THEN 'blocked' ELSE 'ok' END,
            updated_at = now()
        FROM subscriptions s JOIN plans p ON p.id = s.plan_id
        WHERE s.org_id = b.org_id AND b.period = to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM');
        """)

    op.execute("""
        CREATE TABLE payment_orders (
          id                  uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id              uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          provider            text        NOT NULL DEFAULT 'razorpay',
          provider_order_id   text        NOT NULL,
          provider_payment_id text,
          kind                text        NOT NULL,
          plan_code           text,
          tokens              bigint,
          settlement_id       uuid,
          amount_minor        bigint      NOT NULL,
          currency            text        NOT NULL,
          usd_cents           bigint      NOT NULL,
          status              text        NOT NULL DEFAULT 'created',
          created_by          uuid,
          created_at          timestamptz NOT NULL DEFAULT now(),
          paid_at             timestamptz,
          CONSTRAINT pk_payment_orders PRIMARY KEY (id),
          CONSTRAINT uq_payment_orders_provider_order UNIQUE (provider, provider_order_id),
          CONSTRAINT ck_payment_orders_kind
            CHECK (kind IN ('plan','credits','settlement')),
          CONSTRAINT ck_payment_orders_status CHECK (status IN ('created','paid','failed')),
          CONSTRAINT ck_payment_orders_what CHECK (
            (kind = 'plan' AND plan_code IN ('pro','ultra','payg'))
            OR (kind = 'credits' AND tokens > 0)
            OR (kind = 'settlement' AND settlement_id IS NOT NULL)
          ),
          CONSTRAINT ck_payment_orders_amount CHECK (amount_minor > 0 AND usd_cents > 0)
        );
        """)
    op.execute("CREATE INDEX ix_payment_orders_org_created ON payment_orders (org_id, created_at);")
    _tenant_table("payment_orders")

    # One row per Pay as you go deposit, written when its window ends. `balance_cents` > 0 is
    # owed by the workspace (`due` until paid), < 0 is refunded to it, 0 is settled as is.
    op.execute("""
        CREATE TABLE payg_settlements (
          id                 uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id             uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          deposit_order_id   uuid        NOT NULL REFERENCES payment_orders(id),
          window_start       timestamptz NOT NULL,
          window_end         timestamptz NOT NULL,
          tokens             bigint      NOT NULL,
          cost_cents         bigint      NOT NULL,
          deposit_cents      bigint      NOT NULL,
          balance_cents      bigint      NOT NULL,
          status             text        NOT NULL,
          refund_id          text,
          created_at         timestamptz NOT NULL DEFAULT now(),
          settled_at         timestamptz,
          CONSTRAINT pk_payg_settlements PRIMARY KEY (id),
          CONSTRAINT uq_payg_settlements_deposit UNIQUE (deposit_order_id),
          CONSTRAINT ck_payg_settlements_status
            CHECK (status IN ('settled','due','paid','refund_pending','refunded','refund_failed'))
        );
        """)
    op.execute("CREATE INDEX ix_payg_settlements_org ON payg_settlements (org_id, created_at);")
    _tenant_table("payg_settlements")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS payg_settlements;")
    op.execute("DROP TABLE IF EXISTS payment_orders;")
    op.execute("""
        ALTER TABLE agent_budgets
          DROP CONSTRAINT IF EXISTS ck_agent_budgets_credit_tokens_nonneg,
          DROP COLUMN IF EXISTS credit_tokens,
          DROP COLUMN IF EXISTS alert_90_sent,
          DROP COLUMN IF EXISTS alert_75_sent;
        """)
    op.execute("""
        UPDATE subscriptions s SET plan_id = v1.id
        FROM plans cur JOIN plans v1
          ON v1.code = (CASE WHEN cur.code IN ('ultra','payg','unlimited') THEN 'pro'
                             ELSE cur.code END)
         AND v1.version = 1
        WHERE cur.id = s.plan_id;
        """)
    op.execute("DELETE FROM plans WHERE code IN ('ultra','payg','unlimited') OR version > 1;")
    op.execute("ALTER TABLE plans DROP CONSTRAINT ck_plans_code_valid;")
    op.execute(
        "ALTER TABLE plans ADD CONSTRAINT ck_plans_code_valid "
        "CHECK (code IN ('free','pro','enterprise'));"
    )
