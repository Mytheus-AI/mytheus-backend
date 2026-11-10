"""feat(integration-service): integration_connections, id map, cursors, webhook inbox (P12)

Plan 10-13 sections 3.10 and 3.11; P12 contract section 1. `oauth_tokens` already exists
(`24e134664d84`); this adds what it was waiting for and extends nothing it defined.

- `integration_connections` (canon `_canon/02-data-model.md` 2.2, plus `created_at`,
  `updated_at`, `last_synced_at`): one row per (org, provider), `UNIQUE (org_id, provider)`,
  and one live connection per provider account across tenants (partial unique index
  `uq_integration_connections_account` on (provider, external_account_id)).
  Provider is the R8 closed enum; status is active | degraded | disconnected. A disconnect
  keeps the row (status `disconnected`) so the settings page can still say who installed it.
- `oauth_tokens.connection_id` gets the foreign key `24e134664d84` recorded as owed.
- `integration_id_map`: external <-> NEXUS ids (a Jira issue for an action, a calendar event for
  a meeting), with `last_synced_status`, the loop-suppression watermark.
- `integration_cursors`: per connection resume state (Google `syncToken` and push channel, the
  Jira site and its dynamic webhook ids), with `expires_at` for the renewal job.
- `integration_webhook_inbox`: the fast-ACK inbox. The receiver writes one row per provider
  delivery (`UNIQUE (org_id, provider, delivery_id)` is the replay guard) and returns 200; the
  worker drains it.
- `meetings.calendar_event_id`: the idempotency anchor for calendar-created meetings, with a
  partial unique index on (org_id, calendar_event_id). core-api owns `meetings`; its
  `integration-events` consumer is the only writer of this column.

RLS: ENABLE + FORCE + the canonical NULLIF `tenant_isolation` policy on every new table, and
explicit `grant_tenant_table` (ADR-0021).

One extra, narrow policy, `webhook_route`, on `integration_connections`: SELECT only, and only
the single live (not disconnected) row whose `provider:external_account_id` equals the
transaction-local GUC `app.integration_route` (single because of
`uq_integration_connections_account`). An inbound Slack or Jira webhook names its workspace or site, not our
tenant, and must be mapped to an `org_id` before anything tenant-scoped can run. The receiver
sets the GUC only AFTER the provider signature has verified, reads the one row it names, and
then sets `app.current_org` as usual. With the GUC unset or empty the policy matches nothing,
so the table still fails closed. This is the `organizations` reasoning (a keyed lookup before a
tenant is known), confined to an equality on a key the caller must already hold.

Revision ID: 7c3e9a1f5b20
Revises: d4b8e2f61a37
Create Date: 2026-11-10
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "7c3e9a1f5b20"
down_revision: Union[str, None] = "d4b8e2f61a37"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"

_NEW_TABLES = (
    "integration_connections",
    "integration_id_map",
    "integration_cursors",
    "integration_webhook_inbox",
)


def _tenant_policy(table: str) -> None:
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON {table} "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    grant_tenant_table(op, table)


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE integration_connections (
          id                  uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id              uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          provider            text        NOT NULL,
          external_account_id text,
          status              text        NOT NULL DEFAULT 'active',
          scopes              text[]      NOT NULL DEFAULT '{}',
          installed_by        uuid        NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
          created_at          timestamptz NOT NULL DEFAULT now(),
          updated_at          timestamptz NOT NULL DEFAULT now(),
          last_synced_at      timestamptz,
          CONSTRAINT pk_integration_connections PRIMARY KEY (id),
          CONSTRAINT uq_integration_connections_org_id_provider UNIQUE (org_id, provider),
          CONSTRAINT ck_integration_connections_provider
            CHECK (provider IN ('google_calendar', 'slack', 'jira', 'linear')),
          CONSTRAINT ck_integration_connections_status
            CHECK (status IN ('active', 'degraded', 'disconnected'))
        );
        """
    )
    # One live connection per provider account across ALL tenants: a Slack workspace, a Jira
    # site or a Google account belongs to one Mytheus tenant at a time. Without this, two
    # tenants could connect the same workspace and a webhook from it could be routed to either.
    # It is also what makes the `webhook_route` lookup below exact. A disconnected row does not
    # hold the account, so it can move to another tenant after a disconnect.
    op.execute(
        "CREATE UNIQUE INDEX uq_integration_connections_account "
        "ON integration_connections (provider, external_account_id) "
        "WHERE status <> 'disconnected' AND external_account_id IS NOT NULL;"
    )
    _tenant_policy("integration_connections")
    op.execute(
        "CREATE POLICY webhook_route ON integration_connections FOR SELECT USING ("
        "external_account_id IS NOT NULL AND status <> 'disconnected' AND "
        "provider || ':' || external_account_id = "
        "NULLIF(current_setting('app.integration_route', true), ''));"
    )

    op.execute(
        "ALTER TABLE oauth_tokens ADD CONSTRAINT fk_oauth_tokens_connection_id_integration_connections "
        "FOREIGN KEY (connection_id) REFERENCES integration_connections(id) ON DELETE CASCADE;"
    )

    op.execute(
        """
        CREATE TABLE integration_id_map (
          id                 uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id             uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          provider           text        NOT NULL,
          nexus_kind         text        NOT NULL,
          nexus_id           uuid        NOT NULL,
          external_id        text        NOT NULL,
          external_url       text,
          last_synced_status text,
          created_at         timestamptz NOT NULL DEFAULT now(),
          updated_at         timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_integration_id_map PRIMARY KEY (id),
          CONSTRAINT uq_integration_id_map_nexus UNIQUE (org_id, provider, nexus_kind, nexus_id),
          CONSTRAINT uq_integration_id_map_external UNIQUE (org_id, provider, external_id),
          CONSTRAINT ck_integration_id_map_nexus_kind CHECK (nexus_kind IN ('action', 'meeting'))
        );
        """
    )
    _tenant_policy("integration_id_map")

    op.execute(
        """
        CREATE TABLE integration_cursors (
          id            uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id        uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          connection_id uuid        NOT NULL
                        REFERENCES integration_connections(id) ON DELETE CASCADE,
          kind          text        NOT NULL,
          cursor        jsonb       NOT NULL DEFAULT '{}'::jsonb,
          expires_at    timestamptz,
          updated_at    timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_integration_cursors PRIMARY KEY (id),
          CONSTRAINT uq_integration_cursors_connection_kind UNIQUE (org_id, connection_id, kind)
        );
        """
    )
    _tenant_policy("integration_cursors")

    op.execute(
        """
        CREATE TABLE integration_webhook_inbox (
          id           uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id       uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          provider     text        NOT NULL,
          delivery_id  text        NOT NULL,
          event_type   text,
          payload      jsonb       NOT NULL,
          status       text        NOT NULL DEFAULT 'pending',
          attempts     integer     NOT NULL DEFAULT 0,
          last_error   text,
          received_at  timestamptz NOT NULL DEFAULT now(),
          processed_at timestamptz,
          CONSTRAINT pk_integration_webhook_inbox PRIMARY KEY (id),
          CONSTRAINT uq_integration_webhook_inbox_delivery UNIQUE (org_id, provider, delivery_id),
          CONSTRAINT ck_integration_webhook_inbox_status
            CHECK (status IN ('pending', 'processed', 'skipped', 'failed'))
        );
        """
    )
    op.execute(
        "CREATE INDEX ix_integration_webhook_inbox_pending "
        "ON integration_webhook_inbox (org_id, received_at) WHERE status = 'pending';"
    )
    _tenant_policy("integration_webhook_inbox")

    op.execute("ALTER TABLE meetings ADD COLUMN calendar_event_id text;")
    op.execute(
        "CREATE UNIQUE INDEX uq_meetings_org_id_calendar_event_id "
        "ON meetings (org_id, calendar_event_id) WHERE calendar_event_id IS NOT NULL;"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_meetings_org_id_calendar_event_id;")
    op.execute("ALTER TABLE meetings DROP COLUMN IF EXISTS calendar_event_id;")
    op.execute(
        "ALTER TABLE oauth_tokens DROP CONSTRAINT IF EXISTS "
        "fk_oauth_tokens_connection_id_integration_connections;"
    )
    for table in reversed(_NEW_TABLES):
        op.execute(f"DROP TABLE IF EXISTS {table};")
