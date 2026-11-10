"""feat(notification-service): notification_log and notification_preferences (P11)

Plan 10-13 section 3.8; P11 contract section 3. Both tables are owned and written only by
`notification-service`:

- `notification_log`: one row per (notification, recipient). `id` is the source's own id: a
  `ProactiveAgent` alert's deterministic `alert.id` (uuid5, one per recipient), or for a
  `notifications` event uuid5 of `notifications:{source_event_id}:{user_id}`. The consumer
  inserts with `ON CONFLICT (id) DO NOTHING`, so a replay never writes a second row. The row
  is the in-app delivery: `GET /notifications` serves it, and the recipient moves it
  `unread -> read -> dismissed` through `PATCH /notifications/{id}`.
- `notification_preferences`: the recipient's per-channel switch and priority ceiling. A
  channel delivers when `enabled` and `priority <= max_priority` (1 is the most urgent).
  Missing rows fall back to the defaults in `services/notification_service/prefs.py`
  (in_app on, 3; email off, 1; slack off, 1). Email is opt-in.

`meeting_id` has no foreign key: `meetings` belongs to core-api, and the consumer checks the
meeting against the tenant under RLS before writing (gap 31). `user_id` references `users`
like every other per-user table.

RLS ENABLE + FORCE with the canonical NULLIF `tenant_isolation` policy and the explicit
`grant_tenant_table` (ADR-0021).

Revision ID: d4b8e2f61a37
Revises: c8f2d3e4a510
Create Date: 2026-11-10
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "d4b8e2f61a37"
down_revision: Union[str, None] = "c8f2d3e4a510"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

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
        CREATE TABLE notification_log (
          id            uuid        NOT NULL,
          org_id        uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id       uuid        NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
          meeting_id    uuid,
          source        text        NOT NULL,
          alert_type    text,
          priority      integer     NOT NULL,
          title         text        NOT NULL,
          body          text        NOT NULL,
          link          text,
          payload       jsonb       NOT NULL DEFAULT '{}'::jsonb,
          status        text        NOT NULL DEFAULT 'unread',
          created_at    timestamptz NOT NULL DEFAULT now(),
          read_at       timestamptz,
          dismissed_at  timestamptz,
          CONSTRAINT pk_notification_log PRIMARY KEY (id),
          CONSTRAINT ck_notification_log_source CHECK (source IN ('agent_alert', 'notification')),
          CONSTRAINT ck_notification_log_alert_type CHECK (
            alert_type IS NULL
            OR alert_type IN ('conflict', 'workload', 'dependency', 'missing_stakeholder')
          ),
          CONSTRAINT ck_notification_log_priority CHECK (priority BETWEEN 1 AND 3),
          CONSTRAINT ck_notification_log_status CHECK (status IN ('unread', 'read', 'dismissed'))
        );
        """)
    op.execute(
        "CREATE INDEX ix_notification_log_org_user_status_created "
        "ON notification_log (org_id, user_id, status, created_at);"
    )
    _tenant_policy("notification_log")
    op.execute(
        "COMMENT ON TABLE notification_log IS "
        "'Per-recipient notifications and ProactiveAgent alerts (P11). Written only by "
        "notification-service: the consumer inserts, the recipient reads or dismisses.';"
    )

    op.execute("""
        CREATE TABLE notification_preferences (
          org_id        uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id       uuid        NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
          channel       text        NOT NULL,
          enabled       boolean     NOT NULL DEFAULT false,
          max_priority  integer     NOT NULL DEFAULT 1,
          updated_at    timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_notification_preferences PRIMARY KEY (org_id, user_id, channel),
          CONSTRAINT ck_notification_preferences_channel
            CHECK (channel IN ('in_app', 'email', 'slack')),
          CONSTRAINT ck_notification_preferences_max_priority CHECK (max_priority BETWEEN 1 AND 3)
        );
        """)
    _tenant_policy("notification_preferences")
    op.execute(
        "COMMENT ON TABLE notification_preferences IS "
        "'Per-user channel preferences (P11). Deliver on a channel when enabled and "
        "priority <= max_priority. Written only by notification-service.';"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS notification_preferences CASCADE;")
    op.execute("DROP TABLE IF EXISTS notification_log CASCADE;")
