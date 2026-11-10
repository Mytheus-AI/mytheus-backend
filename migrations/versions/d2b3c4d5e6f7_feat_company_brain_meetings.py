"""feat: meeting types, confidential meetings, agenda goals, proxies, promises, status requests (ADR-0039 Part B)

ADR-0039 B1 to B7, the core-api half, in one forward-only change. Every table here is owned by
core-api (one writer), tenant-scoped by `org_id`, ENABLE + FORCE RLS with the canonical NULLIF
`tenant_isolation` policy and the explicit `grant_tenant_table` (ADR-0021), as every tenant
table since `9b2c4d1e6f30`.

- `meeting_templates` (B1): a named meeting type with a default length, an agenda
  (`[{title, duration_minutes}]`, at most 20, a CHECK on the jsonb array) and a confidential
  default. The 50-per-org cap is the route's (422), like the agenda's 20-item cap.
- `meetings.confidential` and `meetings.template_id` (B2). `template_id` is `ON DELETE SET NULL`:
  deleting a meeting type never deletes or blocks the meetings made from it.
- `meeting_agenda_items.goal_id` (B3): an `org_goals` id. No FK: `org_goals` is
  identity-tenant-service's table, and a cross-service FK would couple the two writers.
- `meeting_participants.attendance` / `proxy_update` / `proxy_questions` (B5). `attendance` has
  the two-value CHECK; the length and count rules are CHECKs too, the per-question 300-char
  rule is the route's (a CHECK over array elements needs a function).
- `commitments` (B6): written only by core-api's `agent-results` consumer from
  PersonalListenerAgent records, private to `user_id`. The unique index on
  `(meeting_id, user_id, source_window_id, md5(content))` is what makes a replayed window a
  no-op even when the inbox row is gone.
- `action_status_requests` (B7): one per (action, upcoming meeting), unique, created by the
  internal route workflow-service calls. `action_id` cascades with the action item.
- `notification_log.alert_type` (notification-service): the CHECK gains ADR-0039 C7's two
  ProactiveAgent alert types, `already_decided` and `topic_watch`, so notification-service can
  log them in-app like the other four instead of failing the insert.

Revision ID: d2b3c4d5e6f7
Revises: c1a2b3d4e5f6
Create Date: 2026-11-10
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "d2b3c4d5e6f7"
down_revision: Union[str, None] = "c1a2b3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"


def _rls(table: str, comment: str) -> None:
    op.execute(f"CREATE INDEX ix_{table}_org_id ON {table} (org_id);")
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON {table} "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    grant_tenant_table(op, table)
    op.execute(f"COMMENT ON TABLE {table} IS '{comment}';")


def upgrade() -> None:
    # --- meeting_templates (B1) --------------------------------------------------------------
    op.execute("""
        CREATE TABLE meeting_templates (
          id                       uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id                   uuid        NOT NULL REFERENCES organizations(id)
                                                 ON DELETE RESTRICT,
          name                     text        NOT NULL,
          description              text,
          default_duration_minutes integer     NOT NULL DEFAULT 30,
          agenda                   jsonb       NOT NULL DEFAULT '[]'::jsonb,
          confidential             boolean     NOT NULL DEFAULT false,
          created_by               uuid        REFERENCES users(id) ON DELETE SET NULL,
          created_at               timestamptz NOT NULL DEFAULT now(),
          updated_at               timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_meeting_templates PRIMARY KEY (id),
          CONSTRAINT ck_meeting_templates_name CHECK (char_length(name) BETWEEN 1 AND 80),
          CONSTRAINT ck_meeting_templates_description
            CHECK (description IS NULL OR char_length(description) <= 300),
          CONSTRAINT ck_meeting_templates_duration
            CHECK (default_duration_minutes BETWEEN 5 AND 480),
          CONSTRAINT ck_meeting_templates_agenda
            CHECK (jsonb_typeof(agenda) = 'array' AND jsonb_array_length(agenda) <= 20)
        );
        """)
    _rls(
        "meeting_templates",
        "Meeting types (ADR-0039 B1). Written only by core-api /meeting-templates "
        "(owner/admin).",
    )

    # --- meetings: confidential + template (B2) ------------------------------------------------
    op.execute("""
        ALTER TABLE meetings
          ADD COLUMN confidential boolean NOT NULL DEFAULT false,
          ADD COLUMN template_id  uuid,
          ADD CONSTRAINT fk_meetings_template_id_meeting_templates
            FOREIGN KEY (template_id) REFERENCES meeting_templates(id) ON DELETE SET NULL;
        """)

    # --- meeting_agenda_items: goal (B3) ---------------------------------------------------------
    op.execute("ALTER TABLE meeting_agenda_items ADD COLUMN goal_id uuid;")

    # --- meeting_participants: send your agent instead (B5) ---------------------------------------
    op.execute("""
        ALTER TABLE meeting_participants
          ADD COLUMN attendance      text   NOT NULL DEFAULT 'live',
          ADD COLUMN proxy_update    text,
          ADD COLUMN proxy_questions text[] NOT NULL DEFAULT '{}',
          ADD CONSTRAINT ck_meeting_participants_attendance
            CHECK (attendance IN ('live', 'agent')),
          ADD CONSTRAINT ck_meeting_participants_proxy_update
            CHECK (proxy_update IS NULL OR char_length(proxy_update) <= 2000),
          ADD CONSTRAINT ck_meeting_participants_proxy_questions
            CHECK (cardinality(proxy_questions) <= 5);
        """)

    # --- commitments (B6) --------------------------------------------------------------------
    op.execute("""
        CREATE TABLE commitments (
          id               uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id           uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id          uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          meeting_id       uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          content          text        NOT NULL,
          due_hint         text,
          status           text        NOT NULL DEFAULT 'open',
          source_window_id text        NOT NULL,
          created_at       timestamptz NOT NULL DEFAULT now(),
          updated_at       timestamptz NOT NULL DEFAULT now(),
          done_at          timestamptz,
          CONSTRAINT pk_commitments PRIMARY KEY (id),
          CONSTRAINT ck_commitments_content CHECK (char_length(content) BETWEEN 1 AND 500),
          CONSTRAINT ck_commitments_due_hint
            CHECK (due_hint IS NULL OR char_length(due_hint) <= 100),
          CONSTRAINT ck_commitments_status CHECK (status IN ('open', 'done', 'dismissed'))
        );
        """)
    op.execute(
        "CREATE UNIQUE INDEX uq_commitments_meeting_user_window_content "
        "ON commitments (meeting_id, user_id, source_window_id, md5(content));"
    )
    op.execute(
        "CREATE INDEX ix_commitments_user_status_created "
        "ON commitments (user_id, status, created_at DESC);"
    )
    _rls(
        "commitments",
        "Promises a person made in a meeting (ADR-0039 B6). Written only by core-api: its "
        "agent-results consumer (PersonalListenerAgent payload_json.commitments) and the "
        "owner''s PATCH. Private to user_id.",
    )

    # --- action_status_requests (B7) -----------------------------------------------------------
    op.execute("""
        CREATE TABLE action_status_requests (
          id           uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id       uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          action_id    uuid        NOT NULL REFERENCES action_items(id) ON DELETE CASCADE,
          user_id      uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          meeting_id   uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          state        text        NOT NULL DEFAULT 'pending',
          status_text  text,
          requested_at timestamptz NOT NULL DEFAULT now(),
          answered_at  timestamptz,
          CONSTRAINT pk_action_status_requests PRIMARY KEY (id),
          CONSTRAINT uq_action_status_requests_action_meeting UNIQUE (action_id, meeting_id),
          CONSTRAINT ck_action_status_requests_state
            CHECK (state IN ('pending', 'answered', 'expired')),
          CONSTRAINT ck_action_status_requests_status_text
            CHECK (status_text IS NULL OR char_length(status_text) <= 500)
        );
        """)
    op.execute(
        "CREATE INDEX ix_action_status_requests_user_state "
        "ON action_status_requests (user_id, state, requested_at DESC);"
    )
    _rls(
        "action_status_requests",
        "Status asked of an action''s assignee before the follow-up meeting (ADR-0039 B7). "
        "Written only by core-api: the internal route workflow-service calls, and the "
        "assignee''s answer.",
    )


    # --- notification_log: ADR-0039 C7 alert types ---------------------------------------------
    op.execute("""
        ALTER TABLE notification_log
          DROP CONSTRAINT ck_notification_log_alert_type,
          ADD CONSTRAINT ck_notification_log_alert_type CHECK (
            alert_type IS NULL
            OR alert_type IN ('conflict', 'workload', 'dependency', 'missing_stakeholder',
                              'already_decided', 'topic_watch')
          );
        """)


def downgrade() -> None:
    # Forward-only in practice (CLAUDE.md); kept so a local `alembic downgrade -1` works.
    op.execute("""
        ALTER TABLE notification_log
          DROP CONSTRAINT ck_notification_log_alert_type,
          ADD CONSTRAINT ck_notification_log_alert_type CHECK (
            alert_type IS NULL
            OR alert_type IN ('conflict', 'workload', 'dependency', 'missing_stakeholder')
          );
        """)
    op.execute("DROP TABLE IF EXISTS action_status_requests CASCADE;")
    op.execute("DROP TABLE IF EXISTS commitments CASCADE;")
    op.execute(
        "ALTER TABLE meeting_participants "
        "DROP CONSTRAINT IF EXISTS ck_meeting_participants_proxy_questions, "
        "DROP CONSTRAINT IF EXISTS ck_meeting_participants_proxy_update, "
        "DROP CONSTRAINT IF EXISTS ck_meeting_participants_attendance, "
        "DROP COLUMN IF EXISTS proxy_questions, DROP COLUMN IF EXISTS proxy_update, "
        "DROP COLUMN IF EXISTS attendance;"
    )
    op.execute("ALTER TABLE meeting_agenda_items DROP COLUMN IF EXISTS goal_id;")
    op.execute(
        "ALTER TABLE meetings DROP CONSTRAINT IF EXISTS fk_meetings_template_id_meeting_templates, "
        "DROP COLUMN IF EXISTS template_id, DROP COLUMN IF EXISTS confidential;"
    )
    op.execute("DROP TABLE IF EXISTS meeting_templates CASCADE;")
