"""feat: meeting_minutes (minutes for everyone) and the follow-up's wait for them

Polish program, "Minutes for everyone". After a meeting ends, `agent-runtime-service` writes one
set of minutes per participant and one shared, meeting-level set, and emits them on
`agent-results` (`MeetingAgent`, kind `minutes`). core-api is the one writer of this table: its
`agent-results` consumer upserts one row per (meeting, participant) and one shared row.

- `user_id` is the participant the minutes are for, NULL for the shared minutes. `scope` says the
  same thing in words and `ck_meeting_minutes_scope` keeps the two in step.
- `content` is the minutes JSON (summary, decisions with status, open items with owners,
  unanswered questions, and for a personal row that person's promises), validated and clipped by
  the consumer. `status` is `ready`, `empty` (nothing to record) or `unavailable` (shared only:
  no minutes could be made, `reason` says why).
- `UNIQUE NULLS NOT DISTINCT (meeting_id, user_id)`: one personal row per person and ONE shared
  row per meeting; a later generation (the meeting ended again) replaces the row in place.
- Personal rows are private to their person: the routes serve a personal row only to its
  `user_id`. RLS scopes every row to the tenant like its siblings.

`meeting_followups.awaiting_minutes_since`: the automatic follow-up (workflow-service, at the
meeting's end) used to go out before any minutes existed. Now, when the meeting has a transcript
and its shared minutes have not landed yet, the dispatch records this time and answers
`waiting_for_minutes: true`; the consumer sends the follow-up when the shared minutes arrive
(or say they are unavailable). NULL once sent, or when nothing is waiting.

Revision ID: e1f2a3b4c5d6
Revises: f2a3b4c5d6e7
Create Date: 2026-11-11
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "e1f2a3b4c5d6"
down_revision: Union[str, None] = "f2a3b4c5d6e7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"


def upgrade() -> None:
    op.execute("""
        CREATE TABLE meeting_minutes (
          id             uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id         uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          meeting_id     uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          user_id        uuid        REFERENCES users(id) ON DELETE CASCADE,
          scope          text        NOT NULL,
          status         text        NOT NULL,
          reason         text,
          content        jsonb       NOT NULL,
          prompt_version text        NOT NULL,
          generation     text        NOT NULL,
          created_at     timestamptz NOT NULL DEFAULT now(),
          updated_at     timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_meeting_minutes PRIMARY KEY (id),
          CONSTRAINT uq_meeting_minutes_meeting_user UNIQUE NULLS NOT DISTINCT (meeting_id, user_id),
          CONSTRAINT ck_meeting_minutes_scope
            CHECK (scope IN ('personal', 'shared') AND (scope = 'shared') = (user_id IS NULL)),
          CONSTRAINT ck_meeting_minutes_status CHECK (status IN ('ready', 'empty', 'unavailable')),
          CONSTRAINT ck_meeting_minutes_unavailable
            CHECK (status <> 'unavailable' OR scope = 'shared'),
          CONSTRAINT ck_meeting_minutes_content CHECK (jsonb_typeof(content) = 'object'),
          CONSTRAINT ck_meeting_minutes_prompt CHECK (char_length(prompt_version) BETWEEN 1 AND 64)
        );
        """)
    op.execute("CREATE INDEX ix_meeting_minutes_org_id ON meeting_minutes (org_id);")
    op.execute("CREATE INDEX ix_meeting_minutes_user ON meeting_minutes (user_id, created_at DESC);")
    op.execute("ALTER TABLE meeting_minutes ENABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE meeting_minutes FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON meeting_minutes "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    grant_tenant_table(op, "meeting_minutes")
    op.execute(
        "COMMENT ON TABLE meeting_minutes IS 'Meeting minutes: one row per participant (private "
        "to them) and one shared row (user_id NULL) per meeting. Written only by core-api from "
        "MeetingAgent minutes on agent-results.';"
    )
    op.execute("ALTER TABLE meeting_followups ADD COLUMN awaiting_minutes_since timestamptz;")


def downgrade() -> None:
    # Forward-only in practice (CLAUDE.md); kept so a local `alembic downgrade -1` works.
    op.execute("ALTER TABLE meeting_followups DROP COLUMN IF EXISTS awaiting_minutes_since;")
    op.execute("DROP TABLE IF EXISTS meeting_minutes CASCADE;")
