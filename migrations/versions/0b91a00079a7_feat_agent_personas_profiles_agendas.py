"""feat: agent persona and working profile on users; meeting follow-up link and agenda (ADR-0038)

ADR-0038 D1, D2 and D5, in one forward-only change.

- `users` (owned by identity-tenant-service) gains the working profile (`listen_for`,
  `ignore_topics`, both `text[] NOT NULL DEFAULT '{}'`) and the agent persona (`agent_name`,
  `agent_avatar_seed`, `agent_instructions`, nullable; `agent_personality` NOT NULL DEFAULT
  'balanced' with the five-preset CHECK). Lengths, counts, trimming and de-duplication are the
  API's job (`PATCH /api/v1/users/{id}`); the only database rule ADR-0038 fixes is the
  personality CHECK, and it is here.
- `meetings` (owned by core-api) gains `previous_meeting_id`, a self-reference with
  `ON DELETE SET NULL`: deleting an earlier meeting must not delete or block its follow-ups. The
  FK alone does not confine the reference to the tenant (gap 31), so the route checks it under
  RLS before writing, exactly as it does for participants. `ck_meetings_not_own_follow_up`
  makes "a meeting is not its own follow-up" a constraint, not only a route check.
- `meeting_agenda_items` (owned by core-api), exactly the columns of D5. Deleting a meeting
  deletes its agenda (`ON DELETE CASCADE`). `owner_user_id` references `users` with
  `ON DELETE SET NULL` so a purged person leaves the item, unowned. Indexed on
  `(meeting_id, position)`, the order every read uses. Not unique on purpose: the agenda is
  replaced as a whole (`PUT /meetings/{id}/agenda`), and a unique index would force that
  replace to shuffle positions through temporary values to avoid colliding with itself. The
  20-item cap is the route's.

RLS ENABLE + FORCE with the canonical NULLIF `tenant_isolation` policy and the explicit
`grant_tenant_table` (ADR-0021), same as every tenant table since `9b2c4d1e6f30`.

Revision ID: 0b91a00079a7
Revises: e2b6c4a8f913
Create Date: 2026-11-10
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "0b91a00079a7"
down_revision: Union[str, None] = "e2b6c4a8f913"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"


def upgrade() -> None:
    # --- users: working profile + agent persona (D1, D2) ----------------------------------
    op.execute("""
        ALTER TABLE users
          ADD COLUMN listen_for         text[] NOT NULL DEFAULT '{}',
          ADD COLUMN ignore_topics      text[] NOT NULL DEFAULT '{}',
          ADD COLUMN agent_name         text,
          ADD COLUMN agent_avatar_seed  text,
          ADD COLUMN agent_personality  text   NOT NULL DEFAULT 'balanced',
          ADD COLUMN agent_instructions text,
          ADD CONSTRAINT ck_users_agent_personality
            CHECK (agent_personality IN ('balanced', 'concise', 'detailed', 'direct', 'friendly'));
        """)

    # --- meetings: follow-up link (D5) ------------------------------------------------------
    op.execute("""
        ALTER TABLE meetings
          ADD COLUMN previous_meeting_id uuid,
          ADD CONSTRAINT fk_meetings_previous_meeting_id_meetings
            FOREIGN KEY (previous_meeting_id) REFERENCES meetings(id) ON DELETE SET NULL,
          ADD CONSTRAINT ck_meetings_not_own_follow_up
            CHECK (previous_meeting_id IS NULL OR previous_meeting_id <> id);
        """)
    op.execute(
        "CREATE INDEX ix_meetings_previous_meeting_id ON meetings (previous_meeting_id) "
        "WHERE previous_meeting_id IS NOT NULL;"
    )

    # --- meeting_agenda_items (D5) -----------------------------------------------------------
    op.execute("""
        CREATE TABLE meeting_agenda_items (
          id               uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id           uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          meeting_id       uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          position         integer     NOT NULL,
          title            text        NOT NULL,
          owner_user_id    uuid        REFERENCES users(id) ON DELETE SET NULL,
          duration_minutes integer,
          status           text        NOT NULL DEFAULT 'pending',
          status_source    text,
          agent_note       text,
          created_at       timestamptz NOT NULL DEFAULT now(),
          updated_at       timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_meeting_agenda_items PRIMARY KEY (id),
          CONSTRAINT ck_meeting_agenda_items_position CHECK (position >= 0),
          CONSTRAINT ck_meeting_agenda_items_title
            CHECK (char_length(title) BETWEEN 1 AND 200),
          CONSTRAINT ck_meeting_agenda_items_duration
            CHECK (duration_minutes IS NULL OR duration_minutes BETWEEN 1 AND 240),
          CONSTRAINT ck_meeting_agenda_items_status
            CHECK (status IN ('pending', 'in_progress', 'covered', 'skipped')),
          CONSTRAINT ck_meeting_agenda_items_status_source
            CHECK (status_source IS NULL OR status_source IN ('agent', 'host')),
          CONSTRAINT ck_meeting_agenda_items_agent_note
            CHECK (agent_note IS NULL OR char_length(agent_note) <= 500)
        );
        """)
    op.execute(
        "CREATE INDEX ix_meeting_agenda_items_meeting_position "
        "ON meeting_agenda_items (meeting_id, position);"
    )
    op.execute("CREATE INDEX ix_meeting_agenda_items_org_id ON meeting_agenda_items (org_id);")
    op.execute("ALTER TABLE meeting_agenda_items ENABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE meeting_agenda_items FORCE ROW LEVEL SECURITY;")
    op.execute(
        "CREATE POLICY tenant_isolation ON meeting_agenda_items "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    grant_tenant_table(op, "meeting_agenda_items")
    op.execute(
        "COMMENT ON TABLE meeting_agenda_items IS "
        "'A scheduled meeting''s agenda (ADR-0038 D5). Written only by core-api: the host or "
        "creator through /meetings/{id}/agenda, and the agent-results consumer applying "
        "MeetingAgent meeting_update entries (never over a host-set status).';"
    )


def downgrade() -> None:
    # Forward-only in practice (CLAUDE.md); kept so a local `alembic downgrade -1` works.
    op.execute("DROP TABLE IF EXISTS meeting_agenda_items CASCADE;")
    op.execute("DROP INDEX IF EXISTS ix_meetings_previous_meeting_id;")
    op.execute(
        "ALTER TABLE meetings DROP CONSTRAINT IF EXISTS ck_meetings_not_own_follow_up, "
        "DROP CONSTRAINT IF EXISTS fk_meetings_previous_meeting_id_meetings, "
        "DROP COLUMN IF EXISTS previous_meeting_id;"
    )
    op.execute(
        "ALTER TABLE users DROP CONSTRAINT IF EXISTS ck_users_agent_personality, "
        "DROP COLUMN IF EXISTS agent_instructions, DROP COLUMN IF EXISTS agent_personality, "
        "DROP COLUMN IF EXISTS agent_avatar_seed, DROP COLUMN IF EXISTS agent_name, "
        "DROP COLUMN IF EXISTS ignore_topics, DROP COLUMN IF EXISTS listen_for;"
    )
