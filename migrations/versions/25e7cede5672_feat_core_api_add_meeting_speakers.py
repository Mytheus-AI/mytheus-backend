"""feat(core-api): add meeting_speakers, manual assignment authoritative by trigger

Plan 3-4 section 3.3, step 3 (Task 5 of the band). `meeting_speakers` is the confirmed
diarized-label-to-user mapping: `core-api` is the canon owner and sole writer;
`transcription-service` (in `mytheus-realtime`) only EMITS the diarized label (as
`speaker_id` on `transcript_chunks` / `transcript-chunks`), it never writes this table
(`04-service-catalog.md` section 3.5, R7).

R7 risk checkpoint, resolved structurally rather than by convention: `assignment='manual'`
is AUTHORITATIVE. A diarization suggestion is advisory and confidence-scored, and must never
be able to overwrite a manual assignment - not "should not by convention", but cannot, by a
database constraint, regardless of which future code path attempts it. No automatic-suggestion
writer exists yet in this codebase (there is no consumer wired to write "suggested" rows in
this task - see the task-5 report), so this trigger is the durable guardrail for that future
writer, not a workaround for a bug in an existing one:

  BEFORE UPDATE ... WHEN (OLD.assignment = 'manual' AND NEW.assignment <> 'manual')
    -> RAISE EXCEPTION

Any UPDATE - from any role, any query, any future service that is granted write access to
this table - that would move a row OUT of 'manual' is refused at the database. Concretely:
an automatic-suggestion writer must always upsert with a WHERE clause of its own (e.g.
`... WHERE meeting_speakers.assignment <> 'manual'`) to avoid ever attempting such an UPDATE
in the first place; the trigger is the backstop for the case where it forgets to.

The PATCH /meetings/:id/speakers/:speaker_id route (this task) is the only write path that
ever sets `assignment = 'manual'`, and it always upserts with NEW.assignment = 'manual', so
the trigger never fires against it - the trigger only ever rejects a WOULD-BE downgrade.

Not partitioned (no volume driver the way chat_messages has); `PRIMARY KEY (id)` plus
`UNIQUE (org_id, meeting_id, speaker_id)`, matching `meeting_rooms`'s (`3d13f46b960f`) house
style of a bare `id` primary key with `org_id` as a plain indexed column, not a composite PK.

Revision ID: 25e7cede5672
Revises: 5ed978467cad
Create Date: 2026-08-05 23:45:00.000000

"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "25e7cede5672"
down_revision: Union[str, None] = "5ed978467cad"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"

_TRIGGER_FUNCTION = "meeting_speakers_manual_is_authoritative"
_TRIGGER_NAME = "trg_meeting_speakers_manual_authoritative"


def _force_rls(table: str) -> None:
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON {table} "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )


def upgrade() -> None:
    op.create_table(
        "meeting_speakers",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("org_id", sa.UUID(), nullable=False),
        sa.Column("meeting_id", sa.UUID(), nullable=False),
        # The diarized label from Deepgram (e.g. "0", "1"), not a user id.
        sa.Column("speaker_id", sa.Text(), nullable=False),
        # Nullable until mapped: an automatic suggestion may name a low-confidence guess or
        # none at all before a human confirms it.
        sa.Column("user_id", sa.UUID(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column(
            "assignment", sa.Text(), server_default=sa.text("'suggested'"), nullable=False
        ),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["org_id"],
            ["organizations.id"],
            name=op.f("fk_meeting_speakers_org_id_organizations"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["meeting_id"],
            ["meetings.id"],
            name=op.f("fk_meeting_speakers_meeting_id_meetings"),
        ),
        sa.ForeignKeyConstraint(
            ["user_id"], ["users.id"], name=op.f("fk_meeting_speakers_user_id_users")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_meeting_speakers")),
        sa.UniqueConstraint(
            "org_id",
            "meeting_id",
            "speaker_id",
            name="uq_meeting_speakers_org_id_meeting_id_speaker_id",
        ),
        sa.CheckConstraint(
            "assignment IN ('manual','suggested')", name="ck_meeting_speakers_assignment_valid"
        ),
    )
    op.create_index(
        op.f("ix_meeting_speakers_org_id"), "meeting_speakers", ["org_id"], unique=False
    )
    _force_rls("meeting_speakers")

    op.execute(
        f"""
        CREATE FUNCTION {_TRIGGER_FUNCTION}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF OLD.assignment = 'manual' AND NEW.assignment <> 'manual' THEN
            RAISE EXCEPTION
              'meeting_speakers: cannot downgrade a manual assignment away from manual '
              '(id=%, meeting_id=%, speaker_id=%)', OLD.id, OLD.meeting_id, OLD.speaker_id
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$;
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_TRIGGER_NAME}
        BEFORE UPDATE ON meeting_speakers
        FOR EACH ROW EXECUTE FUNCTION {_TRIGGER_FUNCTION}();
        """
    )
    op.execute(
        f"""
        COMMENT ON TRIGGER {_TRIGGER_NAME} ON meeting_speakers IS
        'R7: assignment=manual is authoritative. Refuses any UPDATE that would move a row '
        'out of manual, regardless of caller - the structural guard for a future automatic '
        '(diarization-suggestion) writer, not a convention.';
        """
    )


def downgrade() -> None:
    op.execute(f"DROP TRIGGER IF EXISTS {_TRIGGER_NAME} ON meeting_speakers;")
    op.execute(f"DROP FUNCTION IF EXISTS {_TRIGGER_FUNCTION}();")
    op.drop_index(op.f("ix_meeting_speakers_org_id"), table_name="meeting_speakers")
    op.drop_table("meeting_speakers")
