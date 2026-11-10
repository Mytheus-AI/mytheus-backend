"""feat: card moments, why lines and agent updates (polish program, calm and fast)

Polish program, "Make the meeting calm and fast" (mytheus-intelligence feat/polish-live-agents).

One card per moment. One sentence used to produce a note, a promise, a decision card and an
action card with nothing linking them. The agent runtime now stamps every item with the moment
it is about (`moment_id`: uuid5 over the meeting and the quoted transcript final, the same for
every agent that cites those words), the words themselves and when they were said, and a `why`
line. Stored on the four rows the cards come from, so a reload shows the same card the live
stream did:

- `decisions`, `action_items`, `agent_suggestions`, `worklogs`: `moment_id uuid NULL`,
  `why text NULL` (<= 200), `quote text NULL` (<= 300), `quote_speaker_id uuid NULL` (a
  `users.id`; no foreign key, like `stakeholder` ids elsewhere: checked visible by the consumer
  before it is stored, gap 31), `quote_start_ms` / `quote_end_ms bigint NULL` (meeting time).
- `commitments`: `moment_id uuid NULL` and `action_item_id uuid NULL REFERENCES action_items`
  (ON DELETE SET NULL): when an action is confirmed and its assignee made a promise at the same
  moment, core-api closes the promise (`done`) and links it to the task that now tracks it.

Memory inside the meeting. The agents may now UPDATE an open card instead of proposing a second
one. A person's edit must never be overwritten by a later agent update, so `decisions` and
`action_items` get `edited_at timestamptz NULL`, set by the PATCH routes; the consumer applies an
update only to a `proposed` row with no `edited_at`.

Indexes: `(meeting_id, moment_id)` on `commitments` (the confirm-time close) and on the two
candidate tables (cards of one moment). No grant changes: core-api (mytheus_app) already
writes every one of these tables, and RLS policies are per row, not per column. On the
partitioned `agent_suggestions` the columns are added on the parent and reach every partition.

Revision ID: c9d0e1f2a3b4
Revises: b8c9d0e1f2a3
Create Date: 2026-11-11
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "c9d0e1f2a3b4"
down_revision: Union[str, None] = "b8c9d0e1f2a3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_CARD_TABLES = ("decisions", "action_items", "agent_suggestions", "worklogs")


def upgrade() -> None:
    for table in _CARD_TABLES:
        op.execute(f"""
            ALTER TABLE {table}
              ADD COLUMN moment_id uuid,
              ADD COLUMN why text,
              ADD COLUMN quote text,
              ADD COLUMN quote_speaker_id uuid,
              ADD COLUMN quote_start_ms bigint,
              ADD COLUMN quote_end_ms bigint,
              ADD CONSTRAINT ck_{table}_why CHECK (why IS NULL OR char_length(why) <= 200),
              ADD CONSTRAINT ck_{table}_quote
                CHECK (quote IS NULL OR char_length(quote) <= 300);
            """)
    for table in ("decisions", "action_items"):
        op.execute(f"ALTER TABLE {table} ADD COLUMN edited_at timestamptz;")
        op.execute(
            f"CREATE INDEX ix_{table}_meeting_moment ON {table} (meeting_id, moment_id) "
            "WHERE moment_id IS NOT NULL;"
        )
    op.execute("""
        ALTER TABLE commitments
          ADD COLUMN moment_id uuid,
          ADD COLUMN action_item_id uuid REFERENCES action_items(id) ON DELETE SET NULL;
        """)
    op.execute(
        "CREATE INDEX ix_commitments_meeting_moment ON commitments (meeting_id, moment_id) "
        "WHERE moment_id IS NOT NULL;"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_commitments_meeting_moment;")
    op.execute("ALTER TABLE commitments DROP COLUMN action_item_id, DROP COLUMN moment_id;")
    for table in ("decisions", "action_items"):
        op.execute(f"DROP INDEX IF EXISTS ix_{table}_meeting_moment;")
        op.execute(f"ALTER TABLE {table} DROP COLUMN edited_at;")
    for table in _CARD_TABLES:
        op.execute(f"""
            ALTER TABLE {table}
              DROP CONSTRAINT ck_{table}_quote,
              DROP CONSTRAINT ck_{table}_why,
              DROP COLUMN quote_end_ms,
              DROP COLUMN quote_start_ms,
              DROP COLUMN quote_speaker_id,
              DROP COLUMN quote,
              DROP COLUMN why,
              DROP COLUMN moment_id;
            """)
