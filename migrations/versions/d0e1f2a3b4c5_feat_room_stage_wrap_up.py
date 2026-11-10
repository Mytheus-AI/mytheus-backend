"""feat: the room's agenda clock, the close-out ritual and the leftover-card owner

Polish program, Phase 1 "The agenda as the spine" and "Close-out ritual".

`meeting_agenda_items`:
- `started_at timestamptz NULL`: when the item first went `in_progress` (host or MeetingAgent),
  so every client draws the same "elapsed versus planned" clock and a reload agrees with live.
  Cleared when the host reopens the item to `pending`.
- `ended_at timestamptz NULL`: when it was last marked `covered` or `skipped`.
- `extended_minutes integer NOT NULL DEFAULT 0`, CHECK 0..240: minutes the host added in the
  room ("Extend"); the plan is `duration_minutes + extended_minutes`, the original plan is kept.

`meetings` (core-api is the one writer):
- `wrap_up_started_at timestamptz NULL`, `wrap_up_by uuid NULL`: the host opened the 60-second
  wrap-up ("End for everyone"); cleared if they keep the meeting going.
- `closed_at timestamptz NULL`, `closed_by uuid NULL`: who closed the meeting through the
  wrap-up, and when (the meeting is also `ended`).

`decisions`, `action_items`:
- `review_owner_id uuid NULL`: the person who owns reviewing a card nobody confirmed in the
  meeting. Set at close-out on every card still `proposed`; the Home "Needs review" queue then
  shows such a card to that person only. NULL keeps today's behaviour (every participant).

No new table, so no new grant: `mytheus_app` already reads and writes these tables.

Revision ID: d0e1f2a3b4c5
Revises: b8c9d0e1f2a3
Create Date: 2026-11-11
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "d0e1f2a3b4c5"
down_revision: Union[str, None] = "b8c9d0e1f2a3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_TS = postgresql.TIMESTAMP(timezone=True)


def upgrade() -> None:
    op.add_column("meeting_agenda_items", sa.Column("started_at", _TS, nullable=True))
    op.add_column("meeting_agenda_items", sa.Column("ended_at", _TS, nullable=True))
    op.add_column(
        "meeting_agenda_items",
        sa.Column(
            "extended_minutes", sa.Integer(), server_default=sa.text("0"), nullable=False
        ),
    )
    op.create_check_constraint(
        "ck_meeting_agenda_items_extended_minutes",
        "meeting_agenda_items",
        "extended_minutes BETWEEN 0 AND 240",
    )

    op.add_column("meetings", sa.Column("wrap_up_started_at", _TS, nullable=True))
    op.add_column(
        "meetings",
        sa.Column(
            "wrap_up_by",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.add_column("meetings", sa.Column("closed_at", _TS, nullable=True))
    op.add_column(
        "meetings",
        sa.Column(
            "closed_by",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )

    for table in ("decisions", "action_items"):
        op.add_column(
            table,
            sa.Column(
                "review_owner_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("users.id", ondelete="SET NULL"),
                nullable=True,
            ),
        )
        op.create_index(
            f"ix_{table}_review_owner_proposed",
            table,
            ["review_owner_id"],
            postgresql_where=sa.text("status = 'proposed' AND review_owner_id IS NOT NULL"),
        )


def downgrade() -> None:
    for table in ("decisions", "action_items"):
        op.drop_index(f"ix_{table}_review_owner_proposed", table_name=table)
        op.drop_column(table, "review_owner_id")
    op.drop_column("meetings", "closed_by")
    op.drop_column("meetings", "closed_at")
    op.drop_column("meetings", "wrap_up_by")
    op.drop_column("meetings", "wrap_up_started_at")
    op.drop_constraint(
        "ck_meeting_agenda_items_extended_minutes", "meeting_agenda_items", type_="check"
    )
    op.drop_column("meeting_agenda_items", "extended_minutes")
    op.drop_column("meeting_agenda_items", "ended_at")
    op.drop_column("meeting_agenda_items", "started_at")
