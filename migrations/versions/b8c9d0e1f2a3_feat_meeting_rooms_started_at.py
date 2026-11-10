"""feat(meeting-service): meeting_rooms.started_at, the meeting's own clock

Polish program, Phase 0 "Agenda and Notes mode": the room's clock showed the time since YOU
opened the room, so two people in one meeting saw two different times. The meeting's time needs
one server-side origin. `meeting_rooms.created_at` cannot be it: the room row is upserted when
the meeting is created (`meeting-events` consumer) and by the internal ensure route, long before
anyone joins.

`started_at timestamptz NULL` is set once, `COALESCE(started_at, now())`, by the first successful
token mint (the first person actually let into the call) and by LiveKit's `room_started`
webhook, whichever comes first. NULL means nobody has joined yet. meeting-service is the one
writer of `meeting_rooms`, so no grant change is needed: `mytheus_app` already updates this
table (`livekit_metadata_synced_at`, `status`).

Revision ID: b8c9d0e1f2a3
Revises: d2b3c4d5e6f7
Create Date: 2026-11-11
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "b8c9d0e1f2a3"
down_revision: Union[str, None] = "d2b3c4d5e6f7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "meeting_rooms",
        sa.Column("started_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("meeting_rooms", "started_at")
