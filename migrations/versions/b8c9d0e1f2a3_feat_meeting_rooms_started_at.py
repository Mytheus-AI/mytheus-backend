"""feat: meeting_rooms.started_at (the meeting's own clock) and meetings.source (captured calls)

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

`meetings.source text NOT NULL DEFAULT 'mytheus'`, CHECK in ('mytheus', 'meet', 'teams', 'zoom')
(polish program, external-call capture): a meeting captured from Google Meet, Microsoft Teams or
Zoom by the browser extension is created through core-api's normal create, but it has no Mytheus
room. Without this, meeting-service opened a LiveKit room for it that nobody joined, LiveKit
closed it after its five-minute empty timeout, and core-api marked the meeting ended while the
external call was still running, so post-processing ran on a partial transcript. With `source`,
meeting-service opens no room and mints no token for an external meeting, and core-api ignores
room-state reports for it. core-api is the one writer of `meetings`; meeting-service only reads
the column (RLS-scoped, like `status`), so the `meeting-events` Avro schema is unchanged.

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
    op.add_column(
        "meetings",
        sa.Column("source", sa.Text(), server_default=sa.text("'mytheus'"), nullable=False),
    )
    op.create_check_constraint(
        "ck_meetings_source", "meetings", "source IN ('mytheus', 'meet', 'teams', 'zoom')"
    )


def downgrade() -> None:
    op.drop_constraint("ck_meetings_source", "meetings", type_="check")
    op.drop_column("meetings", "source")
    op.drop_column("meeting_rooms", "started_at")
