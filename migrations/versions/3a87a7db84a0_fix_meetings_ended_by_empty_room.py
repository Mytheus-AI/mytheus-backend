"""fix: reopen meetings an empty pre-opened room marked live or ended

meeting-service used to open a meeting's LiveKit room as soon as the meeting was created.
LiveKit reported that room started (meeting -> `live`) and, after five minutes with nobody in
it, finished (meeting -> `ended`). A meeting booked for later was therefore ended before it
began: Join was refused and "send my Myte" was locked. The code no longer does that (rooms
open at the first join, `live` follows a person joining, only a `live` meeting can end).

This puts the meetings it already hit back to `scheduled`:
- every Mytheus meeting marked `live`/`ended` whose start is still in the future, and
- one that is under way or about to be (its end is still ahead) whose room nobody ever opened
  through a join (`meeting_rooms.started_at` is set only by a join-token mint).
Their room rows go back to `pending` and unsynced, so the next join opens a fresh room.
Meetings that really took place are left alone. Forward-only: there is nothing to restore.

Revision ID: 3a87a7db84a0
Revises: d4e5f6a7b8c9
Create Date: 2026-11-12
"""

from typing import Sequence, Union

from alembic import op

revision: str = "3a87a7db84a0"
down_revision: Union[str, None] = "d4e5f6a7b8c9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_REOPEN = """
    m.status IN ('live', 'ended')
    AND m.deleted_at IS NULL
    AND m.source = 'mytheus'
    AND (
        m.scheduled_start > now()
        OR (
            m.scheduled_end > now()
            AND NOT EXISTS (
                SELECT 1 FROM meeting_rooms r
                WHERE r.meeting_id = m.id AND r.started_at IS NOT NULL
            )
        )
    )
"""


def upgrade() -> None:
    op.execute(
        f"""
        UPDATE meeting_rooms r
        SET status = 'pending', started_at = NULL, livekit_metadata_synced_at = NULL,
            updated_at = now()
        FROM meetings m
        WHERE r.meeting_id = m.id AND {_REOPEN};
        """
    )
    op.execute(
        f"UPDATE meetings m SET status = 'scheduled', updated_at = now() WHERE {_REOPEN};"
    )


def downgrade() -> None:
    pass
