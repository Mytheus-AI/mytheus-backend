"""Shared `meeting_rooms` writers: the deterministic room-naming convention, plus the upsert
and lifecycle transitions used by the token-mint route (Task 3), the LiveKit webhook, and the
`meeting-events` consumer (both Task 4). One place, so all three writers agree on the room name
`meeting-{meeting_id}` by construction rather than by convention re-typed three times.

This module intentionally has NO opinion on `org_id` provenance or RLS: every caller must have
already called `libs.shared.tenancy.set_rls` (or otherwise established `app.current_org`) on the
session it passes in. That is a route/consumer/handler concern, not a storage-helper concern.

`ensure_meeting_room` requires a `MeetingTransport` (not merely accepts one) so that the ONE
place a `meeting_rooms` row is established is also the ONE place LiveKit is told that room's
tenant (`transport.ensure_room`, Task 4 follow-up). This module never imports the LiveKit SDK
itself - only the `MeetingTransport` Protocol type - so it stays outside ADR-0003 Decision 2's
import boundary (`tests/test_livekit_import_boundary.py`), matching how `routes.py` and
`webhook.py` also only ever reach LiveKit through that Protocol.
"""

from __future__ import annotations

from typing import Any, cast
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from services.meeting_service.transport import MeetingTransport


def room_name_for(meeting_id: UUID) -> str:
    """The one place the deterministic room-naming scheme lives: `meeting-{meeting_id}`."""
    return f"meeting-{meeting_id}"


async def ensure_meeting_room(
    session: AsyncSession, org_id: UUID, meeting_id: UUID, transport: MeetingTransport
) -> str:
    """Upsert the `meeting_rooms` row this meeting's LiveKit room materializes into, AND tell
    LiveKit the room's tenant via `transport.ensure_room`.

    The room name is deterministic rather than randomly generated per call, so a concurrent
    double-call racing this INSERT is a harmless conflict on the same value either way.
    `ON CONFLICT ... DO UPDATE SET updated_at = now()` deliberately does not touch
    `livekit_room` in the SET clause: once a room exists its name is fixed for the life of the
    meeting, and this upsert must return the EXISTING name on every call after the first, not
    silently rename the room out from under a participant who already has a token for it.

    `transport.ensure_room` runs every call, not just the first: it is idempotent (real LiveKit
    `CreateRoom`/`UpdateRoomMetadata` both are), and calling it unconditionally means a room
    whose metadata was ever wrong self-heals on the next mint/consume rather than staying wrong
    for the life of the meeting.
    """
    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO meeting_rooms (org_id, meeting_id, livekit_room) "
                    "VALUES (:org, :meeting, :room) "
                    "ON CONFLICT (org_id, meeting_id) DO UPDATE SET updated_at = now() "
                    "RETURNING livekit_room"
                ),
                {
                    "org": str(org_id),
                    "meeting": str(meeting_id),
                    "room": room_name_for(meeting_id),
                },
            )
        )
        .mappings()
        .one()
    )
    room = str(row["livekit_room"])
    await transport.ensure_room(org_id=org_id, room=room)
    return room


async def mark_room_ended(session: AsyncSession, org_id: UUID, meeting_id: UUID) -> bool:
    """Transition `meeting_rooms.status` to `ended`. Returns whether anything changed.

    Deliberately tolerant of a missing row (a meeting can end without ever having minted a
    token / opened a room), and of an already-ended row (redelivery of `meeting.ended` must be
    a no-op, not a second transition or an error) - both return False rather than raising, since
    neither is a security-relevant fail-closed case: no cross-tenant data is at stake, only
    whether a status transition happened.
    """
    result = cast(
        "CursorResult[Any]",
        await session.execute(
            text(
                "UPDATE meeting_rooms SET status = 'ended', updated_at = now() "
                "WHERE org_id = :org AND meeting_id = :meeting AND status <> 'ended'"
            ),
            {"org": str(org_id), "meeting": str(meeting_id)},
        ),
    )
    return bool(result.rowcount)
