"""Shared `meeting_rooms` writers: the deterministic room-naming convention, plus the upsert
and lifecycle transitions used by the token-mint route (Task 3), the LiveKit webhook, and the
`meeting-events` consumer (both Task 4). One place, so all three writers agree on the room name
`meeting-{meeting_id}` by construction rather than by convention re-typed three times.

This module intentionally has NO opinion on `org_id` provenance or RLS: every caller must have
already called `libs.shared.tenancy.set_rls` (or otherwise established `app.current_org`) on the
session it passes in. That is a route/consumer/handler concern, not a storage-helper concern.

Fix round 1 (Task 4 report follow-up): this module does NOT call `MeetingTransport.ensure_room`
itself, and does not take a `transport` argument at all - it did, briefly, and that was the
bug. `ensure_meeting_room` is called from inside an open write transaction at both its call
sites (the token-mint route's `tenant_session`, the `meeting-events` consumer's per-message
`session.begin()`), and a real LiveKit HTTP call belongs OUTSIDE that window: two real network
round trips inside the token-mint route's transaction undercut the `_BIND_STATEMENT_TIMEOUT`
SLO protection next to it (nothing bounds non-Postgres work), and an uncaught LiveKit failure
inside the `meeting-events` consumer's synchronous path crashed the whole consumer and blocked
the partition for every tenant - reached through a different exception type than the DLQ path
was built to catch.

The fix: `ensure_meeting_room` does ONLY the DB upsert and reports whether a LiveKit sync is
still outstanding (`RoomUpsertResult.needs_livekit_sync`, backed by the new
`meeting_rooms.livekit_metadata_synced_at` column - NULL means "never synced"). Callers run
`transport.ensure_room` AFTER their DB transaction has committed and the connection has been
returned to the pool (a FastAPI `BackgroundTasks` callback for the route, a detached
`asyncio.create_task` for the consumer - see `routes.py` and `consumer.py`), and only when
`needs_livekit_sync` is true, so a synced room costs nothing on every later mint. A failed
sync attempt is caught and logged where it happens, never raised back into a request or the
consumer's poll loop: the row stays unsynced and self-heals the next time this meeting's
`ensure_meeting_room` runs (another mint, or `meeting.ended` triggering nothing new here but
the route being hit again by a reconnect) - see `mark_room_metadata_synced`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from libs.shared.tenancy import set_rls
from services.meeting_service.transport import MeetingTransport

logger = logging.getLogger(__name__)


def room_name_for(meeting_id: UUID) -> str:
    """The one place the deterministic room-naming scheme lives: `meeting-{meeting_id}`."""
    return f"meeting-{meeting_id}"


async def meeting_is_visible(session: AsyncSession, meeting_id: UUID) -> bool:
    """True iff this tenant-scoped session can see a live (not soft-deleted) `meetings` row.

    The same check `services/core_api/visibility.py` makes before a caller-supplied id becomes
    a foreign key (gap 31): PostgreSQL runs FK checks as the referenced table's owner with RLS
    BYPASSED, so `meeting_rooms.meeting_id -> meetings(id)` would happily resolve ANOTHER
    tenant's meeting while the row's own `org_id` WITH CHECK passes. No `org_id` predicate: RLS
    scopes it, so "does not exist" and "exists in another tenant" are indistinguishable.

    MUST run after `set_rls` on a role subject to RLS (`mytheus_app`); on a BYPASSRLS
    connection it would see every tenant's meetings and pass silently.
    """
    row = (
        await session.execute(
            text("SELECT 1 FROM meetings WHERE id = :id AND deleted_at IS NULL"),
            {"id": str(meeting_id)},
        )
    ).first()
    return row is not None


async def meeting_has_ended(session: AsyncSession, meeting_id: UUID) -> bool:
    """True when the (visible) meeting is `ended` or `cancelled`. Same RLS scoping as
    `meeting_is_visible`; call it after that check."""
    row = (
        await session.execute(
            text("SELECT status FROM meetings WHERE id = :id AND deleted_at IS NULL"),
            {"id": str(meeting_id)},
        )
    ).first()
    return row is not None and row[0] in ("ended", "cancelled")


@dataclass(frozen=True)
class RoomUpsertResult:
    room: str
    needs_livekit_sync: bool
    # When the call first opened (`meeting_rooms.started_at`), or None before anyone joined.
    started_at: datetime | None = None


async def ensure_meeting_room(
    session: AsyncSession, org_id: UUID, meeting_id: UUID
) -> RoomUpsertResult:
    """Upsert the `meeting_rooms` row this meeting's LiveKit room materializes into.

    DB-only: no LiveKit call happens here (see the module docstring for why). The room name is
    deterministic rather than randomly generated per call, so a concurrent double-call racing
    this INSERT is a harmless conflict on the same value either way. `ON CONFLICT ... DO UPDATE
    SET updated_at = now()` deliberately does not touch `livekit_room` (or
    `livekit_metadata_synced_at`) in the SET clause: once a room exists its name is fixed for
    the life of the meeting, and a routine upsert must not reset a sync that already succeeded.
    """
    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO meeting_rooms (org_id, meeting_id, livekit_room) "
                    "VALUES (:org, :meeting, :room) "
                    "ON CONFLICT (org_id, meeting_id) DO UPDATE SET updated_at = now() "
                    "RETURNING livekit_room, livekit_metadata_synced_at, started_at"
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
    return RoomUpsertResult(
        room=str(row["livekit_room"]),
        needs_livekit_sync=row["livekit_metadata_synced_at"] is None,
        started_at=row["started_at"],
    )


async def mark_room_started(
    session: AsyncSession, org_id: UUID, meeting_id: UUID
) -> datetime | None:
    """Stamp `meeting_rooms.started_at` the first time the call opens, and return it.

    `COALESCE` keeps the first value forever: a later mint or a redelivered `room_started` webhook
    never moves the meeting's clock. Same RLS-scoped session as the caller's other writes.
    """
    row = (
        await session.execute(
            text(
                "UPDATE meeting_rooms SET started_at = COALESCE(started_at, now()) "
                "WHERE org_id = :org AND meeting_id = :meeting RETURNING started_at"
            ),
            {"org": str(org_id), "meeting": str(meeting_id)},
        )
    ).first()
    return row[0] if row is not None else None


async def mark_room_metadata_synced(session: AsyncSession, org_id: UUID, room: str) -> None:
    """Record that `transport.ensure_room` succeeded for this room. Called from the SAME
    out-of-band step that called `ensure_room` (after it succeeds), in its own short
    transaction - never inside the transaction `ensure_meeting_room` ran in, which has already
    committed and closed by the time this runs.
    """
    await session.execute(
        text(
            "UPDATE meeting_rooms SET livekit_metadata_synced_at = now() "
            "WHERE org_id = :org AND livekit_room = :room"
        ),
        {"org": str(org_id), "room": room},
    )


async def sync_livekit_metadata(
    session_factory: async_sessionmaker[AsyncSession],
    transport: MeetingTransport,
    *,
    org_id: UUID,
    room: str,
) -> None:
    """Out-of-band step: call `transport.ensure_room`, and on success mark the row synced in
    its own short transaction. Both call sites (`routes.py`'s token-mint route, `consumer.py`'s
    `meeting-events` handler) run this ONLY after their own DB transaction has already
    committed - a `BackgroundTasks` callback for the route, a detached `asyncio.create_task`
    for the consumer - so the Postgres connection those held is back in the pool before this
    ever makes a network call.

    Never raises. A LiveKit failure is logged and swallowed: the row stays unsynced (`
    livekit_metadata_synced_at` stays NULL), which is exactly the durable "still needs a sync"
    marker `ensure_meeting_room` already checks, so the NEXT call for this meeting (another
    mint, a reconnect) retries automatically. That is the whole retry mechanism - no queue, no
    new role, no cross-tenant background sweep: state lives in Postgres, on the same row,
    read by the same tenant-scoped session every other write to it already uses.
    """
    try:
        await transport.ensure_room(org_id=org_id, room=room)
    except Exception:  # noqa: BLE001 - a transient LiveKit failure must never propagate into
        # the caller's request/response cycle or the meeting-events poll loop; see docstring.
        logger.warning(
            "meeting-service: transport.ensure_room failed for org=%s room=%s; will retry on "
            "the next call for this meeting",
            org_id,
            room,
            exc_info=True,
        )
        return

    async with session_factory() as session, session.begin():
        await set_rls(session, org_id)
        await mark_room_metadata_synced(session, org_id, room)


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
