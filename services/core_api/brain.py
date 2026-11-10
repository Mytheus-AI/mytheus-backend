"""ADR-0039 Part B rules shared by core-api's routes and bff's screen reads.

Two things several routes need and must agree on:

- **The confidential rule (B2).** A confidential meeting is never a source for another
  meeting's carry-forward shown to someone who was not in it. `may_see_source` answers it for
  one viewer (the public carry-forward, the drafted agenda, the brief); `may_share_source`
  answers it for a whole room (the internal agenda route, whose carry-forward the MeetingAgent
  can repeat to everyone in the meeting): only when every participant of the meeting was also
  in the confidential one.
- **The latest status of an action (B7).** The assignee's most recent answer to a status
  request about it, from any meeting.

Every function runs on a session with `app.current_org` bound, so RLS confines it to the tenant.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from services.core_api.schemas import LatestStatus


async def is_confidential(session: AsyncSession, meeting_id: uuid.UUID) -> bool:
    row = (
        await session.execute(
            text("SELECT confidential FROM meetings WHERE id = :m"), {"m": str(meeting_id)}
        )
    ).first()
    return bool(row[0]) if row is not None else False


async def may_see_source(
    session: AsyncSession, source_meeting_id: uuid.UUID, viewer: uuid.UUID | None
) -> bool:
    """True unless `source_meeting_id` is confidential and `viewer` was not in it."""
    row = (
        await session.execute(
            text(
                "SELECT m.confidential, EXISTS (SELECT 1 FROM meeting_participants p "
                "  WHERE p.meeting_id = m.id AND p.user_id = CAST(:u AS uuid)) "
                "FROM meetings m WHERE m.id = :m"
            ),
            {"m": str(source_meeting_id), "u": str(viewer) if viewer else None},
        )
    ).first()
    if row is None:
        return False
    return not row[0] or bool(row[1])


async def may_share_source(
    session: AsyncSession, source_meeting_id: uuid.UUID, meeting_id: uuid.UUID
) -> bool:
    """True unless `source_meeting_id` is confidential and someone in `meeting_id` was not in
    it."""
    row = (
        await session.execute(
            text(
                "SELECT m.confidential, NOT EXISTS ("
                "  SELECT 1 FROM meeting_participants here WHERE here.meeting_id = :here "
                "  AND NOT EXISTS (SELECT 1 FROM meeting_participants src "
                "    WHERE src.meeting_id = m.id AND src.user_id = here.user_id)) "
                "FROM meetings m WHERE m.id = :m"
            ),
            {"m": str(source_meeting_id), "here": str(meeting_id)},
        )
    ).first()
    if row is None:
        return False
    return not row[0] or bool(row[1])


async def latest_statuses(
    session: AsyncSession, action_ids: list[Any]
) -> dict[uuid.UUID, LatestStatus]:
    """Each action's most recent answered status request, if it has one."""
    if not action_ids:
        return {}
    rows = await session.execute(
        text(
            "SELECT DISTINCT ON (action_id) action_id, status_text, answered_at "
            "FROM action_status_requests WHERE action_id = ANY(:ids) AND state = 'answered' "
            "AND status_text IS NOT NULL "
            "ORDER BY action_id, answered_at DESC, id DESC"
        ),
        {"ids": [uuid.UUID(str(a)) for a in action_ids]},
    )
    return {r[0]: LatestStatus(text=r[1], answered_at=r[2]) for r in rows}
