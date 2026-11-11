"""`GET /api/v1/meetings/{meeting_id}/minutes`: the caller's own minutes and the meeting's shared
minutes (polish program, "Minutes for everyone"). See `services/core_api/minutes.py`.

Participants only: another tenant's meeting is a 404 (RLS hides it), a meeting the caller is not
in is a 403 `NOT_A_PARTICIPANT`. A personal set of minutes is only ever served to its own person.
"""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api import minutes, records, room_signals

router = APIRouter(prefix="/api/v1", tags=["minutes"])


@router.get(
    "/meetings/{meeting_id}/minutes",
    response_model=minutes.MeetingMinutesView,
    summary="Read a meeting's minutes",
    responses={403: {"description": "Not a participant of this meeting."}},
)
async def get_minutes(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> minutes.MeetingMinutesView:
    """Your minutes (decisions with their status, open items with owners, unanswered
    questions and your promises) and the shared minutes everyone in the meeting sees."""
    viewer = ctx.user_id or uuid.UUID(int=0)
    access = await records.access_for(session, meeting_id, viewer)
    if access is None:
        raise not_found()
    if ctx.user_id is None or not access.is_participant:
        raise ProblemDetail(
            403, ErrorCode.NOT_A_PARTICIPANT, "You are not a participant of this meeting"
        )
    shared = await minutes.shared_minutes(session, meeting_id)
    mine = await minutes.personal_minutes(session, meeting_id, ctx.user_id)
    return minutes.MeetingMinutesView(
        meeting_id=meeting_id,
        state=minutes.view_state(access.status, shared),
        mine=mine,
        shared=shared,
        late_contributions=await room_signals.late_contributions(session, meeting_id, ctx.user_id),
    )
