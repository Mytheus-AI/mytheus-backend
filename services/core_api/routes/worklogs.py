"""My Notes read model. Plan 7-9 section 3.3 step 3 and section 3.4 step 2.

`GET /api/v1/meetings/:id/worklogs?user_id=me` returns the caller's own surfaced
`PersonalListenerAgent` notes for one meeting, oldest first. It hydrates the agent sidebar
(and re-hydrates it after a socket drop: `realtime-gateway` replays nothing).

Notes are private to their participant. `user_id` accepts only `me`: there is no way to ask
for another participant's notes, so the endpoint cannot be turned into a probe, and RLS keeps
it inside the tenant on top. An unknown or other-tenant meeting is a 404.

Written only by `core-api`'s `agent-results` consumer (`services/core_api/consumer.py`); this
module only reads.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from services.core_api.routes.transcript import _assert_meeting_visible
from services.core_api.schemas import Worklog, WorklogList

router = APIRouter(prefix="/api/v1", tags=["worklogs"])

# ~90 windows per meeting is the ceiling of one participant's notes, so one page is enough
MAX_NOTES = 500


@router.get("/meetings/{meeting_id}/worklogs", response_model=WorklogList)
async def list_my_worklogs(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    user_id: Annotated[Literal["me"], Query()] = "me",
) -> WorklogList:
    if ctx.user_id is None:
        raise not_found()
    await _assert_meeting_visible(session, meeting_id)
    rows = (
        (
            await session.execute(
                text(
                    "SELECT id, meeting_id, user_id, window_id, content, relevance_score, "
                    "confidence, source_chunk_ids, created_at FROM worklogs "
                    "WHERE meeting_id = :m AND user_id = :u AND surfaced AND deleted_at IS NULL "
                    "ORDER BY created_at ASC, id ASC LIMIT :limit"
                ),
                {"m": str(meeting_id), "u": str(ctx.user_id), "limit": MAX_NOTES},
            )
        )
        .mappings()
        .all()
    )
    return WorklogList(items=[Worklog(**dict(r)) for r in rows])
