"""`GET /internal/meetings/{meeting_id}/post-meeting`: the MeetingLifecycleWorkflow's
post-processing read (P10, plan 3.1).

Plan 3.1 has `run_post_processing` "trigger summary build + final worklog flush via core-api".
Neither exists to trigger: there is no summary builder in any repo yet, and worklogs are written
per window by the `agent-results` consumer, so there is no buffer to flush. What the activity
can honestly do today is confirm, from core-api (which owns `meetings.status`), that the meeting
really is over, and record what it produced. This route is that read, and nothing more: it
writes nothing.

- 404: no such meeting in this tenant (RLS hides other tenants'), or deleted.
- 409 `VERSION_CONFLICT`: the meeting is not `ended`/`cancelled` yet. The workflow is signalled
  from the `ended` event core-api emits after that commit, so this should not happen; when it
  does (a replayed signal, a re-opened meeting) the activity retries with backoff.
- 200: the digest below, which becomes part of the workflow's result in Temporal history.

Service-to-service, the `internal_meetings.py` scheme: `X-Mytheus-Service: workflow-service`,
its secret in `MYTHEUS_CORE_CALLER_TOKENS`, the tenant in `X-Mytheus-Org`.
"""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Request, Security
from pydantic import BaseModel
from sqlalchemy import text

from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.core_api.routes.internal_meetings import authenticate_caller, service_token_scheme

ALLOWED_CALLERS = frozenset({"workflow-service"})

router = APIRouter(tags=["core-internal"])


async def workflow_caller(
    request: Request,
    _token: Annotated[str | None, Security(service_token_scheme)] = None,
) -> uuid.UUID:
    return await authenticate_caller(request, ALLOWED_CALLERS)


class PostMeetingDigest(BaseModel):
    meeting_id: uuid.UUID
    status: str
    participants: int
    worklogs: int
    decisions_proposed: int
    decisions_confirmed: int
    actions_proposed: int
    actions_confirmed: int
    collaborations: int
    context_fills: int


_DIGEST = text("""
    SELECT
      (SELECT count(*) FROM meeting_participants WHERE meeting_id = :m) AS participants,
      (SELECT count(*) FROM worklogs WHERE meeting_id = :m) AS worklogs,
      (SELECT count(*) FROM decisions WHERE meeting_id = :m AND deleted_at IS NULL
         AND status = 'proposed') AS decisions_proposed,
      (SELECT count(*) FROM decisions WHERE meeting_id = :m AND deleted_at IS NULL
         AND status IN ('confirmed', 'superseded')) AS decisions_confirmed,
      (SELECT count(*) FROM action_items WHERE meeting_id = :m AND deleted_at IS NULL
         AND status = 'proposed') AS actions_proposed,
      (SELECT count(*) FROM action_items WHERE meeting_id = :m AND deleted_at IS NULL
         AND status IN ('confirmed', 'done')) AS actions_confirmed,
      (SELECT count(*) FROM agent_collaborations WHERE meeting_id = :m) AS collaborations,
      (SELECT count(*) FROM agent_collaborations WHERE meeting_id = :m
         AND fill_content IS NOT NULL) AS context_fills
    """)


@router.get("/internal/meetings/{meeting_id}/post-meeting", response_model=PostMeetingDigest)
async def post_meeting_digest(
    meeting_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
) -> PostMeetingDigest:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        status = (
            await session.execute(
                text("SELECT status FROM meetings WHERE id = :id AND deleted_at IS NULL"),
                {"id": str(meeting_id)},
            )
        ).scalar()
        if status is None:
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        if status not in ("ended", "cancelled"):
            raise ProblemDetail(409, ErrorCode.VERSION_CONFLICT, "The meeting has not ended")
        counts = (await session.execute(_DIGEST, {"m": str(meeting_id)})).mappings().one()
    return PostMeetingDigest(meeting_id=meeting_id, status=str(status), **dict(counts))
