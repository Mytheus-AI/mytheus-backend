"""Internal reads for `agent-runtime-service` (P10 contract sections 5 and 6).

`GET /internal/meetings/{meeting_id}/related-attendees?user_id=`: the ContextGapAgent's
"related_meeting" hop asks which colleagues are most likely to hold context the requester lacks.
Answer: up to 5 user ids who attended the requester's 10 most recent ENDED meetings in this org
(this meeting excluded), minus this meeting's participants and the requester, ranked by the
number of those meetings shared with the requester, then by how recently, then by id (stable).
Response `{"user_ids": [...]}`, possibly empty.

Service-to-service, the scheme `internal_meetings.py` and `internal_retrieval.py` use:
`X-Mytheus-Service: agent-runtime-service`, its secret from `MYTHEUS_CORE_CALLER_TOKENS`
(`agent-runtime-service=<32+ chars>`), the tenant in `X-Mytheus-Org`. Not routed by
`api-gateway`. An unknown meeting or user is 404, as is one of another tenant (RLS hides them).
"""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request, Security
from pydantic import BaseModel
from sqlalchemy import text

from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.core_api.routes.internal_meetings import authenticate_caller, service_token_scheme

ALLOWED_CALLERS = frozenset({"agent-runtime-service"})
RECENT_MEETINGS = 10
MAX_RELATED = 5

router = APIRouter(tags=["core-internal"])


async def agent_runtime_caller(
    request: Request,
    _token: Annotated[str | None, Security(service_token_scheme)] = None,
) -> uuid.UUID:
    return await authenticate_caller(request, ALLOWED_CALLERS)


class RelatedAttendees(BaseModel):
    user_ids: list[uuid.UUID]


_RELATED = text("""
    WITH recent AS (
        SELECT m.id, m.updated_at
        FROM meetings m JOIN meeting_participants p ON p.meeting_id = m.id
        WHERE p.user_id = :uid AND m.status = 'ended' AND m.deleted_at IS NULL
          AND m.id <> :mid
        ORDER BY m.updated_at DESC, m.id DESC
        LIMIT :recent
    )
    SELECT p.user_id, count(*) AS shared, max(r.updated_at) AS last_seen
    FROM recent r
    JOIN meeting_participants p ON p.meeting_id = r.id
    JOIN users u ON u.id = p.user_id AND u.deleted_at IS NULL
    WHERE p.user_id <> :uid
      AND p.user_id NOT IN (SELECT user_id FROM meeting_participants WHERE meeting_id = :mid)
    GROUP BY p.user_id
    ORDER BY shared DESC, last_seen DESC, p.user_id
    LIMIT :lim
    """)


@router.get("/internal/meetings/{meeting_id}/related-attendees", response_model=RelatedAttendees)
async def related_attendees(
    meeting_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(agent_runtime_caller)],
    user_id: Annotated[uuid.UUID, Query()],
) -> RelatedAttendees:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        known = (
            await session.execute(
                text(
                    "SELECT (SELECT count(*) FROM meetings WHERE id = :m AND deleted_at IS NULL), "
                    "(SELECT count(*) FROM users WHERE id = :u AND deleted_at IS NULL)"
                ),
                {"m": str(meeting_id), "u": str(user_id)},
            )
        ).one()
        if not known[0] or not known[1]:
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        rows = await session.execute(
            _RELATED,
            {
                "uid": str(user_id),
                "mid": str(meeting_id),
                "recent": RECENT_MEETINGS,
                "lim": MAX_RELATED,
            },
        )
        return RelatedAttendees(user_ids=[r[0] for r in rows])
