"""Status requests (ADR-0039 B7), the assignee's side: `GET /api/v1/status-requests?state=`
and `POST /api/v1/status-requests/{id}/answer` `{status_text}`.

A request is created by workflow-service an hour before a follow-up meeting (the internal
`POST /internal/meetings/{id}/status-requests`, `internal_lifecycle.py`), one per open action of
the previous meeting, addressed to its assignee. Only that assignee reads or answers it; anyone
else gets a 404. Answering again replaces the text (the latest answer is what carry-forward
shows); an `expired` request (its meeting is over) is a 409.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api.schemas import (
    StatusAnswer,
    StatusRequest,
    StatusRequestList,
    StatusRequestState,
)

router = APIRouter(prefix="/api/v1", tags=["status-requests"])

MAX_REQUESTS = 200
_SELECT = (
    "SELECT r.id, r.action_id, a.content AS action_content, a.content AS action_title, "
    "r.meeting_id, "
    "m.title AS meeting_title, m.scheduled_start AS meeting_scheduled_start, r.state, "
    "r.status_text, r.requested_at, r.answered_at "
    "FROM action_status_requests r JOIN action_items a ON a.id = r.action_id "
    "JOIN meetings m ON m.id = r.meeting_id "
)


@router.get("/status-requests", response_model=StatusRequestList)
async def list_status_requests(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    state: Annotated[StatusRequestState | None, Query()] = None,
) -> StatusRequestList:
    """The caller's own requests, the soonest meeting first (at most 200)."""
    if ctx.user_id is None:
        return StatusRequestList(items=[])
    where = ["r.user_id = :u", "m.deleted_at IS NULL", "a.deleted_at IS NULL"]
    params: dict[str, Any] = {"u": str(ctx.user_id), "lim": MAX_REQUESTS}
    if state is not None:
        where.append("r.state = :s")
        params["s"] = state
    rows = (
        (
            await session.execute(
                text(
                    f"{_SELECT} WHERE {' AND '.join(where)} "
                    "ORDER BY m.scheduled_start ASC NULLS LAST, r.requested_at DESC, r.id "
                    "LIMIT :lim"
                ),
                params,
            )
        )
        .mappings()
        .all()
    )
    return StatusRequestList(items=[StatusRequest(**dict(r)) for r in rows])


@router.post("/status-requests/{request_id}/answer", response_model=StatusRequest)
async def answer_status_request(
    request_id: uuid.UUID,
    body: StatusAnswer,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> StatusRequest:
    if ctx.user_id is None:
        raise not_found()
    current = (
        await session.execute(
            text(
                "SELECT state FROM action_status_requests WHERE id = :id AND user_id = :u "
                "FOR UPDATE"
            ),
            {"id": str(request_id), "u": str(ctx.user_id)},
        )
    ).first()
    if current is None:
        raise not_found()
    if current[0] == "expired":
        raise ProblemDetail(
            409, ErrorCode.VERSION_CONFLICT, "This status request expired with its meeting"
        )
    await session.execute(
        text(
            "UPDATE action_status_requests SET state = 'answered', status_text = :t, "
            "answered_at = now() WHERE id = :id"
        ),
        {"t": body.status_text, "id": str(request_id)},
    )
    row = (
        (await session.execute(text(f"{_SELECT} WHERE r.id = :id"), {"id": str(request_id)}))
        .mappings()
        .one()
    )
    return StatusRequest(**dict(row))
