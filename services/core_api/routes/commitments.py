"""Promises (ADR-0039 B6): `GET /api/v1/commitments?status=` and `PATCH
/api/v1/commitments/{id}` `{status}`.

A commitment is private to the person who made it (the PersonalListenerAgent record's
`user_id`), like a note: the list is the caller's own, and anyone else's commitment is a 404
(RLS hides other tenants' rows; the `user_id` predicate hides colleagues'). Rows are written by
core-api's `agent-results` consumer; the owner only moves `status` (`open`, `done`,
`dismissed`; `open` again undoes either). `done_at` is set on `done` and cleared otherwise.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from services.core_api.schemas import (
    Commitment,
    CommitmentList,
    CommitmentStatus,
    CommitmentUpdate,
)

router = APIRouter(prefix="/api/v1", tags=["commitments"])

MAX_COMMITMENTS = 200
_SELECT = (
    "SELECT c.id, c.meeting_id, m.title AS meeting_title, "
    "m.scheduled_start AS meeting_scheduled_start, c.content, c.due_hint, c.status, "
    "c.created_at, c.updated_at, c.done_at, c.moment_id, c.action_item_id "
    "FROM commitments c JOIN meetings m ON m.id = c.meeting_id "
)


@router.get("/commitments", response_model=CommitmentList)
async def list_commitments(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    status_filter: Annotated[CommitmentStatus | None, Query(alias="status")] = None,
) -> CommitmentList:
    """The caller's own commitments, newest first (at most 200)."""
    if ctx.user_id is None:
        return CommitmentList(items=[])
    where = ["c.user_id = :u", "m.deleted_at IS NULL"]
    params: dict[str, Any] = {"u": str(ctx.user_id), "lim": MAX_COMMITMENTS}
    if status_filter is not None:
        where.append("c.status = :s")
        params["s"] = status_filter
    rows = (
        (
            await session.execute(
                text(
                    f"{_SELECT} WHERE {' AND '.join(where)} "
                    "ORDER BY c.created_at DESC, c.id DESC LIMIT :lim"
                ),
                params,
            )
        )
        .mappings()
        .all()
    )
    return CommitmentList(items=[Commitment(**dict(r)) for r in rows])


@router.patch("/commitments/{commitment_id}", response_model=Commitment)
async def update_commitment(
    commitment_id: uuid.UUID,
    body: CommitmentUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Commitment:
    """Owner only: anyone else's commitment is a 404, because it is private."""
    if ctx.user_id is None:
        raise not_found()
    updated = (
        await session.execute(
            text(
                "UPDATE commitments SET status = CAST(:s AS text), updated_at = now(), "
                "done_at = CASE WHEN CAST(:s AS text) = 'done' "
                "  THEN COALESCE(CASE WHEN status = 'done' THEN done_at END, now()) END "
                "WHERE id = :id AND user_id = :u RETURNING id"
            ),
            {"s": body.status, "id": str(commitment_id), "u": str(ctx.user_id)},
        )
    ).first()
    if updated is None:
        raise not_found()
    row = (
        (await session.execute(text(f"{_SELECT} WHERE c.id = :id"), {"id": str(commitment_id)}))
        .mappings()
        .one()
    )
    return Commitment(**dict(row))
