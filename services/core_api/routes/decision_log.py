"""The decision log: confirmed decisions the caller may see, newest first.

`GET /api/v1/decisions` backs the web Decisions screen. Two rules decide what it returns, and
both are the rules this service already applies elsewhere rather than new ones:

- **Only what a person confirmed.** `status` is `confirmed` or `superseded` (a confirmed
  decision a later one replaced). `proposed` and `dismissed` rows are agent output nobody
  approved and never appear, the same line `internal_retrieval` draws for RAG citations.
- **Participant-scoped.** A decision is visible to the participants of the meeting it was made
  in (ADR-0033 D1, `_require_participant` for candidates). The log is the union over the
  caller's meetings, so a member never reads a decision from a meeting they were not in.

Another tenant's rows are invisible to RLS, so this route has no tenant parameter at all.

Each entry carries its meeting's title and start, so the screen can group and label without a
second round trip, plus `superseded_by` (the decision that replaced it, if any) so the lineage
link is one hop from the log. `decisions` has no `confirmed_at` column; `created_at` is when the
decision was recorded in the meeting.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, require_tier, tenant_session

router = APIRouter(prefix="/api/v1", tags=["decisions"])

# A tenant's confirmed decisions grow slowly (a handful per meeting); one page covers the log
# for a long time, and the screen filters client-side.
MAX_DECISIONS = 500

LoggedStatus = Literal["confirmed", "superseded"]


class DecisionLogEntry(BaseModel):
    id: uuid.UUID
    meeting_id: uuid.UUID
    meeting_title: str | None
    meeting_started_at: datetime
    content: str
    rationale: str | None = None
    status: LoggedStatus
    supersedes: uuid.UUID | None = None
    superseded_by: uuid.UUID | None = None
    created_at: datetime


class DecisionLog(BaseModel):
    items: list[DecisionLogEntry]


_SQL = text(
    "SELECT d.id, d.meeting_id, m.title AS meeting_title, "
    "COALESCE(m.scheduled_start, m.created_at) AS meeting_started_at, d.content, d.rationale, "
    "d.status, d.supersedes, "
    "(SELECT n.id FROM decisions n WHERE n.supersedes = d.id AND n.deleted_at IS NULL "
    " AND n.status IN ('confirmed', 'superseded') ORDER BY n.created_at DESC LIMIT 1) "
    "AS superseded_by, d.created_at "
    "FROM decisions d JOIN meetings m ON m.id = d.meeting_id "
    "WHERE d.status IN ('confirmed', 'superseded') AND d.deleted_at IS NULL "
    "AND m.deleted_at IS NULL "
    "AND EXISTS (SELECT 1 FROM meeting_participants p "
    "            WHERE p.meeting_id = d.meeting_id AND p.user_id = :u) "
    "ORDER BY d.created_at DESC, d.id DESC LIMIT :limit"
)


@router.get("/decisions", response_model=DecisionLog)
async def list_decisions(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> DecisionLog:
    if ctx.user_id is None:
        return DecisionLog(items=[])
    rows = (
        (await session.execute(_SQL, {"u": str(ctx.user_id), "limit": MAX_DECISIONS}))
        .mappings()
        .all()
    )
    return DecisionLog(items=[DecisionLogEntry(**dict(r)) for r in rows])
