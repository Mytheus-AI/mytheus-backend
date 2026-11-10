"""Public API reads (Phase 4): search, a meeting's outcome, a decision record.

Built for API-token callers (scripts, CRMs, the MCP server) as much as for the web app, and
participant-scoped like the rest of core-api's reads: a person finds and reads what they were
in the room for, nothing else.

- `GET /api/v1/search?q=&limit=`: meetings (by title or agenda item), confirmed decisions and
  confirmed or done actions whose text matches `q` (case-insensitive substring), newest first,
  from meetings the caller took part in, plus tasks assigned to the caller. Without `q`, the
  caller's most recent meetings. This is a fast relational search; the semantic, cited answer
  across transcripts is `POST /api/v1/ask` (bff -> rag-service).
- `GET /api/v1/meetings/{id}/recap`: the meeting's confirmed outcome (agenda with status,
  decisions, actions with owners) for a participant, confidential meetings included (unlike a
  share link, nothing leaves the product here). The same shape a recap share page shows
  (`records.MeetingRecap`), so "minutes" read the same everywhere.
- `GET /api/v1/decisions/{id}/record`: a confirmed decision with its lineage, for a participant
  of its meeting (`records.DecisionRecord`).

404 for anything the tenant cannot see; 403 `NOT_A_PARTICIPANT` for a meeting the caller was
not in, the same rule the candidate routes use.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from services.core_api import records
from services.core_api.routes.chat import _not_a_participant

router = APIRouter(prefix="/api/v1", tags=["public-api"])

MAX_RESULTS = 50
SearchKind = Literal["meeting", "decision", "action"]


class SearchHit(BaseModel):
    kind: SearchKind
    id: uuid.UUID = Field(description="The meeting, decision or action id.")
    text: str = Field(description="The meeting title, or the decision / action text.")
    meeting_id: uuid.UUID | None = None
    meeting_title: str | None = None
    status: str | None = None
    due_date: date | None = None
    occurred_at: datetime = Field(description="Meeting start (or creation) / recorded time.")


class SearchResults(BaseModel):
    query: str | None
    items: list[SearchHit]


def _pattern(q: str) -> str:
    escaped = q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


_MINE = "EXISTS (SELECT 1 FROM meeting_participants p WHERE p.meeting_id = m.id AND p.user_id = :u)"

_SEARCH = f"""
SELECT * FROM (
  SELECT 'meeting' AS kind, m.id, COALESCE(m.title, 'Untitled meeting') AS text, m.id AS meeting_id,
         m.title AS meeting_title, m.status, NULL::date AS due_date,
         COALESCE(m.scheduled_start, m.created_at) AS occurred_at
  FROM meetings m
  WHERE m.deleted_at IS NULL AND {_MINE}
    AND (CAST(:q AS text) IS NULL OR m.title ILIKE :p
         OR EXISTS (SELECT 1 FROM meeting_agenda_items i
                    WHERE i.meeting_id = m.id AND i.title ILIKE :p))
  UNION ALL
  SELECT 'decision', d.id, d.content, m.id, m.title, d.status, NULL::date, d.created_at
  FROM decisions d JOIN meetings m ON m.id = d.meeting_id
  WHERE CAST(:q AS text) IS NOT NULL AND d.deleted_at IS NULL AND m.deleted_at IS NULL
    AND d.status IN ('confirmed', 'superseded') AND {_MINE}
    AND (d.content ILIKE :p OR d.rationale ILIKE :p)
  UNION ALL
  SELECT 'action', a.id, a.content, a.meeting_id, m.title, a.status, a.due_date, a.created_at
  FROM action_items a LEFT JOIN meetings m ON m.id = a.meeting_id
  WHERE CAST(:q AS text) IS NOT NULL AND a.deleted_at IS NULL AND a.status IN ('confirmed', 'done')
    AND a.content ILIKE :p
    AND ((a.meeting_id IS NULL AND a.assignee_id = :u)
         OR (a.meeting_id IS NOT NULL AND m.deleted_at IS NULL AND {_MINE}))
) hits
ORDER BY occurred_at DESC, id DESC
LIMIT :limit
"""


@router.get(
    "/search",
    response_model=SearchResults,
    summary="Search your meetings, decisions and actions",
)
async def search(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    q: Annotated[
        str | None,
        Query(min_length=2, max_length=200, description="Text to find. Omit for recent meetings."),
    ] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_RESULTS)] = 20,
) -> SearchResults:
    if ctx.user_id is None:
        return SearchResults(query=q, items=[])
    query = q.strip() if q else None
    rows = (
        (
            await session.execute(
                text(_SEARCH),
                {
                    "q": query or None,
                    "p": _pattern(query) if query else "%",
                    "u": str(ctx.user_id),
                    "limit": limit,
                },
            )
        )
        .mappings()
        .all()
    )
    return SearchResults(query=query, items=[SearchHit(**dict(r)) for r in rows])


async def _participant_or_raise(
    session: AsyncSession, ctx: RequestContext, meeting_id: uuid.UUID
) -> uuid.UUID:
    if ctx.user_id is None:
        raise _not_a_participant()
    access = await records.access_for(session, meeting_id, ctx.user_id)
    if access is None:
        raise not_found()
    if not access.is_participant:
        raise _not_a_participant()
    return ctx.user_id


@router.get(
    "/meetings/{meeting_id}/recap",
    response_model=records.MeetingRecap,
    summary="A meeting's confirmed outcome",
    description=(
        "Agenda with status, confirmed decisions and confirmed or done actions with owners. "
        "Participants only. Proposed candidates never appear."
    ),
)
async def meeting_recap(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> records.MeetingRecap:
    viewer = await _participant_or_raise(session, ctx, meeting_id)
    recap = await records.build_recap(session, meeting_id, viewer, for_sharing=False)
    if recap is None:
        raise not_found()
    return recap


@router.get(
    "/decisions/{decision_id}/record",
    response_model=records.DecisionRecord,
    summary="A decision with its lineage",
    description=(
        "A confirmed (or superseded) decision, the decisions it replaced and that replaced it, "
        "and the meeting's actions. Steps from meetings you were not in are withheld."
    ),
)
async def decision_record(
    decision_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> records.DecisionRecord:
    meeting_id = (
        await session.execute(
            text(
                "SELECT meeting_id FROM decisions WHERE id = :d AND deleted_at IS NULL "
                "AND status IN ('confirmed', 'superseded')"
            ),
            {"d": str(decision_id)},
        )
    ).scalar()
    if meeting_id is None:
        raise not_found()
    viewer = await _participant_or_raise(session, ctx, uuid.UUID(str(meeting_id)))
    record = await records.build_decision_record(session, decision_id, viewer, for_sharing=False)
    if record is None:
        raise not_found()
    return record
