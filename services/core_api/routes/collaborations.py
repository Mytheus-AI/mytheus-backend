"""Agent-to-agent collaboration log and ContextGapAgent context fills (P10 contract section 6).

The `agent-results` consumer writes one `agent_collaborations` row per ContextGapAgent
collaboration entry, with the fill (if any) on the row that produced it. This module reads and
dismisses them:

- `GET /api/v1/meetings/{meeting_id}/collaborations`: the meeting's collaboration log, newest
  first, keyset-paginated. Meeting participants and the tenant's admins/owners only; everyone
  else gets 404, the same answer another tenant gets, so whether a log exists is not shown.
  The log is metadata only: who asked whom, by which hop, with what result and latency. The
  fill content and citations are never served here; they are the requester's.
- `GET /api/v1/meetings/{meeting_id}/context-fills`: the CALLER'S own surfaced, non-dismissed
  fills for the meeting (the sidebar re-hydrates from this on every socket (re)connect).
- `POST /api/v1/context-fills/{id}/dismiss`: the requester only (404 for anyone else, any
  tenant), idempotent 200, one `context_fill.dismissed` audit row on the first dismiss.

Privacy of the related-meeting hop: a `related_meeting` row's responder is, by construction,
someone who is NOT in this meeting but met the requester recently, so naming them tells every
participant who the requester has been meeting with. Only the requester and the tenant's
admins/owners see that responder's id and label; everyone else gets `responder: "user"` and
`responder_label: "A colleague"`. A `peer` responder is a participant of this meeting already,
so it is shown to everyone who can read the log.

Display names: `users` carries no name column, so a person's label is their email, the one
human-readable identifier this service holds. A `rag` responder is labelled "Knowledge base".
"""

from __future__ import annotations

import base64
import binascii
import json
import uuid
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import ACTION_CONTEXT_FILL_DISMISSED, audit
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail

router = APIRouter(prefix="/api/v1", tags=["collaborations"])

DEFAULT_LIMIT = 50
MAX_LIMIT = 200
MAX_FILLS = 100
KNOWLEDGE_BASE = "Knowledge base"
A_COLLEAGUE = "A colleague"


class Collaboration(BaseModel):
    id: uuid.UUID
    created_at: datetime
    hop: str
    requester_id: uuid.UUID
    requester_name: str | None
    responder: str
    responder_label: str
    query_class: str
    target_scope: str | None
    result: str
    success: bool
    latency_ms: int


class CollaborationPage(BaseModel):
    items: list[Collaboration]
    next_cursor: str | None = None


class ContextFill(BaseModel):
    id: uuid.UUID
    content: str
    confidence: float
    rationale: str | None
    hop: str
    responder: str
    responder_label: str
    citations: list[dict[str, Any]]
    created_at: datetime


class ContextFillList(BaseModel):
    items: list[ContextFill]


class ContextFillDismissed(BaseModel):
    id: uuid.UUID
    dismissed_at: datetime


def _encode_cursor(created_at: datetime, row_id: uuid.UUID) -> str:
    raw = json.dumps({"c": created_at.isoformat(), "i": str(row_id)}).encode()
    return base64.urlsafe_b64encode(raw).decode()


def _decode_cursor(cursor: str) -> tuple[datetime, uuid.UUID]:
    try:
        raw = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        return datetime.fromisoformat(raw["c"]), uuid.UUID(raw["i"])
    except (ValueError, KeyError, TypeError, binascii.Error) as exc:
        raise ProblemDetail(400, ErrorCode.INVALID_CURSOR, "Malformed pagination cursor") from exc


async def _require_meeting(session: AsyncSession, meeting_id: uuid.UUID) -> None:
    row = (
        await session.execute(
            text("SELECT 1 FROM meetings WHERE id = :id AND deleted_at IS NULL"),
            {"id": str(meeting_id)},
        )
    ).first()
    if row is None:
        raise not_found()


async def _is_participant(
    session: AsyncSession, meeting_id: uuid.UUID, user_id: uuid.UUID | None
) -> bool:
    if user_id is None:
        return False
    row = (
        await session.execute(
            text("SELECT 1 FROM meeting_participants WHERE meeting_id = :m AND user_id = :u"),
            {"m": str(meeting_id), "u": str(user_id)},
        )
    ).first()
    return row is not None


def _responder_user(responder: str) -> uuid.UUID | None:
    if not responder.startswith("user:"):
        return None
    try:
        return uuid.UUID(responder[5:])
    except ValueError:
        return None


async def _names(session: AsyncSession, ids: set[uuid.UUID]) -> dict[uuid.UUID, str]:
    if not ids:
        return {}
    rows = await session.execute(
        text("SELECT id, email FROM users WHERE id = ANY(CAST(:ids AS uuid[]))"),
        {"ids": [str(i) for i in ids]},
    )
    return {r[0]: str(r[1]) for r in rows}


def _label(responder: str, names: dict[uuid.UUID, str]) -> str:
    if responder == "rag":
        return KNOWLEDGE_BASE
    user = _responder_user(responder)
    if user is None:
        return responder
    return names.get(user, "A teammate")


@router.get("/meetings/{meeting_id}/collaborations", response_model=CollaborationPage)
async def list_collaborations(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    cursor: str | None = None,
) -> CollaborationPage:
    """The meeting's A2A collaboration log, newest first. Participants and admins only."""
    await _require_meeting(session, meeting_id)
    if not ctx.satisfies("admin") and not await _is_participant(session, meeting_id, ctx.user_id):
        raise not_found()
    params: dict[str, Any] = {"m": str(meeting_id), "limit": limit + 1}
    where = "meeting_id = :m"
    if cursor:
        created_at, row_id = _decode_cursor(cursor)
        where += " AND (created_at, id) < (:cc, :ci)"
        params.update(cc=created_at, ci=str(row_id))
    rows = (
        (
            await session.execute(
                text(
                    "SELECT id, created_at, hop, requester, responder, query_class, "
                    "target_scope, result, success, latency_ms FROM agent_collaborations "
                    f"WHERE {where} ORDER BY created_at DESC, id DESC LIMIT :limit"
                ),
                params,
            )
        )
        .mappings()
        .all()
    )
    has_more = len(rows) > limit
    page = list(rows[:limit])
    is_admin = ctx.satisfies("admin")

    def masked(r: Any) -> bool:
        return r["hop"] == "related_meeting" and not is_admin and r["requester"] != ctx.user_id

    people = {r["requester"] for r in page}
    people |= {
        u for r in page if not masked(r) and (u := _responder_user(r["responder"])) is not None
    }
    names = await _names(session, people)
    return CollaborationPage(
        items=[
            Collaboration(
                id=r["id"],
                created_at=r["created_at"],
                hop=r["hop"],
                requester_id=r["requester"],
                requester_name=names.get(r["requester"]),
                responder="user" if masked(r) else r["responder"],
                responder_label=A_COLLEAGUE if masked(r) else _label(r["responder"], names),
                query_class=r["query_class"],
                target_scope=r["target_scope"],
                result=r["result"],
                success=r["success"],
                latency_ms=r["latency_ms"],
            )
            for r in page
        ],
        next_cursor=(
            _encode_cursor(page[-1]["created_at"], page[-1]["id"]) if has_more and page else None
        ),
    )


@router.get("/meetings/{meeting_id}/context-fills", response_model=ContextFillList)
async def list_context_fills(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ContextFillList:
    """The caller's own surfaced, non-dismissed context fills for this meeting, oldest first."""
    await _require_meeting(session, meeting_id)
    if not await _is_participant(session, meeting_id, ctx.user_id):
        raise not_found()
    rows = (
        (
            await session.execute(
                text(
                    "SELECT id, fill_content, confidence, rationale, hop, responder, citations, "
                    "created_at FROM agent_collaborations WHERE meeting_id = :m "
                    "AND requester = :u AND surfaced AND dismissed_at IS NULL "
                    "AND fill_content IS NOT NULL AND confidence IS NOT NULL "
                    "ORDER BY created_at ASC, id ASC LIMIT :limit"
                ),
                {"m": str(meeting_id), "u": str(ctx.user_id), "limit": MAX_FILLS},
            )
        )
        .mappings()
        .all()
    )
    users = {u for r in rows if (u := _responder_user(r["responder"])) is not None}
    names = await _names(session, users)
    return ContextFillList(
        items=[
            ContextFill(
                id=r["id"],
                content=r["fill_content"],
                confidence=r["confidence"],
                rationale=r["rationale"],
                hop=r["hop"],
                responder=r["responder"],
                responder_label=_label(r["responder"], names),
                citations=list(r["citations"] or []),
                created_at=r["created_at"],
            )
            for r in rows
        ]
    )


@router.post("/context-fills/{fill_id}/dismiss", response_model=ContextFillDismissed)
async def dismiss_context_fill(
    fill_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ContextFillDismissed:
    """Hide a fill card for good. Requester only; repeating it is 200 and writes nothing."""
    if ctx.user_id is None:
        raise not_found()
    row = (
        (
            await session.execute(
                text(
                    "SELECT id, meeting_id, dismissed_at FROM agent_collaborations "
                    "WHERE id = :id AND requester = :u AND fill_content IS NOT NULL FOR UPDATE"
                ),
                {"id": str(fill_id), "u": str(ctx.user_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()  # missing, not a fill, another user's or another tenant's
    if row["dismissed_at"] is not None:
        return ContextFillDismissed(id=fill_id, dismissed_at=row["dismissed_at"])
    dismissed_at = (
        await session.execute(
            text(
                "UPDATE agent_collaborations SET dismissed_at = now() WHERE id = :id "
                "RETURNING dismissed_at"
            ),
            {"id": str(fill_id)},
        )
    ).scalar_one()
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_CONTEXT_FILL_DISMISSED,
        target_type="agent_collaboration",
        target_id=fill_id,
        metadata={"meeting_id": str(row["meeting_id"])},
        ip=ctx.ip,
    )
    return ContextFillDismissed(id=fill_id, dismissed_at=dismissed_at)
