"""Delegated answers: what a person who sent their agent pre-approved it to say, and what it said.

Polish program wave 3 ("Only Mytheus can do these"). Before a meeting, a participant whose
attendance is `agent` (ADR-0039 B5) lists up to 10 statements ("either vendor under $40k is
fine"). In the meeting, their agent answers questions put to them ONLY from those statements
(mytheus-intelligence `delegation/`, on the ContextGapAgent peer path); every answer, and every
refusal ("Sarah didn't pre-approve an answer to that"), comes back on `agent-results` inside a
`context_fill` record's `delegated` block and core-api's consumer logs it here.

- `GET /api/v1/meetings/{id}/delegated-answers`: the caller's statements and whether they can
  still be edited (until the meeting has ended or been cancelled).
- `PUT /api/v1/meetings/{id}/delegated-answers` `{statements: [str]}`: replaces the list. An
  unchanged statement keeps its id (the log points at it); a removed one is soft-deleted.
- `GET /api/v1/meetings/{id}/delegated-answers/log`: what the CALLER's agent said. Only the
  person who delegated sees it.
- `GET /internal/meetings/{id}/delegated-answers` (agent-runtime-service): every participant
  whose attendance is `agent` and who has at least one statement, with a display name.

Participants only (403 `NOT_A_PARTICIPANT`); another tenant's meeting is a 404.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import audit
from libs.shared.authz import RequestContext, require_tier, tenant_session
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.core_api.records import person_name
from services.core_api.routes.chat import (
    _fetch_meeting_exists,
    _not_a_participant,
)
from services.core_api.routes.internal_agents import agent_runtime_caller

router = APIRouter(tags=["delegated-answers"])

MAX_STATEMENTS = 10
MAX_CHARS = 300
ACTION_DELEGATED_ANSWERS_UPDATED = "delegated_answers.updated"


class DelegatedStatement(BaseModel):
    id: uuid.UUID
    text: str
    position: int
    updated_at: datetime


class DelegatedAnswers(BaseModel):
    meeting_id: uuid.UUID
    attendance: Literal["live", "agent"]
    editable: bool
    max_statements: int = MAX_STATEMENTS
    max_chars: int = MAX_CHARS
    statements: list[DelegatedStatement]


class DelegatedAnswersUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    statements: list[str] = Field(max_length=MAX_STATEMENTS)

    @field_validator("statements")
    @classmethod
    def _clean(cls, value: list[str]) -> list[str]:
        out: list[str] = []
        for raw in value:
            item = " ".join(raw.split())
            if not item:
                raise ValueError("a statement cannot be empty")
            if len(item) > MAX_CHARS:
                raise ValueError(f"a statement is at most {MAX_CHARS} characters")
            if item.lower() not in {o.lower() for o in out}:
                out.append(item)
        return out


class DelegatedAnswerEntry(BaseModel):
    id: uuid.UUID
    question: str
    answer: str
    covered: bool
    statement_id: uuid.UUID | None = None
    statement_text: str | None = None
    asked_by: uuid.UUID | None = None
    asked_by_name: str | None = None
    created_at: datetime


class DelegatedAnswerLog(BaseModel):
    items: list[DelegatedAnswerEntry]


class InternalStatement(BaseModel):
    id: uuid.UUID
    text: str


class InternalDelegate(BaseModel):
    user_id: uuid.UUID
    name: str
    statements: list[InternalStatement]


class InternalDelegates(BaseModel):
    delegates: list[InternalDelegate]


async def _participation(
    session: AsyncSession, meeting_id: uuid.UUID, ctx: RequestContext
) -> tuple[uuid.UUID, str, str]:
    """(user_id, attendance, meeting status), 404 then 403."""
    await _fetch_meeting_exists(session, meeting_id)
    if ctx.user_id is None:
        raise _not_a_participant()
    row = (
        await session.execute(
            text(
                "SELECT p.attendance, m.status FROM meeting_participants p "
                "JOIN meetings m ON m.id = p.meeting_id "
                "WHERE p.meeting_id = :m AND p.user_id = :u"
            ),
            {"m": str(meeting_id), "u": str(ctx.user_id)},
        )
    ).first()
    if row is None:
        raise _not_a_participant()
    return ctx.user_id, str(row[0]), str(row[1])


def _editable(meeting_status: str) -> bool:
    return meeting_status not in ("ended", "cancelled")


async def _statements(
    session: AsyncSession, meeting_id: uuid.UUID, user_id: uuid.UUID
) -> list[DelegatedStatement]:
    rows = (
        (
            await session.execute(
                text(
                    "SELECT id, text, position, updated_at FROM delegated_answer_statements "
                    "WHERE meeting_id = :m AND user_id = :u AND deleted_at IS NULL "
                    "ORDER BY position, created_at, id"
                ),
                {"m": str(meeting_id), "u": str(user_id)},
            )
        )
        .mappings()
        .all()
    )
    return [DelegatedStatement(**dict(r)) for r in rows]


@router.get("/api/v1/meetings/{meeting_id}/delegated-answers", response_model=DelegatedAnswers)
async def get_delegated_answers(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> DelegatedAnswers:
    user_id, attendance, status = await _participation(session, meeting_id, ctx)
    return DelegatedAnswers(
        meeting_id=meeting_id,
        attendance=attendance,  # type: ignore[arg-type]
        editable=_editable(status),
        statements=await _statements(session, meeting_id, user_id),
    )


@router.put("/api/v1/meetings/{meeting_id}/delegated-answers", response_model=DelegatedAnswers)
async def put_delegated_answers(
    meeting_id: uuid.UUID,
    body: DelegatedAnswersUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> DelegatedAnswers:
    user_id, attendance, status = await _participation(session, meeting_id, ctx)
    if not _editable(status):
        raise ProblemDetail(
            409, ErrorCode.VERSION_CONFLICT, "The meeting is over: these can no longer change"
        )
    # Serialise concurrent saves of the same list (two tabs, a double click).
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext('delegated:' || :m || ':' || :u))"),
        {"m": str(meeting_id), "u": str(user_id)},
    )
    current = {s.text.lower(): s for s in await _statements(session, meeting_id, user_id)}
    wanted = {t.lower() for t in body.statements}
    gone = [s.id for key, s in current.items() if key not in wanted]
    if gone:
        await session.execute(
            text(
                "UPDATE delegated_answer_statements SET deleted_at = now() "
                "WHERE id = ANY(CAST(:ids AS uuid[]))"
            ),
            {"ids": [str(i) for i in gone]},
        )
    for position, item in enumerate(body.statements):
        existing = current.get(item.lower())
        if existing is not None:
            if existing.position != position or existing.text != item:
                await session.execute(
                    text(
                        "UPDATE delegated_answer_statements SET position = :p, text = :t, "
                        "updated_at = now() WHERE id = :id"
                    ),
                    {"p": position, "t": item, "id": str(existing.id)},
                )
            continue
        await session.execute(
            text(
                "INSERT INTO delegated_answer_statements (org_id, meeting_id, user_id, text, "
                "position) VALUES (:o, :m, :u, :t, :p)"
            ),
            {
                "o": str(ctx.org_id),
                "m": str(meeting_id),
                "u": str(user_id),
                "t": item,
                "p": position,
            },
        )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=user_id,
        action=ACTION_DELEGATED_ANSWERS_UPDATED,
        target_type="meeting",
        target_id=meeting_id,
        metadata={"count": len(body.statements)},
        ip=ctx.ip,
    )
    return DelegatedAnswers(
        meeting_id=meeting_id,
        attendance=attendance,  # type: ignore[arg-type]
        editable=True,
        statements=await _statements(session, meeting_id, user_id),
    )


@router.get(
    "/api/v1/meetings/{meeting_id}/delegated-answers/log", response_model=DelegatedAnswerLog
)
async def delegated_answer_log(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> DelegatedAnswerLog:
    user_id, _, _ = await _participation(session, meeting_id, ctx)
    rows = (
        (
            await session.execute(
                text(
                    "SELECT l.id, l.question, l.answer, l.covered, l.statement_id, "
                    "COALESCE(l.statement_text, s.text) AS statement_text, l.asked_by, "
                    "u.email AS asked_by_email, u.display_name AS asked_by_display_name, "
                    "l.created_at "
                    "FROM delegated_answer_log l "
                    "LEFT JOIN delegated_answer_statements s ON s.id = l.statement_id "
                    "LEFT JOIN users u ON u.id = l.asked_by "
                    "WHERE l.meeting_id = :m AND l.user_id = :u "
                    "ORDER BY l.created_at, l.id LIMIT 500"
                ),
                {"m": str(meeting_id), "u": str(user_id)},
            )
        )
        .mappings()
        .all()
    )
    return DelegatedAnswerLog(
        items=[
            DelegatedAnswerEntry(
                id=r["id"],
                question=r["question"],
                answer=r["answer"],
                covered=r["covered"],
                statement_id=r["statement_id"],
                statement_text=r["statement_text"] if r["covered"] else None,
                asked_by=r["asked_by"],
                asked_by_name=person_name(r["asked_by_display_name"], r["asked_by_email"]),
                created_at=r["created_at"],
            )
            for r in rows
        ]
    )


@router.get(
    "/internal/meetings/{meeting_id}/delegated-answers",
    response_model=InternalDelegates,
    tags=["core-internal"],
)
async def internal_delegated_answers(
    meeting_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(agent_runtime_caller)],
) -> InternalDelegates:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        await _fetch_meeting_exists(session, meeting_id)
        rows = (
            await session.execute(
                text(
                    "SELECT p.user_id, u.email, s.id, s.text, u.display_name "
                    "FROM meeting_participants p "
                    "JOIN users u ON u.id = p.user_id AND u.deleted_at IS NULL "
                    "JOIN delegated_answer_statements s ON s.meeting_id = p.meeting_id "
                    " AND s.user_id = p.user_id AND s.deleted_at IS NULL "
                    "WHERE p.meeting_id = :m AND p.attendance = 'agent' "
                    "ORDER BY p.user_id, s.position, s.created_at, s.id"
                ),
                {"m": str(meeting_id)},
            )
        ).all()
    out: dict[uuid.UUID, InternalDelegate] = {}
    for user_id, email, sid, stext, stored in rows:
        d = out.get(user_id)
        if d is None:
            d = out[user_id] = InternalDelegate(
                user_id=user_id, name=person_name(stored, email) or "Your colleague", statements=[]
            )
        d.statements.append(InternalStatement(id=sid, text=stext))
    return InternalDelegates(delegates=list(out.values()))
