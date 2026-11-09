"""Internal retrieval scope for `rag-service`. mytheus-docs ADR-0033 D1 and D3.

RAG retrieval is participant-scoped: a transcript, chat or decision passage is retrievable only
by a participant of its meeting, the same rule `_require_participant` applies to chat and to
decision/action candidates. `rag-service` is in the AI tier and may not read core-api's tables,
so it asks here, once per query:

- `GET /internal/retrieval/scope`: the asker's `users.id`, the meetings they are a participant
  of (id, title, start) and the people of their org (id, email) for resolving a person named
  in a question. The asker is named by `user_id` (an agent asking for a participant) or by
  `clerk_user_id` (a user's own question; rag-service has verified their token).
- `POST /internal/retrieval/records`: the text of confirmed decisions and confirmed or done
  actions that retrieval surfaced by id (from `decisions__{org_id}` or the graph), restricted
  again to the asker's meetings. Proposed and dismissed rows are never returned: retrieval
  cites what users confirmed, nothing else.

Service-to-service, the scheme `internal_meetings.py` uses: `X-Mytheus-Service: rag-service`,
its secret from `MYTHEUS_CORE_CALLER_TOKENS`, the tenant in `X-Mytheus-Org`. Not routed by
`api-gateway`. An unknown user is a 404, as is a user of another tenant (RLS hides them).
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request, Security
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text

from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.core_api.routes.internal_meetings import authenticate_caller, service_token_scheme

ALLOWED_CALLERS = frozenset({"rag-service"})
MAX_MEETINGS = 2000
MAX_PEOPLE = 2000
MAX_RECORD_IDS = 100

router = APIRouter(tags=["core-internal"])


async def rag_caller(
    request: Request,
    _token: Annotated[str | None, Security(service_token_scheme)] = None,
) -> uuid.UUID:
    return await authenticate_caller(request, ALLOWED_CALLERS)


class ScopeMeeting(BaseModel):
    id: uuid.UUID
    title: str | None
    started_at: datetime


class ScopePerson(BaseModel):
    id: uuid.UUID
    email: str


class RetrievalScope(BaseModel):
    user_id: uuid.UUID
    meetings: list[ScopeMeeting]
    people: list[ScopePerson]


class RecordsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: uuid.UUID
    decision_ids: list[uuid.UUID] = Field(default_factory=list, max_length=MAX_RECORD_IDS)
    action_ids: list[uuid.UUID] = Field(default_factory=list, max_length=MAX_RECORD_IDS)


class RetrievedDecision(BaseModel):
    id: uuid.UUID
    meeting_id: uuid.UUID
    content: str
    rationale: str | None
    status: str
    created_at: datetime


class RetrievedAction(BaseModel):
    id: uuid.UUID
    meeting_id: uuid.UUID
    content: str
    assignee_id: uuid.UUID | None
    due_date: date | None
    status: str


class RetrievedRecords(BaseModel):
    decisions: list[RetrievedDecision]
    actions: list[RetrievedAction]


def _not_found() -> ProblemDetail:
    return ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")


_PARTICIPANT_MEETINGS = """
    SELECT m.id FROM meetings m
    JOIN meeting_participants p ON p.meeting_id = m.id
    WHERE p.user_id = :uid AND m.deleted_at IS NULL
"""


@router.get("/internal/retrieval/scope", response_model=RetrievalScope)
async def retrieval_scope(
    org_id: Annotated[uuid.UUID, Depends(rag_caller)],
    user_id: Annotated[uuid.UUID | None, Query()] = None,
    clerk_user_id: Annotated[str | None, Query(max_length=256)] = None,
) -> RetrievalScope:
    if (user_id is None) == (clerk_user_id is None):
        raise ProblemDetail(
            422, ErrorCode.VALIDATION_FAILED, "Give exactly one of user_id or clerk_user_id"
        )
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        if user_id is not None:
            row = (
                await session.execute(
                    text("SELECT id FROM users WHERE id = :id AND deleted_at IS NULL"),
                    {"id": str(user_id)},
                )
            ).first()
        else:
            row = (
                await session.execute(
                    text("SELECT id FROM users WHERE clerk_user_id = :c AND deleted_at IS NULL"),
                    {"c": clerk_user_id},
                )
            ).first()
        if row is None:
            raise _not_found()
        uid = row[0]
        meetings = (
            (
                await session.execute(
                    text(
                        "SELECT m.id, m.title, "
                        "COALESCE(m.scheduled_start, m.created_at) AS started_at "
                        "FROM meetings m JOIN meeting_participants p ON p.meeting_id = m.id "
                        "WHERE p.user_id = :uid AND m.deleted_at IS NULL "
                        "ORDER BY started_at DESC LIMIT :lim"
                    ),
                    {"uid": str(uid), "lim": MAX_MEETINGS},
                )
            )
            .mappings()
            .all()
        )
        people = (
            (
                await session.execute(
                    text(
                        "SELECT id, email FROM users WHERE deleted_at IS NULL "
                        "ORDER BY email LIMIT :lim"
                    ),
                    {"lim": MAX_PEOPLE},
                )
            )
            .mappings()
            .all()
        )
    return RetrievalScope(
        user_id=uid,
        meetings=[ScopeMeeting(**m) for m in meetings],
        people=[ScopePerson(**p) for p in people],
    )


@router.post("/internal/retrieval/records", response_model=RetrievedRecords)
async def retrieval_records(
    body: RecordsRequest,
    org_id: Annotated[uuid.UUID, Depends(rag_caller)],
) -> RetrievedRecords:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        known = (
            await session.execute(
                text("SELECT 1 FROM users WHERE id = :id AND deleted_at IS NULL"),
                {"id": str(body.user_id)},
            )
        ).first()
        if known is None:
            raise _not_found()
        decisions: list[RetrievedDecision] = []
        if body.decision_ids:
            rows = (
                (
                    await session.execute(
                        text(
                            "SELECT d.id, d.meeting_id, d.content, d.rationale, d.status, "
                            "d.created_at FROM decisions d "
                            "WHERE d.id = ANY(CAST(:ids AS uuid[])) AND d.deleted_at IS NULL "
                            "AND d.status IN ('confirmed', 'superseded') "
                            f"AND d.meeting_id IN ({_PARTICIPANT_MEETINGS})"
                        ),
                        {"ids": [str(i) for i in body.decision_ids], "uid": str(body.user_id)},
                    )
                )
                .mappings()
                .all()
            )
            decisions = [RetrievedDecision(**r) for r in rows]
        actions: list[RetrievedAction] = []
        if body.action_ids:
            rows = (
                (
                    await session.execute(
                        text(
                            "SELECT a.id, a.meeting_id, a.content, a.assignee_id, a.due_date, "
                            "a.status FROM action_items a "
                            "WHERE a.id = ANY(CAST(:ids AS uuid[])) AND a.deleted_at IS NULL "
                            "AND a.status IN ('confirmed', 'done') "
                            f"AND a.meeting_id IN ({_PARTICIPANT_MEETINGS})"
                        ),
                        {"ids": [str(i) for i in body.action_ids], "uid": str(body.user_id)},
                    )
                )
                .mappings()
                .all()
            )
            actions = [RetrievedAction(**r) for r in rows]
    return RetrievedRecords(decisions=decisions, actions=actions)
