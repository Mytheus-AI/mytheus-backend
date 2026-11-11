"""core-api internal routes for "Decisions that follow you".

Same caller-token scheme as every core-api internal route (`internal_meetings.authenticate_caller`,
secrets in `MYTHEUS_CORE_CALLER_TOKENS`). Not routed by api-gateway (`/internal` never is).

- `POST /internal/retrieval/decisions/search` (rag-service): the lexical arm of the check's
  hybrid retrieval. Postgres full-text search (`ix_decisions_fts`) over the CONFIRMED decisions of
  the meetings `user_id` is a participant of, any term matching, best rank first. Same scope rule
  as `/internal/retrieval/records` (ADR-0033 D1).
- `POST /internal/decisions/check` (integration-service): a Slack message. The author is named by
  email (integration-service resolved the Slack user); an author with no Mytheus account in this
  tenant gets no check (404 `reason: unknown_author`): retrieval is participant-scoped, so there
  is nobody to check as. Conflicts are RECORDED as challenges (a channel message is published
  text) and each decision's owner is told.
- `POST /internal/decision-challenges/{id}/dismiss` and `.../reopen` (integration-service): the
  Slack buttons. Only the challenge's author may press them (`acting_email`); anyone else is a 404.
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Request, Security
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import audit
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.core_api import decision_check as dc
from services.core_api.routes import decision_challenges as routes
from services.core_api.routes.internal_meetings import authenticate_caller, service_token_scheme

router = APIRouter(tags=["core-internal"])

ACTION_SLACK_CHECKED = "decision.checked"
MAX_TERMS = 24
_TERM = re.compile(r"[a-z0-9]{3,}")


async def rag_caller(
    request: Request,
    _token: Annotated[str | None, Security(service_token_scheme)] = None,
) -> uuid.UUID:
    return await authenticate_caller(request, frozenset({"rag-service"}))


async def integration_caller(
    request: Request,
    _token: Annotated[str | None, Security(service_token_scheme)] = None,
) -> uuid.UUID:
    return await authenticate_caller(request, frozenset({"integration-service"}))


# --- the lexical arm ---------------------------------------------------------------------------


class LexicalSearch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: uuid.UUID
    query: str = Field(min_length=1, max_length=1000)
    limit: int = Field(default=10, ge=1, le=20)


class LexicalDecision(BaseModel):
    id: uuid.UUID
    meeting_id: uuid.UUID
    content: str
    rationale: str | None = None
    meeting_title: str | None = None
    decided_at: datetime
    rank: float


class LexicalResult(BaseModel):
    items: list[LexicalDecision]


def or_query(query: str) -> str | None:
    """`pricing annual plan` -> `pricing | annual | plan`: any term may match; ranking sorts it
    out. Only [a-z0-9] terms of 3+ characters reach `to_tsquery`, so nothing in the text can be
    tsquery syntax."""
    terms = list(dict.fromkeys(_TERM.findall(query.lower())))[:MAX_TERMS]
    return " | ".join(terms) if terms else None


@router.post("/internal/retrieval/decisions/search", response_model=LexicalResult)
async def lexical_decisions(
    body: LexicalSearch,
    org_id: Annotated[uuid.UUID, Depends(rag_caller)],
) -> LexicalResult:
    terms = or_query(body.query)
    if terms is None:
        return LexicalResult(items=[])
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        rows = (
            (
                await session.execute(
                    text(
                        "WITH q AS (SELECT to_tsquery('english', :q) AS q) "
                        "SELECT d.id, d.meeting_id, d.content, d.rationale, m.title AS "
                        "meeting_title, COALESCE(d.status_changed_at, d.created_at) AS decided_at, "
                        "ts_rank_cd(to_tsvector('english', d.content || ' ' || "
                        "coalesce(d.rationale, '')), q.q) AS rank "
                        "FROM decisions d JOIN meetings m ON m.id = d.meeting_id, q "
                        "WHERE d.status = 'confirmed' AND d.deleted_at IS NULL "
                        "AND to_tsvector('english', d.content || ' ' || coalesce(d.rationale, '')) "
                        "@@ q.q AND EXISTS (SELECT 1 FROM meeting_participants p "
                        "WHERE p.meeting_id = d.meeting_id AND p.user_id = :u) "
                        "ORDER BY rank DESC, d.created_at DESC, d.id LIMIT :n"
                    ),
                    {"q": terms, "u": str(body.user_id), "n": body.limit},
                )
            )
            .mappings()
            .all()
        )
    return LexicalResult(
        items=[LexicalDecision(**{**dict(r), "rank": float(r["rank"])}) for r in rows]
    )


# --- Slack -------------------------------------------------------------------------------------


class SlackCheck(BaseModel):
    model_config = ConfigDict(extra="forbid")

    author_email: str = Field(min_length=3, max_length=320)
    text: str = Field(min_length=1, max_length=4000)
    source: Literal["slack"] = "slack"
    place_label: str | None = Field(default=None, max_length=120)
    link: str | None = Field(default=None, max_length=2000)
    external_ref: str | None = Field(default=None, max_length=200)
    topic: str | None = Field(default=None, max_length=200)


class SlackAct(BaseModel):
    model_config = ConfigDict(extra="forbid")

    acting_email: str = Field(min_length=3, max_length=320)
    reason: str | None = Field(default=None, max_length=1000)


def _unknown_author() -> ProblemDetail:
    return ProblemDetail(
        404, ErrorCode.NOT_FOUND, "No such person here", extra={"reason": "unknown_author"}
    )


def _link(value: str | None) -> str | None:
    return value if value and re.match(r"^https?://", value) else None


@router.post("/internal/decisions/check", response_model=dc.DecisionCheckResult)
async def slack_check(
    body: SlackCheck,
    org_id: Annotated[uuid.UUID, Depends(integration_caller)],
) -> dc.DecisionCheckResult:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session, session.begin():
        await set_rls(session, org_id)
        author = await dc.user_by_email(session, body.author_email)
    if author is None:
        raise _unknown_author()
    context = dc.CheckContext(
        place_label=body.place_label, channel=body.place_label, topic=body.topic
    )
    # The model call runs outside any transaction: no row is locked while it thinks.
    answer = await dc.call_checker(org_id, author, body.text, "slack", context)
    async with sessionmaker() as session, session.begin():
        await set_rls(session, org_id)
        conflicts = await dc.enrich(session, author, list(answer.get("conflicts") or []))
        recorded = await dc.record_challenges(
            session,
            org_id,
            author_id=author,
            source="slack",
            place_label=body.place_label,
            link=_link(body.link),
            external_ref=body.external_ref,
            conflicts=conflicts,
        )
        result = dc.result_of(answer, recorded)
        await audit(
            session,
            org_id=org_id,
            actor_id=author,
            actor_type="user",
            action=ACTION_SLACK_CHECKED,
            target_type="decision_check",
            metadata={
                "source": "slack",
                "status": result.status,
                "reason": result.reason,
                "conflicts": [str(c.decision_id) for c in recorded],
                "external_ref": body.external_ref,
            },
        )
    return result


async def _actor(session: AsyncSession, email: str) -> uuid.UUID:
    actor = await dc.user_by_email(session, email)
    if actor is None:
        raise _unknown_author()
    return actor


@router.post(
    "/internal/decision-challenges/{challenge_id}/dismiss",
    response_model=routes.DecisionChallenge,
)
async def slack_dismiss(
    challenge_id: uuid.UUID,
    body: SlackAct,
    org_id: Annotated[uuid.UUID, Depends(integration_caller)],
) -> routes.DecisionChallenge:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        actor = await _actor(session, body.acting_email)
        current = await routes.load_challenge(session, challenge_id, actor)
        if current.author is None or current.author.id != actor:
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        return await routes.dismiss_challenge(
            session, org_id, challenge_id, actor, ip=None, actor_type="user"
        )


@router.post(
    "/internal/decision-challenges/{challenge_id}/reopen",
    response_model=routes.ReopenRequest,
)
async def slack_reopen(
    challenge_id: uuid.UUID,
    body: SlackAct,
    org_id: Annotated[uuid.UUID, Depends(integration_caller)],
) -> routes.ReopenRequest:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        actor = await _actor(session, body.acting_email)
        current = await routes.load_challenge(session, challenge_id, actor)
        if current.author is None or current.author.id != actor:
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        request, _new = await routes.create_reopen(
            session,
            org_id,
            current.decision_id,
            actor,
            routes.ReopenCreate(
                reason=body.reason,
                source="slack",
                challenge_id=challenge_id,
            ),
            ip=None,
            actor_type="user",
        )
        return request
