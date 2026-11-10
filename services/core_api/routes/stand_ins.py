"""Ask the agent of someone who sent it instead of coming (polish program, Phase 1, "Agents that
stand in properly"; ADR-0039 B5 send-your-agent, ADR-0013 permission-gated A2A context).

`POST /api/v1/meetings/{meeting_id}/stand-ins/{user_id}/ask` with `{question}`.

The same gate the ContextGapAgent's peer hop uses, deny by default:

1. The caller is a participant of the meeting (403 `NOT_A_PARTICIPANT`; another tenant's
   meeting is 404), and the target is a participant of the same meeting who sent their agent
   (`attendance = 'agent'`); anyone else, and asking yourself, is 404.
2. The PEP inputs are derived here, never taken from the request: `query_class` from the
   question (the same deterministic classifier as agent-runtime-service's `a2a/pep.py`),
   `target_scope` from team and department membership (same rules), `grantee_role` = the
   caller's RBAC tier.
3. The PDP (`identity-tenant-service` `a2a.pdp.authorize`, the grant store) answers allow or
   deny. No live allow row, or any deny row, is `denied`: nothing is read for the target.
4. On allow, the answer is drawn ONLY from the target's own records of the kind the class may
   return (the class is enforced, not only a label): `decision_lineage` -> confirmed decisions
   of meetings the target took part in; `action_status` -> actions assigned to the target;
   `meeting_context` -> the target's own written update for this meeting (transcript passages
   are search-service's read model, not core-api's, so none are read here). A confidential
   meeting is never a source unless the CALLER was in it too (ADR-0039 B2).
5. Every ask, allowed or denied, writes one `agent_collaborations` row (hop `peer`, requester
   the caller, responder `user:{target}`) and an audit row: the collaboration log shows it, and
   an answered ask is the caller's own context fill (`GET /meetings/{id}/context-fills`), so it
   re-hydrates with the rest of their sidebar. core-api is the one writer of that table.

No model call: a synchronous route never blocks on an LLM. The answer is the target's own
words and cited records, ranked by how many of the question's words they contain.
"""

from __future__ import annotations

import json
import re
import time
import uuid
from datetime import date, datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import ACTION_STAND_IN_ASKED, audit
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from services.core_api.routes.chat import (
    _fetch_meeting_exists,
    _not_a_participant,
    _require_participant,
)
from services.identity_tenant_service.a2a import pdp

router = APIRouter(prefix="/api/v1", tags=["stand-ins"])

MAX_QUESTION = 300
MAX_ITEMS = 3
QueryClass = Literal["meeting_context", "decision_lineage", "action_status"]
TargetScope = Literal["self", "same_team", "department", "org"]
Outcome = Literal["answered", "no_answer", "denied"]

# Kept identical to agent-runtime-service's `a2a/pep.py` `classify_query`: the same question
# must land in the same class on both paths, or a grant would mean different things.
_DECISION_WORDS = re.compile(
    r"\b(decid\w*|decision\w*|agreed|agree|agreement|approv\w*|chose|chosen|choice|"
    r"rationale|signed off|sign-off|ruled out|went with|settled on)\b",
    re.IGNORECASE,
)
_ACTION_WORDS = re.compile(
    r"\b(actions?|action items?|owners?|owns|owned|ownership|assign\w*|assignee|due|deadlines?|"
    r"tasks?|tickets?|todos?|to-dos?|follow[- ]ups?|status|blockers?|blocked|responsible|"
    r"deliverables?|eta)\b",
    re.IGNORECASE,
)
_WORD = re.compile(r"[a-z0-9][a-z0-9'-]{3,}", re.IGNORECASE)
_STOP = frozenset(
    {
        "what",
        "when",
        "where",
        "which",
        "with",
        "that",
        "this",
        "they",
        "their",
        "them",
        "have",
        "about",
        "your",
        "from",
        "does",
        "will",
        "would",
        "should",
        "could",
        "there",
        "were",
        "been",
        "into",
        "decided",
        "decision",
        "status",
        "task",
        "tasks",
        "action",
        "actions",
        "owner",
    }
)


def classify_query(question: str) -> QueryClass:
    if _DECISION_WORDS.search(question):
        return "decision_lineage"
    if _ACTION_WORDS.search(question):
        return "action_status"
    return "meeting_context"


def keywords(question: str, limit: int = 6) -> list[str]:
    words = [w.lower() for w in _WORD.findall(question)]
    return list(dict.fromkeys(w for w in words if w not in _STOP))[:limit]


class StandInAsk(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question: Annotated[str, Field(min_length=1, max_length=MAX_QUESTION)]

    @field_validator("question")
    @classmethod
    def _trimmed(cls, v: str) -> str:
        value = " ".join(v.split())
        if not value:
            raise ValueError("question must not be blank")
        return value


class StandInSource(BaseModel):
    """One record the answer cites, of the kind the question's class may return."""

    kind: Literal["decision", "action", "update"]
    id: uuid.UUID | None = None
    text: str
    meeting_id: uuid.UUID | None = None
    meeting_title: str | None = None
    status: str | None = None
    due_date: date | None = None
    occurred_at: datetime | None = None


class StandInAnswer(BaseModel):
    question: str
    outcome: Outcome = Field(
        description="`answered`, `no_answer` (nothing of that kind to share), or `denied` "
        "(your workspace's agent permissions do not allow this question)."
    )
    query_class: QueryClass
    target_scope: TargetScope
    sources: list[StandInSource]
    fill_id: uuid.UUID = Field(description="The `agent_collaborations` row this ask wrote.")


async def _target(session: AsyncSession, meeting_id: uuid.UUID, user_id: uuid.UUID) -> Any:
    return (
        (
            await session.execute(
                text(
                    "SELECT user_id, attendance, proxy_update FROM meeting_participants "
                    "WHERE meeting_id = :m AND user_id = :u"
                ),
                {"m": str(meeting_id), "u": str(user_id)},
            )
        )
        .mappings()
        .first()
    )


async def resolve_scope(session: AsyncSession, asker: uuid.UUID, target: uuid.UUID) -> TargetScope:
    """The same rules as `a2a/pep.py` `resolve_scope`, from core-api's own team tables."""
    if asker == target:
        return "self"
    row = (
        await session.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM team_members a JOIN team_members b "
                "  ON a.team_id = b.team_id JOIN teams t ON t.id = a.team_id "
                "  WHERE a.user_id = :a AND b.user_id = :b AND t.deleted_at IS NULL), "
                "EXISTS (SELECT 1 FROM team_members a JOIN teams ta ON ta.id = a.team_id "
                "  JOIN teams tb ON tb.department_id = ta.department_id "
                "  JOIN team_members b ON b.team_id = tb.id "
                "  WHERE a.user_id = :a AND b.user_id = :b AND ta.department_id IS NOT NULL "
                "  AND ta.deleted_at IS NULL AND tb.deleted_at IS NULL)"
            ),
            {"a": str(asker), "b": str(target)},
        )
    ).one()
    if row[0]:
        return "same_team"
    if row[1]:
        return "department"
    return "org"


def _score(text_value: str, words: list[str]) -> int:
    lowered = text_value.lower()
    return sum(1 for w in words if w in lowered)


# A meeting is a source only if it is not confidential, or the ASKER was in it too (B2).
_SOURCE_OK = (
    "(NOT m.confidential OR EXISTS (SELECT 1 FROM meeting_participants pa "
    " WHERE pa.meeting_id = m.id AND pa.user_id = :asker))"
)


async def _decisions(
    session: AsyncSession, asker: uuid.UUID, target: uuid.UUID, words: list[str]
) -> list[StandInSource]:
    rows = (
        (
            await session.execute(
                text(
                    "SELECT d.id, d.content, d.status, d.created_at, m.id AS meeting_id, "
                    "m.title FROM decisions d JOIN meetings m ON m.id = d.meeting_id "
                    "WHERE d.status IN ('confirmed', 'superseded') AND d.deleted_at IS NULL "
                    "AND m.deleted_at IS NULL AND EXISTS (SELECT 1 FROM meeting_participants p "
                    "  WHERE p.meeting_id = m.id AND p.user_id = :target) "
                    f"AND {_SOURCE_OK} ORDER BY d.created_at DESC LIMIT 200"
                ),
                {"target": str(target), "asker": str(asker)},
            )
        )
        .mappings()
        .all()
    )
    ranked = sorted(
        ((_score(r["content"], words), r) for r in rows), key=lambda x: x[0], reverse=True
    )
    return [
        StandInSource(
            kind="decision",
            id=r["id"],
            text=r["content"],
            meeting_id=r["meeting_id"],
            meeting_title=r["title"],
            status=r["status"],
            occurred_at=r["created_at"],
        )
        for score, r in ranked[:MAX_ITEMS]
        if score > 0
    ]


async def _actions(
    session: AsyncSession, asker: uuid.UUID, target: uuid.UUID, words: list[str]
) -> list[StandInSource]:
    rows = (
        (
            await session.execute(
                text(
                    "SELECT a.id, a.content, a.status, a.due_date, a.created_at, "
                    "m.id AS meeting_id, m.title FROM action_items a "
                    "LEFT JOIN meetings m ON m.id = a.meeting_id "
                    "WHERE a.assignee_id = :target AND a.status IN ('confirmed', 'done') "
                    "AND a.deleted_at IS NULL AND (a.meeting_id IS NULL OR "
                    f"(m.deleted_at IS NULL AND {_SOURCE_OK})) "
                    "ORDER BY (a.status = 'confirmed') DESC, a.created_at DESC LIMIT 200"
                ),
                {"target": str(target), "asker": str(asker)},
            )
        )
        .mappings()
        .all()
    )
    scored = [(_score(r["content"], words), r) for r in rows]
    hits = [x for x in scored if x[0] > 0]
    # "What is she working on?" names no task: her open tasks, newest first, answer it.
    chosen = (
        sorted(hits, key=lambda x: x[0], reverse=True)
        if hits
        else [x for x in scored if x[1]["status"] == "confirmed"]
    )
    return [
        StandInSource(
            kind="action",
            id=r["id"],
            text=r["content"],
            meeting_id=r["meeting_id"],
            meeting_title=r["title"],
            status=r["status"],
            due_date=r["due_date"],
            occurred_at=r["created_at"],
        )
        for _, r in chosen[:MAX_ITEMS]
    ]


@router.post(
    "/meetings/{meeting_id}/stand-ins/{user_id}/ask",
    response_model=StandInAnswer,
    summary="Ask the agent of someone who sent it instead of coming",
)
async def ask_stand_in(
    meeting_id: uuid.UUID,
    user_id: uuid.UUID,
    body: StandInAsk,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> StandInAnswer:
    began = time.monotonic()
    await _fetch_meeting_exists(session, meeting_id)
    if ctx.user_id is None:
        raise _not_a_participant()
    await _require_participant(session, meeting_id, ctx.user_id)
    asker = ctx.user_id
    target = await _target(session, meeting_id, user_id)
    if target is None or target["attendance"] != "agent" or user_id == asker:
        raise not_found()

    query_class = classify_query(body.question)
    scope = await resolve_scope(session, asker, user_id)
    decision = await pdp.authorize(session, ctx.org_id, ctx.rbac_tier, query_class, scope)

    sources: list[StandInSource] = []
    outcome: Outcome = "denied"
    if decision == "allow":
        words = keywords(body.question)
        if query_class == "decision_lineage":
            sources = await _decisions(session, asker, user_id, words)
        elif query_class == "action_status":
            sources = await _actions(session, asker, user_id, words)
        elif target["proxy_update"]:
            sources = [StandInSource(kind="update", text=target["proxy_update"])]
        outcome = "answered" if sources else "no_answer"

    fill_id = uuid.uuid4()
    result = {"answered": "answered", "no_answer": "no_answer", "denied": "authz_denied"}[outcome]
    fill = "\n".join(s.text for s in sources)[:2000] if outcome == "answered" else None
    await session.execute(
        text(
            "INSERT INTO agent_collaborations (id, org_id, meeting_id, window_id, requester, "
            "responder, hop, query, query_class, target_scope, result, success, latency_ms, "
            "fill_content, confidence, surfaced, rationale, citations) VALUES (:id, :o, :m, "
            ":w, :req, :resp, 'peer', :q, :qc, :ts, :r, :ok, :lat, :fill, :conf, :surf, :rat, "
            "CAST(:cit AS jsonb))"
        ),
        {
            "id": str(fill_id),
            "o": str(ctx.org_id),
            "m": str(meeting_id),
            "w": f"ask:{fill_id}",
            "req": str(asker),
            "resp": f"user:{user_id}",
            "q": body.question,
            "qc": query_class,
            "ts": scope,
            "r": result,
            "ok": outcome == "answered",
            "lat": int((time.monotonic() - began) * 1000),
            "fill": fill,
            "conf": 1.0 if fill else None,
            "surf": fill is not None,
            "rat": "Asked directly in the meeting" if fill else None,
            "cit": json.dumps([s.model_dump(mode="json") for s in sources]),
        },
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=asker,
        action=ACTION_STAND_IN_ASKED,
        target_type="meeting",
        target_id=meeting_id,
        metadata={
            "target_user_id": str(user_id),
            "query_class": query_class,
            "target_scope": scope,
            "decision": decision,
            "outcome": outcome,
        },
        ip=ctx.ip,
    )
    return StandInAnswer(
        question=body.question,
        outcome=outcome,
        query_class=query_class,
        target_scope=scope,
        sources=sources,
        fill_id=fill_id,
    )
