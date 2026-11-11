"""Decisions that know why: the reasons behind a decision, watched (Beyond, Layer 1).

- `GET    /api/v1/decisions/{id}/why`: the decision's assumptions with their open break and
  history, whether the agent has read it yet, whether it was reopened, which sources are
  watched. Any participant of the decision's meeting.
- `POST   /api/v1/decisions/{id}/assumptions`, `PATCH /api/v1/assumptions/{id}`,
  `DELETE /api/v1/assumptions/{id}`: the decision's owner (`confirmed_by`) adds, edits or stops
  watching a reason. Everyone else in the meeting: 403; anyone else, any tenant: 404.
- `POST /api/v1/assumptions/{id}/reconfirm` `{note}`: the reason still holds; the open break is
  closed as `reconfirmed` and watching resumes.
- `POST /api/v1/assumptions/{id}/reopen-decision` `{note?}`: the decision is reopened
  (`decision_why_state.reopened_*`) and the owners of its meeting's open tasks are asked keep,
  re-scope or close through the existing decision reversal path (`kind = 'reopened'`).
- `GET  /api/v1/assumptions/{id}/revisit-slot` and `POST .../book-revisit` `{start?}`: the
  agent proposes a free 15-minute slot for the decision's people; booking creates the meeting
  through the meetings route itself.

Every write is audited; nothing here emits `decisions` or `actions`.
"""

from __future__ import annotations

import uuid
from datetime import date as Date
from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Response, status
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import (
    ACTION_ASSUMPTION_ADDED,
    ACTION_ASSUMPTION_EDITED,
    ACTION_ASSUMPTION_RECONFIRMED,
    ACTION_ASSUMPTION_REMOVED,
    ACTION_DECISION_REOPENED,
    ACTION_REVISIT_BOOKED,
    audit,
)
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api import decision_why as why
from services.core_api.decision_why import Person
from services.core_api.routes.decision_reversals import record_reopen_reversals
from services.core_api.visibility import is_visible

router = APIRouter(prefix="/api/v1", tags=["decision-why"])

AssumptionKind = Literal["price", "date", "metric", "dependency", "person", "other"]
Comparator = Literal["lt", "lte", "gt", "gte", "eq"]
SourceState = Literal["connected", "not_connected", "unknown"]


class Watch(BaseModel):
    date: Date | None = None
    value: float | None = None
    comparator: Comparator | None = None
    unit: str | None = None
    jira_key: str | None = None
    deal_id: str | None = None


class WatchInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    date: Date | None = None
    value: float | None = None
    comparator: Comparator | None = None
    unit: Annotated[str, Field(min_length=1, max_length=20)] | None = None
    jira_key: str | None = None
    deal_id: str | None = None

    @field_validator("jira_key")
    @classmethod
    def _jira(cls, v: str | None) -> str | None:
        if v is None or not v.strip():
            return None
        key = v.strip().upper()
        if not why.JIRA_KEY.match(key):
            raise ValueError("not a Jira issue key (like SSO-142)")
        return key

    @field_validator("deal_id")
    @classmethod
    def _deal(cls, v: str | None) -> str | None:
        if v is None or not v.strip():
            return None
        if not why.DEAL_ID.match(v.strip()):
            raise ValueError("a HubSpot deal id is a number")
        return v.strip()


class SourceMeeting(BaseModel):
    id: uuid.UUID
    title: str | None = None
    date: datetime


class Evidence(BaseModel):
    id: uuid.UUID
    source: Literal["meeting", "decision", "jira", "linear", "hubspot"]
    quote: str | None = None
    redacted: bool
    speaker: Person | None = None
    source_meeting: SourceMeeting | None = None
    source_decision_id: uuid.UUID | None = None
    source_ref: str | None = None
    source_url: str | None = None
    rationale: str
    confidence: float
    status: Literal["open", "reconfirmed", "reopened", "revisit_booked"]
    created_at: datetime
    resolved_at: datetime | None = None
    resolved_by: Person | None = None
    resolution_note: str | None = None
    revisit_meeting_id: uuid.UUID | None = None


class Assumption(BaseModel):
    id: uuid.UUID
    decision_id: uuid.UUID
    kind: AssumptionKind
    statement: str
    quote: str | None = None
    stated_by: Person | None = None
    source: Literal["agent", "owner"]
    status: Literal["watching", "broken"]
    watch: Watch
    reconfirmed_at: datetime | None = None
    reconfirm_note: str | None = None
    last_checked_at: datetime | None = None
    created_at: datetime
    open_break: Evidence | None = None
    history: list[Evidence] = Field(default_factory=list)


class Reopened(BaseModel):
    at: datetime
    by: Person | None = None
    note: str | None = None


class WatchSources(BaseModel):
    transcripts: Literal["connected"] = "connected"
    jira: SourceState = "unknown"
    linear: SourceState = "unknown"
    hubspot: SourceState = "unknown"
    checked_at: datetime | None = None


class DecisionWhy(BaseModel):
    decision_id: uuid.UUID
    meeting_id: uuid.UUID
    content: str
    owner: Person | None = None
    can_edit: bool
    state: Literal["pending", "ready"]
    reopened: Reopened | None = None
    assumptions: list[Assumption]
    sources: WatchSources


def _trim(v: str | None) -> str | None:
    if v is None:
        return None
    v = " ".join(v.split())
    return v or None


class AssumptionCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: AssumptionKind
    statement: Annotated[str, Field(min_length=1, max_length=300)]
    quote: Annotated[str, Field(max_length=600)] | None = None
    stated_by_user_id: uuid.UUID | None = None
    watch: WatchInput | None = None

    @field_validator("statement")
    @classmethod
    def _statement(cls, v: str) -> str:
        out = _trim(v)
        if not out:
            raise ValueError("say what must stay true")
        return out

    @field_validator("quote")
    @classmethod
    def _quote(cls, v: str | None) -> str | None:
        return _trim(v)


class AssumptionUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: AssumptionKind | None = None
    statement: Annotated[str, Field(min_length=1, max_length=300)] | None = None
    quote: Annotated[str, Field(max_length=600)] | None = None
    stated_by_user_id: uuid.UUID | None = None
    watch: WatchInput | None = None

    @field_validator("statement")
    @classmethod
    def _statement(cls, v: str | None) -> str | None:
        if v is None:
            return None
        out = _trim(v)
        if not out:
            raise ValueError("say what must stay true")
        return out

    @field_validator("quote")
    @classmethod
    def _quote(cls, v: str | None) -> str | None:
        return _trim(v)


class Note(BaseModel):
    model_config = ConfigDict(extra="forbid")

    note: Annotated[str, Field(min_length=1, max_length=500)]

    @field_validator("note")
    @classmethod
    def _note(cls, v: str) -> str:
        out = _trim(v)
        if not out:
            raise ValueError("add a short note")
        return out


class OptionalNote(BaseModel):
    model_config = ConfigDict(extra="forbid")

    note: Annotated[str, Field(max_length=500)] | None = None

    @field_validator("note")
    @classmethod
    def _note(cls, v: str | None) -> str | None:
        return _trim(v)


class SlotProposal(BaseModel):
    start: datetime
    end: datetime
    people: list[Person]
    title: str


class BookRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start: datetime | None = None


class Booked(BaseModel):
    meeting_id: uuid.UUID
    start: datetime
    end: datetime
    title: str


# --- reads ----------------------------------------------------------------------------------

_ASSUMPTION_COLS = (
    "id, decision_id, kind, statement, quote, stated_by, source, status, watch_date, "
    "watch_value, watch_comparator, watch_unit, watch_jira_key, watch_deal_id, reconfirmed_at, "
    "reconfirm_note, last_checked_at, created_at"
)


async def _evidence(
    session: AsyncSession, assumption_ids: list[uuid.UUID], viewer: uuid.UUID | None
) -> dict[uuid.UUID, list[Evidence]]:
    if not assumption_ids:
        return {}
    rows = (
        (
            await session.execute(
                text(
                    "SELECT e.*, m.title AS m_title, COALESCE(m.scheduled_start, m.created_at) "
                    "AS m_date FROM assumption_evidence e LEFT JOIN meetings m ON m.id = "
                    "e.source_meeting_id AND m.deleted_at IS NULL "
                    "WHERE e.assumption_id = ANY(CAST(:ids AS uuid[])) "
                    "ORDER BY e.created_at DESC, e.id"
                ),
                {"ids": [str(a) for a in assumption_ids]},
            )
        )
        .mappings()
        .all()
    )
    names = await why.people(
        session, [r["speaker_id"] for r in rows] + [r["resolved_by"] for r in rows]
    )
    seen: dict[uuid.UUID, bool] = {}
    out: dict[uuid.UUID, list[Evidence]] = {}
    for r in rows:
        meeting_id = r["source_meeting_id"]
        redacted = False
        if r["source"] in ("meeting", "decision"):
            if meeting_id not in seen:
                seen[meeting_id] = await why.in_meeting(session, meeting_id, viewer)
            redacted = not seen[meeting_id]
        out.setdefault(r["assumption_id"], []).append(
            Evidence(
                id=r["id"],
                source=r["source"],
                quote=None if redacted else r["quote"],
                redacted=redacted,
                speaker=None if redacted else names.get(r["speaker_id"]),
                source_meeting=(
                    SourceMeeting(
                        id=meeting_id, title=None if redacted else r["m_title"], date=r["m_date"]
                    )
                    if meeting_id is not None and r["m_date"] is not None
                    else None
                ),
                source_decision_id=None if redacted else r["source_decision_id"],
                source_ref=r["source_ref"],
                source_url=r["source_url"],
                rationale=(
                    "Something said in a meeting you were not in contradicts this reason."
                    if redacted
                    else r["rationale"]
                ),
                confidence=float(r["confidence"]),
                status=r["status"],
                created_at=r["created_at"],
                resolved_at=r["resolved_at"],
                resolved_by=names.get(r["resolved_by"]),
                resolution_note=r["resolution_note"],
                revisit_meeting_id=r["revisit_meeting_id"],
            )
        )
    return out


def _assumption(
    r: Any, names: dict[uuid.UUID, Person], evidence: dict[uuid.UUID, list[Evidence]]
) -> Assumption:
    items = evidence.get(r["id"], [])
    open_break = next((e for e in items if e.status == "open"), None)
    return Assumption(
        id=r["id"],
        decision_id=r["decision_id"],
        kind=r["kind"],
        statement=r["statement"],
        quote=r["quote"],
        stated_by=names.get(r["stated_by"]),
        source=r["source"],
        status=r["status"],
        watch=Watch(
            date=r["watch_date"],
            value=float(r["watch_value"]) if r["watch_value"] is not None else None,
            comparator=r["watch_comparator"],
            unit=r["watch_unit"],
            jira_key=r["watch_jira_key"],
            deal_id=r["watch_deal_id"],
        ),
        reconfirmed_at=r["reconfirmed_at"],
        reconfirm_note=r["reconfirm_note"],
        last_checked_at=r["last_checked_at"],
        created_at=r["created_at"],
        open_break=open_break,
        history=[e for e in items if e.status != "open"],
    )


async def _assumptions(
    session: AsyncSession, decision_id: uuid.UUID, viewer: uuid.UUID | None
) -> list[Assumption]:
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_ASSUMPTION_COLS} FROM decision_assumptions "  # noqa: S608
                    "WHERE decision_id = :d AND status <> 'removed' "
                    "ORDER BY (status = 'broken') DESC, created_at, id"
                ),
                {"d": str(decision_id)},
            )
        )
        .mappings()
        .all()
    )
    names = await why.people(session, [r["stated_by"] for r in rows])
    evidence = await _evidence(session, [r["id"] for r in rows], viewer)
    return [_assumption(r, names, evidence) for r in rows]


async def sources(session: AsyncSession) -> WatchSources:
    row = (
        (
            await session.execute(
                text("SELECT jira, linear, hubspot, checked_at FROM watch_source_status")
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        return WatchSources()
    return WatchSources(
        jira=row["jira"], linear=row["linear"], hubspot=row["hubspot"], checked_at=row["checked_at"]
    )


async def build_why(
    session: AsyncSession, decision: dict[str, Any], viewer: uuid.UUID | None
) -> DecisionWhy:
    state = (
        (
            await session.execute(
                text(
                    "SELECT read_at, reopened_at, reopened_by, reopen_note FROM decision_why_state "
                    "WHERE decision_id = :d"
                ),
                {"d": str(decision["id"])},
            )
        )
        .mappings()
        .first()
    )
    assumptions = await _assumptions(session, decision["id"], viewer)
    owner = await why.person(session, decision["confirmed_by"])
    reopened = None
    if state is not None and state["reopened_at"] is not None:
        reopened = Reopened(
            at=state["reopened_at"],
            by=await why.person(session, state["reopened_by"]),
            note=state["reopen_note"],
        )
    read = (state is not None and state["read_at"] is not None) or any(
        a.source == "agent" for a in assumptions
    )
    return DecisionWhy(
        decision_id=decision["id"],
        meeting_id=decision["meeting_id"],
        content=decision["content"],
        owner=owner,
        can_edit=viewer is not None and viewer == decision["confirmed_by"],
        state="ready" if read else "pending",
        reopened=reopened,
        assumptions=assumptions,
        sources=await sources(session),
    )


@router.get("/decisions/{decision_id}/why", response_model=DecisionWhy)
async def get_why(
    decision_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> DecisionWhy:
    decision = await why.visible_decision(session, decision_id, ctx.user_id)
    return await build_why(session, decision, ctx.user_id)


# --- owner writes ---------------------------------------------------------------------------


def _not_owner() -> ProblemDetail:
    return ProblemDetail(
        403, ErrorCode.FORBIDDEN_ROLE, "Only the person who confirmed this decision can do that"
    )


async def _owned_decision(
    session: AsyncSession, ctx: RequestContext, decision_id: uuid.UUID
) -> dict[str, Any]:
    decision = await why.visible_decision(session, decision_id, ctx.user_id)
    if ctx.user_id is None or decision["confirmed_by"] != ctx.user_id:
        raise _not_owner()
    return decision


async def _owned_assumption(
    session: AsyncSession, ctx: RequestContext, assumption_id: uuid.UUID
) -> tuple[dict[str, Any], dict[str, Any]]:
    row = (
        (
            await session.execute(
                text(
                    f"SELECT {_ASSUMPTION_COLS} FROM decision_assumptions "  # noqa: S608
                    "WHERE id = :a AND status <> 'removed' FOR UPDATE"
                ),
                {"a": str(assumption_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()
    decision = await _owned_decision(session, ctx, row["decision_id"])
    return dict(row), decision


async def _one(
    session: AsyncSession, assumption_id: uuid.UUID, viewer: uuid.UUID | None
) -> Assumption:
    row = (
        (
            await session.execute(
                text(
                    f"SELECT {_ASSUMPTION_COLS} FROM decision_assumptions WHERE id = :a"
                ),  # noqa: S608
                {"a": str(assumption_id)},
            )
        )
        .mappings()
        .one()
    )
    names = await why.people(session, [row["stated_by"]])
    return _assumption(row, names, await _evidence(session, [row["id"]], viewer))


async def _stated_by(session: AsyncSession, user_id: uuid.UUID | None) -> uuid.UUID | None:
    if user_id is None:
        return None
    if not await is_visible(session, "users", user_id):
        raise ProblemDetail(404, ErrorCode.NOT_FOUND, "No such user in this organization")
    return user_id


def _watch_params(w: WatchInput | None) -> dict[str, Any]:
    w = w or WatchInput()
    return {
        "watch_date": w.date,
        "watch_value": w.value,
        "watch_comparator": w.comparator,
        "watch_unit": w.unit,
        "watch_jira_key": w.jira_key,
        "watch_deal_id": w.deal_id,
    }


@router.post(
    "/decisions/{decision_id}/assumptions",
    response_model=Assumption,
    status_code=status.HTTP_201_CREATED,
)
async def add_assumption(
    decision_id: uuid.UUID,
    body: AssumptionCreate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Assumption:
    await _owned_decision(session, ctx, decision_id)
    live = (
        await session.execute(
            text(
                "SELECT count(*) FROM decision_assumptions WHERE decision_id = :d "
                "AND status <> 'removed'"
            ),
            {"d": str(decision_id)},
        )
    ).scalar_one()
    if live >= why.MAX_LIVE_ASSUMPTIONS:
        raise ProblemDetail(
            422,
            ErrorCode.VALIDATION_FAILED,
            f"A decision can have at most {why.MAX_LIVE_ASSUMPTIONS} reasons",
        )
    row = (
        await session.execute(
            text(
                "INSERT INTO decision_assumptions (org_id, decision_id, kind, statement, quote, "
                "stated_by, source, created_by, watch_date, watch_value, watch_comparator, "
                "watch_unit, watch_jira_key, watch_deal_id) VALUES (:o, :d, :k, :s, :q, :by, "
                "'owner', :u, :watch_date, :watch_value, :watch_comparator, :watch_unit, "
                ":watch_jira_key, :watch_deal_id) RETURNING id"
            ),
            {
                "o": str(ctx.org_id),
                "d": str(decision_id),
                "k": body.kind,
                "s": body.statement,
                "q": body.quote,
                "by": await _stated_by(session, body.stated_by_user_id),
                "u": str(ctx.user_id),
                **_watch_params(body.watch),
            },
        )
    ).one()
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_ASSUMPTION_ADDED,
        target_type="decision",
        target_id=decision_id,
        metadata={"assumption_id": str(row[0]), "kind": body.kind},
        ip=ctx.ip,
    )
    return await _one(session, row[0], ctx.user_id)


@router.patch("/assumptions/{assumption_id}", response_model=Assumption)
async def edit_assumption(
    assumption_id: uuid.UUID,
    body: AssumptionUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Assumption:
    row, _decision = await _owned_assumption(session, ctx, assumption_id)
    sets: dict[str, Any] = {}
    given = body.model_fields_set
    if "kind" in given and body.kind is not None:
        sets["kind"] = body.kind
    if "statement" in given and body.statement is not None:
        sets["statement"] = body.statement
    if "quote" in given:
        sets["quote"] = body.quote
    if "stated_by_user_id" in given:
        sets["stated_by"] = await _stated_by(session, body.stated_by_user_id)
    if "watch" in given:
        sets.update(_watch_params(body.watch))
    if sets:
        assign = ", ".join(f"{k} = :{k}" for k in sets)  # keys: a closed set above
        await session.execute(
            text(
                f"UPDATE decision_assumptions SET {assign}, updated_at = now() "  # noqa: S608
                "WHERE id = :id"
            ),
            {**sets, "id": str(assumption_id)},
        )
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=ctx.user_id,
            action=ACTION_ASSUMPTION_EDITED,
            target_type="decision",
            target_id=row["decision_id"],
            metadata={"assumption_id": str(assumption_id), "fields": sorted(sets)},
            ip=ctx.ip,
        )
    return await _one(session, assumption_id, ctx.user_id)


@router.delete(
    "/assumptions/{assumption_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
async def remove_assumption(
    assumption_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    row, _decision = await _owned_assumption(session, ctx, assumption_id)
    await session.execute(
        text(
            "UPDATE decision_assumptions SET status = 'removed', updated_at = now() WHERE id = :a"
        ),
        {"a": str(assumption_id)},
    )
    # an open break goes with it: nothing is left to answer
    await session.execute(
        text(
            "UPDATE assumption_evidence SET status = 'reconfirmed', resolved_at = now(), "
            "resolved_by = :u, resolution_note = 'Stopped watching this reason.' "
            "WHERE assumption_id = :a AND status = 'open'"
        ),
        {"a": str(assumption_id), "u": str(ctx.user_id)},
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_ASSUMPTION_REMOVED,
        target_type="decision",
        target_id=row["decision_id"],
        metadata={"assumption_id": str(assumption_id), "statement": row["statement"][:140]},
        ip=ctx.ip,
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


async def _resolve_open(
    session: AsyncSession,
    assumption_id: uuid.UUID,
    user_id: uuid.UUID | None,
    to: str,
    note: str | None,
    meeting_id: uuid.UUID | None = None,
) -> bool:
    res = await session.execute(
        text(
            "UPDATE assumption_evidence SET status = :s, resolved_at = now(), resolved_by = :u, "
            "resolution_note = :n, revisit_meeting_id = COALESCE(:m, revisit_meeting_id) "
            "WHERE assumption_id = :a AND status = 'open' RETURNING id"
        ),
        {
            "s": to,
            "u": str(user_id) if user_id else None,
            "n": note,
            "m": str(meeting_id) if meeting_id else None,
            "a": str(assumption_id),
        },
    )
    return res.first() is not None


@router.post("/assumptions/{assumption_id}/reconfirm", response_model=Assumption)
async def reconfirm(
    assumption_id: uuid.UUID,
    body: Note,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Assumption:
    row, _decision = await _owned_assumption(session, ctx, assumption_id)
    await _resolve_open(session, assumption_id, ctx.user_id, "reconfirmed", body.note)
    await session.execute(
        text(
            "UPDATE decision_assumptions SET status = 'watching', reconfirmed_at = now(), "
            "reconfirm_note = :n, updated_at = now() WHERE id = :a"
        ),
        {"a": str(assumption_id), "n": body.note},
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_ASSUMPTION_RECONFIRMED,
        target_type="decision",
        target_id=row["decision_id"],
        metadata={"assumption_id": str(assumption_id), "was": row["status"]},
        ip=ctx.ip,
    )
    return await _one(session, assumption_id, ctx.user_id)


@router.post("/assumptions/{assumption_id}/reopen-decision", response_model=DecisionWhy)
async def reopen_decision(
    assumption_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    body: OptionalNote | None = None,
) -> DecisionWhy:
    """Reopen the decision this reason belongs to. Idempotent: a reopened decision stays
    reopened and its task owners are asked once (`decision_reversals` is unique per task)."""
    row, decision = await _owned_assumption(session, ctx, assumption_id)
    if decision["status"] != "confirmed":
        raise ProblemDetail(
            409, ErrorCode.VERSION_CONFLICT, "A replaced decision cannot be reopened"
        )
    note = body.note if body is not None else None
    first = (
        await session.execute(
            text(
                "INSERT INTO decision_why_state (decision_id, org_id, reopened_at, reopened_by, "
                "reopen_note) VALUES (:d, :o, now(), :u, :n) ON CONFLICT (decision_id) DO "
                "UPDATE SET reopened_at = now(), reopened_by = :u, reopen_note = :n, "
                "updated_at = now() WHERE decision_why_state.reopened_at IS NULL "
                "RETURNING decision_id"
            ),
            {"d": str(decision["id"]), "o": str(ctx.org_id), "u": str(ctx.user_id), "n": note},
        )
    ).first()
    await _resolve_open(session, assumption_id, ctx.user_id, "reopened", note)
    if row["status"] == "broken":
        await session.execute(
            text(
                "UPDATE decision_assumptions SET status = 'watching', updated_at = now() "
                "WHERE id = :a"
            ),
            {"a": str(assumption_id)},
        )
    if first is not None:
        asked = await record_reopen_reversals(
            session, org_id=ctx.org_id, decision_id=decision["id"], reason=row["statement"]
        )
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=ctx.user_id,
            action=ACTION_DECISION_REOPENED,
            target_type="decision",
            target_id=decision["id"],
            metadata={"assumption_id": str(assumption_id), "tasks_asked": asked},
            ip=ctx.ip,
        )
    return await build_why(session, decision, ctx.user_id)


async def _slot_people(
    session: AsyncSession, decision: dict[str, Any], assumption: dict[str, Any]
) -> list[uuid.UUID]:
    return await why.decision_people(session, decision, [assumption.get("stated_by")])


def _revisit_title(decision: dict[str, Any]) -> str:
    return f"Revisit: {why.clip(decision['content'], 120)}"


@router.get("/assumptions/{assumption_id}/revisit-slot", response_model=SlotProposal)
async def revisit_slot(
    assumption_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> SlotProposal:
    row, decision = await _owned_assumption(session, ctx, assumption_id)
    ids = await _slot_people(session, decision, row)
    slot = await why.propose_slot(session, ids)
    names = await why.people(session, list(ids))
    return SlotProposal(
        start=slot.start,
        end=slot.end,
        people=[names[i] for i in ids if i in names],
        title=_revisit_title(decision),
    )


@router.post(
    "/assumptions/{assumption_id}/book-revisit",
    response_model=Booked,
    status_code=status.HTTP_201_CREATED,
)
async def book_revisit(
    assumption_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    body: BookRequest | None = None,
) -> Booked:
    row, decision = await _owned_assumption(session, ctx, assumption_id)
    ids = await _slot_people(session, decision, row)
    start = why.parse_start(body.start if body else None)
    if start is None:
        start = (await why.propose_slot(session, ids)).start
    elif start < datetime.now(start.tzinfo):
        raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "Pick a time in the future")
    booked = await why.book_meeting(
        session,
        ctx,
        title=_revisit_title(decision),
        start=start,
        minutes=why.REVISIT_MINUTES,
        attendees=ids,
        agenda_title=f"Does this still hold: {why.clip(row['statement'], 150)}",
    )
    await _resolve_open(
        session, assumption_id, ctx.user_id, "revisit_booked", None, booked["meeting_id"]
    )
    await session.execute(
        text(
            "UPDATE decision_assumptions SET status = 'watching', updated_at = now() "
            "WHERE id = :a"
        ),
        {"a": str(assumption_id)},
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_REVISIT_BOOKED,
        target_type="decision",
        target_id=decision["id"],
        metadata={"assumption_id": str(assumption_id), "meeting_id": str(booked["meeting_id"])},
        ip=ctx.ip,
    )
    return Booked(**booked)
