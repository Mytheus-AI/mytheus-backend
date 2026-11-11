"""Decisions that follow you: the check, challenges and reopen requests (public API).

"Beyond the meeting" program, Layer 2. See `services/core_api/decision_check.py` for how a check
runs and who may see what. Routes (anything the caller may not see is a 404, in any tenant):

- `POST /api/v1/decisions/check`: check a piece of text against the decisions of meetings you
  took part in. Read-only (allowed for `read` API tokens and the MCP tool), records nothing but an
  audit row without the text, rate-limited per person (`DECISION_CHECK_RATE_PER_MINUTE`, default
  30; 429 with `Retry-After`). 503 `reason: check_not_connected` until rag-service is wired.
- `POST /api/v1/decisions/{id}/check-dismissals`: "Dismiss": this sentence does not conflict with
  this decision; it is not flagged to you again (only its hash is kept).
- `GET /api/v1/decisions/{id}/challenges`: where the decision was challenged, and its reopen
  requests ("Challenged in N places").
- `GET /api/v1/decision-challenges`: challenges to the decisions you own (the Inbox).
- `POST /api/v1/decision-challenges/{id}/dismiss`: its author or the decision's owner.
- `POST /api/v1/decisions/{id}/reopen-requests`: "Reopen it". The owner is notified.
- `GET /api/v1/decision-reopen-requests`, `GET .../{id}`, `POST .../{id}/accept` (owner: a
  revisit meeting or a follow-up task), `.../decline` (owner), `.../withdraw` (requester).

Agents propose, people decide: nothing here changes a decision. Accepting a reopen request only
schedules the conversation (a meeting) or a task to have it; the decision stays confirmed until
a new decision supersedes it in the usual way.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, date, datetime, time, timedelta
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query, Response
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import audit
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail, problem_response
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from services.core_api import decision_check as dc
from services.core_api.routes.candidates import enqueue_action_confirmed

router = APIRouter(prefix="/api/v1", tags=["decision-challenges"])

ACTION_CHECKED = "decision.checked"
ACTION_CHECK_DISMISSED = "decision_check.dismissed"
ACTION_CHALLENGE_DISMISSED = "decision_challenge.dismissed"
ACTION_REOPEN_REQUESTED = "decision_reopen.requested"
ACTION_REOPEN_ACCEPTED = "decision_reopen.accepted"
ACTION_REOPEN_DECLINED = "decision_reopen.declined"
ACTION_REOPEN_WITHDRAWN = "decision_reopen.withdrawn"
MAX_ITEMS = 200
DEFAULT_FOLLOW_UP_DAYS = 7
REVISIT_HOUR_UTC = 10

ChallengeStatus = Literal["open", "dismissed", "reopened"]
ReopenStatus = Literal["pending", "accepted", "declined", "withdrawn"]
Outcome = Literal["revisit", "follow_up"]


# --- models ------------------------------------------------------------------------------------


def _strip(value: str | None) -> str | None:
    if value is None:
        return None
    value = " ".join(value.split())
    return value or None


def _http_link(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    if not (value.startswith("https://") or value.startswith("http://")):
        raise ValueError("link must be an http(s) URL")
    return value


class CheckRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str = Field(min_length=1, max_length=4000, description="The text to check.")
    source: Literal["extension", "mcp", "app"] = Field(
        default="app", description="Where the text is being written."
    )
    context: dc.CheckContext | None = None

    @field_validator("text")
    @classmethod
    def _text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("text must not be blank")
        return value


class DismissCheck(BaseModel):
    model_config = ConfigDict(extra="forbid")

    quote: str = Field(min_length=1, max_length=500)
    source: dc.Source = "app"

    @field_validator("quote")
    @classmethod
    def _quote(cls, value: str) -> str:
        if not dc.normalise(value):
            raise ValueError("quote must contain words")
        return value


class CheckDismissed(BaseModel):
    decision_id: uuid.UUID
    dismissed: bool


class DecisionChallenge(BaseModel):
    id: uuid.UUID
    decision_id: uuid.UUID
    decision_content: str
    meeting_id: uuid.UUID
    source: dc.Source
    place_label: str | None = None
    link: str | None = None
    quote: str
    why: str
    relation: dc.Relation
    confidence: float
    status: ChallengeStatus
    author: dc.Person | None = None
    created_at: datetime
    reopen_request_id: uuid.UUID | None = None
    viewer_can_dismiss: bool = Field(
        default=False, description="True for the challenge's author and the decision's owner."
    )


class ReopenRequest(BaseModel):
    id: uuid.UUID
    decision_id: uuid.UUID
    decision_content: str
    meeting_id: uuid.UUID
    meeting_title: str | None = None
    requested_by: dc.Person
    owner: dc.Person | None = None
    reason: str | None = None
    quote: str | None = None
    source: dc.Source
    place_label: str | None = None
    link: str | None = None
    status: ReopenStatus
    outcome: Outcome | None = None
    follow_up_action_id: uuid.UUID | None = None
    revisit_meeting_id: uuid.UUID | None = None
    response_note: str | None = None
    created_at: datetime
    resolved_at: datetime | None = None
    viewer_role: Literal["owner", "requester", "viewer"]


class DecisionChallenges(BaseModel):
    decision_id: uuid.UUID
    owner: dc.Person | None = None
    viewer_is_owner: bool
    open_count: int = Field(description="Challenges still open.")
    places: int = Field(description="Distinct places among open and reopened challenges.")
    items: list[DecisionChallenge]
    reopen_requests: list[ReopenRequest]


class ChallengeList(BaseModel):
    items: list[DecisionChallenge]


class ReopenRequestList(BaseModel):
    items: list[ReopenRequest]


class ReopenCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str | None = Field(default=None, max_length=1000)
    quote: str | None = Field(default=None, max_length=500)
    source: dc.Source = "app"
    place_label: str | None = Field(default=None, max_length=120)
    link: str | None = Field(default=None, max_length=2000)
    challenge_id: uuid.UUID | None = None

    _trim = field_validator("reason", "quote", "place_label")(_strip)
    _url = field_validator("link")(_http_link)


class ReopenAccept(BaseModel):
    model_config = ConfigDict(extra="forbid")

    outcome: Outcome
    note: str | None = Field(default=None, max_length=1000)
    due_date: date | None = Field(default=None, description="follow_up: default a week out.")
    scheduled_start: datetime | None = Field(
        default=None, description="revisit: default the next weekday, 10:00 UTC."
    )
    duration_minutes: int = Field(default=30, ge=15, le=120)

    _trim = field_validator("note")(_strip)


class ReopenNote(BaseModel):
    model_config = ConfigDict(extra="forbid")

    note: str | None = Field(default=None, max_length=1000)

    _trim = field_validator("note")(_strip)


# --- reads -------------------------------------------------------------------------------------

_CHALLENGE_SELECT = f"""
SELECT c.id, c.decision_id, c.source, c.place_label, c.link, c.quote, c.why, c.relation,
       c.confidence, c.status, c.author_user_id, c.created_at, c.reopen_request_id,
       d.content AS decision_content, d.meeting_id, {dc.OWNER_SQL} AS owner_id
FROM decision_challenges c
JOIN decisions d ON d.id = c.decision_id
"""

_REOPEN_SELECT = f"""
SELECT r.id, r.decision_id, r.requested_by, r.reason, r.quote, r.source, r.place_label, r.link,
       r.status, r.outcome, r.follow_up_action_id, r.revisit_meeting_id, r.response_note,
       r.created_at, r.resolved_at, d.content AS decision_content, d.meeting_id,
       m.title AS meeting_title, COALESCE(r.owner_user_id, {dc.OWNER_SQL}) AS owner_id
FROM decision_reopen_requests r
JOIN decisions d ON d.id = r.decision_id
JOIN meetings m ON m.id = d.meeting_id
"""

_PARTICIPANT = (
    "EXISTS (SELECT 1 FROM meeting_participants p WHERE p.meeting_id = d.meeting_id "
    "AND p.user_id = :viewer)"
)


async def _people(session: AsyncSession, rows: list[Any], *keys: str) -> dict[uuid.UUID, dc.Person]:
    ids = {r[k] for r in rows for k in keys if r[k] is not None}
    return await dc.names(session, ids)


def _challenge(r: Any, people: dict[uuid.UUID, dc.Person], viewer: uuid.UUID) -> DecisionChallenge:
    author = r["author_user_id"]
    return DecisionChallenge(
        id=r["id"],
        decision_id=r["decision_id"],
        decision_content=r["decision_content"],
        meeting_id=r["meeting_id"],
        source=r["source"],
        place_label=r["place_label"],
        link=r["link"],
        quote=r["quote"],
        why=r["why"],
        relation=r["relation"],
        confidence=round(float(r["confidence"]), 3),
        status=r["status"],
        author=people.get(author) if author else None,
        created_at=r["created_at"],
        reopen_request_id=r["reopen_request_id"],
        viewer_can_dismiss=r["status"] == "open" and viewer in (author, r["owner_id"]),
    )


def _reopen(r: Any, people: dict[uuid.UUID, dc.Person], viewer: uuid.UUID) -> ReopenRequest:
    role: Literal["owner", "requester", "viewer"] = (
        "owner"
        if r["owner_id"] == viewer
        else "requester" if r["requested_by"] == viewer else "viewer"
    )
    return ReopenRequest(
        id=r["id"],
        decision_id=r["decision_id"],
        decision_content=r["decision_content"],
        meeting_id=r["meeting_id"],
        meeting_title=r["meeting_title"],
        requested_by=people.get(r["requested_by"]) or dc.Person(id=r["requested_by"]),
        owner=people.get(r["owner_id"]) if r["owner_id"] else None,
        # The reason is the requester's words to the owner: shown to the two of them only.
        reason=r["reason"] if role != "viewer" else None,
        quote=r["quote"],
        source=r["source"],
        place_label=r["place_label"],
        link=r["link"],
        status=r["status"],
        outcome=r["outcome"],
        follow_up_action_id=r["follow_up_action_id"],
        revisit_meeting_id=r["revisit_meeting_id"],
        response_note=r["response_note"] if role != "viewer" else None,
        created_at=r["created_at"],
        resolved_at=r["resolved_at"],
        viewer_role=role,
    )


async def load_challenge(
    session: AsyncSession, challenge_id: uuid.UUID, viewer: uuid.UUID, *, lock: bool = False
) -> DecisionChallenge:
    rows = (
        (
            await session.execute(
                text(
                    f"{_CHALLENGE_SELECT} WHERE c.id = :id AND d.deleted_at IS NULL "
                    f"AND {_PARTICIPANT}" + (" FOR UPDATE OF c" if lock else "")
                ),
                {"id": str(challenge_id), "viewer": str(viewer)},
            )
        )
        .mappings()
        .all()
    )
    if not rows:
        raise not_found()
    people = await _people(session, list(rows), "author_user_id")
    return _challenge(rows[0], people, viewer)


async def load_reopen(
    session: AsyncSession, request_id: uuid.UUID, viewer: uuid.UUID, *, lock: bool = False
) -> ReopenRequest:
    rows = (
        (
            await session.execute(
                text(
                    f"{_REOPEN_SELECT} WHERE r.id = :id AND d.deleted_at IS NULL "
                    f"AND ({_PARTICIPANT} OR r.requested_by = :viewer)"
                    + (" FOR UPDATE OF r" if lock else "")
                ),
                {"id": str(request_id), "viewer": str(viewer)},
            )
        )
        .mappings()
        .all()
    )
    if not rows:
        raise not_found()
    people = await _people(session, list(rows), "requested_by", "owner_id")
    return _reopen(rows[0], people, viewer)


def _me(ctx: RequestContext) -> uuid.UUID:
    if ctx.user_id is None:
        raise not_found()
    return ctx.user_id


# --- the check ---------------------------------------------------------------------------------


@router.post(
    "/decisions/check",
    response_model=dc.DecisionCheckResult,
    summary="Check text against your decisions",
    description=(
        "Find the confirmed decisions, from meetings you took part in, that a piece of text "
        "contradicts or reopens: the decision, the conflicting quote, the meeting, the date, the "
        "owner, a confidence and a short reason. Read-only (allowed for `read` API tokens); the "
        "text is not stored. Rate-limited per person (429 with `Retry-After`). 503 with "
        "`reason: check_not_connected` until checks are wired on the server."
    ),
    responses={
        429: {"description": "Too many checks this minute (`Retry-After`)."},
        503: {"description": "Checks are not connected or unavailable (`reason`)."},
    },
)
async def check_text(
    body: CheckRequest,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Any:
    me = _me(ctx)
    allowed, reset = await dc.take_rate(ctx.org_id, me)
    if not allowed:
        limit = dc.rate_limit()
        response = problem_response(
            429,
            ErrorCode.RATE_LIMITED,
            f"Checks are limited to {limit} a minute",
            extra={"retry_after": reset},
        )
        response.headers["Retry-After"] = str(reset)
        return response
    answer = await dc.call_checker(ctx.org_id, me, body.text, body.source, body.context)
    conflicts = await dc.enrich(session, me, list(answer.get("conflicts") or []))
    result = dc.result_of(answer, conflicts)
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=me,
        action=ACTION_CHECKED,
        target_type="decision_check",
        metadata={
            "source": body.source,
            "status": result.status,
            "reason": result.reason,
            "conflicts": [str(c.decision_id) for c in conflicts],
            # A fingerprint, never the text: drafts stay private to their writer.
            "text_sha256": hashlib.sha256(body.text.encode("utf-8")).hexdigest(),
            "via": ctx.auth_method,
        },
        ip=ctx.ip,
    )
    return result


async def remember_dismissal(
    session: AsyncSession,
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    decision_id: uuid.UUID,
    quote: str,
    source: str,
) -> None:
    await session.execute(
        text(
            "INSERT INTO decision_check_dismissals (org_id, user_id, decision_id, quote_hash, "
            "source) VALUES (:o, :u, :d, :h, :s) ON CONFLICT DO NOTHING"
        ),
        {
            "o": str(org_id),
            "u": str(user_id),
            "d": str(decision_id),
            "h": dc.quote_hash(quote),
            "s": source,
        },
    )


@router.post(
    "/decisions/{decision_id}/check-dismissals",
    response_model=CheckDismissed,
    summary="Dismiss a flagged sentence",
    description="This sentence does not conflict with this decision: it is not flagged to you "
    "again. Only a fingerprint of the sentence is kept.",
)
async def dismiss_check(
    decision_id: uuid.UUID,
    body: DismissCheck,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> CheckDismissed:
    me = _me(ctx)
    if await dc.visible_decision(session, decision_id, me) is None:
        raise not_found()
    await remember_dismissal(session, ctx.org_id, me, decision_id, body.quote, body.source)
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=me,
        action=ACTION_CHECK_DISMISSED,
        target_type="decision",
        target_id=decision_id,
        metadata={"source": body.source, "quote_sha256": dc.quote_hash(body.quote)},
        ip=ctx.ip,
    )
    return CheckDismissed(decision_id=decision_id, dismissed=True)


# --- challenges --------------------------------------------------------------------------------


@router.get(
    "/decisions/{decision_id}/challenges",
    response_model=DecisionChallenges,
    summary="Where a decision was challenged",
)
async def decision_challenges(
    decision_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> DecisionChallenges:
    me = _me(ctx)
    decision = await dc.visible_decision(session, decision_id, me)
    if decision is None:
        raise not_found()
    challenges = (
        (
            await session.execute(
                text(
                    f"{_CHALLENGE_SELECT} WHERE c.decision_id = :d "
                    "ORDER BY (c.status = 'dismissed'), c.created_at DESC, c.id LIMIT :n"
                ),
                {"d": str(decision_id), "n": MAX_ITEMS},
            )
        )
        .mappings()
        .all()
    )
    requests = (
        (
            await session.execute(
                text(
                    f"{_REOPEN_SELECT} WHERE r.decision_id = :d "
                    "ORDER BY (r.status <> 'pending'), r.created_at DESC, r.id LIMIT :n"
                ),
                {"d": str(decision_id), "n": MAX_ITEMS},
            )
        )
        .mappings()
        .all()
    )
    people = await dc.names(
        session,
        {r["author_user_id"] for r in challenges if r["author_user_id"]}
        | {r["requested_by"] for r in requests}
        | {r["owner_id"] for r in requests if r["owner_id"]}
        | ({decision["owner_id"]} if decision["owner_id"] else set()),
    )
    items = [_challenge(r, people, me) for r in challenges]
    live = [c for c in items if c.status != "dismissed"]
    owner = decision["owner_id"]
    return DecisionChallenges(
        decision_id=decision_id,
        owner=people.get(owner) if owner else None,
        viewer_is_owner=owner == me,
        open_count=sum(1 for c in items if c.status == "open"),
        places=len({(c.place_label or c.source).lower() for c in live}),
        items=items,
        reopen_requests=[_reopen(r, people, me) for r in requests],
    )


@router.get(
    "/decision-challenges",
    response_model=ChallengeList,
    summary="Challenges to the decisions you own",
)
async def my_challenges(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    status: Annotated[Literal["open", "all"], Query()] = "open",
) -> ChallengeList:
    if ctx.user_id is None:
        return ChallengeList(items=[])
    me = ctx.user_id
    where = [f"{dc.OWNER_SQL} = :viewer", "d.deleted_at IS NULL"]
    if status == "open":
        where.append("c.status = 'open'")
    rows = (
        (
            await session.execute(
                text(
                    f"{_CHALLENGE_SELECT} WHERE {' AND '.join(where)} "
                    "ORDER BY c.created_at DESC, c.id LIMIT :n"
                ),
                {"viewer": str(me), "n": MAX_ITEMS},
            )
        )
        .mappings()
        .all()
    )
    people = await _people(session, list(rows), "author_user_id")
    return ChallengeList(items=[_challenge(r, people, me) for r in rows])


async def dismiss_challenge(
    session: AsyncSession,
    org_id: uuid.UUID,
    challenge_id: uuid.UUID,
    actor: uuid.UUID,
    *,
    ip: str | None,
    actor_type: str = "user",
) -> DecisionChallenge:
    """The author or the owner dismisses a challenge; the author's sentence is remembered too."""
    current = await load_challenge(session, challenge_id, actor, lock=True)
    owner = (
        await session.execute(
            text(f"SELECT {dc.OWNER_SQL} FROM decisions d WHERE d.id = :d"),
            {"d": str(current.decision_id)},
        )
    ).scalar()
    author = current.author.id if current.author else None
    if actor not in (author, owner):
        raise not_found()
    if current.status == "dismissed":
        return current
    if current.status != "open":
        raise ProblemDetail(
            409, ErrorCode.VERSION_CONFLICT, f"This challenge was already {current.status}"
        )
    await session.execute(
        text(
            "UPDATE decision_challenges SET status = 'dismissed', dismissed_by = :u, "
            "dismissed_at = now() WHERE id = :c"
        ),
        {"u": str(actor), "c": str(challenge_id)},
    )
    if author is not None:
        await remember_dismissal(
            session, org_id, author, current.decision_id, current.quote, current.source
        )
    await audit(
        session,
        org_id=org_id,
        actor_id=actor,
        actor_type=actor_type,
        action=ACTION_CHALLENGE_DISMISSED,
        target_type="decision_challenge",
        target_id=challenge_id,
        metadata={
            "decision_id": str(current.decision_id),
            "as": "author" if actor == author else "owner",
        },
        ip=ip,
    )
    return await load_challenge(session, challenge_id, actor)


@router.post(
    "/decision-challenges/{challenge_id}/dismiss",
    response_model=DecisionChallenge,
    summary="Dismiss a challenge",
)
async def dismiss_challenge_route(
    challenge_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> DecisionChallenge:
    return await dismiss_challenge(session, ctx.org_id, challenge_id, _me(ctx), ip=ctx.ip)


# --- reopen requests ---------------------------------------------------------------------------


async def create_reopen(
    session: AsyncSession,
    org_id: uuid.UUID,
    decision_id: uuid.UUID,
    requester: uuid.UUID,
    body: ReopenCreate,
    *,
    ip: str | None,
    actor_type: str = "user",
) -> tuple[ReopenRequest, bool]:
    """(the request, whether it is new). One pending request per (decision, requester)."""
    decision = await dc.visible_decision(session, decision_id, requester, lock=True)
    if decision is None:
        raise not_found()
    if decision["status"] != "confirmed":
        raise ProblemDetail(
            409,
            ErrorCode.VERSION_CONFLICT,
            "This decision was already replaced by a later one",
            extra={"reason": "not_confirmed"},
        )
    existing = (
        await session.execute(
            text(
                "SELECT id FROM decision_reopen_requests WHERE decision_id = :d "
                "AND requested_by = :u AND status = 'pending'"
            ),
            {"d": str(decision_id), "u": str(requester)},
        )
    ).scalar()
    if existing is not None:
        return await load_reopen(session, existing, requester), False
    challenge: DecisionChallenge | None = None
    if body.challenge_id is not None:
        challenge = await load_challenge(session, body.challenge_id, requester, lock=True)
        if challenge.decision_id != decision_id:
            raise not_found()
    quote = body.quote or (challenge.quote if challenge else None)
    owner = decision["owner_id"]
    request_id = (
        await session.execute(
            text(
                "INSERT INTO decision_reopen_requests (org_id, decision_id, requested_by, "
                "owner_user_id, reason, quote, source, place_label, link) VALUES (:o, :d, :u, "
                ":owner, :reason, :q, :s, :p, :l) RETURNING id"
            ),
            {
                "o": str(org_id),
                "d": str(decision_id),
                "u": str(requester),
                "owner": str(owner) if owner else None,
                "reason": body.reason,
                "q": dc.clip(quote, dc.MAX_QUOTE) if quote else None,
                "s": body.source,
                "p": body.place_label or (challenge.place_label if challenge else None),
                "l": body.link or (challenge.link if challenge else None),
            },
        )
    ).scalar_one()
    if challenge is not None:
        await session.execute(
            text(
                "UPDATE decision_challenges SET status = 'reopened', reopen_request_id = :r, "
                "dismissed_at = NULL, dismissed_by = NULL WHERE id = :c"
            ),
            {"r": str(request_id), "c": str(challenge.id)},
        )
    elif quote:
        # A reopen from a draft is a place the decision was challenged, by the writer's choice.
        await session.execute(
            text(
                "INSERT INTO decision_challenges (org_id, decision_id, source, author_user_id, "
                "quote, quote_hash, why, relation, confidence, place_label, link, status, "
                "reopen_request_id) VALUES (:o, :d, :s, :u, :q, :h, :w, 'reopens', 1.0, :p, :l, "
                "'reopened', :r) ON CONFLICT (decision_id, quote_hash) DO UPDATE SET "
                "status = 'reopened', reopen_request_id = EXCLUDED.reopen_request_id, "
                "dismissed_at = NULL, dismissed_by = NULL"
            ),
            {
                "o": str(org_id),
                "d": str(decision_id),
                "s": body.source,
                "u": str(requester),
                "q": dc.clip(quote, dc.MAX_QUOTE),
                "h": dc.quote_hash(quote),
                "w": dc.clip(body.reason or "", dc.MAX_WHY),
                "p": body.place_label,
                "l": body.link,
                "r": str(request_id),
            },
        )
    if owner is not None and owner != requester:
        who = (await dc.names(session, {requester})).get(requester)
        name = who.name if who and who.name else "Someone"
        await dc.notify(
            session,
            org_id,
            source_event_id=f"decision-reopen:{request_id}",
            user_id=owner,
            title=f"{name} asked to reopen your decision",
            body=(
                f'"{dc.clip(decision["content"], 200)}"'
                + (f' Their reason: "{dc.clip(body.reason, 300)}"' if body.reason else "")
                + (f' They wrote: "{dc.clip(quote, 240)}"' if quote else "")
            ),
            link=(
                f"{dc.decision_link(decision['meeting_id'], decision_id)}"
                f"?reopen={request_id}#challenges"
            ),
        )
    await audit(
        session,
        org_id=org_id,
        actor_id=requester,
        actor_type=actor_type,
        action=ACTION_REOPEN_REQUESTED,
        target_type="decision",
        target_id=decision_id,
        metadata={
            "reopen_request_id": str(request_id),
            "source": body.source,
            "challenge_id": str(challenge.id) if challenge else None,
        },
        ip=ip,
    )
    return await load_reopen(session, request_id, requester), True


@router.post(
    "/decisions/{decision_id}/reopen-requests",
    response_model=ReopenRequest,
    status_code=201,
    summary="Ask to reopen a decision",
    description="The decision's owner is asked; nothing about the decision changes until they "
    "act. 200 with your existing request when you already have a pending one. 409 when the "
    "decision was already replaced.",
)
async def request_reopen(
    decision_id: uuid.UUID,
    body: ReopenCreate,
    response: Response,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ReopenRequest:
    if not ctx.can_write:
        raise ProblemDetail(
            403,
            ErrorCode.FORBIDDEN_ROLE,
            "This token is read-only",
            extra={"reason": "read_only_token"},
        )
    request, new = await create_reopen(session, ctx.org_id, decision_id, _me(ctx), body, ip=ctx.ip)
    if not new:
        response.status_code = 200
    return request


@router.get(
    "/decision-reopen-requests",
    response_model=ReopenRequestList,
    summary="Reopen requests you own or made",
)
async def list_reopen_requests(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    role: Annotated[Literal["owner", "requester", "all"], Query()] = "owner",
    status: Annotated[Literal["pending", "resolved", "all"], Query()] = "pending",
) -> ReopenRequestList:
    if ctx.user_id is None:
        return ReopenRequestList(items=[])
    me = ctx.user_id
    owner_clause = f"COALESCE(r.owner_user_id, {dc.OWNER_SQL}) = :viewer"
    where = ["d.deleted_at IS NULL"]
    if role == "owner":
        where.append(owner_clause)
    elif role == "requester":
        where.append("r.requested_by = :viewer")
    else:
        where.append(f"({owner_clause} OR r.requested_by = :viewer)")
    if status == "pending":
        where.append("r.status = 'pending'")
    elif status == "resolved":
        where.append("r.status <> 'pending'")
    rows = (
        (
            await session.execute(
                text(
                    f"{_REOPEN_SELECT} WHERE {' AND '.join(where)} "
                    "ORDER BY r.created_at DESC, r.id LIMIT :n"
                ),
                {"viewer": str(me), "n": MAX_ITEMS},
            )
        )
        .mappings()
        .all()
    )
    people = await _people(session, list(rows), "requested_by", "owner_id")
    return ReopenRequestList(items=[_reopen(r, people, me) for r in rows])


@router.get(
    "/decision-reopen-requests/{request_id}",
    response_model=ReopenRequest,
    summary="One reopen request",
)
async def get_reopen_request(
    request_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ReopenRequest:
    return await load_reopen(session, request_id, _me(ctx))


def next_weekday_morning(now: datetime) -> datetime:
    day = (now + timedelta(days=1)).date()
    while day.weekday() >= 5:
        day += timedelta(days=1)
    return datetime.combine(day, time(REVISIT_HOUR_UTC), tzinfo=UTC)


async def _settle(
    session: AsyncSession,
    ctx: RequestContext,
    request_id: uuid.UUID,
    *,
    as_role: Literal["owner", "requester"],
    target: ReopenStatus,
    outcome: Outcome | None = None,
) -> tuple[ReopenRequest, bool]:
    """(the request, whether this call changes it). Same answer twice is a no-op 200."""
    me = _me(ctx)
    current = await load_reopen(session, request_id, me, lock=True)
    if current.viewer_role != as_role:
        raise not_found()
    if current.status != "pending":
        if current.status == target and current.outcome == outcome:
            return current, False
        raise ProblemDetail(
            409, ErrorCode.VERSION_CONFLICT, f"This request was already {current.status}"
        )
    return current, True


async def _resolved_notice(
    session: AsyncSession, org_id: uuid.UUID, request: ReopenRequest, body: str
) -> None:
    await dc.notify(
        session,
        org_id,
        source_event_id=f"decision-reopen-resolved:{request.id}",
        user_id=request.requested_by.id,
        title=(
            "Your reopen request was accepted"
            if request.status == "accepted"
            else "Your reopen request was declined"
        ),
        body=body,
        link=f"{dc.decision_link(request.meeting_id, request.decision_id)}#challenges",
        priority=3,
    )


@router.post(
    "/decision-reopen-requests/{request_id}/accept",
    response_model=ReopenRequest,
    summary="Accept a reopen request",
    description="The decision's owner schedules a revisit (a new meeting with the person who "
    "asked, the decision on its agenda) or creates a follow-up task for themselves. The "
    "decision itself stays confirmed until a new decision replaces it.",
)
async def accept_reopen(
    request_id: uuid.UUID,
    body: ReopenAccept,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ReopenRequest:
    current, changed = await _settle(
        session, ctx, request_id, as_role="owner", target="accepted", outcome=body.outcome
    )
    if not changed:
        return current
    me = _me(ctx)
    now = datetime.now(UTC)
    topic = dc.clip(current.decision_content, 120)
    action_id: uuid.UUID | None = None
    meeting_id: uuid.UUID | None = None
    if body.outcome == "follow_up":
        due = body.due_date or (now.date() + timedelta(days=DEFAULT_FOLLOW_UP_DAYS))
        row = dict(
            (
                await session.execute(
                    text(
                        "INSERT INTO action_items (org_id, meeting_id, content, assignee_id, "
                        "due_date, status, confidence, confirmed_by) VALUES (:o, :m, :c, :a, :d, "
                        "'confirmed', 1.0, :a) RETURNING id, meeting_id, content, assignee_id, "
                        "due_date, status, confidence"
                    ),
                    {
                        "o": str(ctx.org_id),
                        "m": str(current.meeting_id),
                        "c": f"Revisit the decision: {topic}",
                        "a": str(me),
                        "d": due,
                    },
                )
            )
            .mappings()
            .one()
        )
        await enqueue_action_confirmed(session, ctx.org_id, row)
        action_id = row["id"]
    else:
        start = body.scheduled_start or next_weekday_morning(now)
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
        title = dc.clip(f"Revisit: {topic}", 200)
        meeting = (
            (
                await session.execute(
                    text(
                        "INSERT INTO meetings (org_id, title, scheduled_start, scheduled_end, "
                        "created_by, previous_meeting_id, source) VALUES (:o, :t, :s, :e, :u, "
                        ":prev, 'mytheus') RETURNING id, livekit_room"
                    ),
                    {
                        "o": str(ctx.org_id),
                        "t": title,
                        "s": start,
                        "e": start + timedelta(minutes=body.duration_minutes),
                        "u": str(me),
                        "prev": str(current.meeting_id),
                    },
                )
            )
            .mappings()
            .one()
        )
        meeting_id = meeting["id"]
        for user, role in ((me, "host"), (current.requested_by.id, "attendee")):
            await session.execute(
                text(
                    "INSERT INTO meeting_participants (meeting_id, user_id, org_id, "
                    "role_in_meeting) SELECT :m, u.id, :o, :r FROM users u "
                    "WHERE u.id = :u AND u.deleted_at IS NULL ON CONFLICT DO NOTHING"
                ),
                {"m": str(meeting_id), "u": str(user), "o": str(ctx.org_id), "r": role},
            )
        agenda = [dc.clip(f"Revisit: {current.decision_content}", 200)]
        if current.quote:
            agenda.append(dc.clip(f'What was raised: "{current.quote}"', 200))
        for position, item in enumerate(agenda):
            await session.execute(
                text(
                    "INSERT INTO meeting_agenda_items (org_id, meeting_id, position, title, "
                    "owner_user_id) VALUES (:o, :m, :p, :t, :u)"
                ),
                {
                    "o": str(ctx.org_id),
                    "m": str(meeting_id),
                    "p": position,
                    "t": item,
                    "u": str(me),
                },
            )
        await enqueue(
            session,
            EventEnvelope(
                topic="meeting-events",
                payload={
                    "meeting_id": str(meeting_id),
                    "event_type": "created",
                    "livekit_room": meeting["livekit_room"],
                },
                org_id=str(ctx.org_id),
                producer=dc.PRODUCER,
                idempotency_key=f"meeting:{meeting_id}:created",
            ),
        )
    await session.execute(
        text(
            "UPDATE decision_reopen_requests SET status = 'accepted', outcome = :out, "
            "follow_up_action_id = :a, revisit_meeting_id = :m, response_note = :n, "
            "resolved_by = :u, resolved_at = now() WHERE id = :id"
        ),
        {
            "out": body.outcome,
            "a": str(action_id) if action_id else None,
            "m": str(meeting_id) if meeting_id else None,
            "n": body.note,
            "u": str(me),
            "id": str(request_id),
        },
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=me,
        action=ACTION_REOPEN_ACCEPTED,
        target_type="decision",
        target_id=current.decision_id,
        metadata={
            "reopen_request_id": str(request_id),
            "outcome": body.outcome,
            "follow_up_action_id": str(action_id) if action_id else None,
            "revisit_meeting_id": str(meeting_id) if meeting_id else None,
        },
        ip=ctx.ip,
    )
    updated = await load_reopen(session, request_id, me)
    await _resolved_notice(
        session,
        ctx.org_id,
        updated,
        (
            "A meeting to revisit it is on the calendar, and you are invited."
            if body.outcome == "revisit"
            else "They will revisit it: a follow-up task is on their list."
        )
        + (f' Their note: "{dc.clip(body.note, 300)}"' if body.note else ""),
    )
    return updated


@router.post(
    "/decision-reopen-requests/{request_id}/decline",
    response_model=ReopenRequest,
    summary="Decline a reopen request",
)
async def decline_reopen(
    request_id: uuid.UUID,
    body: ReopenNote,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ReopenRequest:
    current, changed = await _settle(session, ctx, request_id, as_role="owner", target="declined")
    if not changed:
        return current
    me = _me(ctx)
    await session.execute(
        text(
            "UPDATE decision_reopen_requests SET status = 'declined', response_note = :n, "
            "resolved_by = :u, resolved_at = now() WHERE id = :id"
        ),
        {"n": body.note, "u": str(me), "id": str(request_id)},
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=me,
        action=ACTION_REOPEN_DECLINED,
        target_type="decision",
        target_id=current.decision_id,
        metadata={"reopen_request_id": str(request_id)},
        ip=ctx.ip,
    )
    updated = await load_reopen(session, request_id, me)
    await _resolved_notice(
        session,
        ctx.org_id,
        updated,
        "The decision stands for now."
        + (f' Their note: "{dc.clip(body.note, 300)}"' if body.note else ""),
    )
    return updated


@router.post(
    "/decision-reopen-requests/{request_id}/withdraw",
    response_model=ReopenRequest,
    summary="Withdraw your reopen request",
)
async def withdraw_reopen(
    request_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ReopenRequest:
    current, changed = await _settle(
        session, ctx, request_id, as_role="requester", target="withdrawn"
    )
    if not changed:
        return current
    me = _me(ctx)
    await session.execute(
        text(
            "UPDATE decision_reopen_requests SET status = 'withdrawn', resolved_by = :u, "
            "resolved_at = now() WHERE id = :id"
        ),
        {"u": str(me), "id": str(request_id)},
    )
    # The place stays challenged (it was said), but no longer as a reopen.
    await session.execute(
        text(
            "UPDATE decision_challenges SET status = 'open', reopen_request_id = NULL "
            "WHERE reopen_request_id = :id"
        ),
        {"id": str(request_id)},
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=me,
        action=ACTION_REOPEN_WITHDRAWN,
        target_type="decision",
        target_id=current.decision_id,
        metadata={"reopen_request_id": str(request_id)},
        ip=ctx.ip,
    )
    return await load_reopen(session, request_id, me)
