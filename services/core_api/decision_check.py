"""Decisions that follow you: the shared core of the check, the challenges and reopen requests.

"Beyond the meeting" program, Layer 2. Text written anywhere is checked against the org's
confirmed decisions. The judging lives in the AI tier (rag-service `POST
/internal/decisions/check`: hybrid retrieval over `decisions__{org_id}` plus core-api's lexical
arm, a never-record screen, then one budget-gated judge call); core-api owns everything that
outlives a check:

- `call_checker` asks rag-service AS the person whose text it is (`X-Mytheus-User`), so they
  are only ever told about decisions from meetings they attended (ADR-0033 D1: deny by default,
  the same rule as Ask). core-api is the caller (`core-api=<secret>` in
  `MYTHEUS_RAG_CALLER_TOKENS`, `RAG_SERVICE_URL`). Not configured or unreachable -> 503
  `reason: check_not_connected`, never an empty answer that would read as "no conflict".
- `enrich` turns the judge's conflicts into what a person sees: only decisions that are still
  `confirmed` and that the person may see (participant of the decision's meeting, re-checked
  here whatever the AI tier did), with the owner, the meeting and the date, minus the sentences
  that person already dismissed for that decision.
- `record_challenges` (published text: a Slack channel message) and the reopen flow write
  `decision_challenges` / `decision_reopen_requests` and notify the decision's owner through the
  outbox (`notifications`, in-app).

Owner of a decision: `review_owner_id`, else `confirmed_by`, else the host of its meeting.

The never-record rule is enforced in the AI tier, which is the only side that reads the org's
never-record topics: a text naming one comes back `skipped/never_record` with no conflicts, so
nothing about it is recorded here either.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import time
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any, Literal

import httpx
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.cache import get_redis, redis_key
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from services.core_api.records import display_name

logger = logging.getLogger(__name__)

PRODUCER = "core-api"
SERVICE_NAME = "core-api"
TOKENS_ENV = "MYTHEUS_RAG_CALLER_TOKENS"
URL_ENV = "RAG_SERVICE_URL"
RATE_ENV = "DECISION_CHECK_RATE_PER_MINUTE"
DEFAULT_RATE = 30
TIMEOUT = httpx.Timeout(20.0, connect=3.0)
NOTIFY_EVERY = timedelta(hours=24)
MAX_QUOTE = 500
MAX_WHY = 500

Source = Literal["slack", "extension", "mcp", "app"]
Relation = Literal["contradicts", "reopens"]
CheckStatus = Literal["ok", "skipped", "degraded"]
CheckReason = Literal["too_short", "never_record", "budget", "no_candidates"]

_transport: httpx.AsyncBaseTransport | None = None


def set_transport(transport: httpx.AsyncBaseTransport | None) -> None:
    """Tests route rag-service calls to an in-process fake."""
    global _transport
    _transport = transport


# --- wire models -------------------------------------------------------------------------------


class Person(BaseModel):
    id: uuid.UUID
    name: str | None = None


class CheckContext(BaseModel):
    place_label: str | None = Field(default=None, max_length=120)
    channel: str | None = Field(default=None, max_length=120)
    topic: str | None = Field(default=None, max_length=200)
    author: str | None = Field(default=None, max_length=120)


class DecisionConflict(BaseModel):
    decision_id: uuid.UUID
    decision_content: str
    meeting_id: uuid.UUID
    meeting_title: str | None = None
    decided_at: date | None = Field(default=None, description="When the decision was confirmed.")
    owner: Person | None = None
    quote: str = Field(description="The part of the checked text that conflicts, verbatim.")
    relation: Relation
    confidence: float = Field(ge=0, le=1)
    why: str = Field(description="Why this conflicts, in one short sentence.")
    link: str = Field(description="The decision page in the app.")
    challenge_id: uuid.UUID | None = Field(
        default=None, description="Set when the conflict was recorded (published text)."
    )
    reopen_request_id: uuid.UUID | None = Field(
        default=None, description="The caller's own pending reopen request on this decision."
    )


class DecisionCheckResult(BaseModel):
    status: CheckStatus
    reason: CheckReason | None = None
    checked_at: datetime
    conflicts: list[DecisionConflict]


# --- normalisation -----------------------------------------------------------------------------

_WORD = re.compile(r"[\w']+", re.UNICODE)


def normalise(value: str) -> str:
    """Case, punctuation and spacing do not make a sentence new."""
    return " ".join(_WORD.findall(value.lower()))


def quote_hash(quote: str) -> str:
    return hashlib.sha256(normalise(quote).encode("utf-8")).hexdigest()


def clip(value: str, limit: int) -> str:
    flat = " ".join(str(value or "").split())
    return flat if len(flat) <= limit else flat[: limit - 1].rstrip() + "…"


def decision_link(meeting_id: uuid.UUID, decision_id: uuid.UUID) -> str:
    return f"/meetings/{meeting_id}/decisions/{decision_id}"


# --- rate limit --------------------------------------------------------------------------------


def rate_limit() -> int:
    try:
        return max(1, int(os.environ.get(RATE_ENV, DEFAULT_RATE)))
    except ValueError:
        return DEFAULT_RATE


async def take_rate(org_id: uuid.UUID, user_id: uuid.UUID) -> tuple[bool, int]:
    """(allowed, seconds until the window resets). A fixed one-minute window per person, in
    Redis so every replica shares it. Redis unreachable -> 503: an unlimited check is not an
    option (each one can cost a model call)."""
    now = time.time()
    window = int(now // 60)
    key = redis_key(org_id, "ratelimit", "decision-check", user_id, window)
    try:
        redis = get_redis()
        count = int(await redis.incr(key))
        if count == 1:
            await redis.expire(key, 120)
    except Exception:  # noqa: BLE001 - any Redis failure fails closed
        logger.exception("decision check: rate-limit store unavailable")
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "Checks are unavailable; retry shortly"
        ) from None
    return count <= rate_limit(), max(1, 60 - int(now % 60))


# --- the AI tier -------------------------------------------------------------------------------


def _own_secret() -> str | None:
    for entry in os.environ.get(TOKENS_ENV, "").split(","):
        name, _, secret = entry.strip().partition("=")
        if name.strip() == SERVICE_NAME and len(secret.strip()) >= 32:
            return secret.strip()
    return None


def not_connected() -> ProblemDetail:
    return ProblemDetail(
        503,
        ErrorCode.DEPENDENCY_UNAVAILABLE,
        "Decision checks are not connected on this server",
        extra={"reason": "check_not_connected"},
    )


async def call_checker(
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    body: str,
    source: Source,
    context: CheckContext | None,
) -> dict[str, Any]:
    """rag-service's raw answer for `user_id`'s text."""
    secret = _own_secret()
    if secret is None:
        raise not_connected()
    base = os.environ.get(URL_ENV, "http://rag-service:8201").rstrip("/")
    payload = {
        "text": body,
        "source": source,
        "context": (context or CheckContext()).model_dump(),
        "max_conflicts": 3,
    }
    try:
        async with httpx.AsyncClient(transport=_transport, timeout=TIMEOUT) as client:
            res = await client.post(
                f"{base}/internal/decisions/check",
                json=payload,
                headers={
                    "X-Mytheus-Service": SERVICE_NAME,
                    "X-Mytheus-Service-Token": secret,
                    "X-Mytheus-Org": str(org_id),
                    "X-Mytheus-User": str(user_id),
                },
            )
    except httpx.HTTPError:
        logger.warning("decision check: rag-service unreachable")
        raise not_connected() from None
    if res.status_code in (401, 403):
        logger.error("decision check: rag-service refused core-api's credentials")
        raise not_connected()
    if res.status_code >= 400:
        raise ProblemDetail(
            503,
            ErrorCode.DEPENDENCY_UNAVAILABLE,
            "Decision checks are unavailable right now",
            extra={"reason": "check_unavailable"},
        )
    answer: dict[str, Any] = res.json()
    return answer


# --- people and decisions ----------------------------------------------------------------------

OWNER_SQL = (
    "COALESCE(d.review_owner_id, d.confirmed_by, "
    "(SELECT p.user_id FROM meeting_participants p WHERE p.meeting_id = d.meeting_id "
    "AND p.role_in_meeting = 'host' ORDER BY p.user_id LIMIT 1))"
)

DECISION_SELECT = f"""
SELECT d.id, d.meeting_id, d.content, d.status, d.deleted_at,
       COALESCE(d.status_changed_at, d.created_at) AS decided_at,
       m.title AS meeting_title, {OWNER_SQL} AS owner_id
FROM decisions d
JOIN meetings m ON m.id = d.meeting_id
"""


async def names(session: AsyncSession, ids: set[uuid.UUID]) -> dict[uuid.UUID, Person]:
    if not ids:
        return {}
    rows = await session.execute(
        text("SELECT id, email FROM users WHERE id = ANY(CAST(:ids AS uuid[]))"),
        {"ids": [str(i) for i in ids]},
    )
    return {r[0]: Person(id=r[0], name=display_name(r[1])) for r in rows}


async def visible_decision(
    session: AsyncSession, decision_id: uuid.UUID, user_id: uuid.UUID, *, lock: bool = False
) -> Any:
    """The decision when `user_id` may see it (a participant of its meeting), else None.
    Proposed and dismissed rows are never visible here: only what people confirmed."""
    row = (
        (
            await session.execute(
                text(
                    f"{DECISION_SELECT} WHERE d.id = :d AND d.deleted_at IS NULL "
                    "AND d.status IN ('confirmed', 'superseded') AND EXISTS ("
                    "SELECT 1 FROM meeting_participants p WHERE p.meeting_id = d.meeting_id "
                    "AND p.user_id = :u)" + (" FOR UPDATE OF d" if lock else "")
                ),
                {"d": str(decision_id), "u": str(user_id)},
            )
        )
        .mappings()
        .first()
    )
    return row


async def user_by_email(session: AsyncSession, email: str) -> uuid.UUID | None:
    row = (
        await session.execute(
            text(
                "SELECT id FROM users WHERE lower(email) = lower(:e) AND deleted_at IS NULL "
                "ORDER BY created_at LIMIT 1"
            ),
            {"e": email.strip()},
        )
    ).first()
    return row[0] if row else None


async def enrich(
    session: AsyncSession, user_id: uuid.UUID, raw: list[dict[str, Any]]
) -> list[DecisionConflict]:
    """The judge's conflicts as `user_id` may see them, minus their dismissed sentences."""
    parsed: list[tuple[uuid.UUID, dict[str, Any]]] = []
    for c in raw:
        try:
            parsed.append((uuid.UUID(str(c["decision_id"])), c))
        except (KeyError, ValueError):
            continue
    if not parsed:
        return []
    ids = [str(d) for d, _ in parsed]
    rows = {
        r["id"]: r
        for r in (
            await session.execute(
                text(
                    f"{DECISION_SELECT} WHERE d.id = ANY(CAST(:ids AS uuid[])) "
                    "AND d.deleted_at IS NULL AND d.status = 'confirmed' AND EXISTS ("
                    "SELECT 1 FROM meeting_participants p WHERE p.meeting_id = d.meeting_id "
                    "AND p.user_id = :u)"
                ),
                {"ids": ids, "u": str(user_id)},
            )
        )
        .mappings()
        .all()
    }
    dismissed = {
        (r[0], r[1])
        for r in await session.execute(
            text(
                "SELECT decision_id, quote_hash FROM decision_check_dismissals "
                "WHERE user_id = :u AND decision_id = ANY(CAST(:ids AS uuid[]))"
            ),
            {"u": str(user_id), "ids": ids},
        )
    }
    pending = {
        r[0]: r[1]
        for r in await session.execute(
            text(
                "SELECT decision_id, id FROM decision_reopen_requests WHERE requested_by = :u "
                "AND status = 'pending' AND decision_id = ANY(CAST(:ids AS uuid[]))"
            ),
            {"u": str(user_id), "ids": ids},
        )
    }
    people = await names(session, {r["owner_id"] for r in rows.values() if r["owner_id"]})
    out: list[DecisionConflict] = []
    seen: set[uuid.UUID] = set()
    for decision_id, c in parsed:
        row = rows.get(decision_id)
        quote = clip(str(c.get("quote") or ""), MAX_QUOTE)
        if row is None or not quote or decision_id in seen:
            continue
        if (decision_id, quote_hash(quote)) in dismissed:
            continue
        relation = c.get("relation") if c.get("relation") in ("contradicts", "reopens") else None
        try:
            confidence = min(1.0, max(0.0, float(c.get("confidence", 0))))
        except (TypeError, ValueError):
            continue
        if relation is None:
            continue
        seen.add(decision_id)
        decided: datetime = row["decided_at"]
        out.append(
            DecisionConflict(
                decision_id=decision_id,
                decision_content=row["content"],
                meeting_id=row["meeting_id"],
                meeting_title=row["meeting_title"],
                decided_at=decided.date() if decided else None,
                owner=people.get(row["owner_id"]) if row["owner_id"] else None,
                quote=quote,
                relation=relation,
                confidence=round(confidence, 3),
                why=clip(str(c.get("why") or ""), MAX_WHY),
                link=decision_link(row["meeting_id"], decision_id),
                reopen_request_id=pending.get(decision_id),
            )
        )
    return out


def result_of(answer: dict[str, Any], conflicts: list[DecisionConflict]) -> DecisionCheckResult:
    raw_status, raw_reason = answer.get("status"), answer.get("reason")
    status: CheckStatus = raw_status if raw_status in ("ok", "skipped", "degraded") else "ok"
    reason: CheckReason | None = (
        raw_reason
        if raw_reason in ("too_short", "never_record", "budget", "no_candidates")
        else None
    )
    return DecisionCheckResult(
        status=status, reason=reason, checked_at=datetime.now(UTC), conflicts=conflicts
    )


# --- notifications -----------------------------------------------------------------------------


async def notify(
    session: AsyncSession,
    org_id: uuid.UUID,
    *,
    source_event_id: str,
    user_id: uuid.UUID,
    title: str,
    body: str,
    link: str,
    priority: int = 2,
) -> None:
    await enqueue(
        session,
        EventEnvelope(
            topic="notifications",
            org_id=str(org_id),
            producer=PRODUCER,
            idempotency_key=f"notif:{source_event_id}:in_app:{user_id}",
            payload={
                "source_event_id": source_event_id,
                "channel": "in_app",
                "user_id": str(user_id),
                "priority": priority,
                "title": clip(title, 200),
                "body": clip(body, 2000),
                "link": link,
            },
        ),
    )


PLACE_OF_SOURCE: dict[str, str] = {
    "slack": "Slack",
    "extension": "a document",
    "mcp": "an AI assistant",
    "app": "Mytheus",
}


def place_of(source: str, place_label: str | None) -> str:
    return place_label or PLACE_OF_SOURCE.get(source, "another tool")


async def notify_challenge(
    session: AsyncSession, org_id: uuid.UUID, challenge_id: uuid.UUID
) -> bool:
    """ "Your decision was challenged", to the owner: at most once per decision per 24 hours, and
    never for a challenge the owner wrote themselves. Returns whether it was sent."""
    row = (
        (
            await session.execute(
                text(
                    f"SELECT c.id, c.decision_id, c.author_user_id, c.quote, c.why, c.source, "
                    f"c.place_label, d.content, d.meeting_id, {OWNER_SQL} AS owner_id "
                    "FROM decision_challenges c JOIN decisions d ON d.id = c.decision_id "
                    "WHERE c.id = :c"
                ),
                {"c": str(challenge_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None or row["owner_id"] is None or row["owner_id"] == row["author_user_id"]:
        return False
    recent = (
        await session.execute(
            text(
                "SELECT 1 FROM decision_challenges WHERE decision_id = :d AND id <> :c "
                "AND notified_at > now() - make_interval(secs => :s)"
            ),
            {
                "d": str(row["decision_id"]),
                "c": str(challenge_id),
                "s": NOTIFY_EVERY.total_seconds(),
            },
        )
    ).first()
    if recent is not None:
        return False
    place = place_of(row["source"], row["place_label"])
    await notify(
        session,
        org_id,
        source_event_id=f"decision-challenge:{challenge_id}",
        user_id=row["owner_id"],
        title="Your decision was challenged",
        body=(
            f'"{clip(row["quote"], 240)}" ({place}) goes against "{clip(row["content"], 160)}".'
            + (f" {clip(row['why'], 240)}" if row["why"] else "")
        ),
        link=f"{decision_link(row['meeting_id'], row['decision_id'])}#challenges",
    )
    await session.execute(
        text("UPDATE decision_challenges SET notified_at = now() WHERE id = :c"),
        {"c": str(challenge_id)},
    )
    return True


async def record_challenges(
    session: AsyncSession,
    org_id: uuid.UUID,
    *,
    author_id: uuid.UUID,
    source: Source,
    place_label: str | None,
    link: str | None,
    external_ref: str | None,
    conflicts: list[DecisionConflict],
    status: Literal["open", "reopened"] = "open",
) -> list[DecisionConflict]:
    """One challenge per (decision, sentence): a redelivered event or a re-check of the same
    sentence finds the row already there and notifies nobody again."""
    out: list[DecisionConflict] = []
    for c in conflicts:
        row = (
            await session.execute(
                text(
                    "INSERT INTO decision_challenges (org_id, decision_id, source, author_user_id, "
                    "quote, quote_hash, why, relation, confidence, place_label, link, "
                    "external_ref, status) VALUES (:o, :d, :s, :a, :q, :h, :w, :r, :c, :p, :l, "
                    ":x, :st) ON CONFLICT (decision_id, quote_hash) DO NOTHING RETURNING id"
                ),
                {
                    "o": str(org_id),
                    "d": str(c.decision_id),
                    "s": source,
                    "a": str(author_id),
                    "q": c.quote,
                    "h": quote_hash(c.quote),
                    "w": c.why,
                    "r": c.relation,
                    "c": c.confidence,
                    "p": place_label,
                    "l": link,
                    "x": external_ref,
                    "st": status,
                },
            )
        ).first()
        if row is not None:
            challenge_id = row[0]
            await notify_challenge(session, org_id, challenge_id)
        else:
            challenge_id = (
                await session.execute(
                    text(
                        "SELECT id FROM decision_challenges WHERE decision_id = :d "
                        "AND quote_hash = :h"
                    ),
                    {"d": str(c.decision_id), "h": quote_hash(c.quote)},
                )
            ).scalar_one()
        out.append(c.model_copy(update={"challenge_id": challenge_id}))
    return out
