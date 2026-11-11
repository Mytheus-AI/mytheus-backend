"""Room signals: the last word, the agreement check and pick up where we stopped ("After the
meeting" layer 1). The domain half; the routes are `routes/room_signals.py` and
`routes/internal_room_signals.py`, the consumer hook is `consumer._persist_room_signals`.

Signals the room saw. realtime-gateway pushes "didn't get in" moments onto the Redis list
`nexus:{org_id}:room-signals:{meeting_id}` while the call runs (an unmute that was spoken over,
a raised hand never called). When the analyzer asks for its source after the meeting ended,
`file_room_signals` files every well-formed entry for a participant of the meeting into
`room_signals` (the gateway's uuid is the key, so filing twice files nothing twice) and returns
them on the meeting clock. Redis being unreachable files nothing new and serves what is filed.

The analyzer's result (`agent-results` MeetingAgent / `room_signals`, one per meeting end) is
filed by `persist_result` in the consumer's transaction:
- `last_word`: at most one private nudge per participant (`last_word_nudges`, the first
  generation wins) and an in-app notification `lastword:{id}` to that person (pushed to their
  phone when push is connected);
- `consensus`: one `decision_agreement_reads` row per decision of this meeting (strength and
  counts only) and one private `agreement_checks` row per person asked (never the decision's
  owner), each with a notification `agreement:{id}`;
- `resume_point`: at most one per meeting (`resume_points`).
Every id is checked against the meeting under RLS; an entry naming anyone else is skipped.

Privacy. A nudge and an agreement check are served only to their own person. The decision
owner sees agreement as aggregates (`agreement_view`), named notes only from people who added
their name, and unsigned notes only once at least three people answered. Nothing ranks people.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.cache import get_redis, redis_key
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from services.core_api import records
from services.core_api.brain import may_share_source
from services.core_api.records import RecapLateContribution

logger = logging.getLogger(__name__)

PRODUCER = "core-api"
NIL = uuid.UUID(int=0)
MAX_ENTRIES = 50
MAX_ASK = 12
MAX_REDIS_ENTRIES = 500
LATE_WINDOW = timedelta(days=7)
AGREEMENT_WINDOW = timedelta(hours=24)
RESUME_LOOKBACK = timedelta(days=30)
SAME_PEOPLE_JACCARD = 0.6
UNSIGNED_NOTES_MIN_ANSWERS = 3

SignalKind = Literal["spoken_over", "hand_not_called", "cut_off", "topic_silent"]
NudgeStatus = Literal["open", "contributed", "dismissed"]
Strength = Literal["strong", "mixed", "weak", "unclear"]
AskReason = Literal["hedged", "silent", "talked_over"]
Answer = Literal["support", "concerns", "object"]
ResumeReason = Literal["time_ran_out", "agenda_unfinished", "cut_off"]
ResumeStatus = Literal["open", "continued", "dismissed"]

_KINDS = ("spoken_over", "hand_not_called", "cut_off", "topic_silent")
_ROOM_KINDS = ("spoken_over", "hand_not_called")
_STRENGTHS = ("strong", "mixed", "weak", "unclear")
_REASONS = ("hedged", "silent", "talked_over")
_RESUME_REASONS = ("time_ran_out", "agenda_unfinished", "cut_off")


# --- wire models ------------------------------------------------------------------------------


class LastWordNudge(BaseModel):
    id: uuid.UUID
    meeting_id: uuid.UUID
    meeting_title: str
    kind: SignalKind
    topic: str
    prompt: str
    quote: str | None = None
    at_ms: int | None = None
    end_ms: int | None = None
    decision_id: uuid.UUID | None = None
    decision_content: str | None = None
    status: NudgeStatus
    created_at: datetime


class LastWordList(BaseModel):
    items: list[LastWordNudge]


class LateContribution(BaseModel):
    id: uuid.UUID
    meeting_id: uuid.UUID
    user_id: uuid.UUID
    author_name: str
    nudge_id: uuid.UUID | None = None
    decision_id: uuid.UUID | None = None
    at_ms: int | None = None
    body: str | None = None
    has_audio: bool
    audio_duration_ms: int | None = None
    created_at: datetime
    mine: bool


class LateContributionList(BaseModel):
    items: list[LateContribution]


class AgreementCheck(BaseModel):
    id: uuid.UUID
    decision_id: uuid.UUID
    decision_content: str
    meeting_id: uuid.UUID
    meeting_title: str
    status: Literal["asked", "answered"]
    answer: Answer | None = None
    note: str | None = None
    share_name: bool
    answered_at: datetime | None = None
    created_at: datetime


class AgreementCheckList(BaseModel):
    items: list[AgreementCheck]


class InMeetingCounts(BaseModel):
    yes: int
    hedged: int
    silent: int
    objected: int
    talked_over: int


class AgreementSummary(BaseModel):
    participants: int
    support: int
    concerns: int
    object: int
    pending: int
    in_meeting: InMeetingCounts


class AgreementNote(BaseModel):
    text: str
    author_name: str | None = None
    answer: Answer
    at: datetime


class DecisionAgreement(BaseModel):
    decision_id: uuid.UUID
    meeting_id: uuid.UUID
    strength: Strength | None = None
    analyzed_at: datetime | None = None
    you_own: bool
    summary: AgreementSummary | None = None
    notes: list[AgreementNote] = Field(default_factory=list)
    my_check: AgreementCheck | None = None


class ResumePosition(BaseModel):
    user_id: uuid.UUID | None = None
    name: str | None = None
    quote: str
    start_ms: int
    end_ms: int


class ResumeCutOff(BaseModel):
    user_id: uuid.UUID
    name: str
    quote: str
    start_ms: int
    end_ms: int


class ResumePoint(BaseModel):
    id: uuid.UUID
    meeting_id: uuid.UUID
    meeting_title: str
    meeting_ended_at: datetime | None = None
    reason: ResumeReason
    topic: str
    headline: str
    positions: list[ResumePosition]
    cut_off: ResumeCutOff | None = None
    agenda_item_id: uuid.UUID | None = None
    status: ResumeStatus
    continued_in_meeting_id: uuid.UUID | None = None
    created_at: datetime


class ResumePointRead(BaseModel):
    incoming: ResumePoint | None = None
    saved: ResumePoint | None = None


class AfterSignalsDecision(BaseModel):
    decision_id: uuid.UUID
    agreement: Strength | None = None
    late_contribution_count: int
    window_ends_at: datetime
    in_window: bool
    you_own: bool


class AfterSignals(BaseModel):
    meeting_id: uuid.UUID
    analyzed: bool
    my_nudge: LastWordNudge | None = None
    late_contributions: list[LateContribution]
    decisions: list[AfterSignalsDecision]
    resume_point: ResumePoint | None = None


# --- copy -------------------------------------------------------------------------------------


def _sentence_start(value: str) -> str:
    return value[:1].upper() + value[1:] if value else value


def nudge_prompt(kind: str, topic: str) -> str:
    """The one line a nudge says. The topic is a noun phrase from the analyzer."""
    if kind == "hand_not_called":
        return f"Your hand was up while the room discussed {topic}. Want to add your point?"
    if kind == "topic_silent":
        return f"{_sentence_start(topic)} came up and you listen for it. Want to add your view?"
    return f"You started to say something about {topic}. Want to add it?"


def first_name(name: str | None) -> str | None:
    return name.split()[0] if name else None


def resume_headline(reason: str, topic: str, cut_off_name: str | None) -> str:
    if cut_off_name:
        return f"{cut_off_name} was halfway through explaining {topic}"
    if reason == "time_ran_out":
        return f"Time ran out on {topic}"
    return f"{_sentence_start(topic)} was left unfinished"


def _short(value: str | None, limit: int = 80) -> str:
    v = " ".join((value or "").split())
    return v if len(v) <= limit else v[: limit - 1].rstrip() + "…"


async def _notify(
    session: AsyncSession,
    org_id: uuid.UUID,
    *,
    source: str,
    user_id: uuid.UUID,
    priority: int,
    title: str,
    body: str,
    link: str,
) -> None:
    await enqueue(
        session,
        EventEnvelope(
            topic="notifications",
            org_id=str(org_id),
            producer=PRODUCER,
            idempotency_key=f"notif:{source}:in_app:{user_id}",
            payload={
                "source_event_id": source,
                "channel": "in_app",
                "user_id": str(user_id),
                "priority": priority,
                "title": title[:200],
                "body": body[:2000],
                "link": link,
            },
        ),
    )


# --- the room's own signals (Redis -> room_signals) -------------------------------------------


def signals_key(org_id: uuid.UUID, meeting_id: uuid.UUID) -> str:
    """The realtime-gateway's list (mytheus-realtime `libs/keys.ts` `roomSignalsKey`)."""
    return redis_key(org_id, "room-signals", meeting_id)


@dataclass(frozen=True)
class _Entry:
    id: uuid.UUID
    kind: str
    user_id: uuid.UUID
    at: datetime
    end: datetime
    detail: dict[str, Any]


def _parse_entry(raw: bytes | str) -> _Entry | None:
    try:
        v = json.loads(raw)
        if not isinstance(v, dict) or v.get("kind") not in _ROOM_KINDS:
            return None
        at, end = v.get("at"), v.get("end")
        if not isinstance(at, int) or not isinstance(end, int) or end < at:
            return None
        raw_detail = v.get("detail")
        detail: dict[str, Any] = raw_detail if isinstance(raw_detail, dict) else {}
        return _Entry(
            id=uuid.UUID(str(v.get("id"))),
            kind=str(v["kind"]),
            user_id=uuid.UUID(str(v.get("user_id"))),
            at=datetime.fromtimestamp(at / 1000, tz=UTC),
            end=datetime.fromtimestamp(end / 1000, tz=UTC),
            detail={k: detail[k] for k in list(detail)[:8] if isinstance(k, str)},
        )
    except (ValueError, TypeError, OverflowError):
        return None


async def file_room_signals(session: AsyncSession, org_id: uuid.UUID, meeting_id: uuid.UUID) -> int:
    """File the gateway's list into `room_signals` (participants of the meeting only). Returns
    how many rows were new. Redis unreachable: nothing new, never an error."""
    try:
        raw = await get_redis().lrange(signals_key(org_id, meeting_id), 0, MAX_REDIS_ENTRIES - 1)
    except Exception:  # noqa: BLE001 - the filed rows still serve the analyzer
        logger.warning("room signals for %s not read from redis", meeting_id)
        return 0
    entries = [e for e in (_parse_entry(r) for r in raw) if e is not None]
    if not entries:
        return 0
    participants = {
        r[0]
        for r in await session.execute(
            text("SELECT user_id FROM meeting_participants WHERE meeting_id = :m"),
            {"m": str(meeting_id)},
        )
    }
    filed = 0
    for e in entries:
        if e.user_id not in participants:
            continue
        row = (
            await session.execute(
                text(
                    "INSERT INTO room_signals (id, org_id, meeting_id, user_id, kind, "
                    "occurred_at, ended_at, detail) VALUES (:id, :o, :m, :u, :k, :at, :end, "
                    "CAST(:d AS jsonb)) ON CONFLICT (id) DO NOTHING RETURNING 1"
                ),
                {
                    "id": str(e.id),
                    "o": str(org_id),
                    "m": str(meeting_id),
                    "u": str(e.user_id),
                    "k": e.kind,
                    "at": e.at,
                    "end": e.end,
                    "d": json.dumps(e.detail),
                },
            )
        ).first()
        filed += row is not None
    return filed


# --- the analyzer's result --------------------------------------------------------------------


def _uuid(raw: Any) -> uuid.UUID | None:
    if not isinstance(raw, str):
        return None
    try:
        return uuid.UUID(raw)
    except ValueError:
        return None


def _int(raw: Any) -> int | None:
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
        return None
    return raw


def _clip(raw: Any, limit: int) -> str | None:
    if not isinstance(raw, str):
        return None
    v = " ".join(raw.split())
    if not v:
        return None
    return v if len(v) <= limit else v[: limit - 1].rstrip() + "…"


def parse_payload(raw: Any) -> dict[str, Any] | None:
    try:
        payload = json.loads(raw) if isinstance(raw, str) else None
    except ValueError:
        return None
    if not isinstance(payload, dict) or not isinstance(payload.get("generation"), str):
        return None
    return payload


async def persist_result(
    session: AsyncSession, org_id: uuid.UUID, meeting_id: uuid.UUID, payload: dict[str, Any]
) -> bool:
    """File one `room_signals` result. True when anything was written."""
    generation = str(payload["generation"])[:64] or "unknown"
    status = payload.get("status") if payload.get("status") in ("ready", "partial") else "partial"
    m = {"m": str(meeting_id)}
    meeting = (
        (
            await session.execute(
                text(
                    "SELECT m.title, m.confidential, COALESCE((SELECT p.user_id FROM "
                    "meeting_participants p WHERE p.meeting_id = m.id AND "
                    "p.role_in_meeting = 'host' LIMIT 1), m.created_by) AS host_id "
                    "FROM meetings m WHERE m.id = :m"
                ),
                m,
            )
        )
        .mappings()
        .one()
    )
    title = records._title(meeting["title"])
    participants = {
        r[0]
        for r in await session.execute(
            text(
                "SELECT p.user_id FROM meeting_participants p JOIN users u ON u.id = p.user_id "
                "AND u.deleted_at IS NULL WHERE p.meeting_id = :m"
            ),
            m,
        )
    }
    decisions = {
        r[0]: (r[1], r[2])
        for r in await session.execute(
            text(
                "SELECT d.id, d.content, COALESCE(d.review_owner_id, d.confirmed_by) "
                "FROM decisions d WHERE d.meeting_id = :m AND d.deleted_at IS NULL "
                "AND d.status <> 'dismissed'"
            ),
            m,
        )
    }
    run = (
        await session.execute(
            text(
                "INSERT INTO room_signal_runs (meeting_id, org_id, generation, status) "
                "VALUES (:m, :o, :g, :s) ON CONFLICT (meeting_id) DO UPDATE SET "
                "generation = EXCLUDED.generation, status = EXCLUDED.status, analyzed_at = now() "
                "WHERE room_signal_runs.generation IS DISTINCT FROM EXCLUDED.generation "
                "OR room_signal_runs.status IS DISTINCT FROM EXCLUDED.status RETURNING 1"
            ),
            {**m, "o": str(org_id), "g": generation, "s": status},
        )
    ).first()
    written = run is not None

    # --- the last word -----------------------------------------------------------------------
    seen: set[uuid.UUID] = set()
    for e in (payload.get("last_word") or [])[:MAX_ENTRIES]:
        if not isinstance(e, dict):
            continue
        uid = _uuid(e.get("user_id"))
        topic = _clip(e.get("topic"), 80)
        if uid is None or uid in seen or uid not in participants or topic is None:
            continue
        if e.get("kind") not in _KINDS:
            continue
        seen.add(uid)
        decision_id = _uuid(e.get("decision_id"))
        if decision_id not in decisions:
            decision_id = None
        signal_ids = [s for s in (_uuid(x) for x in e.get("signal_ids") or []) if s][:20]
        row = (
            await session.execute(
                text(
                    "INSERT INTO last_word_nudges (org_id, meeting_id, user_id, kind, topic, "
                    "quote, at_ms, end_ms, decision_id, signal_ids, generation) VALUES "
                    "(:o, :m, :u, :k, :t, :q, :at, :end, :d, CAST(:sig AS uuid[]), :g) "
                    "ON CONFLICT (meeting_id, user_id) DO NOTHING RETURNING id"
                ),
                {
                    **m,
                    "o": str(org_id),
                    "u": str(uid),
                    "k": e["kind"],
                    "t": topic,
                    "q": _clip(e.get("quote"), 300),
                    "at": _int(e.get("at_ms")),
                    "end": _int(e.get("end_ms")),
                    "d": str(decision_id) if decision_id else None,
                    "sig": [str(s) for s in signal_ids],
                    "g": generation,
                },
            )
        ).first()
        if row is None:
            continue
        written = True
        quote = _clip(e.get("quote"), 200)
        await _notify(
            session,
            org_id,
            source=f"lastword:{row[0]}",
            user_id=uid,
            priority=3,
            title=nudge_prompt(str(e["kind"]), topic),
            body=(f'In {title} you said: "{quote}"' if quote else f"From {title}.")
            + " Only you see this.",
            link=f"/meetings/{meeting_id}#last-word",
        )

    # --- agreement ---------------------------------------------------------------------------
    for c in (payload.get("consensus") or [])[:MAX_ENTRIES]:
        if not isinstance(c, dict):
            continue
        decision_id = _uuid(c.get("decision_id"))
        if decision_id not in decisions or c.get("strength") not in _STRENGTHS:
            continue
        counts = {k: _int(c.get(k)) for k in ("yes", "hedged", "silent", "objected", "talked_over")}
        n = _int(c.get("participants"))
        if n is None or any(v is None for v in counts.values()):
            continue
        if sum(v for v in counts.values() if v is not None) > n:
            continue
        upserted = (
            await session.execute(
                text(
                    "INSERT INTO decision_agreement_reads (decision_id, org_id, meeting_id, "
                    "strength, participants, yes_count, hedged_count, silent_count, "
                    "objected_count, talked_over_count, generation) VALUES (:d, :o, :m, :s, "
                    ":n, :yes, :hedged, :silent, :objected, :talked_over, :g) "
                    "ON CONFLICT (decision_id) DO UPDATE SET strength = EXCLUDED.strength, "
                    "participants = EXCLUDED.participants, yes_count = EXCLUDED.yes_count, "
                    "hedged_count = EXCLUDED.hedged_count, silent_count = EXCLUDED.silent_count, "
                    "objected_count = EXCLUDED.objected_count, "
                    "talked_over_count = EXCLUDED.talked_over_count, "
                    "generation = EXCLUDED.generation, analyzed_at = now() "
                    "WHERE decision_agreement_reads.generation <> EXCLUDED.generation "
                    "RETURNING 1"
                ),
                {
                    **m,
                    **counts,
                    "d": str(decision_id),
                    "o": str(org_id),
                    "s": c["strength"],
                    "n": n,
                    "g": generation,
                },
            )
        ).first()
        written = written or upserted is not None
        content, owner = decisions[decision_id]
        asked: set[uuid.UUID] = set()
        for a in (c.get("ask") or [])[:MAX_ASK]:
            uid = _uuid(a.get("user_id")) if isinstance(a, dict) else None
            reason = a.get("reason") if isinstance(a, dict) else None
            if uid is None or uid in asked or uid not in participants or reason not in _REASONS:
                continue
            if uid == owner or (owner is None and uid == meeting["host_id"]):
                continue  # the decision's owner is never asked about their own decision
            asked.add(uid)
            row = (
                await session.execute(
                    text(
                        "INSERT INTO agreement_checks (org_id, decision_id, meeting_id, "
                        "user_id, reason) VALUES (:o, :d, :m, :u, :r) "
                        "ON CONFLICT (decision_id, user_id) DO NOTHING RETURNING id"
                    ),
                    {**m, "o": str(org_id), "d": str(decision_id), "u": str(uid), "r": reason},
                )
            ).first()
            if row is None:
                continue
            written = True
            await _notify(
                session,
                org_id,
                source=f"agreement:{row[0]}",
                user_id=uid,
                priority=2,
                title=f'Where do you stand on "{_short(content)}"?',
                body=(
                    f"From {title}. One tap: Support, Have concerns or Object. Your answer is "
                    "anonymous unless you choose to add your name."
                ),
                link="/inbox",
            )

    # --- the resume point --------------------------------------------------------------------
    rp = payload.get("resume_point")
    if isinstance(rp, dict) and rp.get("reason") in _RESUME_REASONS:
        topic = _clip(rp.get("topic"), 120)
        if topic is not None:
            positions = []
            for p in (rp.get("positions") or [])[:2]:
                if not isinstance(p, dict):
                    continue
                quote = _clip(p.get("quote"), 300)
                start, end = _int(p.get("start_ms")), _int(p.get("end_ms"))
                if quote is None or start is None or end is None:
                    continue
                uid = _uuid(p.get("user_id"))
                positions.append(
                    {
                        "user_id": str(uid) if uid in participants else None,
                        "quote": quote,
                        "start_ms": start,
                        "end_ms": max(start, end),
                    }
                )
            cut = rp.get("cut_off") if isinstance(rp.get("cut_off"), dict) else None
            cut_uid = _uuid(cut.get("user_id")) if cut else None
            cut_quote = _clip(cut.get("quote"), 300) if cut else None
            cut_start = _int(cut.get("start_ms")) if cut else None
            cut_end = _int(cut.get("end_ms")) if cut else None
            if cut_uid not in participants or cut_quote is None or cut_start is None:
                cut_uid = cut_quote = cut_start = cut_end = None
            agenda_id = _uuid(rp.get("agenda_item_id"))
            if agenda_id is not None:
                found = (
                    await session.execute(
                        text(
                            "SELECT 1 FROM meeting_agenda_items WHERE id = :a AND meeting_id = :m"
                        ),
                        {**m, "a": str(agenda_id)},
                    )
                ).first()
                agenda_id = agenda_id if found else None
            row = (
                await session.execute(
                    text(
                        "INSERT INTO resume_points (org_id, meeting_id, reason, topic, "
                        "agenda_item_id, positions, cut_off_user_id, cut_off_quote, "
                        "cut_off_start_ms, cut_off_end_ms, generation) VALUES (:o, :m, :r, :t, "
                        ":a, CAST(:p AS jsonb), :cu, :cq, :cs, :ce, :g) "
                        "ON CONFLICT (meeting_id) DO NOTHING RETURNING 1"
                    ),
                    {
                        **m,
                        "o": str(org_id),
                        "r": rp["reason"],
                        "t": topic,
                        "a": str(agenda_id) if agenda_id else None,
                        "p": json.dumps(positions),
                        "cu": str(cut_uid) if cut_uid else None,
                        "cq": cut_quote,
                        "cs": cut_start,
                        "ce": (
                            max(cut_start, cut_end or cut_start) if cut_start is not None else None
                        ),
                        "g": generation,
                    },
                )
            ).first()
            written = written or row is not None
    return written


# --- reads ------------------------------------------------------------------------------------


async def names(session: AsyncSession, ids: set[uuid.UUID]) -> dict[uuid.UUID, str]:
    if not ids:
        return {}
    rows = await session.execute(
        text("SELECT id, email, display_name FROM users WHERE id = ANY(CAST(:ids AS uuid[]))"),
        {"ids": [str(i) for i in ids]},
    )
    return {r[0]: records.person_name(r[2], r[1]) or "A former member" for r in rows}


_NUDGE_SELECT = """
SELECT n.id, n.meeting_id, m.title AS meeting_title, n.kind, n.topic, n.quote, n.at_ms,
       n.end_ms, n.decision_id, d.content AS decision_content, n.status, n.created_at
FROM last_word_nudges n
JOIN meetings m ON m.id = n.meeting_id AND m.deleted_at IS NULL
LEFT JOIN decisions d ON d.id = n.decision_id AND d.deleted_at IS NULL
"""


def _nudge(r: Any) -> LastWordNudge:
    return LastWordNudge(
        id=r["id"],
        meeting_id=r["meeting_id"],
        meeting_title=records._title(r["meeting_title"]),
        kind=r["kind"],
        topic=r["topic"],
        prompt=nudge_prompt(r["kind"], r["topic"]),
        quote=r["quote"],
        at_ms=r["at_ms"],
        end_ms=r["end_ms"],
        decision_id=r["decision_id"],
        decision_content=r["decision_content"],
        status=r["status"],
        created_at=r["created_at"],
    )


async def my_nudges(session: AsyncSession, user_id: uuid.UUID, only_open: bool) -> LastWordList:
    rows = (
        (
            await session.execute(
                text(
                    _NUDGE_SELECT
                    + "WHERE n.user_id = :u "
                    + ("AND n.status = 'open' " if only_open else "")
                    + "ORDER BY n.created_at DESC, n.id LIMIT 100"
                ),
                {"u": str(user_id)},
            )
        )
        .mappings()
        .all()
    )
    return LastWordList(items=[_nudge(r) for r in rows])


async def nudge_for(
    session: AsyncSession, nudge_id: uuid.UUID, user_id: uuid.UUID
) -> LastWordNudge | None:
    row = (
        (
            await session.execute(
                text(_NUDGE_SELECT + "WHERE n.id = :n AND n.user_id = :u"),
                {"n": str(nudge_id), "u": str(user_id)},
            )
        )
        .mappings()
        .first()
    )
    return _nudge(row) if row else None


async def meeting_nudge(
    session: AsyncSession, meeting_id: uuid.UUID, user_id: uuid.UUID
) -> LastWordNudge | None:
    row = (
        (
            await session.execute(
                text(_NUDGE_SELECT + "WHERE n.meeting_id = :m AND n.user_id = :u"),
                {"m": str(meeting_id), "u": str(user_id)},
            )
        )
        .mappings()
        .first()
    )
    return _nudge(row) if row else None


_LATE_SELECT = """
SELECT c.id, c.meeting_id, c.user_id, u.email, u.display_name, c.nudge_id, c.decision_id,
       c.at_ms, c.body,
       c.audio IS NOT NULL AS has_audio, c.audio_duration_ms, c.created_at
FROM late_contributions c LEFT JOIN users u ON u.id = c.user_id
"""


def _late(r: Any, viewer: uuid.UUID | None) -> LateContribution:
    return LateContribution(
        id=r["id"],
        meeting_id=r["meeting_id"],
        user_id=r["user_id"],
        author_name=records.person_name(r["display_name"], r["email"]) or "A former member",
        nudge_id=r["nudge_id"],
        decision_id=r["decision_id"],
        at_ms=r["at_ms"],
        body=r["body"],
        has_audio=bool(r["has_audio"]),
        audio_duration_ms=r["audio_duration_ms"],
        created_at=r["created_at"],
        mine=viewer is not None and r["user_id"] == viewer,
    )


async def late_contributions(
    session: AsyncSession, meeting_id: uuid.UUID, viewer: uuid.UUID | None
) -> list[LateContribution]:
    rows = (
        (
            await session.execute(
                text(
                    _LATE_SELECT + "WHERE c.meeting_id = :m AND c.deleted_at IS NULL "
                    "ORDER BY c.created_at, c.id LIMIT 200"
                ),
                {"m": str(meeting_id)},
            )
        )
        .mappings()
        .all()
    )
    return [_late(r, viewer) for r in rows]


async def late_contribution(
    session: AsyncSession, contribution_id: uuid.UUID, viewer: uuid.UUID | None
) -> LateContribution | None:
    row = (
        (
            await session.execute(
                text(_LATE_SELECT + "WHERE c.id = :c AND c.deleted_at IS NULL"),
                {"c": str(contribution_id)},
            )
        )
        .mappings()
        .first()
    )
    return _late(row, viewer) if row else None


async def recap_late_contributions(
    session: AsyncSession, meeting_id: uuid.UUID
) -> list[RecapLateContribution]:
    """The written late points for a shared recap (recorded-only points have no text)."""
    return [
        RecapLateContribution(author_name=c.author_name, body=c.body, created_at=c.created_at)
        for c in await late_contributions(session, meeting_id, None)
        if c.body
    ]


async def meeting_ended_at(session: AsyncSession, meeting_id: uuid.UUID) -> datetime | None:
    """When the meeting ended: its close-out, else its last status change; None while open."""
    row = (
        await session.execute(
            text(
                "SELECT CASE WHEN status = 'ended' THEN COALESCE(closed_at, updated_at) END "
                "FROM meetings WHERE id = :m"
            ),
            {"m": str(meeting_id)},
        )
    ).first()
    return row[0] if row else None


async def decision_signals(
    session: AsyncSession, meeting_id: uuid.UUID, viewer: uuid.UUID, host_id: uuid.UUID | None
) -> list[AfterSignalsDecision]:
    rows = (
        (
            await session.execute(
                text(
                    "SELECT d.id, r.strength, "
                    "COALESCE(d.review_owner_id, d.confirmed_by) AS owner, "
                    "GREATEST(d.created_at, CASE WHEN m.status = 'ended' "
                    "  THEN COALESCE(m.closed_at, m.updated_at) ELSE d.created_at END) "
                    "  + interval '24 hours' AS window_ends_at, "
                    "(SELECT count(*) FROM late_contributions c WHERE c.decision_id = d.id "
                    "  AND c.deleted_at IS NULL) AS late, now() AS now "
                    "FROM decisions d JOIN meetings m ON m.id = d.meeting_id "
                    "LEFT JOIN decision_agreement_reads r ON r.decision_id = d.id "
                    "WHERE d.meeting_id = :m AND d.deleted_at IS NULL AND d.status <> 'dismissed' "
                    "ORDER BY d.created_at, d.id LIMIT 200"
                ),
                {"m": str(meeting_id)},
            )
        )
        .mappings()
        .all()
    )
    return [
        AfterSignalsDecision(
            decision_id=r["id"],
            agreement=r["strength"],
            late_contribution_count=int(r["late"]),
            window_ends_at=r["window_ends_at"],
            in_window=r["now"] < r["window_ends_at"],
            you_own=(r["owner"] or host_id) == viewer,
        )
        for r in rows
    ]


async def analyzed(session: AsyncSession, meeting_id: uuid.UUID) -> bool:
    row = (
        await session.execute(
            text("SELECT 1 FROM room_signal_runs WHERE meeting_id = :m"), {"m": str(meeting_id)}
        )
    ).first()
    return row is not None


# --- agreement --------------------------------------------------------------------------------

_CHECK_SELECT = """
SELECT k.id, k.decision_id, d.content AS decision_content, k.meeting_id, m.title AS meeting_title,
       k.status, k.answer, k.note, k.share_name, k.answered_at, k.created_at
FROM agreement_checks k
JOIN decisions d ON d.id = k.decision_id AND d.deleted_at IS NULL
JOIN meetings m ON m.id = k.meeting_id AND m.deleted_at IS NULL
"""


def _check(r: Any) -> AgreementCheck:
    return AgreementCheck(
        id=r["id"],
        decision_id=r["decision_id"],
        decision_content=r["decision_content"],
        meeting_id=r["meeting_id"],
        meeting_title=records._title(r["meeting_title"]),
        status=r["status"],
        answer=r["answer"],
        note=r["note"],
        share_name=bool(r["share_name"]),
        answered_at=r["answered_at"],
        created_at=r["created_at"],
    )


async def my_checks(
    session: AsyncSession, user_id: uuid.UUID, only_asked: bool
) -> AgreementCheckList:
    rows = (
        (
            await session.execute(
                text(
                    _CHECK_SELECT
                    + "WHERE k.user_id = :u "
                    + ("AND k.status = 'asked' " if only_asked else "")
                    + "ORDER BY k.created_at DESC, k.id LIMIT 100"
                ),
                {"u": str(user_id)},
            )
        )
        .mappings()
        .all()
    )
    return AgreementCheckList(items=[_check(r) for r in rows])


async def check_for(
    session: AsyncSession, check_id: uuid.UUID, user_id: uuid.UUID
) -> AgreementCheck | None:
    row = (
        (
            await session.execute(
                text(_CHECK_SELECT + "WHERE k.id = :k AND k.user_id = :u"),
                {"k": str(check_id), "u": str(user_id)},
            )
        )
        .mappings()
        .first()
    )
    return _check(row) if row else None


async def agreement_view(
    session: AsyncSession,
    decision_id: uuid.UUID,
    meeting_id: uuid.UUID,
    viewer: uuid.UUID,
    owner: uuid.UUID | None,
) -> DecisionAgreement:
    read = (
        (
            await session.execute(
                text("SELECT * FROM decision_agreement_reads WHERE decision_id = :d"),
                {"d": str(decision_id)},
            )
        )
        .mappings()
        .first()
    )
    mine = (
        (
            await session.execute(
                text(_CHECK_SELECT + "WHERE k.decision_id = :d AND k.user_id = :u"),
                {"d": str(decision_id), "u": str(viewer)},
            )
        )
        .mappings()
        .first()
    )
    you_own = owner == viewer
    view = DecisionAgreement(
        decision_id=decision_id,
        meeting_id=meeting_id,
        strength=read["strength"] if read else None,
        analyzed_at=read["analyzed_at"] if read else None,
        you_own=you_own,
        my_check=_check(mine) if mine else None,
    )
    if not you_own or read is None:
        return view
    checks = (
        (
            await session.execute(
                text(
                    "SELECT k.status, k.answer, k.note, k.share_name, k.answered_at, u.email, "
                    "u.display_name "
                    "FROM agreement_checks k LEFT JOIN users u ON u.id = k.user_id "
                    "WHERE k.decision_id = :d ORDER BY k.answered_at NULLS LAST, k.id"
                ),
                {"d": str(decision_id)},
            )
        )
        .mappings()
        .all()
    )
    answered = [c for c in checks if c["status"] == "answered"]
    tally = {
        a: sum(1 for c in answered if c["answer"] == a) for a in ("support", "concerns", "object")
    }
    view.summary = AgreementSummary(
        participants=read["participants"],
        support=read["yes_count"] + tally["support"],
        concerns=tally["concerns"],
        object=read["objected_count"] + tally["object"],
        pending=len(checks) - len(answered),
        in_meeting=InMeetingCounts(
            yes=read["yes_count"],
            hedged=read["hedged_count"],
            silent=read["silent_count"],
            objected=read["objected_count"],
            talked_over=read["talked_over_count"],
        ),
    )
    unsigned_ok = len(answered) >= UNSIGNED_NOTES_MIN_ANSWERS
    notes = [
        AgreementNote(
            text=c["note"],
            author_name=(
                (records.person_name(c["display_name"], c["email"]) or "A former member")
                if c["share_name"]
                else None
            ),
            answer=c["answer"],
            at=c["answered_at"],
        )
        for c in answered
        if c["note"] and (c["share_name"] or unsigned_ok)
    ]
    # Unsigned notes carry no order that could tie them to a person: newest named first, then
    # the unsigned ones by their text.
    view.notes = sorted(
        notes, key=lambda n: (n.author_name is None, n.text if n.author_name is None else "")
    )
    return view


# --- resume points ----------------------------------------------------------------------------

_RP_SELECT = """
SELECT r.*, m.title AS meeting_title,
       CASE WHEN m.status = 'ended' THEN COALESCE(m.closed_at, m.updated_at) END AS meeting_ended_at
FROM resume_points r JOIN meetings m ON m.id = r.meeting_id AND m.deleted_at IS NULL
"""


async def _resume(session: AsyncSession, row: Any) -> ResumePoint:
    raw_positions = row["positions"] if isinstance(row["positions"], list) else []
    ids = {uuid.UUID(p["user_id"]) for p in raw_positions if p.get("user_id")}
    if row["cut_off_user_id"]:
        ids.add(row["cut_off_user_id"])
    who = await names(session, ids)
    positions = [
        ResumePosition(
            user_id=p.get("user_id"),
            name=who.get(uuid.UUID(p["user_id"])) if p.get("user_id") else None,
            quote=p["quote"],
            start_ms=p["start_ms"],
            end_ms=p["end_ms"],
        )
        for p in raw_positions
    ]
    cut = None
    if row["cut_off_user_id"] and row["cut_off_quote"] and row["cut_off_start_ms"] is not None:
        cut = ResumeCutOff(
            user_id=row["cut_off_user_id"],
            name=who.get(row["cut_off_user_id"], "Someone"),
            quote=row["cut_off_quote"],
            start_ms=row["cut_off_start_ms"],
            end_ms=row["cut_off_end_ms"] or row["cut_off_start_ms"],
        )
    return ResumePoint(
        id=row["id"],
        meeting_id=row["meeting_id"],
        meeting_title=records._title(row["meeting_title"]),
        meeting_ended_at=row["meeting_ended_at"],
        reason=row["reason"],
        topic=row["topic"],
        headline=resume_headline(
            row["reason"], row["topic"], first_name(cut.name) if cut else None
        ),
        positions=positions,
        cut_off=cut,
        agenda_item_id=row["agenda_item_id"],
        status=row["status"],
        continued_in_meeting_id=row["continued_in_meeting_id"],
        created_at=row["created_at"],
    )


async def saved_point(session: AsyncSession, meeting_id: uuid.UUID) -> ResumePoint | None:
    row = (
        (
            await session.execute(
                text(_RP_SELECT + "WHERE r.meeting_id = :m"), {"m": str(meeting_id)}
            )
        )
        .mappings()
        .first()
    )
    return await _resume(session, row) if row else None


async def incoming_point(session: AsyncSession, meeting_id: uuid.UUID) -> ResumePoint | None:
    """The thread this meeting picks up: one already resolved here, else an open one from the
    meeting it follows, else from the most recent ended meeting (30 days) with the same people.
    Only from a source the brain's rule lets into this meeting (a confidential source only when
    everyone here was in it)."""
    m = {"m": str(meeting_id)}
    resolved = (
        (
            await session.execute(
                text(
                    _RP_SELECT + "WHERE r.resolved_in_meeting_id = :m "
                    "ORDER BY r.resolved_at DESC LIMIT 1"
                ),
                m,
            )
        )
        .mappings()
        .first()
    )
    if resolved is not None:
        return await _resume(session, resolved)
    here = (
        (
            await session.execute(
                text(
                    "SELECT previous_meeting_id, COALESCE(scheduled_start, created_at) AS at "
                    "FROM meetings WHERE id = :m"
                ),
                m,
            )
        )
        .mappings()
        .one()
    )
    candidates: list[Any] = []
    if here["previous_meeting_id"] is not None:
        candidates = list(
            (
                await session.execute(
                    text(_RP_SELECT + "WHERE r.meeting_id = :p AND r.status = 'open'"),
                    {"p": str(here["previous_meeting_id"])},
                )
            )
            .mappings()
            .all()
        )
    if not candidates:
        candidates = list(
            (
                await session.execute(
                    text(
                        "WITH mine AS (SELECT user_id FROM meeting_participants "
                        "WHERE meeting_id = :m), "
                        "scored AS ( "
                        "  SELECT r.id, "
                        "    (SELECT count(*) FROM meeting_participants p "
                        "       WHERE p.meeting_id = r.meeting_id "
                        "       AND p.user_id IN (SELECT user_id FROM mine)) AS shared, "
                        "    (SELECT count(DISTINCT u) FROM (SELECT user_id AS u FROM mine UNION "
                        "       SELECT user_id FROM meeting_participants p "
                        "       WHERE p.meeting_id = r.meeting_id) x) "
                        "       AS total "
                        "  FROM resume_points r JOIN meetings s ON s.id = r.meeting_id "
                        "  WHERE r.status = 'open' AND r.meeting_id <> :m "
                        "    AND s.deleted_at IS NULL "
                        "    AND s.status = 'ended' "
                        "    AND r.created_at > now() - CAST(:look AS interval) "
                        "    AND COALESCE(s.scheduled_start, s.created_at) "
                        "        <= CAST(:at AS timestamptz)) "
                        + _RP_SELECT.replace(
                            "FROM resume_points r",
                            "FROM resume_points r JOIN scored ON scored.id = r.id",
                        )
                        + "WHERE scored.shared >= 2 "
                        "AND scored.shared::float / GREATEST(scored.total, 1) >= :j "
                        "ORDER BY r.created_at DESC LIMIT 3"
                    ),
                    {**m, "look": RESUME_LOOKBACK, "at": here["at"], "j": SAME_PEOPLE_JACCARD},
                )
            )
            .mappings()
            .all()
        )
    for row in candidates:
        if await may_share_source(session, row["meeting_id"], meeting_id):
            return await _resume(session, row)
    return None
