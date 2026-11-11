"""Decisions that know why, blast radius and settle threads (Beyond the meeting, Layer 1).

Shared by the `agent-results` consumer and the routes (`routes/decision_why.py`,
`routes/blast_radius.py`, `routes/settlements.py`, `routes/internal_assumptions.py`).

Where the facts come from. `agent-runtime-service` reads the decision and the transcript around
it (`GET /internal/decisions/{id}/why-source`), asks `decision_assumptions@v1` for the stated
reasons and `decision_ripple@v1` for the goals and actions it moves, and emits ONE
`ProactiveAgent` / `alert` record with `alert.alert_type = "decision_why"` and `surfaced: false`
(notification-service delivers only surfaced alerts, so nothing reaches a person from it). Its
watcher (`assumption_check@v1`, on every later confirmed decision and every ended meeting)
emits `alert_type = "assumption_broken"` the same way. The existing `conflict` alert is read too:
a clash between two confirmed decisions, which opens a settle thread when their owners are on
different teams. No Avro change: the alert type rides `payload_json`.

The agent proposes, people decide: an assumption is only ever marked broken (never removed or
changed by an agent), the owner is told with both quotes side by side and chooses re-confirm,
reopen or a 15-minute revisit; a settle thread changes nothing until an owner acts.

Privacy. Everything here is read through the tenant session (RLS). A decision is visible to the
participants of its meeting. Evidence from a meeting, and another team's decision, are shown
only to people who were in that meeting; anyone else gets the fact without the words
(`redacted`). The owner's notification follows the same rule.
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import not_found
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from services.core_api.records import display_name
from services.core_api.visibility import is_visible

PRODUCER = "core-api"
KINDS = frozenset({"price", "date", "metric", "dependency", "person", "other"})
COMPARATORS = frozenset({"lt", "lte", "gt", "gte", "eq"})
JIRA_KEY = re.compile(r"^[A-Z][A-Z0-9_]{0,19}-[0-9]{1,9}$")
DEAL_ID = re.compile(r"^[0-9]{1,30}$")
MAX_AGENT_ASSUMPTIONS = 5
MAX_LIVE_ASSUMPTIONS = 12
MAX_GOALS = 5
MAX_ACTIONS = 12
MIN_ASSUMPTION_CONFIDENCE = 0.5
MIN_RIPPLE_CONFIDENCE = 0.5
MIN_BREAK_CONFIDENCE = 0.7
SETTLE_CONFIDENCE = 0.7  # a clash surfaced to people (the ProactiveAgent "surfaced" band)
REVISIT_MINUTES = 15
MAX_PEOPLE = 12
SNIPPET = 140


class Person(BaseModel):
    user_id: uuid.UUID
    name: str


def clip(value: Any, limit: int) -> str:
    text_ = " ".join(str(value or "").split())
    return text_ if len(text_) <= limit else text_[: limit - 3].rstrip() + "..."


def opt_text(value: Any, limit: int) -> str | None:
    if not isinstance(value, str):
        return None
    out = clip(value, limit)
    return out or None


def as_uuid(raw: Any) -> uuid.UUID | None:
    if raw is None:
        return None
    try:
        return uuid.UUID(str(raw))
    except (TypeError, ValueError):
        return None


def as_conf(raw: Any) -> float | None:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    c = float(raw)
    return c if 0.0 <= c <= 1.0 else None


def as_list(raw: Any) -> list[Any]:
    return raw if isinstance(raw, list) else []


def as_dict(raw: Any) -> dict[str, Any]:
    return raw if isinstance(raw, dict) else {}


# --- people, teams, visibility ------------------------------------------------------------


async def people(session: AsyncSession, ids: list[uuid.UUID | None]) -> dict[uuid.UUID, Person]:
    wanted = sorted({str(i) for i in ids if i is not None})
    if not wanted:
        return {}
    rows = await session.execute(
        text(
            "SELECT id, email FROM users WHERE id = ANY(CAST(:ids AS uuid[])) "
            "AND deleted_at IS NULL"
        ),
        {"ids": wanted},
    )
    return {r[0]: Person(user_id=r[0], name=display_name(r[1]) or "A colleague") for r in rows}


async def person(session: AsyncSession, user_id: uuid.UUID | None) -> Person | None:
    if user_id is None:
        return None
    return (await people(session, [user_id])).get(user_id)


async def in_meeting(
    session: AsyncSession, meeting_id: uuid.UUID | None, user_id: uuid.UUID | None
) -> bool:
    if meeting_id is None or user_id is None:
        return False
    row = (
        await session.execute(
            text(
                "SELECT 1 FROM meeting_participants p JOIN meetings m ON m.id = p.meeting_id "
                "WHERE p.meeting_id = :m AND p.user_id = :u AND m.deleted_at IS NULL"
            ),
            {"m": str(meeting_id), "u": str(user_id)},
        )
    ).first()
    return row is not None


async def teams_of(session: AsyncSession, user_id: uuid.UUID | None) -> list[tuple[uuid.UUID, str]]:
    if user_id is None:
        return []
    rows = await session.execute(
        text(
            "SELECT t.id, t.name FROM team_members tm JOIN teams t ON t.id = tm.team_id "
            "WHERE tm.user_id = :u AND t.deleted_at IS NULL ORDER BY t.name, t.id"
        ),
        {"u": str(user_id)},
    )
    return [(r[0], r[1]) for r in rows]


async def team_name(session: AsyncSession, user_id: uuid.UUID | None) -> str | None:
    teams = await teams_of(session, user_id)
    return teams[0][1] if teams else None


async def same_team(session: AsyncSession, a: uuid.UUID | None, b: uuid.UUID | None) -> bool:
    """Same owner, or a shared team. A person with no team is their own team."""
    if a is None or b is None:
        return a == b
    if a == b:
        return True
    ta = {t for t, _ in await teams_of(session, a)}
    tb = {t for t, _ in await teams_of(session, b)}
    return bool(ta & tb)


_DECISION_SQL = (
    "SELECT d.id, d.meeting_id, d.content, d.rationale, d.status, d.confirmed_by, d.quote, "
    "d.quote_speaker_id, d.quote_start_ms, d.created_at, "
    "COALESCE(d.status_changed_at, d.created_at) AS decided_at, m.title AS meeting_title, "
    "m.confidential FROM decisions d JOIN meetings m ON m.id = d.meeting_id "
    "WHERE d.id = :id AND d.deleted_at IS NULL AND m.deleted_at IS NULL"
)


async def decision_row(session: AsyncSession, decision_id: uuid.UUID) -> dict[str, Any] | None:
    row = (await session.execute(text(_DECISION_SQL), {"id": str(decision_id)})).mappings().first()
    return dict(row) if row is not None else None


async def visible_decision(
    session: AsyncSession, decision_id: uuid.UUID, user_id: uuid.UUID | None
) -> dict[str, Any]:
    """A confirmed (or superseded) decision the caller may see, or 404."""
    row = await decision_row(session, decision_id)
    if row is None or row["status"] not in ("confirmed", "superseded"):
        raise not_found()
    if not await in_meeting(session, row["meeting_id"], user_id):
        raise not_found()
    return row


# --- notifications -------------------------------------------------------------------------


async def notify(
    session: AsyncSession,
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    *,
    source_event_id: str,
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
                "title": title[:200],
                "body": body[:2000],
                "link": link,
            },
        ),
    )


def decision_link(meeting_id: uuid.UUID, decision_id: uuid.UUID) -> str:
    return f"/meetings/{meeting_id}/decisions/{decision_id}"


# --- slots and booking -----------------------------------------------------------------------

WORK_START = time(9, 0)
WORK_END = time(17, 0)
SLOT_STEP = timedelta(minutes=30)
LEAD = timedelta(hours=2)
SEARCH_DAYS = 5
DEFAULT_MEETING = timedelta(minutes=30)


@dataclass(frozen=True)
class Slot:
    start: datetime
    end: datetime


async def busy(
    session: AsyncSession, user_ids: list[uuid.UUID], start: datetime, end: datetime
) -> list[tuple[datetime, datetime]]:
    if not user_ids:
        return []
    rows = await session.execute(
        text(
            "SELECT DISTINCT m.scheduled_start, m.scheduled_end FROM meetings m "
            "JOIN meeting_participants p ON p.meeting_id = m.id "
            "WHERE p.user_id = ANY(CAST(:ids AS uuid[])) AND m.deleted_at IS NULL "
            "AND m.scheduled_start IS NOT NULL AND m.scheduled_start < :end "
            "AND COALESCE(m.scheduled_end, m.scheduled_start + interval '30 minutes') > :start "
            "AND m.status <> 'ended'"
        ),
        {"ids": [str(u) for u in user_ids], "start": start, "end": end},
    )
    return [(r[0], r[1] or r[0] + DEFAULT_MEETING) for r in rows]


def first_free(
    now: datetime, taken: list[tuple[datetime, datetime]], minutes: int = REVISIT_MINUTES
) -> Slot:
    """The first `minutes` slot on :00/:30 in working hours (09:00-17:00 UTC, Mon-Fri), at
    least two hours ahead, that overlaps none of `taken`. Falls back to the first candidate."""
    length = timedelta(minutes=minutes)
    cursor = (now + LEAD).astimezone(UTC)
    cursor = cursor.replace(second=0, microsecond=0)
    if cursor.minute not in (0, 30):
        cursor = cursor.replace(minute=0) + (SLOT_STEP if cursor.minute < 30 else 2 * SLOT_STEP)
    fallback: Slot | None = None
    limit = cursor + timedelta(days=SEARCH_DAYS + 3)
    while cursor < limit:
        day_end = datetime.combine(cursor.date(), WORK_END, tzinfo=UTC)
        day_start = datetime.combine(cursor.date(), WORK_START, tzinfo=UTC)
        if cursor.weekday() >= 5 or cursor + length > day_end:
            nxt = cursor.date() + timedelta(days=1)
            cursor = datetime.combine(nxt, WORK_START, tzinfo=UTC)
            continue
        if cursor < day_start:
            cursor = day_start
            continue
        slot = Slot(cursor, cursor + length)
        fallback = fallback or slot
        if not any(s < slot.end and e > slot.start for s, e in taken):
            return slot
        cursor += SLOT_STEP
    assert fallback is not None
    return fallback


async def propose_slot(
    session: AsyncSession, user_ids: list[uuid.UUID], now: datetime | None = None
) -> Slot:
    now = now or datetime.now(UTC)
    horizon = now + timedelta(days=SEARCH_DAYS + 4)
    return first_free(now, await busy(session, user_ids, now, horizon))


async def decision_people(
    session: AsyncSession, decision: dict[str, Any], extra: list[uuid.UUID | None] = ()  # type: ignore[assignment]
) -> list[uuid.UUID]:
    """The decision's people: its owner, its meeting's participants, whoever stated a reason."""
    rows = await session.execute(
        text(
            "SELECT p.user_id FROM meeting_participants p JOIN users u ON u.id = p.user_id "
            "AND u.deleted_at IS NULL WHERE p.meeting_id = :m ORDER BY u.email, p.user_id"
        ),
        {"m": str(decision["meeting_id"])},
    )
    ordered: list[uuid.UUID] = []
    for uid in [decision.get("confirmed_by"), *[r[0] for r in rows], *extra]:
        if uid is not None and uid not in ordered:
            ordered.append(uid)
    return ordered[:MAX_PEOPLE]


async def book_meeting(
    session: AsyncSession,
    ctx: Any,
    *,
    title: str,
    start: datetime,
    minutes: int,
    attendees: list[uuid.UUID],
    agenda_title: str,
) -> dict[str, Any]:
    """Create the meeting through the meetings route itself (participants, agenda, the
    `meeting-events` outbox row and the audit row in the caller's transaction)."""
    from fastapi import Response

    from services.core_api.routes.meetings import create_meeting
    from services.core_api.schemas import AgendaItemCreate, MeetingCreate

    end = start + timedelta(minutes=minutes)
    body = MeetingCreate(
        title=clip(title, 200),
        scheduled_start=start,
        scheduled_end=end,
        participant_ids=[u for u in attendees if u != ctx.user_id],
        agenda=[AgendaItemCreate(title=clip(agenda_title, 200), duration_minutes=minutes)],
    )
    detail = await create_meeting(body, Response(), ctx, session)
    return {"meeting_id": detail.id, "start": start, "end": end, "title": body.title}


def parse_start(raw: datetime | None) -> datetime | None:
    if raw is None:
        return None
    return raw if raw.tzinfo else raw.replace(tzinfo=UTC)


# --- applying agent output -------------------------------------------------------------------


def _date(raw: Any) -> date | None:
    if not isinstance(raw, str):
        return None
    try:
        return date.fromisoformat(raw.strip()[:10])
    except ValueError:
        return None


def clean_watch(raw: Any) -> dict[str, Any]:
    w = raw if isinstance(raw, dict) else {}
    value = w.get("value")
    comparator = w.get("comparator")
    unit = opt_text(w.get("unit"), 20)
    key = w.get("issue_key") or w.get("jira_key")
    key = key.strip().upper() if isinstance(key, str) else None
    deal = w.get("deal_id")
    deal = (
        str(deal).strip() if isinstance(deal, (str, int)) and not isinstance(deal, bool) else None
    )
    return {
        "watch_date": _date(w.get("date")),
        "watch_value": (
            float(value)
            if isinstance(value, (int, float)) and not isinstance(value, bool)
            else None
        ),
        "watch_comparator": comparator if comparator in COMPARATORS else None,
        "watch_unit": unit,
        "watch_jira_key": key if key and JIRA_KEY.match(key) else None,
        "watch_deal_id": deal if deal and DEAL_ID.match(deal) else None,
    }


async def _user_or_none(session: AsyncSession, raw: Any) -> uuid.UUID | None:
    uid = as_uuid(raw)
    if uid is None or not await is_visible(session, "users", uid):
        return None
    return uid


async def mark_read(session: AsyncSession, org_id: uuid.UUID, decision_id: uuid.UUID) -> None:
    await session.execute(
        text(
            "INSERT INTO decision_why_state (decision_id, org_id, read_at) VALUES (:d, :o, now()) "
            "ON CONFLICT (decision_id) DO UPDATE SET read_at = COALESCE("
            "decision_why_state.read_at, now()), updated_at = now()"
        ),
        {"d": str(decision_id), "o": str(org_id)},
    )


async def customer_meetings(session: AsyncSession, meeting_ids: list[uuid.UUID]) -> set[uuid.UUID]:
    """Meetings that listed external guests (a customer conversation)."""
    if not meeting_ids:
        return set()
    rows = await session.execute(
        text(
            "SELECT meeting_id FROM meeting_followups WHERE meeting_id = ANY(CAST(:ids AS uuid[])) "
            "AND cardinality(guest_emails) > 0"
        ),
        {"ids": [str(m) for m in meeting_ids]},
    )
    return {r[0] for r in rows}


async def apply_decision_why(
    session: AsyncSession, org_id: uuid.UUID, meeting_id: uuid.UUID, payload: dict[str, Any]
) -> bool:
    why = payload.get("why")
    if not isinstance(why, dict):
        return False
    decision_id = as_uuid(why.get("decision_id"))
    if decision_id is None:
        return False
    decision = await decision_row(session, decision_id)
    if (
        decision is None
        or decision["meeting_id"] != meeting_id
        or decision["status"] not in ("confirmed", "superseded")
    ):
        return False  # gone, or not this meeting's: nothing to attach it to
    written = False
    raw_assumptions = as_list(why.get("assumptions"))
    for raw in raw_assumptions[:MAX_AGENT_ASSUMPTIONS]:
        if not isinstance(raw, dict):
            continue
        kind = raw.get("kind")
        statement = opt_text(raw.get("statement"), 300)
        confidence = as_conf(raw.get("confidence"))
        if kind not in KINDS or not statement:
            continue
        if confidence is not None and confidence < MIN_ASSUMPTION_CONFIDENCE:
            continue
        watch = clean_watch(raw.get("watch"))
        inserted = (
            await session.execute(
                text(
                    "INSERT INTO decision_assumptions (org_id, decision_id, kind, statement, "
                    "quote, stated_by, source, confidence, watch_date, watch_value, "
                    "watch_comparator, watch_unit, watch_jira_key, watch_deal_id) "
                    "VALUES (:o, :d, :k, :s, :q, :by, 'agent', :c, :watch_date, :watch_value, "
                    ":watch_comparator, :watch_unit, :watch_jira_key, :watch_deal_id) "
                    "ON CONFLICT (decision_id, md5(lower(statement))) WHERE source = 'agent' "
                    "DO NOTHING RETURNING id"
                ),
                {
                    "o": str(org_id),
                    "d": str(decision_id),
                    "k": kind,
                    "s": statement,
                    "q": opt_text(raw.get("quote"), 600),
                    "by": await _user_or_none(session, raw.get("speaker_user_id")),
                    "c": confidence,
                    **watch,
                },
            )
        ).first()
        written = written or inserted is not None

    ripple = as_dict(why.get("ripple"))
    goals = as_list(ripple.get("goals"))
    for raw in goals[:MAX_GOALS]:
        if not isinstance(raw, dict):
            continue
        goal_id = as_uuid(raw.get("goal_id"))
        label = opt_text(raw.get("label"), 300)
        effect = raw.get("effect")
        confidence = as_conf(raw.get("confidence"))
        if goal_id is None or not label or effect not in ("advances", "risks"):
            continue
        if confidence is not None and confidence < MIN_RIPPLE_CONFIDENCE:
            continue
        written = (
            await _link(
                session,
                org_id,
                decision_id,
                kind="goal",
                target=str(goal_id),
                label=label,
                effect=effect,
                why=opt_text(raw.get("why"), 300),
                ring=2,
                confidence=confidence or 1.0,
                source="agent",
            )
            or written
        )

    actions = as_list(ripple.get("actions"))
    wanted: dict[uuid.UUID, dict[str, Any]] = {}
    for raw in actions[:MAX_ACTIONS]:
        if not isinstance(raw, dict):
            continue
        action_id = as_uuid(raw.get("action_id"))
        effect = raw.get("effect")
        confidence = as_conf(raw.get("confidence"))
        if action_id is None or effect not in ("changes", "blocks", "unblocks"):
            continue
        if confidence is not None and confidence < MIN_RIPPLE_CONFIDENCE:
            continue
        wanted[action_id] = raw
    if wanted:
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT id, meeting_id, content FROM action_items "
                        "WHERE id = ANY(CAST(:ids AS uuid[])) AND deleted_at IS NULL "
                        "AND status IN ('confirmed', 'done')"
                    ),
                    {"ids": [str(a) for a in wanted]},
                )
            )
            .mappings()
            .all()
        )
        customers = await customer_meetings(
            session, [r["meeting_id"] for r in rows if r["meeting_id"] is not None]
        )
        for r in rows:
            raw = wanted[r["id"]]
            customer = r["meeting_id"] in customers
            written = (
                await _link(
                    session,
                    org_id,
                    decision_id,
                    kind="customer_action" if customer else "action",
                    target=str(r["id"]),
                    label=clip(r["content"], 300),
                    effect=raw["effect"],
                    why=opt_text(raw.get("why"), 300),
                    ring=3 if customer else 1,
                    confidence=as_conf(raw.get("confidence")) or 1.0,
                    source="graph" if raw.get("source") == "graph" else "agent",
                )
                or written
            )
    await mark_read(session, org_id, decision_id)
    return True if raw_assumptions or goals or actions else written


async def _link(
    session: AsyncSession,
    org_id: uuid.UUID,
    decision_id: uuid.UUID,
    *,
    kind: str,
    target: str,
    label: str,
    effect: str,
    why: str | None,
    ring: int,
    confidence: float,
    source: str,
    detail: str = "{}",
) -> bool:
    row = (
        await session.execute(
            text(
                "INSERT INTO decision_ripple_links (org_id, decision_id, target_kind, target_id, "
                "label, effect, why, ring, confidence, source, detail) VALUES (:o, :d, :k, :t, "
                ":l, :e, :w, :r, :c, :s, CAST(:x AS jsonb)) "
                "ON CONFLICT (decision_id, target_kind, target_id) DO UPDATE SET label = "
                "EXCLUDED.label, effect = EXCLUDED.effect, why = COALESCE(EXCLUDED.why, "
                "decision_ripple_links.why), confidence = EXCLUDED.confidence, detail = "
                "EXCLUDED.detail, updated_at = now() RETURNING (xmax = 0) AS inserted"
            ),
            {
                "o": str(org_id),
                "d": str(decision_id),
                "k": kind,
                "t": target,
                "l": label,
                "e": effect,
                "w": why,
                "r": ring,
                "c": max(0.0, min(1.0, confidence)),
                "s": source,
                "x": detail,
            },
        )
    ).first()
    return bool(row and row[0])


async def link_deal(
    session: AsyncSession,
    org_id: uuid.UUID,
    decision_id: uuid.UUID,
    *,
    deal_id: str,
    label: str,
    detail: str,
) -> bool:
    return await _link(
        session,
        org_id,
        decision_id,
        kind="deal",
        target=deal_id,
        label=label,
        effect="commits",
        why="An open deal with a customer from the meetings this decision touches.",
        ring=3,
        confidence=1.0,
        source="hubspot",
        detail=detail,
    )


# --- assumption breaks -----------------------------------------------------------------------


async def record_break(
    session: AsyncSession,
    org_id: uuid.UUID,
    assumption_id: uuid.UUID,
    *,
    source: str,
    rationale: str,
    confidence: float,
    dedupe_key: str,
    quote: str | None = None,
    speaker_id: uuid.UUID | None = None,
    source_meeting_id: uuid.UUID | None = None,
    source_decision_id: uuid.UUID | None = None,
    source_ref: str | None = None,
    source_url: str | None = None,
) -> uuid.UUID | None:
    """Break a WATCHING assumption of a confirmed decision with this evidence, once per
    `dedupe_key`, and tell the decision's owner. None when nothing changed."""
    a = (
        (
            await session.execute(
                text(
                    "SELECT a.id, a.decision_id, a.statement, a.status, d.meeting_id, "
                    "d.content, d.confirmed_by, d.status AS d_status FROM decision_assumptions a "
                    "JOIN decisions d ON d.id = a.decision_id AND d.deleted_at IS NULL "
                    "WHERE a.id = :a FOR UPDATE OF a"
                ),
                {"a": str(assumption_id)},
            )
        )
        .mappings()
        .first()
    )
    if a is None or a["status"] != "watching" or a["d_status"] != "confirmed":
        return None
    if source_meeting_id is not None and source_meeting_id == a["meeting_id"]:
        return None  # a meeting cannot break its own reasons
    evidence = (
        await session.execute(
            text(
                "INSERT INTO assumption_evidence (org_id, assumption_id, decision_id, source, "
                "quote, speaker_id, source_meeting_id, source_decision_id, source_ref, "
                "source_url, rationale, confidence, dedupe_key) VALUES (:o, :a, :d, :src, :q, "
                ":sp, :sm, :sd, :ref, :url, :why, :c, :k) "
                "ON CONFLICT (assumption_id, dedupe_key) DO NOTHING RETURNING id"
            ),
            {
                "o": str(org_id),
                "a": str(assumption_id),
                "d": str(a["decision_id"]),
                "src": source,
                "q": opt_text(quote, 600),
                "sp": str(speaker_id) if speaker_id else None,
                "sm": str(source_meeting_id) if source_meeting_id else None,
                "sd": str(source_decision_id) if source_decision_id else None,
                "ref": opt_text(source_ref, 200),
                "url": source_url if source_url and source_url.startswith("https://") else None,
                "why": clip(rationale, 300) or "New evidence contradicts this reason.",
                "c": max(0.0, min(1.0, confidence)),
                "k": dedupe_key[:300],
            },
        )
    ).first()
    if evidence is None:
        return None
    await session.execute(
        text(
            "UPDATE decision_assumptions SET status = 'broken', last_checked_at = now(), "
            "updated_at = now() WHERE id = :a"
        ),
        {"a": str(assumption_id)},
    )
    owner = a["confirmed_by"]
    if owner is not None:
        sees = source not in ("meeting", "decision") or await in_meeting(
            session, source_meeting_id, owner
        )
        body = f'You decided "{clip(a["content"], SNIPPET)}" because {clip(a["statement"], 200)}.'
        if sees and quote:
            body += f' Now: "{clip(quote, 300)}"'
        elif not sees:
            body += " New evidence from a meeting you were not in says this may no longer hold."
        await notify(
            session,
            org_id,
            owner,
            source_event_id=f"assumption-break:{evidence[0]}",
            title=f'A reason for your decision "{clip(a["content"], 40)}" is no longer true',
            body=body,
            link=(
                f"{decision_link(a['meeting_id'], a['decision_id'])}"
                f"?assumption={assumption_id}#why"
            ),
        )
    return uuid.UUID(str(evidence[0]))


async def apply_breaks(
    session: AsyncSession, org_id: uuid.UUID, meeting_id: uuid.UUID, payload: dict[str, Any]
) -> bool:
    breaks = payload.get("breaks")
    if not isinstance(breaks, list):
        return False
    written = False
    for raw in breaks[:20]:
        if not isinstance(raw, dict):
            continue
        assumption_id = as_uuid(raw.get("assumption_id"))
        confidence = as_conf(raw.get("confidence"))
        quote = opt_text(raw.get("evidence_quote"), 600)
        source = "decision" if raw.get("source") == "decision" else "meeting"
        if assumption_id is None or confidence is None or confidence < MIN_BREAK_CONFIDENCE:
            continue
        if not quote:
            continue
        source_meeting = as_uuid(raw.get("source_meeting_id")) or meeting_id
        source_decision = as_uuid(raw.get("source_decision_id"))
        if source_meeting != meeting_id:
            continue  # evidence must come from the meeting the record names (checked in tenant)
        if source_decision is not None and await decision_row(session, source_decision) is None:
            source_decision = None
        key = (
            f"decision:{source_decision}"
            if source == "decision" and source_decision
            else f"meeting:{source_meeting}"
        )
        made = await record_break(
            session,
            org_id,
            assumption_id,
            source=source,
            rationale=opt_text(raw.get("rationale"), 300) or "",
            confidence=confidence,
            dedupe_key=key,
            quote=quote,
            speaker_id=await _user_or_none(session, raw.get("speaker_user_id")),
            source_meeting_id=source_meeting,
            source_decision_id=source_decision,
        )
        written = written or made is not None
    return written


# --- clashes and settle threads ----------------------------------------------------------------


async def apply_conflict(session: AsyncSession, org_id: uuid.UUID, alert: dict[str, Any]) -> bool:
    subject = as_dict(alert.get("subject"))
    related = as_list(alert.get("related"))
    first = related[0] if related and isinstance(related[0], dict) else {}
    decision_id = as_uuid(subject.get("id"))
    other_id = as_uuid(first.get("id"))
    confidence = as_conf(alert.get("confidence"))
    if decision_id is None or other_id is None or confidence is None or decision_id == other_id:
        return False
    a, b = await decision_row(session, decision_id), await decision_row(session, other_id)
    if a is None or b is None or a["status"] != "confirmed" or b["status"] != "confirmed":
        return False
    # The redacted per-recipient view carries an empty label and a generic rationale: keep the
    # model's own rationale from the full view whichever copy arrives first.
    full = bool(str(first.get("label") or "").strip())
    rationale = clip(alert.get("rationale"), 600) if full else ""
    cross = not await same_team(session, a["confirmed_by"], b["confirmed_by"])
    row = (
        await session.execute(
            text(
                "INSERT INTO decision_clashes (org_id, decision_id, other_decision_id, "
                "confidence, rationale, cross_team) VALUES (:o, :a, :b, :c, :r, :x) "
                "ON CONFLICT (decision_id, other_decision_id) DO UPDATE SET rationale = CASE "
                "WHEN EXCLUDED.rationale <> '' THEN EXCLUDED.rationale "
                "ELSE decision_clashes.rationale END RETURNING id, (xmax = 0) AS inserted"
            ),
            {
                "o": str(org_id),
                "a": str(decision_id),
                "b": str(other_id),
                "c": confidence,
                "r": rationale,
                "x": cross,
            },
        )
    ).one()
    clash_id, inserted = row[0], row[1]
    if not inserted:
        return False
    if (
        cross
        and confidence > SETTLE_CONFIDENCE
        and a["confirmed_by"] is not None
        and b["confirmed_by"] is not None
    ):
        settlement = (
            await session.execute(
                text(
                    "INSERT INTO decision_settlements (org_id, clash_id, decision_a_id, "
                    "decision_b_id, owner_a, owner_b) VALUES (:o, :c, :a, :b, :oa, :ob) "
                    "ON CONFLICT (clash_id) DO NOTHING RETURNING id"
                ),
                {
                    "o": str(org_id),
                    "c": str(clash_id),
                    "a": str(decision_id),
                    "b": str(other_id),
                    "oa": str(a["confirmed_by"]),
                    "ob": str(b["confirmed_by"]),
                },
            )
        ).first()
        if settlement is not None:
            for owner, mine, theirs in (
                (a["confirmed_by"], a, b),
                (b["confirmed_by"], b, a),
            ):
                await notify(
                    session,
                    org_id,
                    owner,
                    source_event_id=f"settle:{settlement[0]}:opened",
                    title="Two decisions may clash: settle it together",
                    body=(
                        f'Your decision "{clip(mine["content"], SNIPPET)}" may pull against a '
                        f"decision from another team"
                        + (
                            f' ("{clip(theirs["content"], SNIPPET)}").'
                            if await in_meeting(session, theirs["meeting_id"], owner)
                            else "."
                        )
                        + " Keep both, let one replace the other, or meet."
                    ),
                    link=f"/work/settle/{settlement[0]}",
                    priority=2,
                )
    return True


# --- summaries ---------------------------------------------------------------------------------


def plural(n: int, one: str, many: str) -> str:
    return f"{n} {one if n == 1 else many}"


def summary_line(actions: int, goals: int, decisions: int, customers: int, teams: list[str]) -> str:
    parts: list[str] = []
    if actions:
        parts.append(f"changes {plural(actions, 'task', 'tasks')}")
    if goals:
        parts.append(f"moves {plural(goals, 'goal', 'goals')}")
    if decisions:
        named = sorted({t for t in teams if t})
        suffix = f" from {named[0]}" if len(named) == 1 else ""
        parts.append(f"clashes with {plural(decisions, 'decision', 'decisions')}{suffix}")
    if customers:
        parts.append(f"touches {plural(customers, 'customer commitment', 'customer commitments')}")
    if not parts:
        return "No ripple found"
    line = ", ".join(parts)
    return line[:1].upper() + line[1:]


async def apply_alert(session: AsyncSession, org_id: uuid.UUID, value: dict[str, Any]) -> bool:
    """One `ProactiveAgent` / `alert` record from the `agent-results` consumer (RLS set, inbox
    claimed). True when something was written."""
    raw = value.get("payload_json")
    try:
        payload = json.loads(raw) if isinstance(raw, str) else None
    except ValueError:
        return False
    alert = payload.get("alert") if isinstance(payload, dict) else None
    if not isinstance(payload, dict) or not isinstance(alert, dict):
        return False
    alert_type = alert.get("alert_type")
    if alert_type == "conflict":
        return await apply_conflict(session, org_id, alert)
    if alert_type not in ("decision_why", "assumption_broken"):
        return False
    meeting_id = as_uuid(value.get("meeting_id"))
    if meeting_id is None or not await is_visible(session, "meetings", meeting_id):
        return False
    if alert_type == "decision_why":
        return await apply_decision_why(session, org_id, meeting_id, payload)
    return await apply_breaks(session, org_id, meeting_id, payload)
