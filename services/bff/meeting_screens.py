"""The three meeting screens of ADR-0039 B8: the pre-meeting brief, the health card and the
agent's catch-up. Screen-shaped reads over core-api's tables, under RLS, like the dashboard; `bff`
owns nothing and writes nothing.

Every route is for a participant of the meeting: another tenant's meeting is a 404 (RLS hides
it), a visible meeting the caller is not in is a 403 `NOT_A_PARTICIPANT`. What each one shows
follows core-api's rules: notes, saved questions and proxy questions are the caller's own; a
confidential previous meeting is never a source for someone who was not in it (B2).

`talk_balance`: speaker timing IS stored, as `transcript_chunks` finals (`speaker_id`,
`start_ms`, `end_ms`, written by `transcription-service`) joined to `meeting_speakers`
(`speaker_id` -> `user_id`). Each resolved user's share is their spoken milliseconds over all
resolved spoken milliseconds. It is null when nothing is stored for the meeting: no finals, no
speaker mapped to a user, or `transcript_chunks` absent from this database (it belongs to
`mytheus-realtime`'s migrations).
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api.brain import latest_statuses, may_see_source
from services.core_api.minutes import personal_minutes, shared_minutes
from services.core_api.schemas import LatestStatus

router = APIRouter(prefix="/bff", tags=["bff"])

TREND_DEPTH = 6
MAX_SAVED = 20


# --- shapes -----------------------------------------------------------------------------------


class BriefMeeting(BaseModel):
    id: uuid.UUID
    title: str | None
    scheduled_start: datetime | None


class BriefAgendaItem(BaseModel):
    id: uuid.UUID
    position: int
    title: str
    duration_minutes: int | None
    status: str
    goal_id: uuid.UUID | None


class BriefAction(BaseModel):
    id: uuid.UUID
    title: str
    due_date: date | None
    latest_status: LatestStatus | None


class BriefQuestion(BaseModel):
    id: uuid.UUID
    meeting_id: uuid.UUID
    question: str


class BriefProxy(BaseModel):
    user_id: uuid.UUID
    proxy_update: str | None


class PreviousMeeting(BaseModel):
    id: uuid.UUID
    title: str | None


class BriefOpenItem(BaseModel):
    text: str
    owner_user_id: uuid.UUID | None = None
    owner_name: str | None = None
    due: str | None = None
    mine: bool = False


class BriefPromise(BaseModel):
    text: str
    to_name: str | None = None
    due: str | None = None


class BriefMinutes(BaseModel):
    """What the previous meeting's minutes left open (minutes for everyone): its shared open
    items (the caller's own first, `mine`) and unanswered questions, and the caller's own
    promises from their personal minutes of it."""

    open_items: list[BriefOpenItem]
    unanswered_questions: list[str]
    my_promises: list[BriefPromise]


class MeetingBrief(BaseModel):
    meeting: BriefMeeting
    my_agenda_items: list[BriefAgendaItem]
    my_open_actions: list[BriefAction]
    saved_questions: list[BriefQuestion]
    proxies: list[BriefProxy]
    previous_meeting: PreviousMeeting | None
    previous_minutes: BriefMinutes | None = None


class AgendaCoverage(BaseModel):
    covered: int
    total: int


class TalkShare(BaseModel):
    user_id: uuid.UUID
    share: float


class TrendPoint(BaseModel):
    meeting_id: uuid.UUID
    title: str | None
    scheduled_start: datetime | None
    coverage: float | None  # covered / total agenda items; null when there was no agenda
    decisions: int


class MeetingHealth(BaseModel):
    duration_minutes: int | None
    people_hours: float | None
    agenda: AgendaCoverage
    decisions: int
    actions: int
    talk_balance: list[TalkShare] | None
    async_suggestion: bool
    trend: list[TrendPoint]


class CatchUpAgendaItem(BaseModel):
    id: uuid.UUID
    position: int
    title: str
    owner_user_id: uuid.UUID | None
    status: str
    status_source: str | None
    agent_note: str | None


class CatchUpDecision(BaseModel):
    id: uuid.UUID
    content: str
    rationale: str | None


class CatchUpAction(BaseModel):
    id: uuid.UUID
    content: str
    status: str
    due_date: date | None


class CatchUpNote(BaseModel):
    id: uuid.UUID
    content: str
    created_at: datetime


class MeetingCatchUp(BaseModel):
    attendance: str
    agenda: list[CatchUpAgendaItem]
    decisions: list[CatchUpDecision]
    assigned_to_me: list[CatchUpAction]
    my_notes: list[CatchUpNote]
    my_questions: list[str]


# --- helpers ----------------------------------------------------------------------------------


async def _rows(session: AsyncSession, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    return [dict(r) for r in (await session.execute(text(sql), params)).mappings().all()]


async def _meeting_for_participant(
    session: AsyncSession, ctx: RequestContext, meeting_id: uuid.UUID
) -> dict[str, Any]:
    """The meeting plus the caller's participant row; 404 / 403 as the module says."""
    found = await _rows(
        session,
        "SELECT m.id, m.title, m.status, m.scheduled_start, m.scheduled_end, "
        "m.previous_meeting_id, p.user_id AS me, p.attendance, p.proxy_questions "
        "FROM meetings m LEFT JOIN meeting_participants p "
        "  ON p.meeting_id = m.id AND p.user_id = CAST(:u AS uuid) "
        "WHERE m.id = :m AND m.deleted_at IS NULL",
        {"m": str(meeting_id), "u": str(ctx.user_id) if ctx.user_id else None},
    )
    if not found:
        raise not_found()
    if found[0]["me"] is None:
        raise ProblemDetail(
            403, ErrorCode.NOT_A_PARTICIPANT, "You are not a participant of this meeting"
        )
    return found[0]


async def _visible_previous(
    session: AsyncSession, previous_id: uuid.UUID | None, viewer: uuid.UUID | None
) -> PreviousMeeting | None:
    if previous_id is None:
        return None
    found = await _rows(
        session,
        "SELECT id, title FROM meetings WHERE id = :m AND deleted_at IS NULL",
        {"m": str(previous_id)},
    )
    if not found or not await may_see_source(session, previous_id, viewer):
        return None
    return PreviousMeeting(**found[0])


async def _has_transcript(session: AsyncSession) -> bool:
    return bool(
        (
            await session.execute(
                text("SELECT to_regclass('public.transcript_chunks') IS NOT NULL")
            )
        ).scalar()
    )


async def _previous_minutes(
    session: AsyncSession, previous_id: uuid.UUID, viewer: uuid.UUID
) -> BriefMinutes | None:
    shared = await shared_minutes(session, previous_id)
    mine = await personal_minutes(session, previous_id, viewer)
    if (shared is None or shared.status != "ready") and (mine is None or mine.status != "ready"):
        return None
    items = shared.open_items if shared is not None else []
    ordered = sorted(items, key=lambda o: o.owner_user_id != viewer)
    return BriefMinutes(
        open_items=[
            BriefOpenItem(
                text=o.text,
                owner_user_id=o.owner_user_id,
                owner_name=o.owner_name,
                due=o.due,
                mine=o.owner_user_id == viewer,
            )
            for o in ordered
        ],
        unanswered_questions=[
            q.text for q in (shared.unanswered_questions if shared is not None else [])
        ],
        my_promises=[
            BriefPromise(text=p.text, to_name=p.to_name, due=p.due)
            for p in (mine.promises if mine is not None else [])
        ],
    )


# --- routes -----------------------------------------------------------------------------------


@router.get("/meetings/{meeting_id}/brief", response_model=MeetingBrief)
async def meeting_brief(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MeetingBrief:
    """Before a meeting: the caller's agenda items, their open actions from the previous
    meeting with the latest status, their saved questions, who sent their agent, and what the
    previous meeting's minutes left open (`previous_minutes`, null without minutes)."""
    m = await _meeting_for_participant(session, ctx, meeting_id)
    me = str(ctx.user_id)
    agenda = await _rows(
        session,
        "SELECT id, position, title, duration_minutes, status, goal_id "
        "FROM meeting_agenda_items WHERE meeting_id = :m AND owner_user_id = :u "
        "ORDER BY position, id",
        {"m": str(meeting_id), "u": me},
    )
    previous = await _visible_previous(session, m["previous_meeting_id"], ctx.user_id)
    actions: list[dict[str, Any]] = []
    if previous is not None:
        actions = await _rows(
            session,
            "SELECT id, content AS title, due_date FROM action_items WHERE meeting_id = :p "
            "AND assignee_id = :u AND status = 'confirmed' AND deleted_at IS NULL "
            "ORDER BY created_at, id",
            {"p": str(previous.id), "u": me},
        )
    statuses = await latest_statuses(session, [a["id"] for a in actions])
    saved = await _rows(
        session,
        "SELECT id, meeting_id, question FROM agent_suggestions WHERE user_id = :u "
        "AND status = 'saved' AND deleted_at IS NULL ORDER BY created_at DESC, id DESC "
        "LIMIT :lim",
        {"u": me, "lim": MAX_SAVED},
    )
    proxies = await _rows(
        session,
        "SELECT user_id, proxy_update FROM meeting_participants WHERE meeting_id = :m "
        "AND attendance = 'agent' ORDER BY user_id",
        {"m": str(meeting_id)},
    )
    return MeetingBrief(
        previous_minutes=(
            await _previous_minutes(session, previous.id, ctx.user_id)
            if previous is not None and ctx.user_id is not None
            else None
        ),
        meeting=BriefMeeting(id=m["id"], title=m["title"], scheduled_start=m["scheduled_start"]),
        my_agenda_items=[BriefAgendaItem(**a) for a in agenda],
        my_open_actions=[BriefAction(**a, latest_status=statuses.get(a["id"])) for a in actions],
        saved_questions=[BriefQuestion(**q) for q in saved],
        proxies=[BriefProxy(**p) for p in proxies],
        previous_meeting=previous,
    )


_COUNTS = (
    "(SELECT count(*) FROM meeting_agenda_items i WHERE i.meeting_id = m.id "
    " AND i.status = 'covered') AS covered, "
    "(SELECT count(*) FROM meeting_agenda_items i WHERE i.meeting_id = m.id) AS total, "
    "(SELECT count(*) FROM decisions d WHERE d.meeting_id = m.id "
    " AND d.status IN ('confirmed', 'superseded') AND d.deleted_at IS NULL) AS decisions, "
    "(SELECT count(*) FROM action_items a WHERE a.meeting_id = m.id "
    " AND a.status IN ('confirmed', 'done') AND a.deleted_at IS NULL) AS actions"
)


def _coverage(covered: int, total: int) -> float | None:
    return round(covered / total, 3) if total else None


@router.get("/meetings/{meeting_id}/health", response_model=MeetingHealth)
async def meeting_health(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MeetingHealth:
    """After a meeting (409 before it ended): length, people-hours, agenda coverage, what was
    confirmed, who talked, whether it could have been async, and the trend over the meetings it
    follows up (up to 6 back through `previous_meeting_id`, oldest first, then this one).

    `duration_minutes` is the scheduled length (no actual start and end are stored); without a
    schedule it falls back to the transcript's span, else null. `people_hours` counts the
    participants who attended live."""
    m = await _meeting_for_participant(session, ctx, meeting_id)
    if m["status"] != "ended":
        raise ProblemDetail(409, ErrorCode.VERSION_CONFLICT, "The meeting has not ended")
    counts = (
        await _rows(
            session, f"SELECT {_COUNTS} FROM meetings m WHERE m.id = :m", {"m": str(meeting_id)}
        )
    )[0]
    has_transcript = await _has_transcript(session)

    duration: int | None = None
    if m["scheduled_start"] is not None and m["scheduled_end"] is not None:
        duration = round((m["scheduled_end"] - m["scheduled_start"]).total_seconds() / 60)
    elif has_transcript:
        span = (
            await session.execute(
                text(
                    "SELECT max(end_ms) - min(start_ms) FROM transcript_chunks "
                    "WHERE meeting_id = :m AND is_final"
                ),
                {"m": str(meeting_id)},
            )
        ).scalar()
        duration = round(span / 60000) if span else None
    live = (
        await session.execute(
            text(
                "SELECT count(*) FROM meeting_participants WHERE meeting_id = :m "
                "AND attendance = 'live'"
            ),
            {"m": str(meeting_id)},
        )
    ).scalar_one()
    people_hours = round(duration * live / 60, 2) if duration is not None else None

    talk: list[TalkShare] | None = None
    if has_transcript:
        spoken = await _rows(
            session,
            "SELECT s.user_id, sum(GREATEST(c.end_ms - c.start_ms, 0)) AS ms "
            "FROM transcript_chunks c JOIN meeting_speakers s "
            "  ON s.meeting_id = c.meeting_id AND s.speaker_id = c.speaker_id "
            "WHERE c.meeting_id = :m AND c.is_final AND s.user_id IS NOT NULL "
            "GROUP BY s.user_id ORDER BY s.user_id",
            {"m": str(meeting_id)},
        )
        total_ms = sum(int(r["ms"]) for r in spoken)
        if total_ms > 0:
            talk = [
                TalkShare(user_id=r["user_id"], share=round(int(r["ms"]) / total_ms, 3))
                for r in spoken
            ]

    chain = await _rows(
        session,
        "WITH RECURSIVE back(id, depth) AS ("
        "  SELECT previous_meeting_id, 1 FROM meetings WHERE id = :m "
        "  UNION ALL "
        "  SELECT m.previous_meeting_id, b.depth + 1 FROM back b JOIN meetings m ON m.id = b.id "
        "  WHERE b.depth < :depth"
        ") "
        f"SELECT m.id, m.title, m.scheduled_start, {_COUNTS}, b.depth "
        "FROM back b JOIN meetings m ON m.id = b.id AND m.deleted_at IS NULL "
        "WHERE m.id <> :m ORDER BY b.depth DESC",
        {"m": str(meeting_id), "depth": TREND_DEPTH},
    )
    trend: list[TrendPoint] = []
    seen: set[uuid.UUID] = set()
    for r in chain:
        if r["id"] in seen or not await may_see_source(session, r["id"], ctx.user_id):
            continue
        seen.add(r["id"])
        trend.append(
            TrendPoint(
                meeting_id=r["id"],
                title=r["title"],
                scheduled_start=r["scheduled_start"],
                coverage=_coverage(r["covered"], r["total"]),
                decisions=r["decisions"],
            )
        )
    trend.append(
        TrendPoint(
            meeting_id=m["id"],
            title=m["title"],
            scheduled_start=m["scheduled_start"],
            coverage=_coverage(counts["covered"], counts["total"]),
            decisions=counts["decisions"],
        )
    )
    return MeetingHealth(
        duration_minutes=duration,
        people_hours=people_hours,
        agenda=AgendaCoverage(covered=counts["covered"], total=counts["total"]),
        decisions=counts["decisions"],
        actions=counts["actions"],
        talk_balance=talk,
        async_suggestion=(
            counts["decisions"] == 0
            and counts["actions"] == 0
            and counts["covered"] * 2 < counts["total"]
        ),
        trend=trend,
    )


@router.get("/meetings/{meeting_id}/catch-up", response_model=MeetingCatchUp)
async def meeting_catch_up(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MeetingCatchUp:
    """What the meeting did, for a participant who sent their agent (or missed it): the agenda
    with statuses and agent notes, the confirmed decisions, the actions assigned to the caller,
    the caller's own surfaced notes and the questions they left their agent. Any participant
    may read it: attendance is not tracked beyond `attendance`, so "missed it" cannot be told
    apart from "was there"."""
    m = await _meeting_for_participant(session, ctx, meeting_id)
    params = {"m": str(meeting_id), "u": str(ctx.user_id)}
    agenda = await _rows(
        session,
        "SELECT id, position, title, owner_user_id, status, status_source, agent_note "
        "FROM meeting_agenda_items WHERE meeting_id = :m ORDER BY position, id",
        params,
    )
    decisions = await _rows(
        session,
        "SELECT id, content, rationale FROM decisions WHERE meeting_id = :m "
        "AND status = 'confirmed' AND deleted_at IS NULL ORDER BY created_at, id",
        params,
    )
    mine = await _rows(
        session,
        "SELECT id, content, status, due_date FROM action_items WHERE meeting_id = :m "
        "AND assignee_id = :u AND status IN ('confirmed', 'done') AND deleted_at IS NULL "
        "ORDER BY created_at, id",
        params,
    )
    notes = await _rows(
        session,
        "SELECT id, content, created_at FROM worklogs WHERE meeting_id = :m AND user_id = :u "
        "AND surfaced AND deleted_at IS NULL ORDER BY created_at, id",
        params,
    )
    return MeetingCatchUp(
        attendance=m["attendance"],
        agenda=[CatchUpAgendaItem(**a) for a in agenda],
        decisions=[CatchUpDecision(**d) for d in decisions],
        assigned_to_me=[CatchUpAction(**a) for a in mine],
        my_notes=[CatchUpNote(**n) for n in notes],
        my_questions=list(m["proxy_questions"] or []),
    )
