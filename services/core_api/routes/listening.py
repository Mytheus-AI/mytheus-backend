"""In every meeting at once: the person's own routes (the rules are in `listening.py`).

Per meeting (participants only, 403 `NOT_A_PARTICIPANT`; another tenant's meeting is 404):
- `PUT /api/v1/meetings/{id}/attendance-choice` `{choice}`: attend, my agent listens or skip,
  by hand (beats any rule). A confidential meeting takes no listening agent (422); a meeting
  that is over takes no change (409). In a live meeting the room hears the new listener count.
- `DELETE /api/v1/meetings/{id}/attendance-choice`: back to what the rules say (the undo).
- `GET /api/v1/meetings/{id}/views`: views and held lines posted into the meeting. The host
  sees every one; anyone else only their own.
- `POST /api/v1/meetings/{id}/views/{view_id}/share` | `/dismiss`: the host's call. Sharing
  answers the text the host's room then puts in the meeting chat.

The caller's own listening (`/api/v1/listening/...`):
- `GET week?start=`: their invited meetings for the week with each one's choice, the totals
  ("attend 12, agent listens to 28, saved 19 hours") and the rules.
- `GET|POST rules`, `PUT|DELETE rules/{id}`: standing rules; every change is applied to the
  upcoming meetings at once and answers how many follow it.
- `GET|PUT preferences`: interruptions on/off, the hourly cap, quiet hours, focus blocks, the
  evening digest and the time zone.
- `GET|POST positions`, `PUT|DELETE positions/{id}`: the lines the agent may hold for them.
- `GET interruptions`, `GET interruptions/{id}`, and the three answers to one:
  `POST interruptions/{id}/join` (the choice becomes attend; the room asks for consent at mint
  as usual), `POST interruptions/{id}/view` `{text}` (posted to the host as "Sushan's view"),
  `POST interruptions/{id}/hold` `{position_ids}` (only pre-approved statements: they become the
  meeting's delegated answers and go to the host as the held line), and `dismiss`.
- `GET digests`, `GET digests/latest`, `GET digests/{day}`: the evening digests (a day with
  none stored is built on read and not stored).
- `GET whoami`: the caller's internal ids, for realtime-gateway's per-person channel.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, date, datetime, time, timedelta
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query, Response, status
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import ACTION_MEETING_UPDATED, audit
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api import listening, records
from services.core_api.listening import (
    MAX_POSITIONS,
    MAX_RULES,
    AttendanceRule,
    Choice,
    EveningDigest,
    ListeningPreferences,
    Source,
)

router = APIRouter(prefix="/api/v1", tags=["listening"])

ACTION_ATTENDANCE_RULE_CHANGED = "attendance_rule.changed"
ACTION_INTERRUPTION_ANSWERED = "interruption.answered"
ACTION_MEETING_VIEW_POSTED = "meeting_view.posted"
ACTION_MEETING_VIEW_DECIDED = "meeting_view.decided"
MAX_VIEW = 600
MAX_HOLD = 5
MAX_DELEGATED = 10  # delegated_answers.MAX_STATEMENTS


def _me(ctx: RequestContext) -> uuid.UUID:
    if ctx.user_id is None:
        raise not_found()
    return ctx.user_id


def _not_a_participant() -> ProblemDetail:
    return ProblemDetail(
        status.HTTP_403_FORBIDDEN,
        ErrorCode.NOT_A_PARTICIPANT,
        "You are not a participant of this meeting",
    )


async def _access(
    session: AsyncSession, meeting_id: uuid.UUID, me: uuid.UUID
) -> records.MeetingAccess:
    access = await records.access_for(session, meeting_id, me)
    if access is None:
        raise not_found()
    if not access.is_participant:
        raise _not_a_participant()
    return access


# --- per meeting: the choice -----------------------------------------------------------------


class AttendanceChoiceUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    choice: Choice


class MeetingAttendanceChoice(BaseModel):
    meeting_id: uuid.UUID
    choice: Choice
    source: Source
    rule_id: uuid.UUID | None = None
    attendance: Literal["live", "agent"]
    agents_listening: int


async def _choice_view(
    session: AsyncSession, meeting_id: uuid.UUID, me: uuid.UUID
) -> MeetingAttendanceChoice:
    facts = await listening.meeting_facts(session, user_id=me, meeting_id=meeting_id)
    if not facts:
        raise _not_a_participant()
    m = facts[0]
    choice, source = listening.effective(m)
    return MeetingAttendanceChoice(
        meeting_id=meeting_id,
        choice=choice,
        source=source,
        rule_id=m["attendance_rule_id"],
        attendance=m["attendance"],
        agents_listening=len(await listening.listening_agents(session, meeting_id)),
    )


async def _set_choice(
    session: AsyncSession,
    ctx: RequestContext,
    meeting_id: uuid.UUID,
    choice: Choice | None,
) -> MeetingAttendanceChoice:
    me = _me(ctx)
    access = await _access(session, meeting_id, me)
    if access.status in ("ended", "cancelled"):
        raise ProblemDetail(
            status.HTTP_409_CONFLICT,
            ErrorCode.VERSION_CONFLICT,
            "The meeting is over; attendance can no longer change",
        )
    if choice == "agent_listens" and access.confidential:
        raise ProblemDetail(
            422,
            ErrorCode.VALIDATION_FAILED,
            "This meeting is confidential, so no Myte listens to it. Attend or skip.",
        )
    before = await listening.listening_agents(session, meeting_id)
    if choice is None:
        await session.execute(
            text(
                "UPDATE meeting_participants SET attendance_choice = NULL, "
                "attendance_choice_source = NULL, attendance_rule_id = NULL, "
                "attendance_choice_at = now(), attendance = 'live' "
                "WHERE meeting_id = :m AND user_id = :u"
            ),
            {"m": str(meeting_id), "u": str(me)},
        )
        await listening.apply_rules_for_meeting(session, meeting_id)
    else:
        await session.execute(
            text(
                "UPDATE meeting_participants SET attendance_choice = :c, "
                "attendance_choice_source = 'manual', attendance_rule_id = NULL, "
                "attendance_choice_at = now(), attendance = :a "
                "WHERE meeting_id = :m AND user_id = :u"
            ),
            {
                "c": choice,
                "a": "agent" if choice == "agent_listens" else "live",
                "m": str(meeting_id),
                "u": str(me),
            },
        )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=me,
        action=ACTION_MEETING_UPDATED,
        target_type="meeting",
        target_id=meeting_id,
        metadata={"changed": ["attendance_choice"], "attendance_choice": choice or "rules"},
        ip=ctx.ip,
    )
    view = await _choice_view(session, meeting_id, me)
    if access.status == "live" and view.agents_listening != len(before):
        await listening.announce_listeners(session, ctx.org_id, meeting_id)
    return view


@router.put("/meetings/{meeting_id}/attendance-choice", response_model=MeetingAttendanceChoice)
async def put_attendance_choice(
    meeting_id: uuid.UUID,
    body: AttendanceChoiceUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MeetingAttendanceChoice:
    return await _set_choice(session, ctx, meeting_id, body.choice)


@router.delete("/meetings/{meeting_id}/attendance-choice", response_model=MeetingAttendanceChoice)
async def delete_attendance_choice(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MeetingAttendanceChoice:
    return await _set_choice(session, ctx, meeting_id, None)


@router.get("/meetings/{meeting_id}/attendance-choice", response_model=MeetingAttendanceChoice)
async def get_attendance_choice(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MeetingAttendanceChoice:
    me = _me(ctx)
    await _access(session, meeting_id, me)
    return await _choice_view(session, meeting_id, me)


# --- the week ---------------------------------------------------------------------------------


class WeekMeeting(BaseModel):
    meeting_id: uuid.UUID
    title: str
    status: str
    scheduled_start: datetime
    scheduled_end: datetime | None = None
    duration_minutes: int
    confidential: bool
    recurring: bool
    hosting: bool
    choice: Choice
    source: Source
    rule_id: uuid.UUID | None = None
    can_agent_listen: bool


class WeekSummary(BaseModel):
    invited: int
    attend: int
    agent_listens: int
    skip: int
    minutes_saved: int
    hours_saved: float


class ListeningWeek(BaseModel):
    week_start: date
    week_end: date
    time_zone: str
    summary: WeekSummary
    meetings: list[WeekMeeting]
    rules: list[AttendanceRule]


@router.get("/listening/week", response_model=ListeningWeek)
async def get_week(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    start: Annotated[date | None, Query(description="Any day of the week; local dates.")] = None,
) -> ListeningWeek:
    me = _me(ctx)
    prefs = await listening.load_preferences(session, me)
    zone = prefs.zone()
    today = datetime.now(UTC).astimezone(zone).date()
    day = start or today
    monday = day - timedelta(days=day.weekday())
    lo = datetime.combine(monday, time.min, tzinfo=zone).astimezone(UTC)
    hi = datetime.combine(monday + timedelta(days=7), time.min, tzinfo=zone).astimezone(UTC)
    changed = await listening.apply_rules_for_user(session, me)
    for meeting_id in changed:
        await _announce_if_live(session, ctx.org_id, meeting_id)
    facts = [
        m
        for m in await listening.meeting_facts(session, user_id=me, start=lo, end=hi)
        if m["status"] != "cancelled"
    ]
    meetings: list[WeekMeeting] = []
    counts = {"attend": 0, "agent_listens": 0, "skip": 0}
    saved = 0
    for m in facts:
        choice, source = listening.effective(m)
        minutes = listening.duration_minutes(m)
        counts[choice] += 1
        if choice != "attend":
            saved += minutes
        meetings.append(
            WeekMeeting(
                meeting_id=m["id"],
                title=records._title(m["title"]),
                status=m["status"],
                scheduled_start=m["scheduled_start"],
                scheduled_end=m["scheduled_end"],
                duration_minutes=minutes,
                confidential=bool(m["confidential"]),
                recurring=bool(m["recurring"]),
                hosting=bool(m["hosting"]),
                choice=choice,
                source=source,
                rule_id=m["attendance_rule_id"],
                can_agent_listen=not m["confidential"],
            )
        )
    return ListeningWeek(
        week_start=monday,
        week_end=monday + timedelta(days=6),
        time_zone=prefs.time_zone,
        summary=WeekSummary(
            invited=len(meetings),
            attend=counts["attend"],
            agent_listens=counts["agent_listens"],
            skip=counts["skip"],
            minutes_saved=saved,
            hours_saved=round(saved / 60, 1),
        ),
        meetings=meetings,
        rules=await listening.load_rules(session, me),
    )


async def _announce_if_live(
    session: AsyncSession, org_id: uuid.UUID, meeting_id: uuid.UUID
) -> None:
    live = (
        await session.execute(
            text("SELECT status FROM meetings WHERE id = :m"), {"m": str(meeting_id)}
        )
    ).scalar()
    if live == "live":
        await listening.announce_listeners(session, org_id, meeting_id)


# --- rules ------------------------------------------------------------------------------------


class AttendanceRuleWrite(BaseModel):
    model_config = ConfigDict(extra="forbid")

    choice: Choice
    title_contains: str | None = Field(default=None, max_length=80)
    template_id: uuid.UUID | None = None
    recurring_only: bool = False
    include_hosted: bool = False
    position: int | None = Field(default=None, ge=0, le=99)

    @field_validator("title_contains")
    @classmethod
    def _title(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = " ".join(value.split())
        if not value:
            return None
        if len(value) < 2:
            raise ValueError("a title match needs at least 2 characters")
        return value

    @model_validator(mode="after")
    def _something(self) -> AttendanceRuleWrite:
        if not (self.title_contains or self.template_id or self.recurring_only):
            raise ValueError("a rule matches on a title, a meeting type or recurring meetings")
        return self


class AttendanceRuleResult(BaseModel):
    rule: AttendanceRule
    applied: int = Field(description="Upcoming meetings that now follow this rule.")


class AttendanceRuleList(BaseModel):
    items: list[AttendanceRule]


async def _check_template(session: AsyncSession, template_id: uuid.UUID | None) -> None:
    if template_id is None:
        return
    found = (
        await session.execute(
            text("SELECT 1 FROM meeting_templates WHERE id = :t"), {"t": str(template_id)}
        )
    ).first()
    if found is None:
        raise ProblemDetail(
            422,
            ErrorCode.VALIDATION_FAILED,
            "That meeting type does not exist",
        )


async def _apply_and_count(
    session: AsyncSession, org_id: uuid.UUID, me: uuid.UUID, rule_id: uuid.UUID | None
) -> int:
    for meeting_id in await listening.apply_rules_for_user(session, me):
        await _announce_if_live(session, org_id, meeting_id)
    if rule_id is None:
        return 0
    return int(
        (
            await session.execute(
                text(
                    "SELECT count(*) FROM meeting_participants p JOIN meetings m "
                    "ON m.id = p.meeting_id WHERE p.user_id = :u AND p.attendance_rule_id = :r "
                    "AND m.status IN ('scheduled', 'live') AND m.deleted_at IS NULL"
                ),
                {"u": str(me), "r": str(rule_id)},
            )
        ).scalar()
        or 0
    )


async def _rule(session: AsyncSession, me: uuid.UUID, rule_id: uuid.UUID) -> AttendanceRule:
    rule = next((r for r in await listening.load_rules(session, me) if r.id == rule_id), None)
    if rule is None:
        raise not_found()
    return rule


@router.get("/listening/rules", response_model=AttendanceRuleList)
async def list_rules(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> AttendanceRuleList:
    return AttendanceRuleList(items=await listening.load_rules(session, _me(ctx)))


@router.post(
    "/listening/rules", response_model=AttendanceRuleResult, status_code=status.HTTP_201_CREATED
)
async def create_rule(
    body: AttendanceRuleWrite,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> AttendanceRuleResult:
    me = _me(ctx)
    existing = await listening.load_rules(session, me)
    if len(existing) >= MAX_RULES:
        raise ProblemDetail(
            422,
            ErrorCode.VALIDATION_FAILED,
            f"At most {MAX_RULES} rules; remove one first",
        )
    await _check_template(session, body.template_id)
    position = body.position if body.position is not None else len(existing)
    rule_id = (
        await session.execute(
            text(
                "INSERT INTO attendance_rules (org_id, user_id, choice, title_contains, "
                "template_id, recurring_only, include_hosted, position) VALUES (:o, :u, :c, "
                ":t, :tpl, :r, :h, :p) RETURNING id"
            ),
            {
                "o": str(ctx.org_id),
                "u": str(me),
                "c": body.choice,
                "t": body.title_contains,
                "tpl": str(body.template_id) if body.template_id else None,
                "r": body.recurring_only,
                "h": body.include_hosted,
                "p": min(position, 99),
            },
        )
    ).scalar_one()
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=me,
        action=ACTION_ATTENDANCE_RULE_CHANGED,
        target_type="attendance_rule",
        target_id=rule_id,
        metadata={"op": "created", "choice": body.choice},
        ip=ctx.ip,
    )
    applied = await _apply_and_count(session, ctx.org_id, me, rule_id)
    return AttendanceRuleResult(rule=await _rule(session, me, rule_id), applied=applied)


@router.put("/listening/rules/{rule_id}", response_model=AttendanceRuleResult)
async def update_rule(
    rule_id: uuid.UUID,
    body: AttendanceRuleWrite,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> AttendanceRuleResult:
    me = _me(ctx)
    current = await _rule(session, me, rule_id)
    await _check_template(session, body.template_id)
    await session.execute(
        text(
            "UPDATE attendance_rules SET choice = :c, title_contains = :t, template_id = :tpl, "
            "recurring_only = :r, include_hosted = :h, position = :p, updated_at = now() "
            "WHERE id = :id AND user_id = :u"
        ),
        {
            "c": body.choice,
            "t": body.title_contains,
            "tpl": str(body.template_id) if body.template_id else None,
            "r": body.recurring_only,
            "h": body.include_hosted,
            "p": body.position if body.position is not None else current.position,
            "id": str(rule_id),
            "u": str(me),
        },
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=me,
        action=ACTION_ATTENDANCE_RULE_CHANGED,
        target_type="attendance_rule",
        target_id=rule_id,
        metadata={"op": "updated", "choice": body.choice},
        ip=ctx.ip,
    )
    applied = await _apply_and_count(session, ctx.org_id, me, rule_id)
    return AttendanceRuleResult(rule=await _rule(session, me, rule_id), applied=applied)


@router.delete("/listening/rules/{rule_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_rule(
    rule_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    me = _me(ctx)
    await _rule(session, me, rule_id)
    # meetings that followed it go back to the other rules (or to no choice)
    await session.execute(
        text(
            "UPDATE meeting_participants SET attendance_choice = NULL, "
            "attendance_choice_source = NULL, attendance_rule_id = NULL, attendance = 'live' "
            "WHERE user_id = :u AND attendance_rule_id = :r AND attendance_choice_source = 'rule'"
        ),
        {"u": str(me), "r": str(rule_id)},
    )
    await session.execute(
        text("DELETE FROM attendance_rules WHERE id = :id AND user_id = :u"),
        {"id": str(rule_id), "u": str(me)},
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=me,
        action=ACTION_ATTENDANCE_RULE_CHANGED,
        target_type="attendance_rule",
        target_id=rule_id,
        metadata={"op": "deleted"},
        ip=ctx.ip,
    )
    await _apply_and_count(session, ctx.org_id, me, None)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# --- preferences ------------------------------------------------------------------------------


class ListeningPreferencesView(ListeningPreferences):
    model_config = ConfigDict(extra="ignore")

    interruptible_now: bool = Field(
        default=True, description="Whether an interruption would reach you right now."
    )
    quiet_reason: str | None = None
    quiet_until: datetime | None = None


@router.get("/listening/preferences", response_model=ListeningPreferencesView)
async def get_listening_preferences(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ListeningPreferencesView:
    me = _me(ctx)
    prefs = await listening.load_preferences(session, me)
    return await _prefs_view(session, me, prefs)


async def _prefs_view(
    session: AsyncSession, me: uuid.UUID, prefs: ListeningPreferences
) -> ListeningPreferencesView:
    g = await listening.gate(session, me, None, datetime.now(UTC), prefs)
    return ListeningPreferencesView(
        **prefs.model_dump(),
        interruptible_now=g.open,
        quiet_reason=g.reason,
        quiet_until=g.next_allowed_at,
    )


@router.put("/listening/preferences", response_model=ListeningPreferencesView)
async def put_listening_preferences(
    body: ListeningPreferences,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ListeningPreferencesView:
    me = _me(ctx)
    await listening.save_preferences(session, ctx.org_id, me, body)
    return await _prefs_view(session, me, body)


# --- positions (the line you gave me) --------------------------------------------------------


class StandingPositionWrite(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic: str | None = Field(default=None, max_length=80)
    text: str = Field(min_length=1, max_length=300)

    @field_validator("text", "topic")
    @classmethod
    def _flat(cls, value: str | None) -> str | None:
        if value is None:
            return None
        flat = " ".join(value.split())
        return flat or None

    @model_validator(mode="after")
    def _text(self) -> StandingPositionWrite:
        if not self.text:
            raise ValueError("a position needs some text")
        return self


class StandingPosition(BaseModel):
    id: uuid.UUID
    topic: str | None = None
    text: str
    created_at: datetime
    updated_at: datetime


class StandingPositionList(BaseModel):
    items: list[StandingPosition]


async def _positions(
    session: AsyncSession, me: uuid.UUID, ids: list[uuid.UUID] | None = None
) -> list[StandingPosition]:
    where, params = "", {"u": str(me)}
    if ids is not None:
        where = " AND id = ANY(CAST(:ids AS uuid[]))"
        params["ids"] = [str(i) for i in ids]  # type: ignore[assignment]
    rows = (
        (
            await session.execute(
                text(
                    "SELECT id, topic, text, created_at, updated_at FROM standing_positions "
                    f"WHERE user_id = :u AND deleted_at IS NULL{where} ORDER BY created_at, id"
                ),
                params,
            )
        )
        .mappings()
        .all()
    )
    return [StandingPosition(**dict(r)) for r in rows]


@router.get("/listening/positions", response_model=StandingPositionList)
async def list_positions(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> StandingPositionList:
    return StandingPositionList(items=await _positions(session, _me(ctx)))


@router.post(
    "/listening/positions", response_model=StandingPosition, status_code=status.HTTP_201_CREATED
)
async def create_position(
    body: StandingPositionWrite,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> StandingPosition:
    me = _me(ctx)
    if len(await _positions(session, me)) >= MAX_POSITIONS:
        raise ProblemDetail(
            422,
            ErrorCode.VALIDATION_FAILED,
            f"At most {MAX_POSITIONS} positions; remove one first",
        )
    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO standing_positions (org_id, user_id, topic, text) "
                    "VALUES (:o, :u, :t, :x) RETURNING id, topic, text, created_at, updated_at"
                ),
                {"o": str(ctx.org_id), "u": str(me), "t": body.topic, "x": body.text},
            )
        )
        .mappings()
        .one()
    )
    return StandingPosition(**dict(row))


@router.put("/listening/positions/{position_id}", response_model=StandingPosition)
async def update_position(
    position_id: uuid.UUID,
    body: StandingPositionWrite,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> StandingPosition:
    row = (
        (
            await session.execute(
                text(
                    "UPDATE standing_positions SET topic = :t, text = :x, updated_at = now() "
                    "WHERE id = :id AND user_id = :u AND deleted_at IS NULL "
                    "RETURNING id, topic, text, created_at, updated_at"
                ),
                {"t": body.topic, "x": body.text, "id": str(position_id), "u": str(_me(ctx))},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()
    return StandingPosition(**dict(row))


@router.delete("/listening/positions/{position_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_position(
    position_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    done = (
        await session.execute(
            text(
                "UPDATE standing_positions SET deleted_at = now() WHERE id = :id AND user_id = :u "
                "AND deleted_at IS NULL RETURNING id"
            ),
            {"id": str(position_id), "u": str(_me(ctx))},
        )
    ).first()
    if done is None:
        raise not_found()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# --- interruptions ----------------------------------------------------------------------------


class Interruption(BaseModel):
    id: uuid.UUID
    meeting_id: uuid.UUID
    meeting_title: str
    meeting_status: str
    scheduled_start: datetime | None = None
    reason: Literal["decision", "question", "promise"]
    title: str = Field(description="The sentence the person saw, e.g. '... in the 3pm right now.'")
    headline: str
    detail: str | None = None
    topic: str | None = None
    quote: str | None = None
    start_ms: int | None = None
    status: Literal["delivered", "held"]
    held_reason: str | None = None
    response: Literal["joined", "view_sent", "holding_line", "dismissed"] | None = None
    responded_at: datetime | None = None
    created_at: datetime
    join_url: str
    moment_url: str | None = None
    can_respond: bool = Field(description="The meeting is live and nothing was answered yet.")


class InterruptionList(BaseModel):
    items: list[Interruption]


_INTERRUPTION_COLUMNS = (
    "i.id, i.meeting_id, m.title AS meeting_title, m.status AS meeting_status, "
    "m.scheduled_start, i.reason, i.headline, i.detail, i.topic, i.quote, i.start_ms, i.status, "
    "i.held_reason, i.response, i.responded_at, i.created_at"
)


def interruption_view(row: Any, zone: Any) -> Interruption:
    mid = row["meeting_id"]
    return Interruption(
        id=row["id"],
        meeting_id=mid,
        meeting_title=records._title(row["meeting_title"]),
        meeting_status=row["meeting_status"],
        scheduled_start=row["scheduled_start"],
        reason=row["reason"],
        title=listening.interruption_title(
            row["headline"], row["meeting_title"], row["scheduled_start"], zone
        ),
        headline=row["headline"],
        detail=row["detail"],
        topic=row["topic"],
        quote=row["quote"],
        start_ms=row["start_ms"],
        status=row["status"],
        held_reason=row["held_reason"],
        response=row["response"],
        responded_at=row["responded_at"],
        created_at=row["created_at"],
        join_url=f"/meetings/{mid}/room",
        moment_url=(
            f"/meetings/{mid}/transcript#t={int(row['start_ms']) // 1000}"
            if row["start_ms"] is not None
            else None
        ),
        can_respond=row["meeting_status"] == "live" and row["response"] is None,
    )


@router.get("/listening/interruptions", response_model=InterruptionList)
async def list_interruptions(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    status_: Annotated[Literal["delivered", "held"] | None, Query(alias="status")] = None,
    meeting_id: uuid.UUID | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 30,
) -> InterruptionList:
    me = _me(ctx)
    zone = (await listening.load_preferences(session, me)).zone()
    where, params = "", {"u": str(me), "lim": limit}
    if status_ is not None:
        where += " AND i.status = :s"
        params["s"] = status_
    if meeting_id is not None:
        where += " AND i.meeting_id = :m"
        params["m"] = str(meeting_id)
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_INTERRUPTION_COLUMNS} FROM agent_interruptions i "
                    "JOIN meetings m ON m.id = i.meeting_id AND m.deleted_at IS NULL "
                    f"WHERE i.user_id = :u{where} ORDER BY i.created_at DESC LIMIT :lim"
                ),
                params,
            )
        )
        .mappings()
        .all()
    )
    return InterruptionList(items=[interruption_view(r, zone) for r in rows])


async def _interruption(session: AsyncSession, me: uuid.UUID, interruption_id: uuid.UUID) -> Any:
    row = (
        (
            await session.execute(
                text(
                    f"SELECT {_INTERRUPTION_COLUMNS} FROM agent_interruptions i "
                    "JOIN meetings m ON m.id = i.meeting_id AND m.deleted_at IS NULL "
                    "WHERE i.id = :id AND i.user_id = :u FOR UPDATE OF i"
                ),
                {"id": str(interruption_id), "u": str(me)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()
    return row


@router.get("/listening/interruptions/{interruption_id}", response_model=Interruption)
async def get_interruption(
    interruption_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Interruption:
    me = _me(ctx)
    zone = (await listening.load_preferences(session, me)).zone()
    return interruption_view(await _interruption(session, me, interruption_id), zone)


async def _answer(
    session: AsyncSession,
    ctx: RequestContext,
    interruption_id: uuid.UUID,
    response: str,
    *,
    needs_live: bool,
) -> Any:
    me = _me(ctx)
    row = await _interruption(session, me, interruption_id)
    if needs_live and row["meeting_status"] != "live":
        raise ProblemDetail(
            status.HTTP_409_CONFLICT,
            ErrorCode.VERSION_CONFLICT,
            "That meeting is no longer live",
        )
    await session.execute(
        text("UPDATE agent_interruptions SET response = :r, responded_at = now() WHERE id = :id"),
        {"r": response, "id": str(interruption_id)},
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=me,
        action=ACTION_INTERRUPTION_ANSWERED,
        target_type="agent_interruption",
        target_id=interruption_id,
        metadata={"response": response, "meeting_id": str(row["meeting_id"])},
        ip=ctx.ip,
    )
    return row


async def _reloaded(
    session: AsyncSession, me: uuid.UUID, interruption_id: uuid.UUID
) -> Interruption:
    zone = (await listening.load_preferences(session, me)).zone()
    return interruption_view(await _interruption(session, me, interruption_id), zone)


@router.post("/listening/interruptions/{interruption_id}/dismiss", response_model=Interruption)
async def dismiss_interruption(
    interruption_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Interruption:
    await _answer(session, ctx, interruption_id, "dismissed", needs_live=False)
    return await _reloaded(session, _me(ctx), interruption_id)


@router.post("/listening/interruptions/{interruption_id}/join", response_model=Interruption)
async def join_from_interruption(
    interruption_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Interruption:
    """The person is going in: their choice for this meeting becomes attend, so their agent
    stops listening for them and the room counts one agent fewer. The token mint still asks
    for consent as usual."""
    row = await _answer(session, ctx, interruption_id, "joined", needs_live=True)
    await _set_choice(session, ctx, row["meeting_id"], "attend")
    return await _reloaded(session, _me(ctx), interruption_id)


class ViewPost(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str = Field(min_length=1, max_length=MAX_VIEW)

    @field_validator("text")
    @classmethod
    def _flat(cls, value: str) -> str:
        flat = value.strip()
        if not flat:
            raise ValueError("a view needs some text")
        return flat


class HoldPost(BaseModel):
    model_config = ConfigDict(extra="forbid")

    position_ids: list[uuid.UUID] = Field(min_length=1, max_length=MAX_HOLD)


class MeetingView(BaseModel):
    id: uuid.UUID
    meeting_id: uuid.UUID
    user_id: uuid.UUID
    author_name: str
    kind: Literal["view", "hold_line"]
    text: str
    status: Literal["pending", "shared", "dismissed"]
    decided_at: datetime | None = None
    created_at: datetime
    chat_text: str = Field(description="What goes into the meeting chat when it is shared.")


class MeetingViewList(BaseModel):
    items: list[MeetingView]
    is_host: bool


def _chat_text(kind: str, name: str, body: str) -> str:
    first = listening.first_name(name)
    if kind == "hold_line":
        return f"{first}'s Myte is holding the line for {first}: {body}"
    return f"{first}'s view: {body}"


_VIEW_COLUMNS = (
    "v.id, v.meeting_id, v.user_id, u.email, u.display_name, v.kind, v.text, v.status, "
    "v.decided_at, v.created_at"
)


def _view(row: Any) -> MeetingView:
    name = records.person_name(row["display_name"], row["email"]) or "A teammate"
    return MeetingView(
        id=row["id"],
        meeting_id=row["meeting_id"],
        user_id=row["user_id"],
        author_name=name,
        kind=row["kind"],
        text=row["text"],
        status=row["status"],
        decided_at=row["decided_at"],
        created_at=row["created_at"],
        chat_text=_chat_text(row["kind"], name, row["text"]),
    )


async def _post_view(
    session: AsyncSession,
    ctx: RequestContext,
    meeting_id: uuid.UUID,
    interruption_id: uuid.UUID,
    kind: str,
    body: str,
    position_ids: list[uuid.UUID] | None = None,
) -> MeetingView:
    me = _me(ctx)
    access = await _access(session, meeting_id, me)
    row = (
        await session.execute(
            text(
                "INSERT INTO meeting_views (org_id, meeting_id, user_id, interruption_id, "
                "kind, text, position_ids) VALUES (:o, :m, :u, :i, :k, :t, "
                "CAST(:p AS uuid[])) RETURNING id"
            ),
            {
                "o": str(ctx.org_id),
                "m": str(meeting_id),
                "u": str(me),
                "i": str(interruption_id),
                "k": kind,
                "t": body,
                "p": [str(p) for p in position_ids or []],
            },
        )
    ).scalar_one()
    view = await _load_view(session, meeting_id, row)
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=me,
        action=ACTION_MEETING_VIEW_POSTED,
        target_type="meeting",
        target_id=meeting_id,
        metadata={"view_id": str(view.id), "kind": kind},
        ip=ctx.ip,
    )
    if access.host_id is not None and access.host_id != me:
        await listening.publish(
            listening.meeting_channel(ctx.org_id, meeting_id),
            {
                "type": "meeting-view",
                "meetingId": str(meeting_id),
                "userId": str(access.host_id),
                "payload": json.loads(view.model_dump_json()),
            },
        )
    return view


async def _load_view(session: AsyncSession, meeting_id: uuid.UUID, view_id: Any) -> MeetingView:
    row = (
        (
            await session.execute(
                text(
                    f"SELECT {_VIEW_COLUMNS} FROM meeting_views v JOIN users u ON u.id = v.user_id "
                    "WHERE v.id = :id AND v.meeting_id = :m"
                ),
                {"id": str(view_id), "m": str(meeting_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()
    return _view(row)


@router.post("/listening/interruptions/{interruption_id}/view", response_model=MeetingView)
async def send_view(
    interruption_id: uuid.UUID,
    body: ViewPost,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MeetingView:
    """ "Send a view": a short text from the person, posted into the live meeting as their view.
    The host sees it first and shares it with the room."""
    row = await _answer(session, ctx, interruption_id, "view_sent", needs_live=True)
    return await _post_view(session, ctx, row["meeting_id"], interruption_id, "view", body.text)


@router.post("/listening/interruptions/{interruption_id}/hold", response_model=MeetingView)
async def hold_line(
    interruption_id: uuid.UUID,
    body: HoldPost,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MeetingView:
    """ "Let my agent hold the line": only statements the person pre-approved. They are put to
    the host as the held line and become the meeting's delegated answers, so the stand-in
    answers questions with them for the rest of the meeting (and says so when they do not
    cover a question)."""
    me = _me(ctx)
    wanted = list(dict.fromkeys(body.position_ids))
    positions = await _positions(session, me, wanted)
    if len(positions) != len(wanted):
        raise ProblemDetail(
            422,
            ErrorCode.VALIDATION_FAILED,
            "Only your own saved positions can be held",
        )
    row = await _answer(session, ctx, interruption_id, "holding_line", needs_live=True)
    meeting_id = row["meeting_id"]
    current = [
        str(r[0])
        for r in await session.execute(
            text(
                "SELECT text FROM delegated_answer_statements WHERE meeting_id = :m "
                "AND user_id = :u AND deleted_at IS NULL ORDER BY position"
            ),
            {"m": str(meeting_id), "u": str(me)},
        )
    ]
    for p in positions:
        if p.text in current or len(current) >= MAX_DELEGATED:
            continue
        await session.execute(
            text(
                "INSERT INTO delegated_answer_statements (org_id, meeting_id, user_id, text, "
                "position) VALUES (:o, :m, :u, :t, :p)"
            ),
            {
                "o": str(ctx.org_id),
                "m": str(meeting_id),
                "u": str(me),
                "t": p.text,
                "p": len(current),
            },
        )
        current.append(p.text)
    # the agent holds the line only where it actually listens
    await session.execute(
        text(
            "UPDATE meeting_participants SET attendance = 'agent', "
            "attendance_choice = 'agent_listens', attendance_choice_source = 'manual', "
            "attendance_rule_id = NULL, attendance_choice_at = now() "
            "WHERE meeting_id = :m AND user_id = :u AND attendance <> 'agent'"
        ),
        {"m": str(meeting_id), "u": str(me)},
    )
    held = " · ".join((f"On {p.topic}: {p.text}" if p.topic else p.text) for p in positions)[:2000]
    return await _post_view(
        session, ctx, meeting_id, interruption_id, "hold_line", held, [p.id for p in positions]
    )


# --- views in the room -----------------------------------------------------------------------


@router.get("/meetings/{meeting_id}/views", response_model=MeetingViewList)
async def list_views(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MeetingViewList:
    me = _me(ctx)
    access = await _access(session, meeting_id, me)
    host = access.viewer_is_host(me)
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_VIEW_COLUMNS} FROM meeting_views v JOIN users u ON u.id = v.user_id "
                    "WHERE v.meeting_id = :m AND (:host OR v.user_id = :u OR v.status = 'shared') "
                    "ORDER BY v.created_at, v.id"
                ),
                {"m": str(meeting_id), "u": str(me), "host": host},
            )
        )
        .mappings()
        .all()
    )
    return MeetingViewList(items=[_view(r) for r in rows], is_host=host)


async def _decide(
    session: AsyncSession,
    ctx: RequestContext,
    meeting_id: uuid.UUID,
    view_id: uuid.UUID,
    verdict: Literal["shared", "dismissed"],
) -> MeetingView:
    me = _me(ctx)
    access = await _access(session, meeting_id, me)
    if not access.viewer_is_host(me):
        raise ProblemDetail(
            status.HTTP_403_FORBIDDEN,
            ErrorCode.FORBIDDEN_ROLE,
            "Only the host shares or sets aside a view",
        )
    view = await _load_view(session, meeting_id, view_id)
    if view.status == "pending":
        await session.execute(
            text(
                "UPDATE meeting_views SET status = :s, decided_by = :u, decided_at = now() "
                "WHERE id = :id"
            ),
            {"s": verdict, "u": str(me), "id": str(view_id)},
        )
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=me,
            action=ACTION_MEETING_VIEW_DECIDED,
            target_type="meeting",
            target_id=meeting_id,
            metadata={"view_id": str(view_id), "status": verdict},
            ip=ctx.ip,
        )
        view = await _load_view(session, meeting_id, view_id)
        await listening.publish(
            listening.user_channel(ctx.org_id, view.user_id),
            {
                "type": "view-status",
                "userId": str(view.user_id),
                "payload": json.loads(view.model_dump_json()),
            },
        )
    return view


@router.post("/meetings/{meeting_id}/views/{view_id}/share", response_model=MeetingView)
async def share_view(
    meeting_id: uuid.UUID,
    view_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MeetingView:
    return await _decide(session, ctx, meeting_id, view_id, "shared")


@router.post("/meetings/{meeting_id}/views/{view_id}/dismiss", response_model=MeetingView)
async def dismiss_view(
    meeting_id: uuid.UUID,
    view_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MeetingView:
    return await _decide(session, ctx, meeting_id, view_id, "dismissed")


# --- the evening digest ----------------------------------------------------------------------


class EveningDigestSummary(BaseModel):
    id: uuid.UUID
    day: date
    meeting_count: int
    created_at: datetime


class EveningDigestList(BaseModel):
    items: list[EveningDigestSummary]


def stored_digest(row: Any) -> EveningDigest:
    content = row["content"] if isinstance(row["content"], dict) else json.loads(row["content"])
    digest = EveningDigest.model_validate(content)
    digest.id = row["id"]
    digest.created_at = row["created_at"]
    return digest


@router.get("/listening/digests", response_model=EveningDigestList)
async def list_evening_digests(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    limit: Annotated[int, Query(ge=1, le=60)] = 14,
) -> EveningDigestList:
    rows = (
        (
            await session.execute(
                text(
                    "SELECT id, day, meeting_count, created_at FROM evening_digests "
                    "WHERE user_id = :u ORDER BY day DESC LIMIT :lim"
                ),
                {"u": str(_me(ctx)), "lim": limit},
            )
        )
        .mappings()
        .all()
    )
    return EveningDigestList(items=[EveningDigestSummary(**dict(r)) for r in rows])


async def _digest_for(session: AsyncSession, me: uuid.UUID, day: date) -> EveningDigest:
    row = (
        (
            await session.execute(
                text(
                    "SELECT id, content, created_at FROM evening_digests "
                    "WHERE user_id = :u AND day = :d"
                ),
                {"u": str(me), "d": day},
            )
        )
        .mappings()
        .first()
    )
    if row is not None:
        return stored_digest(row)
    prefs = await listening.load_preferences(session, me)
    return await listening.build_evening(session, me, day, prefs.zone())


@router.get("/listening/digests/latest", response_model=EveningDigest)
async def latest_evening_digest(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> EveningDigest:
    """The newest stored digest, or today's so far when none is stored yet."""
    me = _me(ctx)
    row = (
        (
            await session.execute(
                text(
                    "SELECT id, content, created_at FROM evening_digests WHERE user_id = :u "
                    "ORDER BY day DESC LIMIT 1"
                ),
                {"u": str(me)},
            )
        )
        .mappings()
        .first()
    )
    prefs = await listening.load_preferences(session, me)
    today = datetime.now(UTC).astimezone(prefs.zone()).date()
    if row is not None and stored_digest(row).day >= today - timedelta(days=1):
        return stored_digest(row)
    return await listening.build_evening(session, me, today, prefs.zone())


@router.get("/listening/digests/{day}", response_model=EveningDigest)
async def get_evening_digest(
    day: date,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> EveningDigest:
    return await _digest_for(session, _me(ctx), day)


# --- identity for the realtime channel -------------------------------------------------------


class ListeningIdentity(BaseModel):
    user_id: uuid.UUID
    org_id: uuid.UUID


@router.get("/listening/whoami", response_model=ListeningIdentity)
async def whoami(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
) -> ListeningIdentity:
    return ListeningIdentity(user_id=_me(ctx), org_id=ctx.org_id)
