"""In every meeting at once: the internal routes (agent-runtime-service and workflow-service).

- `GET /internal/meetings/{id}/listeners` (agent-runtime-service, the live watch): the people
  whose agent listens to this meeting, with what the watch listens for on their behalf (their
  name, teams, projects, listen-for topics and what they own) and whether an interruption may
  reach them right now. A confidential meeting has no listeners. Rules are applied first, so a
  rule made after the meeting was booked still counts.
- `POST /internal/meetings/{id}/interruptions` (agent-runtime-service): the watch proposes an
  interruption; core-api decides. Suppressed (nothing stored) when the meeting is confidential or
  not live, the person is not a listening participant any more, or is in the room; otherwise
  stored, and `delivered` (an in-app notification, which notification-service mirrors to their
  phones, plus a live toast over realtime-gateway's per-person channel) or `held` (quiet hours,
  a focus block, interruptions off, the throttle: kept for the evening digest, no toast, no
  push). Idempotent on (user, key).
- `POST /internal/listening/evening` (workflow-service, schedule `listening-evening`, hourly):
  for every person whose digest is on and whose local hour has reached their digest hour, build
  and store the day's digest once; a day with meetings gets one in-app notification. It also
  re-applies everyone's rules to the next three weeks, so meetings booked since are covered.

Contract with mytheus-intelligence: scratchpad b2-contract/internal.md (agent-runtime-service
`watch.py`).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import not_found
from libs.shared.db import get_sessionmaker
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from libs.shared.tenancy import set_rls
from services.core_api import listening, records
from services.core_api.routes.internal_agents import agent_runtime_caller
from services.core_api.routes.internal_lifecycle import workflow_caller
from services.core_api.routes.listening import interruption_view

router = APIRouter(tags=["core-internal"])

PRODUCER = "core-api"
MAX_OWNS = 10
MAX_PROJECTS = 10


class Listener(BaseModel):
    user_id: uuid.UUID
    name: str
    first_name: str
    teams: list[str]
    projects: list[str]
    listen_for: list[str]
    owns: list[str]
    interruptible: bool
    quiet_reason: str | None = None
    next_allowed_at: datetime | None = None


class MeetingListeners(BaseModel):
    meeting_id: uuid.UUID
    title: str
    confidential: bool
    status: str
    listeners: list[Listener]


@router.get("/internal/meetings/{meeting_id}/listeners", response_model=MeetingListeners)
async def meeting_listeners(
    meeting_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(agent_runtime_caller)],
) -> MeetingListeners:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        access = await records.access_for(session, meeting_id, uuid.UUID(int=0))
        if access is None:
            raise not_found()
        if access.confidential:
            return MeetingListeners(
                meeting_id=meeting_id,
                title=access.title,
                confidential=True,
                status=access.status,
                listeners=[],
            )
        if await listening.apply_rules_for_meeting(session, meeting_id) and access.status == "live":
            await listening.announce_listeners(session, org_id, meeting_id)
        now = datetime.now(UTC)
        people = (
            (
                await session.execute(
                    text(
                        "SELECT p.user_id, u.email, u.listen_for FROM meeting_participants p "
                        "JOIN users u ON u.id = p.user_id AND u.deleted_at IS NULL "
                        "WHERE p.meeting_id = :m AND p.attendance = 'agent' "
                        "AND p.attendance_choice IS DISTINCT FROM 'skip' ORDER BY p.user_id"
                    ),
                    {"m": str(meeting_id)},
                )
            )
            .mappings()
            .all()
        )
        listeners: list[Listener] = []
        for p in people:
            uid = p["user_id"]
            name = records.display_name(p["email"]) or "Your colleague"
            teams = [
                r[0]
                for r in await session.execute(
                    text(
                        "SELECT t.name FROM team_members tm JOIN teams t ON t.id = tm.team_id "
                        "WHERE tm.user_id = :u ORDER BY t.name"
                    ),
                    {"u": str(uid)},
                )
            ]
            projects = [
                r[0]
                for r in await session.execute(
                    text(
                        "SELECT title FROM org_goals WHERE owner_user_id = :u "
                        "AND status = 'active' ORDER BY position, title LIMIT :n"
                    ),
                    {"u": str(uid), "n": MAX_PROJECTS},
                )
            ]
            projects += [
                r[0]
                for r in await session.execute(
                    text(
                        "SELECT title FROM meeting_agenda_items WHERE meeting_id = :m "
                        "AND owner_user_id = :u ORDER BY position LIMIT 5"
                    ),
                    {"m": str(meeting_id), "u": str(uid)},
                )
            ]
            owns = [
                r[0]
                for r in await session.execute(
                    text(
                        "SELECT content FROM action_items WHERE assignee_id = :u "
                        "AND deleted_at IS NULL AND status = 'confirmed' "
                        "ORDER BY created_at DESC LIMIT :n"
                    ),
                    {"u": str(uid), "n": MAX_OWNS},
                )
            ]
            g = await listening.gate(session, uid, meeting_id, now)
            listeners.append(
                Listener(
                    user_id=uid,
                    name=name,
                    first_name=listening.first_name(name),
                    teams=teams,
                    projects=list(dict.fromkeys(projects)),
                    listen_for=[t for t in (p["listen_for"] or []) if t],
                    owns=owns,
                    interruptible=g.open,
                    quiet_reason=g.reason,
                    next_allowed_at=g.next_allowed_at,
                )
            )
    return MeetingListeners(
        meeting_id=meeting_id,
        title=access.title,
        confidential=False,
        status=access.status,
        listeners=listeners,
    )


class InterruptionProposal(BaseModel):
    model_config = ConfigDict(extra="ignore")

    user_id: uuid.UUID
    key: str = Field(min_length=1, max_length=200)
    reason: Literal["decision", "question", "promise"]
    headline: str = Field(min_length=1, max_length=160)
    detail: str | None = Field(default=None, max_length=400)
    topic: str | None = Field(default=None, max_length=120)
    quote: str | None = Field(default=None, max_length=300)
    start_ms: int | None = Field(default=None, ge=0)
    end_ms: int | None = Field(default=None, ge=0)
    confidence: float = Field(ge=0, le=1)
    prompt: str | None = Field(default=None, max_length=64)


Suppressed = Literal["in_room", "not_listening", "confidential", "ended"]


class InterruptionDecision(BaseModel):
    id: uuid.UUID | None = None
    status: Literal["delivered", "held", "suppressed"]
    reason: str | None = None
    next_allowed_at: datetime | None = None


async def _existing(
    session: AsyncSession, user_id: uuid.UUID, key: str
) -> InterruptionDecision | None:
    row = (
        await session.execute(
            text(
                "SELECT id, status, held_reason FROM agent_interruptions "
                "WHERE user_id = :u AND idempotency_key = :k"
            ),
            {"u": str(user_id), "k": key},
        )
    ).first()
    if row is None:
        return None
    return InterruptionDecision(id=row[0], status=row[1], reason=row[2])


@router.post("/internal/meetings/{meeting_id}/interruptions", response_model=InterruptionDecision)
async def propose_interruption(
    meeting_id: uuid.UUID,
    body: InterruptionProposal,
    org_id: Annotated[uuid.UUID, Depends(agent_runtime_caller)],
) -> InterruptionDecision:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        access = await records.access_for(session, meeting_id, body.user_id)
        if access is None or not access.is_participant:
            raise not_found()
        found = await _existing(session, body.user_id, body.key)
        if found is not None:
            return found
        if access.confidential:
            return InterruptionDecision(status="suppressed", reason="confidential")
        if access.status != "live":
            return InterruptionDecision(status="suppressed", reason="ended")
        attendance = (
            await session.execute(
                text(
                    "SELECT attendance FROM meeting_participants WHERE meeting_id = :m "
                    "AND user_id = :u"
                ),
                {"m": str(meeting_id), "u": str(body.user_id)},
            )
        ).scalar()
        if attendance != "agent":
            return InterruptionDecision(status="suppressed", reason="not_listening")
        if await listening.in_room(org_id, meeting_id, body.user_id):
            return InterruptionDecision(status="suppressed", reason="in_room")
        prefs = await listening.load_preferences(session, body.user_id)
        g = await listening.gate(session, body.user_id, meeting_id, datetime.now(UTC), prefs)
        status: Literal["delivered", "held"] = "delivered" if g.open else "held"
        row_id = (
            await session.execute(
                text(
                    "INSERT INTO agent_interruptions (org_id, user_id, meeting_id, "
                    "idempotency_key, reason, headline, detail, topic, quote, start_ms, end_ms, "
                    "confidence, prompt_version, status, held_reason) VALUES (:o, :u, :m, :k, "
                    ":r, :h, :d, :t, :q, :s, :e, :c, :p, :st, :hr) "
                    "ON CONFLICT (user_id, idempotency_key) DO NOTHING RETURNING id"
                ),
                {
                    "o": str(org_id),
                    "u": str(body.user_id),
                    "m": str(meeting_id),
                    "k": body.key,
                    "r": body.reason,
                    "h": " ".join(body.headline.split()),
                    "d": body.detail,
                    "t": body.topic,
                    "q": body.quote,
                    "s": body.start_ms,
                    "e": body.end_ms,
                    "c": body.confidence,
                    "p": body.prompt,
                    "st": status,
                    "hr": g.reason,
                },
            )
        ).scalar()
        if row_id is None:  # a concurrent replay won the insert
            again = await _existing(session, body.user_id, body.key)
            assert again is not None
            return again
        if status == "delivered":
            await _deliver(session, org_id, body.user_id, row_id, prefs)
        return InterruptionDecision(
            id=row_id, status=status, reason=g.reason, next_allowed_at=g.next_allowed_at
        )


async def _deliver(
    session: AsyncSession,
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    interruption_id: uuid.UUID,
    prefs: listening.ListeningPreferences,
) -> None:
    row = (
        (
            await session.execute(
                text(
                    "SELECT i.id, i.meeting_id, m.title AS meeting_title, m.status AS "
                    "meeting_status, m.scheduled_start, i.reason, i.headline, i.detail, i.topic, "
                    "i.quote, i.start_ms, i.status, i.held_reason, i.response, i.responded_at, "
                    "i.created_at FROM agent_interruptions i JOIN meetings m "
                    "ON m.id = i.meeting_id WHERE i.id = :id"
                ),
                {"id": str(interruption_id)},
            )
        )
        .mappings()
        .one()
    )
    view = interruption_view(row, prefs.zone())
    body = view.detail or (f"“{view.quote}”" if view.quote else "")
    body = f"{view.meeting_title}: {body}" if body else view.meeting_title
    await enqueue(
        session,
        EventEnvelope(
            topic="notifications",
            org_id=str(org_id),
            producer=PRODUCER,
            idempotency_key=f"notif:interrupt:{interruption_id}:in_app:{user_id}",
            payload={
                "source_event_id": f"interrupt:{interruption_id}",
                "channel": "in_app",
                "user_id": str(user_id),
                "priority": 1,
                "title": view.title[:200],
                "body": body[:2000],
                "link": f"/inbox?interruption={interruption_id}",
            },
        ),
    )
    await listening.publish(
        listening.user_channel(org_id, user_id),
        {
            "type": "agent-interrupt",
            "userId": str(user_id),
            "payload": view.model_dump(mode="json"),
        },
    )


# --- the evening run -------------------------------------------------------------------------


class EveningRun(BaseModel):
    model_config = ConfigDict(extra="forbid")

    now: datetime | None = Field(default=None, description="Defaults to the server's clock.")


class EveningRunResult(BaseModel):
    built: int
    notified: int
    skipped: int
    rules_applied: int


@router.post("/internal/listening/evening", response_model=EveningRunResult)
async def evening_run(
    body: EveningRun,
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
) -> EveningRunResult:
    now = body.now or datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        people = [
            r[0]
            for r in await session.execute(
                text("SELECT id FROM users WHERE deleted_at IS NULL ORDER BY id")
            )
        ]
    built = notified = skipped = applied = 0
    for user_id in people:
        async with get_sessionmaker()() as session, session.begin():
            await set_rls(session, org_id)
            for meeting_id in await listening.apply_rules_for_user(session, user_id, now):
                applied += 1
                status = (
                    await session.execute(
                        text("SELECT status FROM meetings WHERE id = :m"), {"m": str(meeting_id)}
                    )
                ).scalar()
                if status == "live":
                    await listening.announce_listeners(session, org_id, meeting_id)
            prefs = await listening.load_preferences(session, user_id)
            local = now.astimezone(prefs.zone())
            if not prefs.digest_enabled or local.hour < prefs.digest_hour:
                skipped += 1
                continue
            day = local.date()
            exists = (
                await session.execute(
                    text("SELECT 1 FROM evening_digests WHERE user_id = :u AND day = :d"),
                    {"u": str(user_id), "d": day},
                )
            ).first()
            if exists is not None:
                skipped += 1
                continue
            digest = await listening.build_evening(session, user_id, day, prefs.zone())
            created = (
                await session.execute(
                    text(
                        "INSERT INTO evening_digests (org_id, user_id, day, content, "
                        "meeting_count) VALUES (:o, :u, :d, CAST(:c AS jsonb), :n) "
                        "ON CONFLICT (user_id, day) DO NOTHING RETURNING id"
                    ),
                    {
                        "o": str(org_id),
                        "u": str(user_id),
                        "d": day,
                        "c": digest.model_dump_json(exclude={"id", "created_at"}),
                        "n": digest.meeting_count,
                    },
                )
            ).scalar()
            if created is None:
                skipped += 1
                continue
            built += 1
            if digest.status != "ready":
                continue
            listened = sum(1 for m in digest.meetings if m.choice == "agent_listens")
            hours = round(digest.minutes_saved / 60, 1)
            await enqueue(
                session,
                EventEnvelope(
                    topic="notifications",
                    org_id=str(org_id),
                    producer=PRODUCER,
                    idempotency_key=f"notif:evening-digest:{created}:in_app:{user_id}",
                    payload={
                        "source_event_id": f"evening-digest:{created}",
                        "channel": "in_app",
                        "user_id": str(user_id),
                        "priority": 3,
                        "title": "What your agent heard today",
                        "body": (
                            f"{listened} meeting{'s' if listened != 1 else ''} covered for you, "
                            f"{_hours(hours)} back."
                        ),
                        "link": f"/meetings/listening?day={day.isoformat()}",
                    },
                ),
            )
            notified += 1
    return EveningRunResult(built=built, notified=notified, skipped=skipped, rules_applied=applied)


def _hours(hours: float) -> str:
    if hours < 1:
        return f"{int(round(hours * 60))} minutes"
    whole = int(hours) if float(hours).is_integer() else hours
    return f"{whole} hour{'s' if hours != 1 else ''}"
