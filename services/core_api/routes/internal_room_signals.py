"""`GET /internal/meetings/{meeting_id}/room-signals-source`: everything the MeetingAgent's
room-signals analyzer needs for an ENDED meeting (409 before it ended), for
`agent-runtime-service` only (the minutes-source scheme: `X-Mytheus-Service`, its secret from
`MYTHEUS_CORE_CALLER_TOKENS`, the tenant in `X-Mytheus-Org`; not routed by `api-gateway`).

Reading it files the realtime-gateway's Redis list of room signals into `room_signals` first
(`room_signals.file_room_signals`), so the durable table is the analyzer's input. Times are on
the meeting clock (ms since the room opened, `meeting_rooms.started_at`, the transcript's own
origin); a room signal from before the room opened is dropped. Names are display names from the
email, never the email. `listen_for` is the participant's own listen-for topics.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import text

from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.core_api import records
from services.core_api import room_signals as rs
from services.core_api.routes.internal_agents import MAX_TURNS, SourceTurn, agent_runtime_caller

router = APIRouter(tags=["core-internal"])


class SignalsParticipant(BaseModel):
    user_id: uuid.UUID
    name: str | None
    attendance: str
    role_in_meeting: str | None
    listen_for: list[str]


class SignalsDecision(BaseModel):
    id: uuid.UUID
    content: str
    status: str
    owner_user_id: uuid.UUID | None
    quote: str | None
    quote_speaker_id: uuid.UUID | None
    quote_start_ms: int | None
    quote_end_ms: int | None


class SignalsAgendaItem(BaseModel):
    id: uuid.UUID
    title: str
    status: str
    started_ms: int | None
    ended_ms: int | None


class SignalsRoomSignal(BaseModel):
    id: uuid.UUID
    kind: str
    user_id: uuid.UUID
    at_ms: int
    end_ms: int
    detail: dict[str, Any]


class RoomSignalsSource(BaseModel):
    meeting_id: uuid.UUID
    title: str
    confidential: bool
    host_user_id: uuid.UUID | None
    previous_meeting_id: uuid.UUID | None
    scheduled_start: datetime | None
    scheduled_end: datetime | None
    room_started_at: datetime | None
    ended_at: datetime | None
    call_ms: int | None
    participants: list[SignalsParticipant]
    speakers: dict[str, uuid.UUID]
    turns: list[SourceTurn]
    turns_truncated: bool
    decisions: list[SignalsDecision]
    agenda: list[SignalsAgendaItem]
    room_signals: list[SignalsRoomSignal]


def _clock(at: datetime | None, origin: datetime | None) -> int | None:
    if at is None or origin is None:
        return None
    ms = int((at - origin).total_seconds() * 1000)
    return ms if ms >= 0 else None


@router.get("/internal/meetings/{meeting_id}/room-signals-source", response_model=RoomSignalsSource)
async def room_signals_source(
    meeting_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(agent_runtime_caller)],
) -> RoomSignalsSource:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        access = await records.access_for(session, meeting_id, rs.NIL)
        if access is None:
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        if access.status != "ended":
            raise ProblemDetail(409, ErrorCode.VERSION_CONFLICT, "The meeting has not ended")
        m = {"m": str(meeting_id)}
        await rs.file_room_signals(session, org_id, meeting_id)
        meeting = (
            (
                await session.execute(
                    text(
                        "SELECT m.scheduled_start, m.scheduled_end, m.previous_meeting_id, "
                        "COALESCE(m.closed_at, m.updated_at) AS ended_at, "
                        "(SELECT r.started_at FROM meeting_rooms r WHERE r.meeting_id = m.id "
                        " ORDER BY r.started_at NULLS LAST LIMIT 1) AS room_started_at "
                        "FROM meetings m WHERE m.id = :m"
                    ),
                    m,
                )
            )
            .mappings()
            .one()
        )
        origin = meeting["room_started_at"]
        people = (
            (
                await session.execute(
                    text(
                        "SELECT p.user_id, u.email, u.display_name, p.attendance, "
                        "p.role_in_meeting, "
                        "COALESCE(u.listen_for, '{}') AS listen_for "
                        "FROM meeting_participants p JOIN users u ON u.id = p.user_id "
                        "AND u.deleted_at IS NULL WHERE p.meeting_id = :m "
                        "ORDER BY (p.role_in_meeting = 'host') DESC NULLS LAST, u.email, p.user_id"
                    ),
                    m,
                )
            )
            .mappings()
            .all()
        )
        speakers = {
            str(r[0]): r[1]
            for r in await session.execute(
                text(
                    "SELECT speaker_id, user_id FROM meeting_speakers "
                    "WHERE meeting_id = :m AND user_id IS NOT NULL"
                ),
                m,
            )
        }
        turns: list[SourceTurn] = []
        truncated = False
        if (
            await session.execute(
                text("SELECT to_regclass('public.transcript_chunks') IS NOT NULL")
            )
        ).scalar():
            rows = (
                (
                    await session.execute(
                        text(
                            "SELECT speaker_id, start_ms, end_ms, text FROM transcript_chunks "
                            "WHERE meeting_id = :m AND is_final = true "
                            "ORDER BY start_ms, id LIMIT :n"
                        ),
                        {**m, "n": MAX_TURNS + 1},
                    )
                )
                .mappings()
                .all()
            )
            truncated = len(rows) > MAX_TURNS
            turns = [SourceTurn(**dict(r)) for r in rows[:MAX_TURNS]]
        decisions = (
            (
                await session.execute(
                    text(
                        "SELECT id, content, status, COALESCE(review_owner_id, confirmed_by) "
                        "AS owner_user_id, quote, quote_speaker_id, quote_start_ms, quote_end_ms "
                        "FROM decisions WHERE meeting_id = :m AND deleted_at IS NULL "
                        "AND status <> 'dismissed' ORDER BY created_at, id LIMIT 200"
                    ),
                    m,
                )
            )
            .mappings()
            .all()
        )
        agenda = (
            (
                await session.execute(
                    text(
                        "SELECT id, title, status, started_at, ended_at FROM meeting_agenda_items "
                        "WHERE meeting_id = :m ORDER BY position, id"
                    ),
                    m,
                )
            )
            .mappings()
            .all()
        )
        signals = (
            (
                await session.execute(
                    text(
                        "SELECT id, kind, user_id, occurred_at, ended_at, detail FROM room_signals "
                        "WHERE meeting_id = :m ORDER BY occurred_at, id LIMIT 500"
                    ),
                    m,
                )
            )
            .mappings()
            .all()
        )
    room_signals: list[SignalsRoomSignal] = []
    for s in signals:
        at, end = _clock(s["occurred_at"], origin), _clock(s["ended_at"], origin)
        if at is None or end is None:
            continue
        room_signals.append(
            SignalsRoomSignal(
                id=s["id"],
                kind=s["kind"],
                user_id=s["user_id"],
                at_ms=at,
                end_ms=end,
                detail=dict(s["detail"] or {}),
            )
        )
    ended_at = meeting["ended_at"]
    return RoomSignalsSource(
        meeting_id=meeting_id,
        title=access.title,
        confidential=access.confidential,
        host_user_id=access.host_id,
        previous_meeting_id=meeting["previous_meeting_id"],
        scheduled_start=meeting["scheduled_start"],
        scheduled_end=meeting["scheduled_end"],
        room_started_at=origin,
        ended_at=ended_at,
        call_ms=_clock(ended_at, origin),
        participants=[
            SignalsParticipant(
                user_id=p["user_id"],
                name=records.person_name(p["display_name"], p["email"]),
                attendance=p["attendance"],
                role_in_meeting=p["role_in_meeting"],
                listen_for=[str(t) for t in p["listen_for"]][:15],
            )
            for p in people
        ],
        speakers=speakers,
        turns=turns,
        turns_truncated=truncated,
        decisions=[SignalsDecision(**dict(d)) for d in decisions],
        agenda=[
            SignalsAgendaItem(
                id=a["id"],
                title=a["title"],
                status=a["status"],
                started_ms=_clock(a["started_at"], origin),
                ended_ms=_clock(a["ended_at"], origin),
            )
            for a in agenda
        ],
        room_signals=room_signals,
    )
