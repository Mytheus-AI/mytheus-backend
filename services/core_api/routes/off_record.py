"""Off the record (polish program wave 3, "Only Mytheus can do these").

A meeting can go off the record for a stretch: every agent pauses and nothing said is
transcribed, captioned, indexed or kept. Mytheus owns the room, so this is enforced on the
server, never only in the UI.

THE RULE (who may do what):
- The meeting's host or its creator starts and ends it (`POST .../off-record`,
  `POST .../off-record/end`), the same people who steer the agenda (ADR-0038 D6).
- Any participant may ASK: the room carries the request to the host through realtime-gateway
  (`off-record-request`); the host grants it by starting a segment with `requested_by` naming the
  person who asked. Asking stores nothing.
- Ending the meeting closes an open segment (`close_open_segment`, called by the meeting PATCH).

WHAT STARTING DOES, in one request:
1. One `meeting_off_record_segments` row (at most one open per meeting: a second start while one
   runs answers the running one, 200, idempotent).
2. The Redis flag `nexus:{org_id}:off-record:{meeting_id}` = JSON `{segment_id, started_at,
   started_by}`, TTL 24 h as a safety net. transcription-service stops persisting finals,
   producing `transcript-chunks` and publishing captions (interim and final) while it is set;
   agent-runtime-service runs no agent for a window that starts while it is set; the indexers
   (embedding-service, search-service) store nothing. The flag is written BEFORE the row commits:
   if Redis cannot be written the request fails (503) and the row rolls back, so the room never
   says "off the record" while the pipeline keeps recording. A row that then fails to commit
   leaves the flag set, which errs on the side of not recording.
3. After commit, the new state is published on the meeting's fan-out channel
   `nexus:{org_id}:pubsub:meeting:{meeting_id}` as `{"type": "off-record", "meetingId", "payload":
   OffRecordState-as-camelCase}`; realtime-gateway forwards it to every socket in the meeting
   ("Off the record since 12:04, agents paused"). Best-effort: a missed publish is repaired by the
   gateway reading the flag on every join and by the room re-reading `GET .../off-record`.

Ending deletes the flag first (503 on failure, nothing changes), then closes the row and
publishes.

RECORDING (polish wave 3, Recording playback). After a start commits, core-api asks
meeting-service to stop the recording egress (`recording_client.pause`); after an end, to start
a new one (`recording_client.resume`). So the bucket keeps nothing from the stretch that was off
the record and the replay shows it as a gap. Best-effort after the response: meeting-service
also refuses to start a recording while the flag is set.

`start_ms`/`end_ms` place the gap on the meeting's clock (`meeting_rooms.started_at` is zero,
the transcript's origin), so the transcript, the minutes and a replay show "Off the record,
4 min" in place.

Routes (member tier, participants only; another tenant's meeting is a 404):
- `GET  /api/v1/meetings/{id}/off-record`      the state and every segment so far.
- `POST /api/v1/meetings/{id}/off-record`      start (host or creator) `{requested_by?}`.
- `POST /api/v1/meetings/{id}/off-record/end`  end (host or creator).
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, BackgroundTasks, Depends, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import (
    ACTION_MEETING_OFF_RECORD_ENDED,
    ACTION_MEETING_OFF_RECORD_STARTED,
    audit,
)
from libs.shared.authz import RequestContext, not_found, require_tier
from libs.shared.cache import get_redis, redis_key
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.core_api import recording_client

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["off-the-record"])

# A safety net, not the end of a segment: a flag nobody ended (a host who lost the tab and a
# meeting nobody ended) stops pausing the pipeline a day later. Erring long is erring private.
FLAG_TTL_S = 24 * 3600
_CLOSED = frozenset({"ended", "cancelled"})
_COLS = (
    "id, started_at, started_by, requested_by, ended_at, ended_by, start_ms, end_ms"  # noqa: S608
)


def off_record_key(org_id: uuid.UUID | str, meeting_id: uuid.UUID | str) -> str:
    """`nexus:{org_id}:off-record:{meeting_id}`, read by transcription-service, agent-runtime
    and the indexers (mytheus-realtime, mytheus-intelligence `libs/shared/off_record.py`)."""
    return redis_key(org_id, "off-record", meeting_id)


def pubsub_channel(org_id: uuid.UUID | str, meeting_id: uuid.UUID | str) -> str:
    """The meeting's fan-out channel realtime-gateway subscribes to (`libs/keys.ts`)."""
    return redis_key(org_id, "pubsub", "meeting", meeting_id)


# --- models -------------------------------------------------------------------------------


class OffRecordStart(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requested_by: uuid.UUID | None = Field(
        default=None,
        description="The participant whose request the host is granting, if any. Must be a "
        "participant of the meeting.",
    )


class OffRecordSegment(BaseModel):
    """One stretch of the meeting that was (or is) off the record."""

    id: uuid.UUID
    started_at: datetime
    started_by: uuid.UUID | None
    requested_by: uuid.UUID | None = Field(
        default=None, description="Who asked for it, when the host granted a request."
    )
    ended_at: datetime | None = Field(default=None, description="Null while it runs.")
    ended_by: uuid.UUID | None = Field(
        default=None, description="Null while it runs, or when the meeting ending closed it."
    )
    start_ms: int | None = Field(
        default=None,
        description="Where it began on the meeting's clock (the transcript's time origin), "
        "or null when the room clock is unknown.",
    )
    end_ms: int | None = None
    duration_ms: int | None = Field(
        default=None, description="Length of a finished segment; null while it runs."
    )


class OffRecordState(BaseModel):
    """Whether the meeting is off the record now, and every off-the-record stretch so far."""

    meeting_id: uuid.UUID
    active: bool
    current: OffRecordSegment | None = Field(
        default=None, description="The running segment, when active."
    )
    segments: list[OffRecordSegment] = Field(
        default_factory=list, description="Every segment in time order, the running one last."
    )
    total_ms: int = Field(default=0, description="Total length of the finished segments.")
    can_toggle: bool = Field(
        description="Whether the caller (the host or the creator) may start and end it."
    )


# --- helpers ------------------------------------------------------------------------------


def _not_a_participant() -> ProblemDetail:
    return ProblemDetail(
        status.HTTP_403_FORBIDDEN,
        ErrorCode.NOT_A_PARTICIPANT,
        "You are not a participant of this meeting",
    )


def _host_only() -> ProblemDetail:
    return ProblemDetail(
        status.HTTP_403_FORBIDDEN,
        ErrorCode.FORBIDDEN_ROLE,
        "Only the meeting's host or its creator can take the meeting off the record; "
        "ask them from the room",
    )


def _unavailable() -> ProblemDetail:
    return ProblemDetail(
        status.HTTP_503_SERVICE_UNAVAILABLE,
        ErrorCode.DEPENDENCY_UNAVAILABLE,
        "Could not pause recording right now. Nothing changed; try again.",
    )


async def _meeting(session: AsyncSession, meeting_id: uuid.UUID, viewer: uuid.UUID | None) -> Any:
    """The meeting under RLS (404 if invisible), whether the viewer takes part, and whether the
    viewer steers it (host or creator, ADR-0038 D6)."""
    row = (
        (
            await session.execute(
                text(
                    "SELECT m.id, m.status, m.created_by, "
                    "EXISTS (SELECT 1 FROM meeting_participants p WHERE p.meeting_id = m.id "
                    "        AND p.user_id = :v) AS is_participant, "
                    "EXISTS (SELECT 1 FROM meeting_participants p WHERE p.meeting_id = m.id "
                    "        AND p.user_id = :v AND p.role_in_meeting = 'host') AS is_host, "
                    "(SELECT r.started_at FROM meeting_rooms r WHERE r.meeting_id = m.id) "
                    "  AS room_started_at "
                    "FROM meetings m WHERE m.id = :m AND m.deleted_at IS NULL"
                ),
                {"m": str(meeting_id), "v": str(viewer or uuid.UUID(int=0))},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()
    return row


def _steers(row: Any, viewer: uuid.UUID | None) -> bool:
    return viewer is not None and (bool(row["is_host"]) or row["created_by"] == viewer)


def _ms_since(origin: datetime | None, at: datetime) -> int | None:
    if origin is None:
        return None
    return max(0, int((at - origin).total_seconds() * 1000))


def _segment(row: Any) -> OffRecordSegment:
    duration = None
    if row["ended_at"] is not None:
        duration = max(0, int((row["ended_at"] - row["started_at"]).total_seconds() * 1000))
    return OffRecordSegment(**dict(row), duration_ms=duration)


async def _segments(session: AsyncSession, meeting_id: uuid.UUID) -> list[OffRecordSegment]:
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_COLS} FROM meeting_off_record_segments "  # noqa: S608
                    "WHERE meeting_id = :m ORDER BY started_at, id"
                ),
                {"m": str(meeting_id)},
            )
        )
        .mappings()
        .all()
    )
    return [_segment(r) for r in rows]


def _state(
    meeting_id: uuid.UUID, segments: list[OffRecordSegment], can_toggle: bool
) -> OffRecordState:
    current = next((s for s in segments if s.ended_at is None), None)
    return OffRecordState(
        meeting_id=meeting_id,
        active=current is not None,
        current=current,
        segments=segments,
        total_ms=sum(s.duration_ms or 0 for s in segments),
        can_toggle=can_toggle,
    )


def _wire(state: OffRecordState, actor: uuid.UUID | None) -> dict[str, Any]:
    """The broadcast payload: camelCase like every other room event."""
    cur = state.current
    last = state.segments[-1] if state.segments else None
    seg = cur or last
    return {
        "active": state.active,
        "segmentId": str(seg.id) if seg else None,
        "startedAt": seg.started_at.isoformat() if seg else None,
        "startedBy": str(seg.started_by) if seg and seg.started_by else None,
        "requestedBy": str(seg.requested_by) if seg and seg.requested_by else None,
        "endedAt": seg.ended_at.isoformat() if seg and seg.ended_at else None,
        "endedBy": str(seg.ended_by) if seg and seg.ended_by else None,
        "totalMs": state.total_ms,
        "byUserId": str(actor) if actor else None,
        "at": datetime.now(UTC).isoformat(),
    }


async def _publish(org_id: uuid.UUID, meeting_id: uuid.UUID, payload: dict[str, Any]) -> None:
    message = {"type": "off-record", "meetingId": str(meeting_id), "payload": payload}
    try:
        await get_redis().publish(pubsub_channel(org_id, meeting_id), json.dumps(message))
    except Exception:  # noqa: BLE001 - best-effort; the gateway re-reads the flag on join
        logger.warning("off-record broadcast failed for meeting %s", meeting_id, exc_info=True)


async def _participant(session: AsyncSession, meeting_id: uuid.UUID, user_id: uuid.UUID) -> bool:
    return (
        await session.execute(
            text("SELECT 1 FROM meeting_participants WHERE meeting_id = :m AND user_id = :u"),
            {"m": str(meeting_id), "u": str(user_id)},
        )
    ).first() is not None


async def close_open_segment(
    session: AsyncSession,
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    ended_by: uuid.UUID | None,
) -> OffRecordSegment | None:
    """Close the meeting's running segment, if any, and clear the flag. Used by the end route
    and by the meeting PATCH when the meeting ends (`ended_by` None there: the meeting ending
    closed it). The flag delete is best-effort here: a meeting that ended has no audio left."""
    room_started = (
        await session.execute(
            text("SELECT started_at FROM meeting_rooms WHERE meeting_id = :m"),
            {"m": str(meeting_id)},
        )
    ).scalar()
    now = datetime.now(UTC)
    row = (
        (
            await session.execute(
                text(
                    "UPDATE meeting_off_record_segments SET ended_at = :t, ended_by = :u, "
                    "end_ms = :e WHERE meeting_id = :m AND ended_at IS NULL "
                    f"RETURNING {_COLS}"  # noqa: S608
                ),
                {
                    "t": now,
                    "u": str(ended_by) if ended_by else None,
                    "e": _ms_since(room_started, now),
                    "m": str(meeting_id),
                },
            )
        )
        .mappings()
        .first()
    )
    try:
        await get_redis().delete(off_record_key(org_id, meeting_id))
    except Exception:  # noqa: BLE001 - see docstring; the 24 h TTL is the backstop
        logger.warning("off-record flag delete failed for meeting %s", meeting_id, exc_info=True)
    return _segment(row) if row is not None else None


# --- routes -------------------------------------------------------------------------------


@router.get("/meetings/{meeting_id}/off-record", response_model=OffRecordState)
async def get_off_record(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
) -> OffRecordState:
    """Whether the meeting is off the record now, and every off-the-record stretch so far
    (participants only). The transcript, the minutes and the room read it."""
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, ctx.org_id)
        row = await _meeting(session, meeting_id, ctx.user_id)
        if not row["is_participant"]:
            raise _not_a_participant()
        segments = await _segments(session, meeting_id)
    return _state(meeting_id, segments, _steers(row, ctx.user_id))


@router.post("/meetings/{meeting_id}/off-record", response_model=OffRecordState)
async def start_off_record(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    background_tasks: BackgroundTasks,
    body: OffRecordStart | None = None,
) -> OffRecordState:
    """Take the meeting off the record (the host or the creator). Pauses every agent and all
    transcript persistence, captions included, until it is ended. Idempotent: while a segment
    runs, this answers it unchanged."""
    requested_by = body.requested_by if body is not None else None
    async with get_sessionmaker()() as session:
        async with session.begin():
            await set_rls(session, ctx.org_id)
            row = await _meeting(session, meeting_id, ctx.user_id)
            if not row["is_participant"]:
                raise _not_a_participant()
            if not _steers(row, ctx.user_id):
                raise _host_only()
            if row["status"] in _CLOSED:
                raise ProblemDetail(
                    status.HTTP_409_CONFLICT, ErrorCode.VERSION_CONFLICT, "This meeting has ended"
                )
            if requested_by is not None and not await _participant(
                session, meeting_id, requested_by
            ):
                message = "requested_by must be a participant of the meeting"
                raise ProblemDetail(
                    422,
                    ErrorCode.VALIDATION_FAILED,
                    message,
                    extra={
                        "errors": [
                            {"loc": ["body", "requested_by"], "msg": message, "type": "value_error"}
                        ]
                    },
                )
            now = datetime.now(UTC)
            inserted = (
                (
                    await session.execute(
                        text(
                            "INSERT INTO meeting_off_record_segments (org_id, meeting_id, "
                            "started_at, started_by, requested_by, start_ms) "
                            "VALUES (:o, :m, :t, :u, :r, :s) "
                            "ON CONFLICT (meeting_id) WHERE ended_at IS NULL DO NOTHING "
                            f"RETURNING {_COLS}"  # noqa: S608
                        ),
                        {
                            "o": str(ctx.org_id),
                            "m": str(meeting_id),
                            "t": now,
                            "u": str(ctx.user_id),
                            "r": str(requested_by) if requested_by else None,
                            "s": _ms_since(row["room_started_at"], now),
                        },
                    )
                )
                .mappings()
                .first()
            )
            if inserted is not None:
                flag = json.dumps(
                    {
                        "segment_id": str(inserted["id"]),
                        "started_at": inserted["started_at"].isoformat(),
                        "started_by": str(ctx.user_id),
                    }
                )
                try:
                    await get_redis().set(
                        off_record_key(ctx.org_id, meeting_id), flag, ex=FLAG_TTL_S
                    )
                except Exception as exc:  # noqa: BLE001 - fail the request: never say "paused"
                    logger.error("off-record flag write failed for meeting %s", meeting_id)
                    raise _unavailable() from exc
                await audit(
                    session,
                    org_id=ctx.org_id,
                    actor_id=ctx.user_id,
                    action=ACTION_MEETING_OFF_RECORD_STARTED,
                    target_type="meeting",
                    target_id=meeting_id,
                    metadata={
                        "segment_id": str(inserted["id"]),
                        "requested_by": str(requested_by) if requested_by else None,
                    },
                    ip=ctx.ip,
                )
            segments = await _segments(session, meeting_id)
        state = _state(meeting_id, segments, True)
    if inserted is not None:
        await _publish(ctx.org_id, meeting_id, _wire(state, ctx.user_id))
        # Recording playback: stop the recording too, so nothing from this stretch is kept.
        background_tasks.add_task(recording_client.pause, ctx.org_id, meeting_id)
    return state


@router.post("/meetings/{meeting_id}/off-record/end", response_model=OffRecordState)
async def end_off_record(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    background_tasks: BackgroundTasks,
) -> OffRecordState:
    """Back on the record (the host or the creator): transcription and the agents resume.
    Idempotent: with nothing running, this answers the state unchanged."""
    async with get_sessionmaker()() as session:
        async with session.begin():
            await set_rls(session, ctx.org_id)
            row = await _meeting(session, meeting_id, ctx.user_id)
            if not row["is_participant"]:
                raise _not_a_participant()
            if not _steers(row, ctx.user_id):
                raise _host_only()
            running = (
                await session.execute(
                    text(
                        "SELECT 1 FROM meeting_off_record_segments "
                        "WHERE meeting_id = :m AND ended_at IS NULL FOR UPDATE"
                    ),
                    {"m": str(meeting_id)},
                )
            ).first()
            closed = None
            if running is not None:
                # The flag goes first: if Redis cannot be written, the row stays open and the
                # room keeps saying "off the record", which is what is actually happening.
                try:
                    await get_redis().delete(off_record_key(ctx.org_id, meeting_id))
                except Exception as exc:  # noqa: BLE001
                    logger.error("off-record flag delete failed for meeting %s", meeting_id)
                    raise _unavailable() from exc
                closed = await close_open_segment(session, ctx.org_id, meeting_id, ctx.user_id)
                if closed is not None:
                    await audit(
                        session,
                        org_id=ctx.org_id,
                        actor_id=ctx.user_id,
                        action=ACTION_MEETING_OFF_RECORD_ENDED,
                        target_type="meeting",
                        target_id=meeting_id,
                        metadata={"segment_id": str(closed.id), "duration_ms": closed.duration_ms},
                        ip=ctx.ip,
                    )
            segments = await _segments(session, meeting_id)
        state = _state(meeting_id, segments, True)
    if closed is not None:
        await _publish(ctx.org_id, meeting_id, _wire(state, ctx.user_id))
        # Recording playback: back on the record, a new recording segment starts.
        background_tasks.add_task(recording_client.resume, ctx.org_id, meeting_id)
    return state
