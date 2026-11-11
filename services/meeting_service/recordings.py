"""Recording playback (polish wave 3): what meeting-service records about each egress, and the
routes that let a participant play it back.

WHERE THE FACTS COME FROM. LiveKit's room-composite egress (started on `room_started` by
`room_lifecycle.start_recording`, gated by `RECORDING_ENABLED`, the S3 settings and the consent
gate) writes the file straight into `mytheus-recordings` under `{org_id}/{meeting_id}/`. LiveKit
then sends three webhooks this service now records, one `meeting_recordings` row per egress:

- `egress_started` / `egress_updated`: the segment is `recording` (or `processing` once LiveKit
  reports `EGRESS_ENDING`);
- `egress_ended`: `ready` with the uploaded object's key, its length and size (status
  `EGRESS_COMPLETE`, or `EGRESS_LIMIT_REACHED`, which still uploads what it has), else `failed`.

`room_finished` moves any still-`recording` segment to `processing` (the room closed; the upload
is finishing). A status never moves backwards (a late `egress_updated` cannot undo `ready`).

OFF THE RECORD. When the host takes a meeting off the record, core-api calls
`POST /internal/meetings/{id}/recording/pause`, which stops the running egress; back on the
record it calls `.../resume`, which starts a new one. So the bucket holds nothing from the
stretch that was off the record, and a meeting has one segment per recorded stretch, placed on
the meeting's clock by `offset_ms` (the gaps are exactly the off-the-record segments).
`start_recording` also refuses to start while the off-record flag is set.

PLAYBACK. `GET /api/meetings/{meeting_id}/recording` (participants only; another tenant's
meeting is 404, a non-participant 403, deny-by-default) answers the recording's status and, for
each ready segment, a presigned GET valid for `RECORDING_URL_TTL_SECONDS` (default 600 s).
Every answer that carries URLs writes one `recording.playback_issued` audit row. Consent: a
recording exists only when the consent gate allowed it (`ConsentGate.recording_allowed`), and a
refusal is remembered (`meeting_rooms.recording_skip_reason = consent_missing`) so the page can
say why there is none.

INTERNAL (core-api only, `X-Mytheus-Service: core-api` with its `MYTHEUS_MEETING_CALLER_TOKENS`
secret; never routed by api-gateway):
- `POST /internal/meetings/{id}/recording/pause|resume` (off the record);
- `POST /internal/meetings/{id}/recording/erase` (a meeting's data erasure: every object under
  the meeting's prefix, clips included, and its `meeting_recordings` rows);
- `POST /internal/meetings/{id}/clips/{clip_id}/video` (the video of a publicly shared clip:
  the clip's own rendered object, never the whole recording; see `clips.py`).

NOT CONNECTED in production today: with `RECORDING_ENABLED` unset the route answers
`not_enabled`, and with the flag on but no bucket credentials `not_enabled` +
`storage_not_configured`. Everything lights up when those are set.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, cast

from fastapi import APIRouter, BackgroundTasks, Depends, Path
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from libs.shared.audit import (
    ACTION_RECORDING_CLIP_VIDEO_ISSUED,
    ACTION_RECORDING_ERASED,
    ACTION_RECORDING_PLAYBACK_ISSUED,
    audit,
)
from libs.shared.authz import RequestContext, not_found, tenant_context
from libs.shared.cache import get_redis
from libs.shared.db import get_sessionmaker
from libs.shared.tenancy import set_rls
from services.meeting_service import clips
from services.meeting_service.internal import core_api_caller
from services.meeting_service.recording_objects import (
    RecordingObjects,
    get_recording_objects,
    url_ttl_seconds,
)
from services.meeting_service.rooms import meeting_has_ended, meeting_is_visible
from services.meeting_service.routes import (
    _BIND_STATEMENT_TIMEOUT,
    _fetch_participant,
    _not_a_participant,
    get_transport,
)
from services.meeting_service.schemas import (
    ClipVideo,
    ClipVideoRequest,
    RecordingControlResult,
    RecordingErased,
    RecordingSegment,
    RecordingState,
)
from services.meeting_service.storage import RecordingConfig, recording_prefix
from services.meeting_service.transport import MeetingTransport

logger = logging.getLogger(__name__)

router = APIRouter(tags=["recordings"])
internal_router = APIRouter(tags=["meeting-internal"])

# livekit.EgressStatus, by number (this module must not import the SDK; see transport.py).
EGRESS_STARTING, EGRESS_ACTIVE, EGRESS_ENDING = 0, 1, 2
EGRESS_COMPLETE, EGRESS_FAILED, EGRESS_ABORTED, EGRESS_LIMIT_REACHED = 3, 4, 5, 6

SKIP_CONSENT_MISSING = "consent_missing"
_RANK = "CASE meeting_recordings.status WHEN 'recording' THEN 0 WHEN 'processing' THEN 1 ELSE 2 END"
_SEGMENT_COLS = (
    "id, egress_id, status, object_key, media_type, started_at, ended_at, duration_ms, size_bytes"
)


# --- facts from LiveKit ---------------------------------------------------------------------


def _ns_to_dt(value: Any) -> datetime | None:
    try:
        ns = int(value or 0)
    except (TypeError, ValueError):
        return None
    if ns <= 0:
        return None
    return datetime.fromtimestamp(ns / 1_000_000_000, tz=UTC)


def _ns_to_ms(value: Any) -> int | None:
    try:
        ns = int(value or 0)
    except (TypeError, ValueError):
        return None
    return ns // 1_000_000 if ns > 0 else None


def media_type_for(key: str | None, *, audio_only: bool = False) -> str:
    if key:
        return "audio/ogg" if key.lower().endswith(".ogg") else "video/mp4"
    return "audio/ogg" if audio_only else "video/mp4"


def _first_file(egress_info: object) -> object | None:
    for result in list(getattr(egress_info, "file_results", None) or []):
        if getattr(result, "filename", None):
            return cast(object, result)
    legacy: object | None = getattr(egress_info, "file", None)
    if legacy is not None and getattr(legacy, "filename", None):
        return legacy
    return None


async def record_segment_started(
    session: AsyncSession,
    *,
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    egress_id: str,
    media_type: str,
) -> None:
    """The egress was just started by `start_recording`. Idempotent per egress."""
    await session.execute(
        text(
            "INSERT INTO meeting_recordings (org_id, meeting_id, egress_id, status, media_type) "
            "VALUES (:o, :m, :e, 'recording', :mt) ON CONFLICT (org_id, egress_id) DO NOTHING"
        ),
        {"o": str(org_id), "m": str(meeting_id), "e": egress_id, "mt": media_type},
    )


async def apply_egress_info(
    session: AsyncSession,
    *,
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    egress_info: object,
    ended: bool,
    object_key: str | None = None,
) -> str | None:
    """Upsert the segment for one egress webhook. Returns the status it now has (or None when
    the event carried no egress id). A status never moves backwards."""
    egress_id = getattr(egress_info, "egress_id", None)
    if not egress_id:
        return None
    raw_status = getattr(egress_info, "status", None)
    try:
        code = int(raw_status) if raw_status is not None else None
    except (TypeError, ValueError):
        code = None
    file = _first_file(egress_info)
    if ended:
        ok = code in (EGRESS_COMPLETE, EGRESS_LIMIT_REACHED) or (code is None and object_key)
        status = "ready" if ok and object_key else "failed"
    elif code == EGRESS_ENDING:
        status = "processing"
    elif code in (EGRESS_FAILED, EGRESS_ABORTED):
        status = "failed"
    else:
        status = "recording"
    rank = {"recording": 0, "processing": 1}.get(status, 2)
    started = _ns_to_dt(getattr(file, "started_at", None)) or _ns_to_dt(
        getattr(egress_info, "started_at", None)
    )
    ended_at = _ns_to_dt(getattr(file, "ended_at", None)) or _ns_to_dt(
        getattr(egress_info, "ended_at", None)
    )
    duration = _ns_to_ms(getattr(file, "duration", None))
    size = getattr(file, "size", None)
    error = str(getattr(egress_info, "error", "") or "")[:500] or None
    await session.execute(
        text(
            "INSERT INTO meeting_recordings (org_id, meeting_id, egress_id, status, object_key, "
            "media_type, started_at, ended_at, duration_ms, size_bytes, error) "
            "VALUES (:o, :m, :e, :s, :k, :mt, COALESCE(:st, now()), :en, :d, :sz, :err) "
            "ON CONFLICT (org_id, egress_id) DO UPDATE SET "
            "status = EXCLUDED.status, "
            "object_key = COALESCE(EXCLUDED.object_key, meeting_recordings.object_key), "
            "media_type = EXCLUDED.media_type, "
            "started_at = COALESCE(:st, meeting_recordings.started_at), "
            "ended_at = COALESCE(EXCLUDED.ended_at, meeting_recordings.ended_at), "
            "duration_ms = COALESCE(EXCLUDED.duration_ms, meeting_recordings.duration_ms), "
            "size_bytes = COALESCE(EXCLUDED.size_bytes, meeting_recordings.size_bytes), "
            "error = COALESCE(EXCLUDED.error, meeting_recordings.error), "
            f"updated_at = now() WHERE {_RANK} <= :rank"
        ),
        {
            "o": str(org_id),
            "m": str(meeting_id),
            "e": str(egress_id),
            "s": status,
            "k": object_key if status == "ready" else None,
            "mt": media_type_for(object_key),
            "st": started,
            "en": ended_at,
            "d": duration,
            "sz": int(size) if size else None,
            "err": error if status == "failed" else None,
            "rank": rank,
        },
    )
    return status


async def mark_processing(session: AsyncSession, org_id: uuid.UUID, meeting_id: uuid.UUID) -> int:
    """Every still-recording segment of the meeting is now finishing its upload."""
    result = await session.execute(
        text(
            "UPDATE meeting_recordings SET status = 'processing', updated_at = now() "
            "WHERE org_id = :o AND meeting_id = :m AND status = 'recording'"
        ),
        {"o": str(org_id), "m": str(meeting_id)},
    )
    return int(result.rowcount or 0)  # type: ignore[attr-defined]


async def set_skip_reason(
    session: AsyncSession, org_id: uuid.UUID, meeting_id: uuid.UUID, reason: str | None
) -> None:
    await session.execute(
        text(
            "UPDATE meeting_rooms SET recording_skip_reason = :r, updated_at = now() "
            "WHERE org_id = :o AND meeting_id = :m"
        ),
        {"r": reason, "o": str(org_id), "m": str(meeting_id)},
    )


async def off_record_active(org_id: uuid.UUID, meeting_id: uuid.UUID) -> bool:
    """Whether core-api's off-record flag is set for this meeting. A Redis outage answers
    False: recording would start, and core-api's next pause stops it."""
    from libs.shared.cache import redis_key

    try:
        return bool(await get_redis().exists(redis_key(org_id, "off-record", meeting_id)))
    except Exception:  # noqa: BLE001
        logger.warning("off-record flag unreadable for meeting %s", meeting_id)
        return False


async def _segments(session: AsyncSession, meeting_id: uuid.UUID) -> list[dict[str, Any]]:
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_SEGMENT_COLS} FROM meeting_recordings "  # noqa: S608
                    "WHERE meeting_id = :m ORDER BY started_at, id"
                ),
                {"m": str(meeting_id)},
            )
        )
        .mappings()
        .all()
    )
    return [dict(r) for r in rows]


async def _room(session: AsyncSession, meeting_id: uuid.UUID) -> dict[str, Any] | None:
    row = (
        (
            await session.execute(
                text(
                    "SELECT livekit_room, status, started_at, recording_skip_reason "
                    "FROM meeting_rooms WHERE meeting_id = :m"
                ),
                {"m": str(meeting_id)},
            )
        )
        .mappings()
        .first()
    )
    return dict(row) if row else None


def _offset_ms(origin: datetime | None, started: datetime) -> int | None:
    if origin is None:
        return None
    return max(0, int((started - origin).total_seconds() * 1000))


def build_state(
    meeting_id: uuid.UUID,
    *,
    config: RecordingConfig,
    objects: RecordingObjects | None,
    rows: list[dict[str, Any]],
    room: dict[str, Any] | None,
    ended: bool,
    ttl_seconds: int,
) -> RecordingState:
    origin = room["started_at"] if room else None
    if not rows:
        if not config.enabled:
            return RecordingState(meeting_id=meeting_id, status="not_enabled")
        if objects is None:
            return RecordingState(
                meeting_id=meeting_id, status="not_enabled", reason="storage_not_configured"
            )
        if room and room["recording_skip_reason"] == SKIP_CONSENT_MISSING:
            return RecordingState(meeting_id=meeting_id, status="none", reason="consent_missing")
        if not ended and (room is None or origin is None):
            return RecordingState(meeting_id=meeting_id, status="none", reason="not_started")
        return RecordingState(meeting_id=meeting_id, status="none", reason="not_recorded")

    expires = datetime.now(UTC) + timedelta(seconds=ttl_seconds)
    segments: list[RecordingSegment] = []
    any_url = False
    for r in rows:
        url = None
        if r["status"] == "ready" and objects is not None and r["object_key"]:
            url = objects.presign_get(
                r["object_key"], ttl_seconds=ttl_seconds, content_type=r["media_type"]
            )
            any_url = True
        segments.append(
            RecordingSegment(
                id=r["id"],
                status=r["status"],
                media_type=r["media_type"],
                offset_ms=_offset_ms(origin, r["started_at"]),
                duration_ms=r["duration_ms"],
                started_at=r["started_at"],
                ended_at=r["ended_at"],
                url=url,
            )
        )
    statuses = {s.status for s in segments}
    if objects is None:
        overall, reason = "not_enabled", "storage_not_configured"
    elif "recording" in statuses:
        overall, reason = "recording", None
    elif "processing" in statuses:
        overall, reason = "processing", None
    elif "ready" in statuses:
        overall, reason = "ready", None
    else:
        overall, reason = "failed", None
    return RecordingState(
        meeting_id=meeting_id,
        status=overall,  # type: ignore[arg-type]
        reason=reason,  # type: ignore[arg-type]
        segments=segments,
        duration_ms=sum(s.duration_ms or 0 for s in segments if s.status == "ready"),
        url_expires_at=expires if any_url else None,
    )


# --- the participant route ------------------------------------------------------------------


@router.get(
    "/api/meetings/{meeting_id}/recording",
    response_model=RecordingState,
    summary="A meeting's recording, for playback",
    responses={
        403: {"description": "Not a participant of this meeting."},
        404: {"description": "No such meeting in this workspace."},
    },
)
async def get_recording(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(tenant_context)],
    objects: Annotated[RecordingObjects | None, Depends(get_recording_objects)],
) -> RecordingState:
    """Participants only, deny-by-default: the recording's status and, when ready, a
    short-lived presigned URL per segment (never a public URL). Each answer with URLs is
    audited (`recording.playback_issued`). Off-the-record stretches are not in the recording:
    they are the gaps between segments."""
    config = RecordingConfig.from_env()
    ttl = url_ttl_seconds()
    async with get_sessionmaker()() as session, session.begin():
        await session.execute(_BIND_STATEMENT_TIMEOUT)
        await set_rls(session, ctx.org_id)
        if not await meeting_is_visible(session, meeting_id):
            raise not_found()
        if ctx.user_id is None:
            raise _not_a_participant()
        if await _fetch_participant(session, meeting_id, ctx.user_id) is None:
            raise _not_a_participant()
        state = build_state(
            meeting_id,
            config=config,
            objects=objects,
            rows=await _segments(session, meeting_id),
            room=await _room(session, meeting_id),
            ended=await meeting_has_ended(session, meeting_id),
            ttl_seconds=ttl,
        )
        issued = [s for s in state.segments if s.url]
        if issued:
            await audit(
                session,
                org_id=ctx.org_id,
                actor_id=ctx.user_id,
                action=ACTION_RECORDING_PLAYBACK_ISSUED,
                target_type="meeting",
                target_id=meeting_id,
                metadata={"segments": len(issued), "ttl_seconds": ttl},
                ip=ctx.ip,
            )
    return state


# --- internal: off the record, erasure, clip video ------------------------------------------


async def _internal_meeting(session: AsyncSession, meeting_id: uuid.UUID) -> None:
    if not await meeting_is_visible(session, meeting_id):
        raise not_found()


@internal_router.post(
    "/internal/meetings/{meeting_id}/recording/pause", response_model=RecordingControlResult
)
async def pause_recording(
    meeting_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(core_api_caller)],
    transport: Annotated[MeetingTransport, Depends(get_transport)],
) -> RecordingControlResult:
    """Off the record began: stop the running egress so nothing from the stretch is kept.
    Idempotent (nothing running is `changed: false`)."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session, session.begin():
        await set_rls(session, org_id)
        await _internal_meeting(session, meeting_id)
        running = (
            (
                await session.execute(
                    text(
                        "SELECT egress_id FROM meeting_recordings "
                        "WHERE meeting_id = :m AND status = 'recording'"
                    ),
                    {"m": str(meeting_id)},
                )
            )
            .scalars()
            .all()
        )
    stopped = 0
    for egress_id in running:
        try:
            await transport.stop_egress(egress_id=str(egress_id))
            stopped += 1
        except Exception as exc:  # noqa: BLE001 - an egress LiveKit already ended is fine
            logger.info("recording: pause stop_egress did not apply (%s)", type(exc).__name__)
    if running:
        async with sessionmaker() as session, session.begin():
            await set_rls(session, org_id)
            await mark_processing(session, org_id, meeting_id)
    return RecordingControlResult(
        meeting_id=meeting_id,
        changed=bool(running),
        detail=f"stopped {stopped} of {len(running)} running segment(s)",
    )


@internal_router.post(
    "/internal/meetings/{meeting_id}/recording/resume", response_model=RecordingControlResult
)
async def resume_recording(
    meeting_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(core_api_caller)],
    transport: Annotated[MeetingTransport, Depends(get_transport)],
) -> RecordingControlResult:
    """Back on the record: start a new segment when the room is live, recording is on, the
    consent gate allows it and nothing is already recording. Idempotent."""
    from services.meeting_service.room_lifecycle import (
        RoomStarted,
        load_consent_gate,
        start_recording,
    )

    config = RecordingConfig.from_env()
    if not config.enabled or not config.s3_configured:
        return RecordingControlResult(
            meeting_id=meeting_id, changed=False, detail="recording is not enabled"
        )
    sessionmaker: async_sessionmaker[AsyncSession] = get_sessionmaker()
    async with sessionmaker() as session, session.begin():
        await set_rls(session, org_id)
        await _internal_meeting(session, meeting_id)
        room = await _room(session, meeting_id)
        if (
            room is None
            or room["status"] != "active"
            or await meeting_has_ended(session, meeting_id)
        ):
            return RecordingControlResult(
                meeting_id=meeting_id, changed=False, detail="the call is not live"
            )
        if any(r["status"] == "recording" for r in await _segments(session, meeting_id)):
            return RecordingControlResult(
                meeting_id=meeting_id, changed=False, detail="already recording"
            )
        gate = await load_consent_gate(session, org_id, meeting_id)
    egress_id = await start_recording(
        sessionmaker,
        transport,
        RoomStarted(org_id=org_id, meeting_id=meeting_id, room=room["livekit_room"], gate=gate),
        recording=config,
    )
    return RecordingControlResult(
        meeting_id=meeting_id,
        changed=egress_id is not None,
        detail="recording resumed" if egress_id else "recording did not start",
    )


@internal_router.post(
    "/internal/meetings/{meeting_id}/recording/erase", response_model=RecordingErased
)
async def erase_recording(
    meeting_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(core_api_caller)],
    objects: Annotated[RecordingObjects | None, Depends(get_recording_objects)],
) -> RecordingErased:
    """A meeting's data erasure (core-api `POST /api/v1/erasures`): delete every object under
    the meeting's `{org_id}/{meeting_id}/` prefix (segments and rendered clips) and its
    recording rows. Works on a meeting core-api has already soft-deleted. Idempotent."""
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        exists = (
            await session.execute(
                text("SELECT 1 FROM meetings WHERE id = :m"), {"m": str(meeting_id)}
            )
        ).first()
        if exists is None:
            raise not_found()
    deleted = 0
    if objects is not None:
        deleted = await objects.delete_prefix(
            recording_prefix(org_id=org_id, meeting_id=meeting_id)
        )
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        result = await session.execute(
            text("DELETE FROM meeting_recordings WHERE meeting_id = :m"), {"m": str(meeting_id)}
        )
        segments = int(result.rowcount or 0)  # type: ignore[attr-defined]
        await session.execute(
            text(
                "UPDATE meeting_rooms SET recording_ref = NULL, egress_id = NULL, "
                "recording_skip_reason = NULL, updated_at = now() WHERE meeting_id = :m"
            ),
            {"m": str(meeting_id)},
        )
        await audit(
            session,
            org_id=org_id,
            actor_type="system",
            action=ACTION_RECORDING_ERASED,
            target_type="meeting",
            target_id=meeting_id,
            metadata={
                "objects_deleted": deleted,
                "segments_deleted": segments,
                "storage": "connected" if objects is not None else "not_connected",
            },
        )
    return RecordingErased(
        meeting_id=meeting_id,
        status="erased" if objects is not None else "not_connected",
        objects_deleted=deleted,
        segments_deleted=segments,
    )


async def _render_in_background(
    objects: RecordingObjects,
    *,
    org_id: uuid.UUID,
    clip_id: uuid.UUID,
    source_key: str,
    media_type: str,
    start_s: float,
    duration_s: float,
    key: str,
) -> None:
    source = objects.presign_get(
        source_key, ttl_seconds=clips.SOURCE_URL_TTL_SECONDS, content_type=media_type
    )
    ok = await clips.render_clip(
        objects,
        source_url=source,
        start_s=start_s,
        duration_s=duration_s,
        key=key,
        media_type=media_type,
    )
    try:
        lock = clips.render_lock_key(org_id, clip_id)
        if ok:
            await get_redis().delete(lock)
        else:
            await get_redis().set(lock, b"failed", ex=300)
    except Exception:  # noqa: BLE001
        logger.warning("clip render lock update failed for clip %s", clip_id)


@internal_router.post(
    "/internal/meetings/{meeting_id}/clips/{clip_id}/video", response_model=ClipVideo
)
async def clip_video(
    meeting_id: uuid.UUID,
    clip_id: Annotated[uuid.UUID, Path()],
    body: ClipVideoRequest,
    org_id: Annotated[uuid.UUID, Depends(core_api_caller)],
    objects: Annotated[RecordingObjects | None, Depends(get_recording_objects)],
    background_tasks: BackgroundTasks,
) -> ClipVideo:
    """The video of one shared clip (core-api checked the share and the clip). Only the clip's
    own object is ever presigned; it is cut on first request (`rendering`) and served after."""
    if objects is None:
        return ClipVideo(status="unavailable", reason="not_connected")
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        await _internal_meeting(session, meeting_id)
        rows = await _segments(session, meeting_id)
        room = await _room(session, meeting_id)
        origin = room["started_at"] if room else None
        ready = [r for r in rows if r["status"] == "ready" and r["object_key"]]
        if not ready:
            return ClipVideo(status="unavailable", reason="no_recording")
        segment = None
        for r in ready:
            offset = _offset_ms(origin, r["started_at"])
            if offset is None or r["duration_ms"] is None:
                continue
            if offset <= body.start_ms and body.end_ms <= offset + r["duration_ms"]:
                segment = (r, offset)
                break
        if segment is None:
            return ClipVideo(status="unavailable", reason="not_in_recording")
        row, offset = segment
        media_type = str(row["media_type"])
        key = clips.clip_key(
            org_id=org_id, meeting_id=meeting_id, clip_id=clip_id, media_type=media_type
        )
        if await objects.exists(key):
            ttl = url_ttl_seconds()
            await audit(
                session,
                org_id=org_id,
                actor_type="system",
                action=ACTION_RECORDING_CLIP_VIDEO_ISSUED,
                target_type="clip",
                target_id=clip_id,
                metadata={"meeting_id": str(meeting_id), "ttl_seconds": ttl},
            )
            return ClipVideo(
                status="ready",
                url=objects.presign_get(key, ttl_seconds=ttl, content_type=media_type),
                expires_at=datetime.now(UTC) + timedelta(seconds=ttl),
                media_type=media_type,  # type: ignore[arg-type]
            )
    if clips.ffmpeg_path() is None:
        return ClipVideo(status="unavailable", reason="renderer_not_installed")
    if not await clips.claim_render(org_id, clip_id):
        try:
            state = await get_redis().get(clips.render_lock_key(org_id, clip_id))
        except Exception:  # noqa: BLE001
            state = None
        if state == b"failed":
            return ClipVideo(status="unavailable", reason="render_failed")
        return ClipVideo(status="rendering", media_type=media_type)  # type: ignore[arg-type]
    background_tasks.add_task(
        _render_in_background,
        objects,
        org_id=org_id,
        clip_id=clip_id,
        source_key=str(row["object_key"]),
        media_type=media_type,
        start_s=(body.start_ms - offset) / 1000,
        duration_s=(body.end_ms - body.start_ms) / 1000,
        key=key,
    )
    return ClipVideo(status="rendering", media_type=media_type)  # type: ignore[arg-type]


__all__ = [
    "apply_egress_info",
    "build_state",
    "internal_router",
    "mark_processing",
    "media_type_for",
    "off_record_active",
    "record_segment_started",
    "router",
    "set_skip_reason",
]
