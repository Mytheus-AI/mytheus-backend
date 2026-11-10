"""LiveKit webhook: `POST /livekit/webhook`. Plan 3-4 section 3.1 step 5.

This endpoint is reachable by anyone who finds the URL - there is no Clerk JWT and no
`api-gateway` in front of it, only `MeetingTransport.verify_webhook`'s HMAC check. That makes
it a remote write primitive if verification, or tenant resolution, is ever wrong. The order of
operations below is the security property and mirrors the other unauthenticated-by-URL webhook
in this codebase, `identity_tenant_service.routes.auth.clerk_webhook`, for the same reason:

1. **Verify the signature FIRST**, through `MeetingTransport.verify_webhook`, on the raw body,
   before parsing anything else out of it. An unverified request is spoofing; a bad signature
   is rejected (401) and nothing is written.
2. **Resolve the tenant from the verified event's ROOM METADATA.** Team lead's ambiguity
   resolution #1: room metadata is attacker-adjacent data (it round-trips whatever was set at
   room-creation time; nothing here re-derives it from a source this service already trusts),
   so every failure mode fails CLOSED - metadata absent, not valid JSON, missing/non-UUID
   `org_id`, or a well-formed UUID naming an org that does not exist or is not active. All of
   them reject with 400 and write nothing. Never default to a tenant, never create one.
3. **Dedupe on `nexus:{org_id}:dedupe:livekit:{room}:{event}:{participant_sid}`** - Redis first
   for speed, then the durable `inbox` table, which is the authority once the cache evicts or
   two deliveries race the same SETNX. A redelivered identical event is a complete no-op: no
   second row, no second outbox entry.
4. **Apply the state change and enqueue the `meeting-room-events` outbox row in ONE transaction
   with the inbox record.** A crash between them cannot double-apply on LiveKit's at-least-once
   redelivery, and nothing is ever published to Redpanda directly from this request.

ROOM METADATA (closed follow-up). `_resolve_org_id` reads `evt.room.metadata`, which is now
actually populated: both call sites that establish a `meeting_rooms` row (the token-mint
route, the `meeting-events` consumer's `meeting.created` handler) schedule
`MeetingTransport.ensure_room(org_id=..., room=...)` out of band via
`services.meeting_service.rooms.sync_livekit_metadata`, writing `{"org_id": "<internal
uuid>"}` as the room's OWN metadata - a genuinely different thing from `mint_join_token`'s
participant-grant metadata, which is what this path originally, and mistakenly, had nothing
setting. Still fails CLOSED on every negative case below; that never weakens.

FIX ROUND 1: existence/status validation now goes through `libs.shared.org_resolver.
resolve_org_by_id`, the sanctioned second reader that module's docstring names, instead of a
second hand-written `SELECT ... FROM organizations` living here. Same predicate, same table,
one fewer independently-maintained query that could drift from it.

ROOM LIFECYCLE (audio tap + recording). `room_started` / `room_finished` are now handled:
same verify -> resolve -> dedupe -> one-transaction order as every other event, but the
transaction only READS (the room's meeting, its consent gate, any stored egress id) and
records the inbox row - no `meeting-room-events` row is emitted (the topic's enum has no
symbol for it, and nothing downstream needs one). The actual work - joining the room as a
hidden audio tap, starting/stopping egress - runs in a FastAPI background task AFTER the
transaction commits (`room_lifecycle.on_room_started` / `on_room_finished`, which never
raise). The dedupe key uses the room SID, not "-": a LiveKit room name is reused when people
rejoin after the room closed, and each new room instance must start a new tap.

Internal participants (the tap itself, LiveKit egress recorders) produce participant
webhooks too. Those are a 200 no-op here, so the tap is never counted as an attendee.

AUDIO_TAP_MODE. In `in-process` mode (development default) the background task above joins
the room in this process, exactly as before. In `worker` mode (default elsewhere) it instead
records the trigger in Redis (`audio_tap_worker.record_tap_wanted` / `clear_tap_wanted`, keyed
`nexus:{org_id}:tap:want:{meeting_id}`) and the dedicated audio tap worker claims and runs the
tap; recording egress still starts/stops here in both modes. See `audio_tap_worker.py`.

`egress_ended` stores the REAL uploaded object key from the egress file result when it sits
under this meeting's `{org_id}/{meeting_id}/` prefix (see `storage.py` for why the egress id
cannot be in the filename), falling back to `recording_key` otherwise.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, Request, Response, status
from redis.asyncio import from_url
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.events import EventEnvelope
from libs.shared.inbox import is_duplicate, record_consumed
from libs.shared.org_resolver import resolve_org_by_id
from libs.shared.outbox import enqueue
from libs.shared.tenancy import set_rls
from services.meeting_service.audio_tap import AudioTapConfig, is_internal_identity
from services.meeting_service.audio_tap_worker import (
    MODE_IN_PROCESS,
    TapWant,
    audio_tap_mode,
    clear_tap_wanted,
    record_tap_wanted,
)
from services.meeting_service.meeting_status import report_room_state
from services.meeting_service.room_lifecycle import (
    TAPS,
    RoomStarted,
    load_consent_gate,
    on_room_finished,
    on_room_started,
    start_audio_tap,
)
from services.meeting_service.routes import get_transport
from services.meeting_service.storage import DEFAULT_STORAGE, RecordingConfig, recording_prefix
from services.meeting_service.transport import MeetingTransport

logger = logging.getLogger(__name__)

router = APIRouter(tags=["meetings"])

CONSUMER_GROUP = "meeting-service.livekit-webhook"
DEDUPE_TOPIC = "livekit"

# LiveKit sends many more event types (track_published, ingress_*, ...) than this service
# has anything to do with yet. Anything not in this set is a recognized 200 no-op: acknowledged
# so LiveKit stops retrying it, but nothing is read from the body and nothing is written.
_ROOM_EVENT_TYPE = {"participant_joined": "joined", "participant_left": "left"}
_ROOM_LIFECYCLE_EVENTS = {"room_started", "room_finished"}
_HANDLED_EVENTS = {*_ROOM_EVENT_TYPE, "egress_ended", *_ROOM_LIFECYCLE_EVENTS}


def _invalid_signature() -> ProblemDetail:
    return ProblemDetail(
        status.HTTP_401_UNAUTHORIZED,
        ErrorCode.AUTH_REQUIRED,
        "invalid LiveKit webhook signature",
    )


def _org_unresolvable(reason: str) -> ProblemDetail:
    return ProblemDetail(
        status.HTTP_400_BAD_REQUEST,
        ErrorCode.MALFORMED_BODY,
        f"webhook room metadata carries no resolvable org_id: {reason}",
    )


def _room_name(evt: object) -> str | None:
    room = getattr(evt, "room", None)
    name = getattr(room, "name", None) if room is not None else None
    return str(name) if name else None


def _participant_sid(evt: object) -> str | None:
    participant = getattr(evt, "participant", None)
    sid = getattr(participant, "sid", None) if participant is not None else None
    return str(sid) if sid else None


def _participant_identity(evt: object) -> str | None:
    participant = getattr(evt, "participant", None)
    identity = getattr(participant, "identity", None) if participant is not None else None
    return str(identity) if identity else None


def _room_sid(evt: object) -> str | None:
    room = getattr(evt, "room", None)
    sid = getattr(room, "sid", None) if room is not None else None
    return str(sid) if sid else None


def _uploaded_filename(egress_info: object) -> str | None:
    results = getattr(egress_info, "file_results", None)
    for result in list(results or []):
        name = getattr(result, "filename", None)
        if name:
            return str(name)
    legacy = getattr(egress_info, "file", None)
    name = getattr(legacy, "filename", None) if legacy is not None else None
    return str(name) if name else None


async def _resolve_org_id(session: AsyncSession, evt: object) -> uuid.UUID:
    """Room-metadata tenant resolution. Fails CLOSED on every negative case - see module
    docstring step 2. Existence/active validation goes through `libs.shared.org_resolver.
    resolve_org_by_id`, the sanctioned second reader of `organizations` (that table carries no
    RLS policy; see that module's docstring for why), so this runs before `app.current_org` is
    ever set - no tenant is established until this returns.
    """
    room = getattr(evt, "room", None)
    metadata = getattr(room, "metadata", None) if room is not None else None
    if not metadata:
        raise _org_unresolvable("room metadata is absent")

    try:
        parsed = json.loads(metadata)
    except (TypeError, ValueError) as exc:
        raise _org_unresolvable("room metadata is not valid JSON") from exc

    raw_org_id = parsed.get("org_id") if isinstance(parsed, dict) else None
    if not raw_org_id:
        raise _org_unresolvable("room metadata carries no org_id field")

    try:
        candidate_org_id = uuid.UUID(str(raw_org_id))
    except ValueError as exc:
        raise _org_unresolvable("room metadata org_id is not a UUID") from exc

    org_id = await resolve_org_by_id(session, candidate_org_id)
    if org_id is None:
        raise _org_unresolvable("room metadata names an unknown or inactive org")

    return org_id


async def _apply_egress_ended(
    session: AsyncSession, org_id: uuid.UUID, room: str, evt: object, dedupe_key: str
) -> None:
    egress_info = getattr(evt, "egress_info", None)
    egress_id = getattr(egress_info, "egress_id", None) if egress_info is not None else None
    if not egress_id:
        logger.warning(
            "livekit webhook: egress_ended with no egress_id - dropping (org=%s room=%s)",
            org_id,
            room,
        )
        return

    found = (
        (
            await session.execute(
                text(
                    "SELECT meeting_id FROM meeting_rooms "
                    "WHERE org_id = :org AND livekit_room = :room FOR UPDATE"
                ),
                {"org": str(org_id), "room": room},
            )
        )
        .mappings()
        .first()
    )
    if found is None:
        logger.warning(
            "livekit webhook: egress_ended for unknown room (org=%s room=%s)", org_id, room
        )
        return
    meeting_id = found["meeting_id"]

    ref = DEFAULT_STORAGE.recording_key(
        org_id=org_id, meeting_id=meeting_id, egress_id=str(egress_id)
    )
    uploaded = _uploaded_filename(egress_info)
    if uploaded:
        if uploaded.startswith(recording_prefix(org_id=org_id, meeting_id=meeting_id)):
            ref = uploaded
        else:
            # Never point a tenant's recording_ref outside its own prefix.
            logger.warning(
                "livekit webhook: egress file outside the meeting prefix - using the key "
                "convention instead (org=%s room=%s)",
                org_id,
                room,
            )

    await session.execute(
        text(
            "UPDATE meeting_rooms SET recording_ref = :ref, egress_id = :egress_id, "
            "updated_at = now() WHERE org_id = :org AND meeting_id = :meeting"
        ),
        {"ref": ref, "egress_id": str(egress_id), "org": str(org_id), "meeting": str(meeting_id)},
    )

    await enqueue(
        session,
        EventEnvelope(
            topic="meeting-room-events",
            payload={
                "meeting_id": str(meeting_id),
                "room": room,
                "event_type": "recording_ready",
                "participant_id": None,
            },
            org_id=str(org_id),
            producer="meeting-service",
            idempotency_key=dedupe_key,
        ),
    )


@dataclass(frozen=True)
class _RoomFinished:
    org_id: uuid.UUID
    meeting_id: uuid.UUID
    room: str
    egress_id: str | None


async def _load_room_lifecycle(
    session: AsyncSession, org_id: uuid.UUID, room: str, event_type: str
) -> RoomStarted | _RoomFinished | None:
    found = (
        (
            await session.execute(
                text(
                    "SELECT meeting_id, egress_id FROM meeting_rooms "
                    "WHERE org_id = :org AND livekit_room = :room"
                ),
                {"org": str(org_id), "room": room},
            )
        )
        .mappings()
        .first()
    )
    if found is None:
        logger.warning(
            "livekit webhook: %s for unknown room (org=%s room=%s)", event_type, org_id, room
        )
        return None
    meeting_id = uuid.UUID(str(found["meeting_id"]))
    egress_id = str(found["egress_id"]) if found["egress_id"] else None
    if event_type == "room_finished":
        return _RoomFinished(org_id=org_id, meeting_id=meeting_id, room=room, egress_id=egress_id)
    gate = await load_consent_gate(session, org_id, meeting_id)
    return RoomStarted(
        org_id=org_id,
        meeting_id=meeting_id,
        room=room,
        gate=gate,
        existing_egress_id=egress_id,
    )


async def _apply_participant_event(
    session: AsyncSession,
    org_id: uuid.UUID,
    room: str,
    room_event_type: str,
    participant_sid: str,
    dedupe_key: str,
    participant_identity: str | None = None,
) -> uuid.UUID | None:
    found = (
        (
            await session.execute(
                text(
                    "SELECT meeting_id FROM meeting_rooms "
                    "WHERE org_id = :org AND livekit_room = :room FOR UPDATE"
                ),
                {"org": str(org_id), "room": room},
            )
        )
        .mappings()
        .first()
    )
    if found is None:
        logger.warning(
            "livekit webhook: %s for unknown room (org=%s room=%s)", room_event_type, org_id, room
        )
        return None
    meeting_id = found["meeting_id"]

    if room_event_type == "joined":
        await session.execute(
            text(
                "UPDATE meeting_rooms SET status = 'active', "
                "started_at = COALESCE(started_at, now()), updated_at = now() "
                "WHERE org_id = :org AND meeting_id = :meeting AND status = 'pending'"
            ),
            {"org": str(org_id), "meeting": str(meeting_id)},
        )

    await enqueue(
        session,
        EventEnvelope(
            topic="meeting-room-events",
            payload={
                "meeting_id": str(meeting_id),
                "room": room,
                "event_type": room_event_type,
                # The LiveKit identity, which for a human is `users.id` (mint_join_token sets
                # it). `agent-runtime-service` spawns one PersonalListenerAgent per user from
                # this, so it must be the user, not the per-connection participant SID (which
                # changes on every reconnect and still keys the dedupe above).
                "participant_id": participant_identity,
            },
            org_id=str(org_id),
            producer="meeting-service",
            idempotency_key=dedupe_key,
        ),
    )
    return uuid.UUID(str(meeting_id))


async def _tap_trigger(
    *,
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    room: str,
    room_sid: str,
    started: bool,
    generation: str = "",
) -> None:
    """Worker mode: record (room_started) or clear (room_finished) the tap want. Runs after the
    response like the in-process tap does, and likewise never raises into the webhook."""
    redis = from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"), decode_responses=False)
    try:
        if started:
            await record_tap_wanted(
                redis,
                TapWant(
                    org_id=org_id,
                    meeting_id=meeting_id,
                    room=room,
                    room_sid=room_sid,
                    generation=generation,
                ),
            )
        else:
            await clear_tap_wanted(redis, org_id=org_id, meeting_id=meeting_id, room_sid=room_sid)
    except Exception:  # noqa: BLE001
        logger.exception(
            "audio tap: could not %s the worker trigger (org=%s meeting=%s)",
            "record" if started else "clear",
            org_id,
            meeting_id,
        )
    finally:
        await redis.aclose()  # type: ignore[attr-defined]


async def _ensure_tap_in_process(
    sessionmaker: async_sessionmaker[AsyncSession],
    transport: MeetingTransport,
    *,
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    room: str,
) -> None:
    """In-process mode's rejoin leg: start the tap unless one is running (`TapManager.start`
    is idempotent and replaces a tap that ended). Never raises into the webhook."""
    try:
        async with sessionmaker() as session, session.begin():
            await set_rls(session, org_id)
            gate = await load_consent_gate(session, org_id, meeting_id)
        await start_audio_tap(
            transport,
            TAPS,
            RoomStarted(org_id=org_id, meeting_id=meeting_id, room=room, gate=gate),
            tap_config=AudioTapConfig.from_env(),
        )
    except Exception:  # noqa: BLE001
        logger.exception("audio tap: rejoin start failed (org=%s meeting=%s)", org_id, meeting_id)


@router.post("/livekit/webhook", status_code=status.HTTP_200_OK)
async def livekit_webhook(
    request: Request,
    transport: Annotated[MeetingTransport, Depends(get_transport)],
    background_tasks: BackgroundTasks,
) -> Response:
    body = await request.body()
    auth_header = request.headers.get("authorization", "")
    try:
        evt = transport.verify_webhook(body=body, auth_header=auth_header)
    except Exception as exc:  # noqa: BLE001 - any verification failure means "reject", not "crash"
        raise _invalid_signature() from exc

    sessionmaker = get_sessionmaker()

    # Step 2: resolve the tenant. Own short transaction - a read-only check with nothing to
    # commit, kept separate from the apply transaction below (same shape as clerk_webhook).
    async with sessionmaker() as session, session.begin():
        org_id = await _resolve_org_id(session, evt)

    event_type = str(getattr(evt, "event", "") or "")
    if event_type not in _HANDLED_EVENTS:
        logger.info("livekit webhook: unhandled event type %r - no-op", event_type)
        return Response(status_code=status.HTTP_200_OK)

    room = _room_name(evt)
    if not room:
        raise ProblemDetail(
            status.HTTP_400_BAD_REQUEST,
            ErrorCode.MALFORMED_BODY,
            "webhook event carries no room name",
        )

    if event_type in _ROOM_EVENT_TYPE and is_internal_identity(_participant_identity(evt)):
        # The audio tap / an egress recorder joining or leaving: not an attendee.
        return Response(status_code=status.HTTP_200_OK)

    participant_sid = _participant_sid(evt) or "-"
    if event_type in _ROOM_LIFECYCLE_EVENTS:
        dedupe_key = f"{room}:{event_type}:{_room_sid(evt) or '-'}"
    else:
        dedupe_key = f"{room}:{event_type}:{participant_sid}"
    lifecycle: RoomStarted | _RoomFinished | None = None
    joined_meeting: uuid.UUID | None = None

    # Step 3a: Redis dedupe, fast path.
    redis = from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"), decode_responses=False)
    try:
        if await is_duplicate(redis, str(org_id), DEDUPE_TOPIC, dedupe_key):
            return Response(status_code=status.HTTP_200_OK)
    finally:
        await redis.aclose()  # type: ignore[attr-defined]

    # Step 3b + 4: durable dedupe re-check, apply, and record - one transaction.
    async with sessionmaker() as session, session.begin():
        await set_rls(session, org_id)

        already = (
            await session.execute(
                text(
                    "SELECT 1 FROM inbox WHERE consumer_group = :cg AND org_id = :o "
                    "AND idempotency_key = :k"
                ),
                {"cg": CONSUMER_GROUP, "o": str(org_id), "k": dedupe_key},
            )
        ).first()
        if already:
            return Response(status_code=status.HTTP_200_OK)

        if event_type == "egress_ended":
            await _apply_egress_ended(session, org_id, room, evt, dedupe_key)
        elif event_type in _ROOM_LIFECYCLE_EVENTS:
            lifecycle = await _load_room_lifecycle(session, org_id, room, event_type)
        else:
            applied = await _apply_participant_event(
                session,
                org_id,
                room,
                _ROOM_EVENT_TYPE[event_type],
                participant_sid,
                dedupe_key,
                participant_identity=_participant_identity(evt),
            )
            if _ROOM_EVENT_TYPE[event_type] == "joined":
                joined_meeting = applied

        await record_consumed(session, CONSUMER_GROUP, str(org_id), dedupe_key)

    # Committed. The tap/egress work is network I/O, so it runs after the response. In
    # worker mode the tap leg is only a Redis trigger; the audio tap worker runs the tap.
    in_process = audio_tap_mode() == MODE_IN_PROCESS
    room_sid = _room_sid(evt) or "-"
    if joined_meeting is not None:
        # Someone (back) in the room. LiveKit keeps an emptied room, same sid, for its
        # departure timeout, and the tap leaves when the room empties, so a rejoin inside that
        # window brings no `room_started` and transcription stayed silent for the rest of the
        # call. Re-arm the tap on every human join; a running tap is left alone.
        if in_process:
            background_tasks.add_task(
                _ensure_tap_in_process,
                sessionmaker,
                transport,
                org_id=org_id,
                meeting_id=joined_meeting,
                room=room,
            )
        else:
            background_tasks.add_task(
                _tap_trigger,
                org_id=org_id,
                meeting_id=joined_meeting,
                room=room,
                room_sid=room_sid,
                started=True,
                generation=uuid.uuid4().hex,
            )
    if isinstance(lifecycle, RoomStarted):
        if not in_process:
            background_tasks.add_task(
                _tap_trigger,
                org_id=lifecycle.org_id,
                meeting_id=lifecycle.meeting_id,
                room=lifecycle.room,
                room_sid=room_sid,
                started=True,
            )
        # The call is live: core-api owns `meetings.status` (see meeting_status.py).
        background_tasks.add_task(
            report_room_state,
            org_id=lifecycle.org_id,
            meeting_id=lifecycle.meeting_id,
            state="live",
        )
        background_tasks.add_task(
            on_room_started,
            sessionmaker,
            transport,
            lifecycle,
            tap_config=AudioTapConfig.from_env(),
            recording=RecordingConfig.from_env(),
            run_tap=in_process,
        )
    elif isinstance(lifecycle, _RoomFinished):
        if not in_process:
            background_tasks.add_task(
                _tap_trigger,
                org_id=lifecycle.org_id,
                meeting_id=lifecycle.meeting_id,
                room=lifecycle.room,
                room_sid=room_sid,
                started=False,
            )
        # LiveKit closed the room: nobody has been in it for the departure timeout (five
        # minutes, set in transport.ensure_room), so the meeting is over.
        background_tasks.add_task(
            report_room_state,
            org_id=lifecycle.org_id,
            meeting_id=lifecycle.meeting_id,
            state="ended",
        )
        background_tasks.add_task(
            on_room_finished,
            transport,
            org_id=lifecycle.org_id,
            room=lifecycle.room,
            egress_id=lifecycle.egress_id,
            run_tap=in_process,
        )

    return Response(status_code=status.HTTP_200_OK)
