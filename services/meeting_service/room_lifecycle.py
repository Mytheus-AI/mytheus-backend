"""What meeting-service does when a LiveKit room starts and finishes: the audio tap to
transcription-service and the recording egress. Driven by the `room_started` /
`room_finished` webhooks (`webhook.py`), always out of band, after the webhook's own
transaction has committed - the same "no network call inside an open transaction" rule
`rooms.py` documents for `ensure_room`.

CONSENT (ADR-0018). Reuses the exact columns and semantics the token-mint gate in
`routes.py` uses: `tenant_settings.consent_required` (missing row means false, same
`COALESCE`) and `meeting_participants.consented_at`. When consent is required:

- the tap starts only if at least one participant has consented, and forwards audio ONLY
  for participants whose LiveKit identity (= `users.id`) has `consented_at` set;
- recording (a room composite captures everyone) starts only if NO participant of the
  meeting is still unconsented.

When `consent_required` is false the gate is inert, exactly like the token-mint gate.

RECORDING GATE. No tenant-level recording setting exists in `tenant_settings`, so
`RECORDING_ENABLED` (env, default false) gates it, plus the S3 settings having been
provided. Either missing means no egress, logged, never an error.

STATE. Where a tap runs is `AUDIO_TAP_MODE` (`audio_tap_worker.audio_tap_mode`):

- `in-process` (the development default): taps live in this API process (`TAPS`). Only
  correct with ONE meeting-service replica, because the `room_finished` webhook must reach the
  replica that holds the tap.
- `worker` (the default everywhere else): the webhook only records the trigger in Redis and
  calls the hooks below with `run_tap=False`, so the API stays stateless; the dedicated
  `python -m services.meeting_service.audio_tap_worker` process claims the room
  (`nexus:{org_id}:tap:{meeting_id}`, TTL + heartbeat) and runs the tap through the same
  `start_audio_tap` (same consent gate, same `INTERNAL_AUDIO_TOKEN` check).

Recording egress is a stateless LiveKit API call, so it stays in the webhook in both modes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from libs.shared.tenancy import set_rls
from services.meeting_service.audio_tap import (
    AudioSinkFactory,
    AudioTapConfig,
    AudioTapHandle,
    TapManager,
    make_sink_factory,
)
from services.meeting_service.storage import RecordingConfig
from services.meeting_service.transport import MeetingTransport

logger = logging.getLogger(__name__)

TAPS = TapManager()


@dataclass(frozen=True)
class ConsentGate:
    consent_required: bool
    consented_identities: frozenset[str]
    unconsented_count: int

    def tap_allowed(self) -> bool:
        return not self.consent_required or bool(self.consented_identities)

    def allows_identity(self, identity: str) -> bool:
        return not self.consent_required or identity in self.consented_identities

    def recording_allowed(self) -> bool:
        return not self.consent_required or self.unconsented_count == 0


async def load_consent_gate(session: AsyncSession, org_id: UUID, meeting_id: UUID) -> ConsentGate:
    """Caller has already set `app.current_org` on `session` (RLS scopes both tables)."""
    row = (
        (
            await session.execute(
                text(
                    "SELECT "
                    "COALESCE((SELECT ts.consent_required FROM tenant_settings ts "
                    "          WHERE ts.org_id = :org), false) AS consent_required, "
                    "ARRAY(SELECT mp.user_id::text FROM meeting_participants mp "
                    "      WHERE mp.meeting_id = :meeting AND mp.consented_at IS NOT NULL) "
                    "  AS consented, "
                    "(SELECT count(*) FROM meeting_participants mp "
                    " WHERE mp.meeting_id = :meeting AND mp.consented_at IS NULL) AS unconsented"
                ),
                {"org": str(org_id), "meeting": str(meeting_id)},
            )
        )
        .mappings()
        .one()
    )
    return ConsentGate(
        consent_required=bool(row["consent_required"]),
        consented_identities=frozenset(str(x) for x in (row["consented"] or [])),
        unconsented_count=int(row["unconsented"] or 0),
    )


@dataclass(frozen=True)
class RoomStarted:
    org_id: UUID
    meeting_id: UUID
    room: str
    gate: ConsentGate
    existing_egress_id: str | None = None


async def persist_egress_id(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    org_id: UUID,
    meeting_id: UUID,
    egress_id: str,
) -> None:
    async with session_factory() as session, session.begin():
        await set_rls(session, org_id)
        await session.execute(
            text(
                "UPDATE meeting_rooms SET egress_id = :egress_id, updated_at = now() "
                "WHERE org_id = :org AND meeting_id = :meeting"
            ),
            {"egress_id": egress_id, "org": str(org_id), "meeting": str(meeting_id)},
        )


async def start_audio_tap(
    transport: MeetingTransport,
    taps: TapManager,
    started: RoomStarted,
    *,
    tap_config: AudioTapConfig | None,
    sink_factory: AudioSinkFactory | None = None,
) -> bool:
    if not started.gate.tap_allowed():
        logger.info(
            "audio tap: not started - consent required and none recorded (org=%s meeting=%s)",
            started.org_id,
            started.meeting_id,
        )
        return False
    if tap_config is None and sink_factory is None:
        logger.error(
            "audio tap: not started - INTERNAL_AUDIO_TOKEN is unset (org=%s meeting=%s)",
            started.org_id,
            started.meeting_id,
        )
        return False
    factory = sink_factory
    if factory is None:
        assert tap_config is not None
        factory = make_sink_factory(tap_config)
    gate = started.gate

    async def starter() -> AudioTapHandle:
        return await transport.subscribe_room_audio(
            org_id=started.org_id,
            meeting_id=started.meeting_id,
            room=started.room,
            sink_factory=factory,
            allow_identity=gate.allows_identity,
        )

    return await taps.start(org_id=started.org_id, room=started.room, starter=starter)


async def start_recording(
    session_factory: async_sessionmaker[AsyncSession],
    transport: MeetingTransport,
    started: RoomStarted,
    *,
    recording: RecordingConfig,
) -> str | None:
    if not recording.enabled:
        return None
    if not recording.s3_configured:
        logger.warning(
            "recording: RECORDING_ENABLED but S3 settings are missing; not recording "
            "(org=%s meeting=%s)",
            started.org_id,
            started.meeting_id,
        )
        return None
    if not started.gate.recording_allowed():
        logger.info(
            "recording: not started - consent required and not every participant consented "
            "(org=%s meeting=%s)",
            started.org_id,
            started.meeting_id,
        )
        return None
    egress_id = await transport.start_egress(
        org_id=started.org_id, meeting_id=started.meeting_id, room=started.room
    )
    await persist_egress_id(
        session_factory, org_id=started.org_id, meeting_id=started.meeting_id, egress_id=egress_id
    )
    return egress_id


async def on_room_started(
    session_factory: async_sessionmaker[AsyncSession],
    transport: MeetingTransport,
    started: RoomStarted,
    *,
    taps: TapManager = TAPS,
    tap_config: AudioTapConfig | None = None,
    recording: RecordingConfig | None = None,
    sink_factory: AudioSinkFactory | None = None,
    run_tap: bool = True,
) -> None:
    """Never raises: a failure in one leg (tap or egress) is logged and does not stop the
    other, and nothing propagates back into the webhook response. `run_tap=False` (worker
    mode) skips the tap leg: the audio tap worker owns it."""
    if run_tap:
        try:
            await start_audio_tap(
                transport, taps, started, tap_config=tap_config, sink_factory=sink_factory
            )
        except Exception:  # noqa: BLE001
            logger.exception(
                "audio tap: start failed (org=%s meeting=%s)", started.org_id, started.meeting_id
            )
    try:
        await start_recording(
            session_factory,
            transport,
            started,
            recording=recording if recording is not None else RecordingConfig.from_env(),
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "recording: start_egress failed (%s; org=%s meeting=%s)",
            type(exc).__name__,
            started.org_id,
            started.meeting_id,
        )


async def on_room_finished(
    transport: MeetingTransport,
    *,
    org_id: UUID,
    room: str,
    egress_id: str | None,
    taps: TapManager = TAPS,
    run_tap: bool = True,
) -> None:
    """Stop the tap and the egress. LiveKit also ends a room-composite egress on its own when
    the room closes, so a failed `stop_egress` is logged at info, not treated as an error.
    `run_tap=False` (worker mode) leaves the tap to the audio tap worker."""
    if run_tap:
        try:
            await taps.stop(org_id=org_id, room=room)
        except Exception:  # noqa: BLE001
            logger.exception("audio tap: stop failed (org=%s room=%s)", org_id, room)
    if egress_id:
        try:
            await transport.stop_egress(egress_id=egress_id)
        except Exception as exc:  # noqa: BLE001
            logger.info(
                "recording: stop_egress did not apply (%s; org=%s room=%s)",
                type(exc).__name__,
                org_id,
                room,
            )


__all__ = [
    "TAPS",
    "ConsentGate",
    "RoomStarted",
    "load_consent_gate",
    "on_room_finished",
    "on_room_started",
    "persist_egress_id",
    "start_audio_tap",
    "start_recording",
]
