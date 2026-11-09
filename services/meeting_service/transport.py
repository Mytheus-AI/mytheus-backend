"""The `MeetingTransport` adapter — the ONLY place the LiveKit SDK is imported.

ADR-0003 section "Decision 2", verbatim. `core-api`, `transcription-service`, and the agent
tier never import `livekit`: they call this interface (once Tasks 3/4 wire routes on top of
it) or consume `meeting-room-events`. That boundary is what makes the eventual self-host
swap (Tech Decision 2.7) a one-interface reimplementation instead of a codebase-wide
migration, and it is enforced by `tests/test_livekit_import_boundary.py`, not left as a
review convention.

`FakeTransport` is not in the plan text. There is no LiveKit account for this project as of
this build (Task 2, Deviation 1): `LIVEKIT_URL`/`LIVEKIT_API_KEY`/`LIVEKIT_API_SECRET` are
unavailable and will not become available during this band. `LiveKitTransport` is still
written for real, exactly as the plan specifies — it is simply not exercisable end-to-end
today, and no test in this build claims otherwise. `FakeTransport` is deterministic and
dependency-free so the routes Tasks 3/4 add on top of `MeetingTransport` (and their tests)
have something real to run against in the meantime. `select_transport` defaults to the fake
when `LIVEKIT_API_KEY` is unset, so a misconfigured environment fails toward "obviously not
talking to LiveKit" rather than toward a silent no-op against a real provider.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Protocol
from uuid import UUID

import aiohttp
from livekit import api, rtc  # the ONLY imports of the LiveKit SDK in the codebase

from services.meeting_service.audio_tap import (
    NUM_CHANNELS,
    SAMPLE_RATE,
    AudioSinkFactory,
    AudioTapHandle,
    Backoff,
    pcm16le_bytes,
    pump_frames,
    tap_identity,
)
from services.meeting_service.storage import RecordingConfig, egress_filepath

logger = logging.getLogger(__name__)

# Fix round 1: LiveKitAPI's own default is 60s PER CALL (120s worst case across the two
# ensure_room calls). ensure_room now only ever runs out-of-band (see its docstring), but a
# short explicit bound still matters: it is what makes a LiveKit outage fail fast instead of
# tying up a background task for up to two minutes per attempt.
# A meeting nobody has been in for this long is over (see ensure_room).
ROOM_EMPTY_TIMEOUT_SECONDS = 300
_ENSURE_ROOM_TIMEOUT_SECONDS = 5.0
_EGRESS_TIMEOUT_SECONDS = 10.0
_TAP_TOKEN_TTL = timedelta(minutes=10)


def _allow_all(_identity: str) -> bool:
    return True


class MeetingTransport(Protocol):
    async def ensure_room(self, *, org_id: UUID, room: str) -> None: ...

    async def mint_join_token(
        self,
        *,
        org_id: UUID,
        meeting_id: UUID,
        user_id: UUID,
        room: str,
        role_in_meeting: str,
        ttl_seconds: int = 600,
    ) -> str: ...

    async def start_egress(self, *, org_id: UUID, meeting_id: UUID, room: str) -> str: ...

    async def stop_egress(self, *, egress_id: str) -> None: ...

    async def subscribe_room_audio(
        self,
        *,
        org_id: UUID,
        meeting_id: UUID,
        room: str,
        sink_factory: AudioSinkFactory,
        allow_identity: Callable[[str], bool] = _allow_all,
    ) -> AudioTapHandle: ...  # handle for the transcription tap; `stop()` ends it

    def verify_webhook(self, *, body: bytes, auth_header: str) -> object: ...


class LiveKitTransport:
    """Concrete LiveKit binding. Swappable per Tech Decisions 2.7.

    Real, not stubbed: this is the binding Tasks 3/4 route against once LiveKit credentials
    exist. It is not exercised end-to-end by this build's test suite (Task 2, Deviation 1) —
    only `FakeTransport` is, and no test here claims coverage of the real SDK calls below.
    """

    def __init__(
        self,
        url: str,
        key: str,
        secret: str,
        *,
        recording: RecordingConfig | None = None,
        tap_backoff: Backoff | None = None,
    ):
        self._url, self._key, self._secret = url, key, secret
        self._recording = recording if recording is not None else RecordingConfig()
        self._tap_backoff = tap_backoff if tap_backoff is not None else Backoff()

    async def ensure_room(self, *, org_id: UUID, room: str) -> None:
        """Make the LiveKit room's OWN metadata carry `{"org_id": ...}` (the INTERNAL uuid,
        never the Clerk org id - a related bug elsewhere in this band came from exactly that
        confusion, and `webhook.py`'s `_resolve_org_id` parses whatever is written here).

        This is distinct from `mint_join_token`'s `.with_metadata(...)` call, which sets
        metadata on the PARTICIPANT's access-token grant, not on the room. Room metadata is
        what a LiveKit webhook event's `evt.room.metadata` actually carries (confirmed against
        the installed `livekit-api` package's `WebhookEvent`/`Room` protobuf field lists), so
        it is the only place that can make `services.meeting_service.webhook._resolve_org_id`
        resolve a tenant for a REAL LiveKit deployment.

        Two calls, not one, because `CreateRoom` is idempotent but does not update the
        metadata of a room that already exists (e.g. one LiveKit auto-created on an earlier
        join before this ever ran), while `UpdateRoomMetadata` requires an existing room. The
        pair is correct regardless of which of those states the room was already in.

        Fix round 1 finding: `LiveKitAPI`'s default `aiohttp.ClientTimeout` is 60 seconds PER
        CALL. This method is only ever invoked out-of-band now (`services.meeting_service.
        rooms`'s callers schedule it via a background task, never inside an open Postgres
        transaction or a request the caller is waiting on - see the fix-round report), so a
        slow LiveKit no longer risks a connection-pool or SLO incident the way it did before.
        An explicit, much shorter timeout is still set regardless, so a LiveKit outage fails
        fast (this attempt gives up in seconds, not up to two minutes across both calls) and
        the retry-on-next-mint self-heal (`meeting_rooms.livekit_metadata_synced_at IS NULL`)
        gets its next chance sooner rather than later.
        """
        metadata = json.dumps({"org_id": str(org_id)})
        timeout = aiohttp.ClientTimeout(total=_ENSURE_ROOM_TIMEOUT_SECONDS)
        lkapi = api.LiveKitAPI(self._url, self._key, self._secret, timeout=timeout)
        try:
            await lkapi.room.create_room(
                api.CreateRoomRequest(
                    name=room,
                    metadata=metadata,
                    # LiveKit closes the room (and sends room_finished, which ends the meeting)
                    # once nobody has been in it this long: before anyone joins, and after the
                    # last person leaves. Five minutes leaves room to rejoin after a drop.
                    empty_timeout=ROOM_EMPTY_TIMEOUT_SECONDS,
                    departure_timeout=ROOM_EMPTY_TIMEOUT_SECONDS,
                )
            )
            await lkapi.room.update_room_metadata(
                api.UpdateRoomMetadataRequest(room=room, metadata=metadata)
            )
        finally:
            await lkapi.aclose()

    async def mint_join_token(
        self,
        *,
        org_id: UUID,
        meeting_id: UUID,
        user_id: UUID,
        room: str,
        role_in_meeting: str,
        ttl_seconds: int = 600,
    ) -> str:
        grant = api.VideoGrants(room=room, room_join=True, can_publish=True, can_subscribe=True)
        tok = (
            api.AccessToken(self._key, self._secret)
            .with_identity(str(user_id))  # one identity == user_id
            .with_grants(grant)
            # `livekit-api` 1.x's `with_ttl` takes a `timedelta`, not raw seconds; the plan
            # text's `.with_ttl(ttl_seconds)` matches the >=0.7 pin's minimum, not the 1.1.1
            # actually resolved. 600 s default; reconnect re-mints.
            .with_ttl(timedelta(seconds=ttl_seconds))
            .with_metadata(f'{{"org_id":"{org_id}","role":"{role_in_meeting}"}}')
        )
        return tok.to_jwt()

    async def start_egress(self, *, org_id: UUID, meeting_id: UUID, room: str) -> str:
        """Room-composite egress straight into the S3-compatible `mytheus-recordings` bucket
        (R2 per ADR-0023; MinIO locally). LiveKit's egress worker writes the object itself
        with the credentials carried in this request; nothing here logs them. Returns the
        LiveKit egress id, which the caller persists on `meeting_rooms.egress_id`.
        """
        cfg = self._recording
        if not cfg.s3_configured:
            raise RuntimeError("recording storage is not configured (S3_* env vars unset)")
        output = api.EncodedFileOutput(
            file_type=api.EncodedFileType.OGG if cfg.audio_only else api.EncodedFileType.MP4,
            filepath=egress_filepath(
                org_id=org_id, meeting_id=meeting_id, audio_only=cfg.audio_only
            ),
            s3=api.S3Upload(
                access_key=cfg.access_key_id,
                secret=cfg.secret_access_key,
                region=cfg.region,
                endpoint=cfg.endpoint_url or "",
                bucket=cfg.bucket,
                force_path_style=cfg.force_path_style,
            ),
        )
        request = api.RoomCompositeEgressRequest(
            room_name=room, audio_only=cfg.audio_only, file_outputs=[output]
        )
        timeout = aiohttp.ClientTimeout(total=_EGRESS_TIMEOUT_SECONDS)
        lkapi = api.LiveKitAPI(self._url, self._key, self._secret, timeout=timeout)
        try:
            info = await lkapi.egress.start_room_composite_egress(request)
        finally:
            await lkapi.aclose()
        return str(info.egress_id)

    async def stop_egress(self, *, egress_id: str) -> None:
        timeout = aiohttp.ClientTimeout(total=_EGRESS_TIMEOUT_SECONDS)
        lkapi = api.LiveKitAPI(self._url, self._key, self._secret, timeout=timeout)
        try:
            await lkapi.egress.stop_egress(api.StopEgressRequest(egress_id=egress_id))
        finally:
            await lkapi.aclose()

    def _mint_tap_token(self, *, meeting_id: UUID, room: str) -> str:
        """A subscribe-only, HIDDEN participant grant for one room: the tap is invisible to
        other participants, cannot publish anything, and its token expires in minutes (a
        reconnect re-mints)."""
        grant = api.VideoGrants(
            room=room,
            room_join=True,
            can_publish=False,
            can_publish_data=False,
            can_subscribe=True,
            hidden=True,
            recorder=True,
        )
        return (
            api.AccessToken(self._key, self._secret)
            .with_identity(tap_identity(meeting_id))
            .with_grants(grant)
            .with_ttl(_TAP_TOKEN_TTL)
            .to_jwt()
        )

    async def subscribe_room_audio(
        self,
        *,
        org_id: UUID,
        meeting_id: UUID,
        room: str,
        sink_factory: AudioSinkFactory,
        allow_identity: Callable[[str], bool] = _allow_all,
    ) -> AudioTapHandle:
        tap = LiveKitAudioTap(
            url=self._url,
            token_factory=lambda: self._mint_tap_token(meeting_id=meeting_id, room=room),
            org_id=org_id,
            meeting_id=meeting_id,
            room=room,
            sink_factory=sink_factory,
            allow_identity=allow_identity,
            backoff=self._tap_backoff,
        )
        tap.start()
        return tap

    def verify_webhook(self, *, body: bytes, auth_header: str) -> object:
        # `livekit-api` 1.x's `WebhookReceiver` takes a `TokenVerifier`, not the raw
        # (key, secret) pair the plan text passes directly; same >=0.7-vs-1.1.1 gap as
        # `with_ttl` above.
        verifier = api.TokenVerifier(self._key, self._secret)
        return api.WebhookReceiver(verifier).receive(body.decode(), auth_header)


class LiveKitAudioTap:
    """The audio tap: a hidden participant subscribed to every remote AUDIO track.

    Each subscribed track becomes an `rtc.AudioStream` that the SDK itself resamples and
    downmixes to 16 kHz mono int16, so frames go out as PCM16LE with no resampling code here.
    Each track gets its own sink (one WebSocket to transcription-service, see `audio_tap`).

    Reconnect: transient media blips are handled inside the SDK (`reconnecting`/`reconnected`).
    A full disconnect (or a failed connect) re-joins with capped exponential backoff and a
    freshly minted token, until `stop()` is called. When the last remote participant leaves,
    the tap leaves too and does NOT reconnect, so it never holds an empty room open; the next
    `room_started` webhook (a new room sid) starts a fresh tap.

    Consent: `allow_identity` is consulted per participant; a track whose participant fails it
    is never subscribed into a sink (its audio never leaves LiveKit through this tap).
    """

    def __init__(
        self,
        *,
        url: str,
        token_factory: Callable[[], str],
        org_id: UUID,
        meeting_id: UUID,
        room: str,
        sink_factory: AudioSinkFactory,
        allow_identity: Callable[[str], bool],
        backoff: Backoff,
        max_attempts: int | None = None,
    ) -> None:
        self._url = url
        self._token_factory = token_factory
        self._org_id = org_id
        self._meeting_id = meeting_id
        self._room = room
        self._sink_factory = sink_factory
        self._allow_identity = allow_identity
        self._backoff = backoff
        self._max_attempts = max_attempts
        self._stopping = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self.connect_attempts = 0
        self.skipped_identities: set[str] = set()

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    @property
    def done(self) -> bool:
        return self._task is not None and self._task.done()

    async def stop(self) -> None:
        self._stopping.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(self._task), timeout=5.0)
            except (TimeoutError, asyncio.CancelledError):
                self._task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._task

    def _log_ctx(self) -> str:
        return f"org={self._org_id} meeting={self._meeting_id} room={self._room}"

    async def _run(self) -> None:
        failures = 0
        while not self._stopping.is_set():
            room_empty = await self._session_once()
            if self._stopping.is_set() or room_empty:
                break
            failures += 1
            if self._max_attempts is not None and failures >= self._max_attempts:
                logger.error(
                    "audio tap: giving up after %d attempts (%s)", failures, self._log_ctx()
                )
                break
            delay = self._backoff.delay(failures)
            logger.info("audio tap: rejoining in %.1fs (%s)", delay, self._log_ctx())
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stopping.wait(), timeout=delay)

    async def _session_once(self) -> bool:
        """One join of the room. Returns True when the room emptied (do not rejoin)."""
        lk_room = rtc.Room()
        ended = asyncio.Event()
        state = {"room_empty": False}
        track_tasks: dict[str, asyncio.Task[None]] = {}

        def on_track_subscribed(track: Any, publication: Any, participant: Any) -> None:
            if not isinstance(track, rtc.RemoteAudioTrack):
                return
            identity = str(participant.identity)
            if not self._allow_identity(identity):
                self.skipped_identities.add(identity)
                logger.info(
                    "audio tap: skipping a participant without consent (%s)", self._log_ctx()
                )
                return
            sid = str(publication.sid)
            if sid in track_tasks:
                return
            track_tasks[sid] = asyncio.create_task(self._pump_track(track, identity))

        def on_track_unsubscribed(track: Any, publication: Any, participant: Any) -> None:
            task = track_tasks.pop(str(publication.sid), None)
            if task is not None:
                task.cancel()

        def on_participant_disconnected(participant: Any) -> None:
            if not lk_room.remote_participants:
                state["room_empty"] = True
                ended.set()

        def on_disconnected(*_args: Any) -> None:
            ended.set()

        lk_room.on("track_subscribed", on_track_subscribed)
        lk_room.on("track_unsubscribed", on_track_unsubscribed)
        lk_room.on("participant_disconnected", on_participant_disconnected)
        lk_room.on("disconnected", on_disconnected)

        self.connect_attempts += 1
        try:
            await lk_room.connect(
                self._url, self._token_factory(), rtc.RoomOptions(auto_subscribe=True)
            )
            stop_wait = asyncio.create_task(self._stopping.wait())
            end_wait = asyncio.create_task(ended.wait())
            try:
                await asyncio.wait({stop_wait, end_wait}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                stop_wait.cancel()
                end_wait.cancel()
        except Exception as exc:  # noqa: BLE001 - a failed join is retried with backoff
            logger.warning(
                "audio tap: LiveKit join failed (%s; %s)", type(exc).__name__, self._log_ctx()
            )
            return False
        finally:
            for task in track_tasks.values():
                task.cancel()
            if track_tasks:
                await asyncio.gather(*track_tasks.values(), return_exceptions=True)
            with contextlib.suppress(Exception):
                await lk_room.disconnect()
        return state["room_empty"]

    async def _pump_track(self, track: Any, identity: str) -> None:
        stream = rtc.AudioStream.from_track(
            track=track, sample_rate=SAMPLE_RATE, num_channels=NUM_CHANNELS
        )
        try:
            sink = await self._sink_factory(
                org_id=self._org_id, meeting_id=self._meeting_id, participant_identity=identity
            )

            async def frames() -> AsyncIterator[bytes]:
                async for event in stream:
                    yield pcm16le_bytes(event.frame.data)

            await pump_frames(frames(), sink)
        finally:
            with contextlib.suppress(Exception):
                await stream.aclose()


@dataclass
class FakeAudioTap:
    """`FakeTransport`'s tap handle. `run()` pushes each scripted participant's frames through
    the real sink factory, honoring `allow_identity`, so tests exercise consent filtering and
    the WebSocket sink without LiveKit."""

    org_id: UUID
    meeting_id: UUID
    sink_factory: AudioSinkFactory
    allow_identity: Callable[[str], bool]
    scripted: dict[str, list[bytes]]
    stopped: bool = False
    forwarded_identities: list[str] = field(default_factory=list)
    _task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        for identity, chunks in self.scripted.items():
            if not self.allow_identity(identity):
                continue
            sink = await self.sink_factory(
                org_id=self.org_id, meeting_id=self.meeting_id, participant_identity=identity
            )
            self.forwarded_identities.append(identity)

            async def frames(items: list[bytes] = chunks) -> AsyncIterator[bytes]:
                for item in items:
                    yield item

            await pump_frames(frames(), sink)

    async def wait(self) -> None:
        if self._task is not None:
            await self._task

    @property
    def done(self) -> bool:
        return self.stopped or (self._task is not None and self._task.done())

    async def stop(self) -> None:
        self.stopped = True
        if self._task is not None and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task


@dataclass
class _RecordedCall:
    method: str
    kwargs: dict[str, object]


@dataclass
class FakeTransport:
    """Deterministic, dependency-free stand-in for `LiveKitTransport`.

    Not in the plan text — required because no LiveKit account exists for this project
    (Task 2, Deviation 1). Tokens are structurally valid (three dot-separated base64url-ish
    segments, like a real unsigned JWT) but carry no real signature: nothing downstream may
    treat a `FakeTransport` token as authenticating against an actual LiveKit deployment.
    Every call is recorded on `self.calls` so tests can assert against what was invoked and
    with what arguments, without a network call or a real credential.
    """

    calls: list[_RecordedCall] = field(default_factory=list)
    verify_webhook_result: object = None
    verify_webhook_error: Exception | None = None
    # Room name -> the JSON string `ensure_room` would have written as LiveKit room metadata.
    # Lets a test drive the real round trip: establish a room via `ensure_room`, then build a
    # webhook event whose `room.metadata` is `room_metadata[room]`, exactly what a real LiveKit
    # webhook would carry back once `ensure_room` has actually run against it.
    room_metadata: dict[str, str] = field(default_factory=dict)
    # participant identity -> PCM frames the fake tap "receives" from that participant.
    scripted_audio: dict[str, list[bytes]] = field(default_factory=dict)
    taps: list[FakeAudioTap] = field(default_factory=list)
    start_egress_error: Exception | None = None

    async def ensure_room(self, *, org_id: UUID, room: str) -> None:
        self.calls.append(_RecordedCall("ensure_room", {"org_id": org_id, "room": room}))
        self.room_metadata[room] = json.dumps({"org_id": str(org_id)})

    async def mint_join_token(
        self,
        *,
        org_id: UUID,
        meeting_id: UUID,
        user_id: UUID,
        room: str,
        role_in_meeting: str,
        ttl_seconds: int = 600,
    ) -> str:
        self.calls.append(
            _RecordedCall(
                "mint_join_token",
                {
                    "org_id": org_id,
                    "meeting_id": meeting_id,
                    "user_id": user_id,
                    "room": room,
                    "role_in_meeting": role_in_meeting,
                    "ttl_seconds": ttl_seconds,
                },
            )
        )
        # Structurally a JWT (header.payload.signature) so callers that only ever inspect
        # shape, not cryptographic validity, cannot tell the fake from the real thing by
        # accident. The "signature" segment makes clear it is not one: real verification of
        # a fake token must fail, not silently pass.
        payload = _b64url(
            json.dumps(
                {
                    "sub": str(user_id),
                    "room": room,
                    "org_id": str(org_id),
                    "role": role_in_meeting,
                    "ttl": ttl_seconds,
                },
                sort_keys=True,
            ).encode()
        )
        return f"fake.{payload}.unsigned"

    async def start_egress(self, *, org_id: UUID, meeting_id: UUID, room: str) -> str:
        self.calls.append(
            _RecordedCall(
                "start_egress", {"org_id": org_id, "meeting_id": meeting_id, "room": room}
            )
        )
        if self.start_egress_error is not None:
            raise self.start_egress_error
        return f"fake-egress-{org_id}-{meeting_id}"

    async def stop_egress(self, *, egress_id: str) -> None:
        self.calls.append(_RecordedCall("stop_egress", {"egress_id": egress_id}))

    async def subscribe_room_audio(
        self,
        *,
        org_id: UUID,
        meeting_id: UUID,
        room: str,
        sink_factory: AudioSinkFactory,
        allow_identity: Callable[[str], bool] = _allow_all,
    ) -> AudioTapHandle:
        self.calls.append(
            _RecordedCall(
                "subscribe_room_audio", {"org_id": org_id, "meeting_id": meeting_id, "room": room}
            )
        )
        tap = FakeAudioTap(
            org_id=org_id,
            meeting_id=meeting_id,
            sink_factory=sink_factory,
            allow_identity=allow_identity,
            scripted=dict(self.scripted_audio),
        )
        tap.start()
        self.taps.append(tap)
        return tap

    def verify_webhook(self, *, body: bytes, auth_header: str) -> object:
        self.calls.append(
            _RecordedCall("verify_webhook", {"body": body, "auth_header": auth_header})
        )
        if self.verify_webhook_error is not None:
            raise self.verify_webhook_error
        return self.verify_webhook_result


def _b64url(data: bytes) -> str:
    import base64

    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def select_transport() -> MeetingTransport:
    """Config-selected binding: `FakeTransport` unless `LIVEKIT_API_KEY` is set.

    Not called by anything in this task (no route exists yet to call it from); provided so
    Tasks 3/4 have one sanctioned selection point rather than each re-implementing the same
    `if key is set` check. Defaulting to the fake means an environment that simply forgot to
    configure LiveKit fails toward "obviously fake tokens downstream" rather than toward
    `LiveKitTransport` raising deep inside a request on missing credentials.
    """
    key = os.environ.get("LIVEKIT_API_KEY")
    if not key:
        return FakeTransport()
    url = os.environ["LIVEKIT_URL"]
    secret = os.environ["LIVEKIT_API_SECRET"]
    return LiveKitTransport(url, key, secret, recording=RecordingConfig.from_env())


# --- Static protocol-conformance proof ---------------------------------------------------
#
# Acceptance check 5: assert conformance via the type checker, not only at runtime. A
# Protocol is structural, so a class can silently stop conforming (a renamed method, a
# dropped parameter) with no runtime error until something finally calls the missing method.
# These two lines make `mypy` (via `make typecheck` / `uv run mypy .`) the thing that catches
# that, at the point the signature drifts rather than the point something calls it.
def _assert_conforms(_cls: type[MeetingTransport]) -> None:
    """No-op at runtime; exists so mypy checks the argument against the Protocol."""


_assert_conforms(LiveKitTransport)
_assert_conforms(FakeTransport)

__all__ = [
    "MeetingTransport",
    "LiveKitTransport",
    "LiveKitAudioTap",
    "FakeTransport",
    "FakeAudioTap",
    "select_transport",
]
