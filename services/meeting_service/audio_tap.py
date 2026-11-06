"""Server-side audio tap plumbing: PCM sinks to `transcription-service`'s `/internal/audio`.

Plan 3-4: meeting-service owns "server-side audio hook to transcription" (CLAUDE.md service
map). This module is the provider-NEUTRAL half of that hook. It never imports the LiveKit SDK
(`tests/test_livekit_import_boundary.py` holds that to `transport.py` alone): the LiveKit half
- joining the room as a hidden participant and turning each remote audio track into a stream
of PCM frames - lives in `transport.LiveKitTransport.subscribe_room_audio`, which hands those
frames to the `AudioSink` objects built here. Swapping the transport (Tech Decision 2.7)
leaves everything in this file untouched.

WIRE CONTRACT with `mytheus-realtime/services/transcription-service` (`/internal/audio`).
The receiver's stub documents no contract yet, so this side defines it (the receiving agent
must match it exactly):

- One WebSocket connection PER (meeting, participant audio track). Several connections for
  the same `meeting_id` are open concurrently when several people speak; the receiver feeds
  them into its one-per-meeting provider session.
- Auth: request header `X-Internal-Auth: <INTERNAL_AUDIO_TOKEN>`. No query parameters - the
  token and the tenant never ride the URL (privacy rule: nothing sensitive in a query string).
- First frame: a TEXT frame carrying JSON exactly
  `{"org_id", "meeting_id", "participant_identity", "sample_rate": 16000,
  "encoding": "pcm_s16le"}`. `participant_identity` is the LiveKit identity, which for a human
  participant is the internal `users.id` (`LiveKitTransport.mint_join_token` sets it so).
- Every later frame: a BINARY frame of raw PCM, signed 16-bit little-endian, 16 kHz, mono.
  LiveKit delivers ~10 ms frames (320 bytes); no framing header is added.
- A reconnect repeats the handshake from the top (a fresh hello), so the receiver never has to
  remember a half-open connection.

Logging rule: no log line in this module ever carries audio bytes or the internal token.
Connection failures log the exception TYPE only, never its message, because an aiohttp error
message can echo request details.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import sys
import time
from array import array
from collections.abc import AsyncIterable, Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Protocol
from uuid import UUID

logger = logging.getLogger(__name__)

AUTH_HEADER = "X-Internal-Auth"
SAMPLE_RATE = 16000
NUM_CHANNELS = 1
ENCODING = "pcm_s16le"

# Identity prefix of the hidden participant the tap joins as. The webhook uses it to ignore the
# tap's own participant_joined/left events, so the tap never counts as a meeting attendee
# downstream (transcription-service's per-meeting participant counter, for one).
TAP_IDENTITY_PREFIX = "nexus-audio-tap"

_DEFAULT_AUDIO_URL = "ws://localhost:8091/internal/audio"


def is_internal_identity(identity: str | None) -> bool:
    """True for participants this platform puts in a room itself, not people: the audio tap,
    and LiveKit egress recorders (identities `EG_...`)."""
    if not identity:
        return False
    return identity.startswith(TAP_IDENTITY_PREFIX) or identity.startswith("EG_")


def tap_identity(meeting_id: UUID) -> str:
    return f"{TAP_IDENTITY_PREFIX}-{meeting_id}"


@dataclass(frozen=True)
class Backoff:
    """Capped exponential backoff with jitter: attempt n waits about base * 2^(n-1), capped,
    scaled into [50%, 100%] so a fleet of taps that lost transcription-service at the same
    moment does not reconnect in lockstep."""

    base_seconds: float = 0.5
    cap_seconds: float = 30.0

    def delay(self, attempt: int, rng: Callable[[], float] = random.random) -> float:
        raw = min(self.cap_seconds, self.base_seconds * (2 ** max(attempt - 1, 0)))
        return float(raw * (0.5 + rng() / 2))


@dataclass(frozen=True)
class AudioTapConfig:
    url: str
    # repr=False: a config object that lands in a log line or a traceback must not print it.
    token: str = field(repr=False)
    backoff: Backoff = Backoff()

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> AudioTapConfig | None:
        """`None` when `INTERNAL_AUDIO_TOKEN` is unset: the tap then does not start at all.
        Fails closed - an unauthenticated audio stream is never opened."""
        source = os.environ if env is None else env
        token = source.get("INTERNAL_AUDIO_TOKEN", "")
        if not token:
            return None
        return cls(
            url=source.get("TRANSCRIPTION_AUDIO_URL", _DEFAULT_AUDIO_URL),
            token=token,
            backoff=Backoff(
                base_seconds=float(source.get("AUDIO_TAP_BACKOFF_BASE_SECONDS", "0.5")),
                cap_seconds=float(source.get("AUDIO_TAP_BACKOFF_CAP_SECONDS", "30")),
            ),
        )


def hello_message(*, org_id: UUID, meeting_id: UUID, participant_identity: str) -> str:
    return json.dumps(
        {
            "org_id": str(org_id),
            "meeting_id": str(meeting_id),
            "participant_identity": participant_identity,
            "sample_rate": SAMPLE_RATE,
            "encoding": ENCODING,
        }
    )


def pcm16le_bytes(samples: memoryview) -> bytes:
    """Raw bytes of an int16 sample buffer, little-endian regardless of host byte order."""
    if sys.byteorder == "little":
        return samples.tobytes()
    swapped = array("h", samples.tobytes())
    swapped.byteswap()
    return swapped.tobytes()


# --- WebSocket seam -------------------------------------------------------------------------


class WsConnection(Protocol):
    async def send_str(self, data: str) -> None: ...
    async def send_bytes(self, data: bytes) -> None: ...
    async def close(self) -> object: ...


class WsConnector(Protocol):
    async def connect(self, url: str, *, headers: Mapping[str, str]) -> WsConnection: ...


class _AiohttpConnection:
    """One aiohttp ClientSession per connection, closed with it: no shared session state to
    leak across tenants or outlive the meeting."""

    def __init__(self, session: object, ws: object) -> None:
        self._session = session
        self._ws = ws

    async def send_str(self, data: str) -> None:
        await self._ws.send_str(data)  # type: ignore[attr-defined]

    async def send_bytes(self, data: bytes) -> None:
        await self._ws.send_bytes(data)  # type: ignore[attr-defined]

    async def close(self) -> object:
        try:
            await self._ws.close()  # type: ignore[attr-defined]
        finally:
            await self._session.close()  # type: ignore[attr-defined]
        return None


class AiohttpWsConnector:
    def __init__(self, *, connect_timeout_seconds: float = 5.0, heartbeat_seconds: float = 20.0):
        self._connect_timeout = connect_timeout_seconds
        self._heartbeat = heartbeat_seconds

    async def connect(self, url: str, *, headers: Mapping[str, str]) -> WsConnection:
        import aiohttp

        session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None))
        try:
            ws = await session.ws_connect(
                url,
                headers=dict(headers),
                heartbeat=self._heartbeat,
                timeout=aiohttp.ClientWSTimeout(ws_close=self._connect_timeout),
            )
        except BaseException:
            await session.close()
            raise
        return _AiohttpConnection(session, ws)


# --- Sinks ---------------------------------------------------------------------------------


class AudioSink(Protocol):
    async def send(self, pcm: bytes) -> None: ...
    async def aclose(self) -> None: ...


class AudioSinkFactory(Protocol):
    async def __call__(
        self, *, org_id: UUID, meeting_id: UUID, participant_identity: str
    ) -> AudioSink: ...


class TranscriptionAudioSink:
    """A self-reconnecting WebSocket sink for one participant's track.

    Never blocks the frame producer on a reconnect: while disconnected, frames are DROPPED
    (and counted) until the backoff deadline passes, then the next frame triggers one connect
    attempt. Sleeping inside `send` instead would let LiveKit's per-track queue grow without
    bound for as long as transcription-service is down. Live captions have no use for audio
    that is 30 seconds stale, so dropping is the correct degradation, not a loss to recover.
    """

    def __init__(
        self,
        *,
        connector: WsConnector,
        url: str,
        token: str,
        hello: str,
        backoff: Backoff,
        log_context: str,
        clock: Callable[[], float] = time.monotonic,
        rng: Callable[[], float] = random.random,
    ) -> None:
        self._connector = connector
        self._url = url
        self._token = token
        self._hello = hello
        self._backoff = backoff
        self._log_context = log_context
        self._clock = clock
        self._rng = rng
        self._ws: WsConnection | None = None
        self._attempt = 0
        self._next_attempt_at = 0.0
        self._closed = False
        self.dropped_frames = 0
        self.sent_frames = 0
        self.connects = 0

    @property
    def connected(self) -> bool:
        return self._ws is not None

    async def send(self, pcm: bytes) -> None:
        if self._closed:
            return
        if self._ws is None:
            if self._clock() < self._next_attempt_at or not await self._connect():
                self.dropped_frames += 1
                return
        assert self._ws is not None
        try:
            await self._ws.send_bytes(pcm)
        except Exception as exc:  # noqa: BLE001 - any send failure means "reconnect"
            logger.warning(
                "audio tap: connection to transcription lost (%s; %s); reconnecting with backoff",
                type(exc).__name__,
                self._log_context,
            )
            await self._drop_connection()
            self._schedule_retry()
            self.dropped_frames += 1
            return
        self.sent_frames += 1

    async def aclose(self) -> None:
        self._closed = True
        await self._drop_connection()

    async def _connect(self) -> bool:
        ws: WsConnection | None = None
        try:
            ws = await self._connector.connect(self._url, headers={AUTH_HEADER: self._token})
            await ws.send_str(self._hello)
        except Exception as exc:  # noqa: BLE001 - connection failures are retried, not raised
            if ws is not None:
                await _quiet_close(ws)
            self._schedule_retry()
            logger.warning(
                "audio tap: connect to transcription failed (%s; %s; attempt %d)",
                type(exc).__name__,
                self._log_context,
                self._attempt,
            )
            return False
        self._ws = ws
        self._attempt = 0
        self.connects += 1
        return True

    def _schedule_retry(self) -> None:
        self._attempt += 1
        self._next_attempt_at = self._clock() + self._backoff.delay(self._attempt, self._rng)

    async def _drop_connection(self) -> None:
        ws, self._ws = self._ws, None
        if ws is not None:
            await _quiet_close(ws)


async def _quiet_close(ws: WsConnection) -> None:
    try:
        await ws.close()
    except Exception:  # noqa: BLE001 - closing a broken socket may itself fail; nothing to do
        pass


def make_sink_factory(
    config: AudioTapConfig, connector: WsConnector | None = None
) -> AudioSinkFactory:
    conn = connector if connector is not None else AiohttpWsConnector()

    async def factory(*, org_id: UUID, meeting_id: UUID, participant_identity: str) -> AudioSink:
        return TranscriptionAudioSink(
            connector=conn,
            url=config.url,
            token=config.token,
            hello=hello_message(
                org_id=org_id, meeting_id=meeting_id, participant_identity=participant_identity
            ),
            backoff=config.backoff,
            log_context=f"org={org_id} meeting={meeting_id}",
        )

    return factory


async def pump_frames(frames: AsyncIterable[bytes], sink: AudioSink) -> None:
    """Forward every PCM frame to `sink`; always close the sink, however the stream ends."""
    try:
        async for pcm in frames:
            await sink.send(pcm)
    finally:
        await sink.aclose()


# --- Process-wide tap registry ---------------------------------------------------------------


class AudioTapHandle(Protocol):
    @property
    def done(self) -> bool: ...

    async def stop(self) -> None: ...


@dataclass
class TapManager:
    """One live tap per (org_id, room) in this process. `start` is idempotent: a redelivered or
    duplicate `room_started` for a room whose tap is still running does nothing. A tap that has
    already finished on its own (the room emptied) is replaced."""

    _taps: dict[tuple[UUID, str], AudioTapHandle] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def start(
        self, *, org_id: UUID, room: str, starter: Callable[[], Awaitable[AudioTapHandle]]
    ) -> bool:
        async with self._lock:
            existing = self._taps.get((org_id, room))
            if existing is not None and not existing.done:
                return False
            self._taps[(org_id, room)] = await starter()
            return True

    async def stop(self, *, org_id: UUID, room: str) -> bool:
        async with self._lock:
            handle = self._taps.pop((org_id, room), None)
        if handle is None:
            return False
        await handle.stop()
        return True

    async def stop_all(self) -> None:
        async with self._lock:
            handles = list(self._taps.values())
            self._taps.clear()
        for handle in handles:
            try:
                await handle.stop()
            except Exception:  # noqa: BLE001 - shutdown must stop every tap regardless
                logger.exception("audio tap: stop failed during shutdown")

    def is_running(self, *, org_id: UUID, room: str) -> bool:
        handle = self._taps.get((org_id, room))
        return handle is not None and not handle.done


__all__ = [
    "AUTH_HEADER",
    "SAMPLE_RATE",
    "ENCODING",
    "TAP_IDENTITY_PREFIX",
    "AiohttpWsConnector",
    "AudioSink",
    "AudioSinkFactory",
    "AudioTapConfig",
    "AudioTapHandle",
    "Backoff",
    "TapManager",
    "TranscriptionAudioSink",
    "WsConnection",
    "WsConnector",
    "hello_message",
    "is_internal_identity",
    "make_sink_factory",
    "pcm16le_bytes",
    "pump_frames",
    "tap_identity",
]
