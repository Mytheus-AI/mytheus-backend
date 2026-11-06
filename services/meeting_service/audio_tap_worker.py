"""Dedicated audio tap worker: `python -m services.meeting_service.audio_tap_worker`.

Why it exists. A running tap is a live LiveKit + WebSocket connection, so whichever process
holds it is stateful. Running it inside the API (`AUDIO_TAP_MODE=in-process`) is only correct
with one meeting-service replica: `room_finished` must reach the replica holding the tap. In
`AUDIO_TAP_MODE=worker` the API stays stateless and this process owns tap lifecycle.

Trigger (least invasive: no new topic, no Avro enum change). The verified, deduped
`room_started` webhook already computes everything after its transaction commits; in worker
mode it records a WANT in Redis instead of joining the room:

    nexus:{org_id}:tap:want:{meeting_id} = {"org_id", "meeting_id", "room", "room_sid"}

(12 h safety TTL, so a lost `room_finished` cannot leave a want forever). `room_finished`
deletes the want, compare-and-delete on `room_sid`, so a late `room_finished` for an old room
instance cannot cancel the next one.

Claim. Each worker polls the wants (SCAN, `nexus:*:tap:want:*`) and claims a free one with

    SET nexus:{org_id}:tap:{meeting_id} <worker_id> NX PX <TAP_CLAIM_TTL_SECONDS>

so exactly one worker taps a room. The owner refreshes the TTL every poll (heartbeat,
compare-and-expire under WATCH). A crashed worker stops heartbeating, its claim expires, and
the next poll of any live worker re-claims the room. A worker that finds its claim gone or
taken (it stalled past the TTL) stops its tap at once, so two taps never overlap for long.

Consent and auth are unchanged: the worker reloads the consent gate (`load_consent_gate`) at
claim time and starts the tap through `room_lifecycle.start_audio_tap`, the same function the
in-process path uses, so the consent rule, the `INTERNAL_AUDIO_TOKEN` check and every log line
are identical. A tap the gate refuses, or one that ends on its own (the room emptied), drops
its want, matching in-process behaviour (no restart until the next `room_started`).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import signal
import socket
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import WatchError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from libs.shared.crypto.providers import DEV_ENVIRONMENTS
from libs.shared.metrics import serve_metrics, set_service
from libs.shared.tenancy import set_rls
from services.meeting_service.audio_tap import AudioSinkFactory, AudioTapConfig, TapManager
from services.meeting_service.room_lifecycle import (
    ConsentGate,
    RoomStarted,
    load_consent_gate,
    start_audio_tap,
)
from services.meeting_service.transport import MeetingTransport

logger = logging.getLogger(__name__)

MODE_ENV = "AUDIO_TAP_MODE"
MODE_IN_PROCESS = "in-process"
MODE_WORKER = "worker"
CLAIM_TTL_ENV = "TAP_CLAIM_TTL_SECONDS"
DEFAULT_CLAIM_TTL_SECONDS = 30.0
POLL_INTERVAL_SECONDS = 2.0
WANT_TTL_SECONDS = 12 * 3600
_WANT_PATTERN = "nexus:*:tap:want:*"

GateLoader = Callable[[UUID, UUID], Awaitable[ConsentGate]]


def audio_tap_mode(env: Mapping[str, str] | None = None) -> str:
    """`AUDIO_TAP_MODE` if set (validated), else `in-process` in a development `ENVIRONMENT`
    and `worker` everywhere else (including unset)."""
    source = os.environ if env is None else env
    raw = source.get(MODE_ENV, "").strip().lower()
    if raw:
        if raw not in (MODE_IN_PROCESS, MODE_WORKER):
            raise ValueError(f"{MODE_ENV} must be '{MODE_IN_PROCESS}' or '{MODE_WORKER}'")
        return raw
    environment = source.get("ENVIRONMENT", "").strip().lower()
    return MODE_IN_PROCESS if environment in DEV_ENVIRONMENTS else MODE_WORKER


def claim_ttl_seconds(env: Mapping[str, str] | None = None) -> float:
    source = os.environ if env is None else env
    raw = source.get(CLAIM_TTL_ENV, "")
    ttl = float(raw) if raw else DEFAULT_CLAIM_TTL_SECONDS
    if ttl <= POLL_INTERVAL_SECONDS:
        # The heartbeat runs once per poll; a TTL at or below the poll interval would expire
        # live claims between heartbeats and make two workers tap the same room.
        raise ValueError(f"{CLAIM_TTL_ENV} must be greater than {POLL_INTERVAL_SECONDS}")
    return ttl


def claim_key(org_id: UUID, meeting_id: UUID) -> str:
    return f"nexus:{org_id}:tap:{meeting_id}"


def want_key(org_id: UUID, meeting_id: UUID) -> str:
    return f"nexus:{org_id}:tap:want:{meeting_id}"


@dataclass(frozen=True)
class TapWant:
    org_id: UUID
    meeting_id: UUID
    room: str
    room_sid: str

    def encode(self) -> str:
        return json.dumps(
            {
                "org_id": str(self.org_id),
                "meeting_id": str(self.meeting_id),
                "room": self.room,
                "room_sid": self.room_sid,
            }
        )

    @classmethod
    def decode(cls, raw: bytes | str) -> TapWant | None:
        try:
            data = json.loads(raw)
            return cls(
                org_id=UUID(str(data["org_id"])),
                meeting_id=UUID(str(data["meeting_id"])),
                room=str(data["room"]),
                room_sid=str(data["room_sid"]),
            )
        except (ValueError, KeyError, TypeError):
            return None


# --- trigger side (called by the webhook in worker mode) ---------------------------------------


async def record_tap_wanted(redis: Redis[bytes], want: TapWant) -> None:
    await redis.set(want_key(want.org_id, want.meeting_id), want.encode(), ex=WANT_TTL_SECONDS)


async def clear_tap_wanted(
    redis: Redis[bytes], *, org_id: UUID, meeting_id: UUID, room_sid: str | None
) -> bool:
    """Delete the want if it still names `room_sid` (any want when `room_sid` is None)."""
    key = want_key(org_id, meeting_id)
    async with redis.pipeline(transaction=True) as pipe:
        try:
            await pipe.watch(key)
            raw = await pipe.get(key)
            current = TapWant.decode(raw) if raw is not None else None
            if current is None or (room_sid is not None and current.room_sid != room_sid):
                await pipe.unwatch()
                return False
            pipe.multi()
            pipe.delete(key)
            await pipe.execute()
            return True
        except WatchError:
            return False


async def _compare_and(redis: Redis[bytes], key: str, owner: str, *, ttl_ms: int | None) -> bool:
    """If `key` still holds `owner`: refresh its TTL (`ttl_ms`) or delete it (`None`)."""
    async with redis.pipeline(transaction=True) as pipe:
        try:
            await pipe.watch(key)
            value = await pipe.get(key)
            if value is None or value.decode() != owner:
                await pipe.unwatch()
                return False
            pipe.multi()
            if ttl_ms is None:
                pipe.delete(key)
            else:
                pipe.pexpire(key, ttl_ms)
            await pipe.execute()
            return True
        except WatchError:
            return False


def _db_gate_loader(session_factory: async_sessionmaker[AsyncSession]) -> GateLoader:
    async def load(org_id: UUID, meeting_id: UUID) -> ConsentGate:
        async with session_factory() as session, session.begin():
            await set_rls(session, org_id)
            return await load_consent_gate(session, org_id, meeting_id)

    return load


# --- the worker --------------------------------------------------------------------------------


class AudioTapWorker:
    def __init__(
        self,
        *,
        redis: Redis[bytes],
        transport: MeetingTransport,
        gate_loader: GateLoader,
        tap_config: AudioTapConfig | None,
        sink_factory: AudioSinkFactory | None = None,
        claim_ttl: float = DEFAULT_CLAIM_TTL_SECONDS,
        poll_interval: float = POLL_INTERVAL_SECONDS,
        worker_id: str | None = None,
        taps: TapManager | None = None,
    ) -> None:
        self._redis = redis
        self._transport = transport
        self._gate_loader = gate_loader
        self._tap_config = tap_config
        self._sink_factory = sink_factory
        self._ttl_ms = int(claim_ttl * 1000)
        self._poll_interval = poll_interval
        self.worker_id = worker_id or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self.taps = taps if taps is not None else TapManager()
        self.owned: dict[tuple[UUID, UUID], TapWant] = {}

    async def _scan_wants(self) -> dict[tuple[UUID, UUID], TapWant]:
        keys = [k async for k in self._redis.scan_iter(match=_WANT_PATTERN, count=500)]
        if not keys:
            return {}
        wants: dict[tuple[UUID, UUID], TapWant] = {}
        for raw in await self._redis.mget(keys):
            want = TapWant.decode(raw) if raw is not None else None
            if want is not None:
                wants[(want.org_id, want.meeting_id)] = want
        return wants

    async def _stop_local(self, want: TapWant) -> None:
        self.owned.pop((want.org_id, want.meeting_id), None)
        try:
            await self.taps.stop(org_id=want.org_id, room=want.room)
        except Exception:  # noqa: BLE001
            logger.exception("audio tap: stop failed (org=%s room=%s)", want.org_id, want.room)

    async def _release(self, want: TapWant) -> None:
        await self._stop_local(want)
        await _compare_and(
            self._redis, claim_key(want.org_id, want.meeting_id), self.worker_id, ttl_ms=None
        )

    async def _drop(self, want: TapWant) -> None:
        """Release AND forget the want: the tap ended or was refused, so no worker retries."""
        await self._release(want)
        await clear_tap_wanted(
            self._redis, org_id=want.org_id, meeting_id=want.meeting_id, room_sid=want.room_sid
        )

    async def _start(self, want: TapWant) -> bool:
        try:
            gate = await self._gate_loader(want.org_id, want.meeting_id)
            started = RoomStarted(
                org_id=want.org_id, meeting_id=want.meeting_id, room=want.room, gate=gate
            )
            return await start_audio_tap(
                self._transport,
                self.taps,
                started,
                tap_config=self._tap_config,
                sink_factory=self._sink_factory,
            )
        except Exception:  # noqa: BLE001
            logger.exception(
                "audio tap: start failed (org=%s meeting=%s)", want.org_id, want.meeting_id
            )
            return False

    async def tick(self) -> None:
        wants = await self._scan_wants()
        dropped: set[tuple[UUID, UUID]] = set()

        for key, owned in list(self.owned.items()):
            want = wants.get(key)
            if want is None or want.room_sid != owned.room_sid:
                await self._release(owned)  # room_finished, or a new room instance
                continue
            if not self.taps.is_running(org_id=owned.org_id, room=owned.room):
                await self._drop(owned)  # the tap ended on its own (room emptied)
                dropped.add(key)
                continue
            if not await _compare_and(
                self._redis, claim_key(*key), self.worker_id, ttl_ms=self._ttl_ms
            ):
                logger.warning(
                    "audio tap: claim lost, stopping local tap (org=%s meeting=%s)",
                    owned.org_id,
                    owned.meeting_id,
                )
                await self._stop_local(owned)

        for key, want in wants.items():
            if key in self.owned or key in dropped:
                continue
            claimed = await self._redis.set(
                claim_key(*key), self.worker_id, nx=True, px=self._ttl_ms
            )
            if not claimed:
                continue
            self.owned[key] = want
            if not await self._start(want):
                await self._drop(want)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.tick()
            except Exception:  # noqa: BLE001 - a Redis blip must not kill the worker
                logger.exception("audio tap worker: poll failed")
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self._poll_interval)

    async def shutdown(self) -> None:
        """Stop every tap and release the claims so another worker takes over immediately."""
        for want in list(self.owned.values()):
            try:
                await self._release(want)
            except Exception:  # noqa: BLE001 - shutdown must try every room
                logger.exception("audio tap worker: release failed during shutdown")
        await self.taps.stop_all()


async def run_worker(stop: asyncio.Event | None = None) -> None:
    from redis.asyncio import from_url

    from libs.shared.db import get_sessionmaker
    from services.meeting_service.transport import select_transport

    set_service("meeting-service-audio-tap-worker")
    serve_metrics()  # no-op unless METRICS_PORT is set
    if audio_tap_mode() != MODE_WORKER:
        logger.warning(
            "audio tap worker: %s is not '%s'; the API runs taps itself and no wants are "
            "recorded, so this worker will stay idle",
            MODE_ENV,
            MODE_WORKER,
        )
    redis: Redis[bytes] = from_url(
        os.environ.get("REDIS_URL", "redis://localhost:6379"), decode_responses=False
    )
    worker = AudioTapWorker(
        redis=redis,
        transport=select_transport(),
        gate_loader=_db_gate_loader(get_sessionmaker()),
        tap_config=AudioTapConfig.from_env(),
        claim_ttl=claim_ttl_seconds(),
    )
    if stop is None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
    logger.info("audio tap worker started (id=%s)", worker.worker_id)
    try:
        await worker.run(stop)
    finally:
        await worker.shutdown()
        await redis.aclose()  # type: ignore[attr-defined]


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    asyncio.run(run_worker())


__all__ = [
    "CLAIM_TTL_ENV",
    "MODE_ENV",
    "MODE_IN_PROCESS",
    "MODE_WORKER",
    "AudioTapWorker",
    "TapWant",
    "audio_tap_mode",
    "claim_key",
    "claim_ttl_seconds",
    "clear_tap_wanted",
    "record_tap_wanted",
    "run_worker",
    "want_key",
]
