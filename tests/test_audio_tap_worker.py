"""Audio tap worker: Redis claim (exactly one worker per room), heartbeat, crash re-claim,
room_finished release, consent/token gates, and AUDIO_TAP_MODE selection. fakeredis + fakes
only: no Postgres, LiveKit or transcription-service."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from typing import Any
from uuid import UUID

import fakeredis.aioredis
import pytest

from services.meeting_service import audio_tap_worker as w
from services.meeting_service.audio_tap import AudioTapConfig
from services.meeting_service.room_lifecycle import ConsentGate

OPEN = ConsentGate(consent_required=False, consented_identities=frozenset(), unconsented_count=0)
CLOSED = ConsentGate(consent_required=True, consented_identities=frozenset(), unconsented_count=2)
TAP_CONFIG = AudioTapConfig(url="ws://transcription.test/internal/audio", token="t")


class Handle:
    def __init__(self) -> None:
        self.stopped = False
        self.ended = False

    @property
    def done(self) -> bool:
        return self.stopped or self.ended

    async def stop(self) -> None:
        self.stopped = True


class Transport:
    """Only `subscribe_room_audio` is used by the worker; the handle stays live until stopped."""

    def __init__(self) -> None:
        self.handles: list[tuple[UUID, str, Handle]] = []

    async def subscribe_room_audio(self, *, org_id: UUID, room: str, **_: Any) -> Handle:
        handle = Handle()
        self.handles.append((org_id, room, handle))
        return handle

    def live(self) -> list[str]:
        return [room for _, room, h in self.handles if not h.done]


def _loader(gate: ConsentGate) -> Callable[[UUID, UUID], Any]:
    async def load(org_id: UUID, meeting_id: UUID) -> ConsentGate:
        return gate

    return load


def _worker(redis: Any, transport: Transport, name: str, **kw: Any) -> w.AudioTapWorker:
    kw.setdefault("gate_loader", _loader(OPEN))
    kw.setdefault("tap_config", TAP_CONFIG)
    return w.AudioTapWorker(
        redis=redis, transport=transport, worker_id=name, claim_ttl=30.0, **kw  # type: ignore[arg-type]
    )


def _want(sid: str = "RM_1") -> w.TapWant:
    return w.TapWant(org_id=uuid.uuid4(), meeting_id=uuid.uuid4(), room="meeting-x", room_sid=sid)


@pytest.fixture
def redis() -> Any:
    return fakeredis.aioredis.FakeRedis()


async def test_exactly_one_worker_claims_and_taps_a_room(redis: Any) -> None:
    want = _want()
    await w.record_tap_wanted(redis, want)
    ta, tb = Transport(), Transport()
    a, b = _worker(redis, ta, "a"), _worker(redis, tb, "b")

    await asyncio.gather(a.tick(), b.tick())
    await a.tick()
    await b.tick()

    assert len(ta.live()) + len(tb.live()) == 1
    owner = "a" if ta.live() else "b"
    assert await redis.get(w.claim_key(want.org_id, want.meeting_id)) == owner.encode()
    assert w.claim_key(want.org_id, want.meeting_id) == f"nexus:{want.org_id}:tap:{want.meeting_id}"


async def test_heartbeat_keeps_the_claim_alive(redis: Any) -> None:
    want = _want()
    await w.record_tap_wanted(redis, want)
    a = _worker(redis, Transport(), "a")
    await a.tick()
    key = w.claim_key(want.org_id, want.meeting_id)
    await redis.pexpire(key, 500)  # nearly expired
    await a.tick()
    assert await redis.pttl(key) > 25_000


async def test_crashed_workers_room_is_reclaimed_after_expiry(redis: Any) -> None:
    want = _want()
    await w.record_tap_wanted(redis, want)
    ta, tb = Transport(), Transport()
    a, b = _worker(redis, ta, "a"), _worker(redis, tb, "b")
    await a.tick()
    await b.tick()
    assert ta.live() == ["meeting-x"] and tb.live() == []

    # Worker a "crashes": it stops heartbeating, so its claim expires.
    await redis.delete(w.claim_key(want.org_id, want.meeting_id))
    await b.tick()
    assert tb.live() == ["meeting-x"]
    assert await redis.get(w.claim_key(want.org_id, want.meeting_id)) == b"b"

    # If a was only stalled, its next heartbeat finds the claim taken and it stops its tap.
    await a.tick()
    assert ta.live() == [] and tb.live() == ["meeting-x"]


async def test_room_finished_clears_the_want_and_the_owner_releases(redis: Any) -> None:
    want = _want()
    await w.record_tap_wanted(redis, want)
    t = Transport()
    a = _worker(redis, t, "a")
    await a.tick()

    # A stale room_finished for an older room instance must not cancel this one.
    assert not await w.clear_tap_wanted(
        redis, org_id=want.org_id, meeting_id=want.meeting_id, room_sid="RM_OLD"
    )
    await a.tick()
    assert t.live() == ["meeting-x"]

    assert await w.clear_tap_wanted(
        redis, org_id=want.org_id, meeting_id=want.meeting_id, room_sid="RM_1"
    )
    await a.tick()
    assert t.live() == [] and a.owned == {}
    assert await redis.get(w.claim_key(want.org_id, want.meeting_id)) is None


async def test_a_new_room_instance_replaces_the_tap(redis: Any) -> None:
    want = _want("RM_1")
    await w.record_tap_wanted(redis, want)
    t = Transport()
    a = _worker(redis, t, "a")
    await a.tick()
    await w.record_tap_wanted(
        redis,
        w.TapWant(org_id=want.org_id, meeting_id=want.meeting_id, room=want.room, room_sid="RM_2"),
    )
    await a.tick()
    assert len(t.handles) == 2 and t.handles[0][2].stopped and t.live() == ["meeting-x"]


async def test_a_tap_that_ends_on_its_own_drops_the_want(redis: Any) -> None:
    want = _want()
    await w.record_tap_wanted(redis, want)
    t = Transport()
    a = _worker(redis, t, "a")
    await a.tick()
    t.handles[0][2].ended = True  # room emptied
    await a.tick()
    assert await redis.get(w.want_key(want.org_id, want.meeting_id)) is None
    assert await redis.get(w.claim_key(want.org_id, want.meeting_id)) is None
    await a.tick()
    assert len(t.handles) == 1  # not restarted until the next room_started


@pytest.mark.parametrize(
    ("gate", "tap_config"), [(CLOSED, TAP_CONFIG), (OPEN, None)], ids=["no-consent", "no-token"]
)
async def test_consent_and_token_gates_still_apply(
    redis: Any, gate: ConsentGate, tap_config: AudioTapConfig | None
) -> None:
    want = _want()
    await w.record_tap_wanted(redis, want)
    t = Transport()
    a = _worker(redis, t, "a", gate_loader=_loader(gate), tap_config=tap_config)
    await a.tick()
    assert t.handles == []
    assert await redis.get(w.want_key(want.org_id, want.meeting_id)) is None
    assert await redis.get(w.claim_key(want.org_id, want.meeting_id)) is None


async def test_shutdown_releases_claims_for_immediate_takeover(redis: Any) -> None:
    want = _want()
    await w.record_tap_wanted(redis, want)
    ta, tb = Transport(), Transport()
    a, b = _worker(redis, ta, "a"), _worker(redis, tb, "b")
    await a.tick()
    await a.shutdown()
    assert ta.live() == []
    await b.tick()
    assert tb.live() == ["meeting-x"]


async def test_run_stops_on_event(redis: Any) -> None:
    stop = asyncio.Event()
    a = _worker(redis, Transport(), "a", poll_interval=0.01)
    task = asyncio.create_task(a.run(stop))
    await asyncio.sleep(0.03)
    stop.set()
    await asyncio.wait_for(task, 1)


def test_undecodable_want_is_ignored() -> None:
    assert w.TapWant.decode(b"not json") is None
    assert w.TapWant.decode(b'{"org_id": "x"}') is None


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        ({}, "worker"),
        ({"ENVIRONMENT": "production"}, "worker"),
        ({"ENVIRONMENT": "development"}, "in-process"),
        ({"ENVIRONMENT": "test"}, "in-process"),
        ({"ENVIRONMENT": "development", "AUDIO_TAP_MODE": "worker"}, "worker"),
        ({"ENVIRONMENT": "production", "AUDIO_TAP_MODE": "In-Process"}, "in-process"),
    ],
)
def test_audio_tap_mode_defaults(env: dict[str, str], expected: str) -> None:
    assert w.audio_tap_mode(env) == expected


def test_audio_tap_mode_rejects_unknown_values() -> None:
    with pytest.raises(ValueError):
        w.audio_tap_mode({"AUDIO_TAP_MODE": "sidecar"})


def test_claim_ttl_must_exceed_the_heartbeat_interval() -> None:
    assert w.claim_ttl_seconds({}) == 30.0
    assert w.claim_ttl_seconds({"TAP_CLAIM_TTL_SECONDS": "12"}) == 12.0
    with pytest.raises(ValueError):
        w.claim_ttl_seconds({"TAP_CLAIM_TTL_SECONDS": "1"})


# --- rejoin within the same room instance --------------------------------------------------
# LiveKit keeps an emptied room for its departure timeout (five minutes), so people who leave
# and come back get the SAME room sid and no new `room_started`. The tap had left with the last
# person and nothing brought it back: no audio reached transcription for the rest of the call.
# A human `participant_joined` now re-records the want with a fresh `generation`.


def _rejoin(want: w.TapWant) -> w.TapWant:
    return w.TapWant(
        org_id=want.org_id,
        meeting_id=want.meeting_id,
        room=want.room,
        room_sid=want.room_sid,
        generation=uuid.uuid4().hex,
    )


async def test_rejoin_after_the_worker_dropped_the_want_restarts_the_tap(redis: Any) -> None:
    want = _want()
    await w.record_tap_wanted(redis, want)
    t = Transport()
    a = _worker(redis, t, "a")
    await a.tick()
    t.handles[0][2].ended = True  # everyone left; the tap left with them
    await a.tick()  # want dropped

    await w.record_tap_wanted(redis, _rejoin(want))  # someone joins the same room again
    await a.tick()
    assert len(t.handles) == 2 and t.live() == ["meeting-x"]


async def test_rejoin_before_the_worker_noticed_the_tap_ended_restarts_it(redis: Any) -> None:
    want = _want()
    await w.record_tap_wanted(redis, want)
    t = Transport()
    a = _worker(redis, t, "a")
    await a.tick()
    t.handles[0][2].ended = True
    await w.record_tap_wanted(redis, _rejoin(want))  # the join lands before the next poll

    await a.tick()
    assert len(t.handles) == 2 and t.live() == ["meeting-x"]
    assert await redis.get(w.want_key(want.org_id, want.meeting_id)) is not None


async def test_a_join_while_the_tap_runs_does_not_restart_it(redis: Any) -> None:
    want = _want()
    await w.record_tap_wanted(redis, want)
    t = Transport()
    a = _worker(redis, t, "a")
    await a.tick()
    await w.record_tap_wanted(redis, _rejoin(want))  # a second person joins

    await a.tick()
    await a.tick()
    assert len(t.handles) == 1 and t.live() == ["meeting-x"]


async def test_dropping_an_old_want_never_deletes_a_newer_rejoin(redis: Any) -> None:
    want = _want()
    await w.record_tap_wanted(redis, _rejoin(want))
    cleared = await w.clear_tap_wanted(
        redis,
        org_id=want.org_id,
        meeting_id=want.meeting_id,
        room_sid=want.room_sid,
        generation=want.generation,
    )
    assert cleared is False
    assert await redis.get(w.want_key(want.org_id, want.meeting_id)) is not None


def test_a_want_without_generation_still_decodes() -> None:
    raw = (
        f'{{"org_id": "{uuid.uuid4()}", "meeting_id": "{uuid.uuid4()}", '
        '"room": "r", "room_sid": "RM_1"}'
    )
    decoded = w.TapWant.decode(raw)
    assert decoded is not None and decoded.generation == ""
