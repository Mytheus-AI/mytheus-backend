"""Audio tap plumbing (`services.meeting_service.audio_tap`) with fakes - no LiveKit, no
transcription-service, no network. Covers the wire contract the transcription-service side
must match (auth header, hello frame, binary PCM), reconnect-with-backoff, frame dropping
while disconnected, the token never being logged, and the per-room tap registry.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import Mapping

import pytest

from services.meeting_service.audio_tap import (
    AUTH_HEADER,
    AudioTapConfig,
    Backoff,
    TapManager,
    TranscriptionAudioSink,
    hello_message,
    is_internal_identity,
    make_sink_factory,
    pcm16le_bytes,
    pump_frames,
    tap_identity,
)

TOKEN = "test-internal-token-not-a-secret"


class FakeConn:
    def __init__(self, fail_sends_after: int | None = None) -> None:
        self.texts: list[str] = []
        self.binaries: list[bytes] = []
        self.closed = False
        self._fail_after = fail_sends_after

    async def send_str(self, data: str) -> None:
        self.texts.append(data)

    async def send_bytes(self, data: bytes) -> None:
        if self._fail_after is not None and len(self.binaries) >= self._fail_after:
            raise ConnectionResetError("peer went away")
        self.binaries.append(data)

    async def close(self) -> object:
        self.closed = True
        return None


class FakeConnector:
    def __init__(self, plan: list[FakeConn | Exception]) -> None:
        self.plan = list(plan)
        self.calls: list[tuple[str, dict[str, str]]] = []
        self.conns: list[FakeConn] = []

    async def connect(self, url: str, *, headers: Mapping[str, str]) -> FakeConn:
        self.calls.append((url, dict(headers)))
        item = self.plan.pop(0) if self.plan else FakeConn()
        if isinstance(item, Exception):
            raise item
        self.conns.append(item)
        return item


class Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def _sink(connector: FakeConnector, clock: Clock) -> TranscriptionAudioSink:
    return TranscriptionAudioSink(
        connector=connector,
        url="ws://transcription.test/internal/audio",
        token=TOKEN,
        hello=hello_message(
            org_id=uuid.UUID(int=1), meeting_id=uuid.UUID(int=2), participant_identity="u-1"
        ),
        backoff=Backoff(base_seconds=1.0, cap_seconds=8.0),
        log_context="org=x meeting=y",
        clock=clock,
        rng=lambda: 1.0,  # no jitter: delay == the full exponential step
    )


async def test_handshake_matches_the_internal_audio_contract():
    connector = FakeConnector([FakeConn()])
    sink = _sink(connector, Clock())

    await sink.send(b"\x01\x00" * 160)

    url, headers = connector.calls[0]
    assert url == "ws://transcription.test/internal/audio"
    assert "?" not in url  # nothing rides the query string
    assert headers == {AUTH_HEADER: TOKEN}
    conn = connector.conns[0]
    assert json.loads(conn.texts[0]) == {
        "org_id": str(uuid.UUID(int=1)),
        "meeting_id": str(uuid.UUID(int=2)),
        "participant_identity": "u-1",
        "sample_rate": 16000,
        "encoding": "pcm_s16le",
    }
    assert conn.binaries == [b"\x01\x00" * 160]


async def test_connect_failure_backs_off_and_drops_frames_until_the_deadline():
    clock = Clock()
    connector = FakeConnector([OSError("refused"), OSError("refused"), FakeConn()])
    sink = _sink(connector, clock)

    await sink.send(b"a")  # attempt 1 fails -> retry after 1s
    await sink.send(b"b")  # before deadline: dropped without a connect attempt
    assert len(connector.calls) == 1
    clock.now += 1.0
    await sink.send(b"c")  # attempt 2 fails -> retry after 2s
    clock.now += 1.5
    await sink.send(b"d")  # still inside the 2s window
    assert len(connector.calls) == 2
    clock.now += 0.5
    await sink.send(b"e")  # attempt 3 succeeds

    assert sink.connected
    assert sink.dropped_frames == 4
    assert connector.conns[0].binaries == [b"e"]


async def test_send_failure_reconnects_with_a_fresh_hello():
    clock = Clock()
    first, second = FakeConn(fail_sends_after=1), FakeConn()
    connector = FakeConnector([first, second])
    sink = _sink(connector, clock)

    await sink.send(b"1")
    await sink.send(b"2")  # fails -> connection dropped, retry scheduled
    assert first.closed and not sink.connected
    clock.now += 1.0
    await sink.send(b"3")

    assert second.texts and json.loads(second.texts[0])["participant_identity"] == "u-1"
    assert second.binaries == [b"3"]
    assert sink.connects == 2


async def test_backoff_is_capped():
    b = Backoff(base_seconds=0.5, cap_seconds=4.0)
    assert [b.delay(n, rng=lambda: 1.0) for n in (1, 2, 3, 4, 5, 10)] == [
        0.5,
        1.0,
        2.0,
        4.0,
        4.0,
        4.0,
    ]
    assert b.delay(3, rng=lambda: 0.0) == 1.0  # jitter floor is 50%


async def test_token_and_audio_never_reach_the_logs(caplog):
    caplog.set_level(logging.DEBUG)
    clock = Clock()
    connector = FakeConnector([OSError(f"boom {TOKEN}"), FakeConn(fail_sends_after=0)])
    sink = _sink(connector, clock)

    await sink.send(b"SECRETAUDIO")
    clock.now += 10
    await sink.send(b"SECRETAUDIO")

    assert caplog.records, "failures should be logged"
    for record in caplog.records:
        message = record.getMessage()
        assert TOKEN not in message
        assert "SECRETAUDIO" not in message


async def test_closed_sink_ignores_frames_and_closes_its_socket():
    connector = FakeConnector([FakeConn()])
    sink = _sink(connector, Clock())
    await sink.send(b"x")
    await sink.aclose()
    await sink.send(b"y")
    assert connector.conns[0].closed
    assert connector.conns[0].binaries == [b"x"]


async def test_pump_frames_closes_the_sink_even_when_the_stream_fails():
    connector = FakeConnector([FakeConn()])
    sink = _sink(connector, Clock())

    async def frames():
        yield b"ok"
        raise RuntimeError("track ended badly")

    with pytest.raises(RuntimeError):
        await pump_frames(frames(), sink)
    assert connector.conns[0].closed


def test_config_fails_closed_without_a_token():
    assert AudioTapConfig.from_env({}) is None
    cfg = AudioTapConfig.from_env(
        {"INTERNAL_AUDIO_TOKEN": TOKEN, "TRANSCRIPTION_AUDIO_URL": "ws://t/internal/audio"}
    )
    assert cfg is not None and cfg.url == "ws://t/internal/audio"
    assert TOKEN not in repr(cfg)


async def test_sink_factory_builds_a_sink_per_participant():
    cfg = AudioTapConfig(url="ws://t/internal/audio", token=TOKEN)
    connector = FakeConnector([FakeConn(), FakeConn()])
    factory = make_sink_factory(cfg, connector)
    a = await factory(org_id=uuid.uuid4(), meeting_id=uuid.uuid4(), participant_identity="a")
    b = await factory(org_id=uuid.uuid4(), meeting_id=uuid.uuid4(), participant_identity="b")
    await a.send(b"1")
    await b.send(b"2")
    identities = [json.loads(c.texts[0])["participant_identity"] for c in connector.conns]
    assert identities == ["a", "b"]


def test_pcm16le_bytes_is_little_endian():
    import array

    samples = array.array("h", [1, -2])
    assert pcm16le_bytes(memoryview(samples)) == b"\x01\x00\xfe\xff"


def test_internal_identities():
    mid = uuid.uuid4()
    assert is_internal_identity(tap_identity(mid))
    assert is_internal_identity("EG_abc")
    assert not is_internal_identity(str(uuid.uuid4()))
    assert not is_internal_identity(None)


class _Handle:
    def __init__(self) -> None:
        self.stopped = False
        self.finished = False

    @property
    def done(self) -> bool:
        return self.stopped or self.finished

    async def stop(self) -> None:
        self.stopped = True


async def test_tap_manager_is_idempotent_per_room_and_replaces_finished_taps():
    manager = TapManager()
    org = uuid.uuid4()
    handles: list[_Handle] = []

    async def starter() -> _Handle:
        h = _Handle()
        handles.append(h)
        return h

    assert await manager.start(org_id=org, room="r", starter=starter)
    assert not await manager.start(org_id=org, room="r", starter=starter)  # duplicate
    assert len(handles) == 1
    handles[0].finished = True  # room emptied, tap left on its own
    assert await manager.start(org_id=org, room="r", starter=starter)
    assert len(handles) == 2
    # Same room name in another tenant is a different tap.
    assert await manager.start(org_id=uuid.uuid4(), room="r", starter=starter)

    assert await manager.stop(org_id=org, room="r")
    assert handles[1].stopped
    assert not await manager.stop(org_id=org, room="r")
    await manager.stop_all()
    assert handles[2].stopped


async def test_tap_manager_serializes_concurrent_starts():
    manager = TapManager()
    org = uuid.uuid4()
    count = 0

    async def starter() -> _Handle:
        nonlocal count
        count += 1
        await asyncio.sleep(0)
        return _Handle()

    results = await asyncio.gather(
        *(manager.start(org_id=org, room="r", starter=starter) for _ in range(5))
    )
    assert results.count(True) == 1 and count == 1
