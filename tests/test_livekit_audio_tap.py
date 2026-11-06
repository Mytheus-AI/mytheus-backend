"""`LiveKitAudioTap` / `LiveKitTransport.subscribe_room_audio`, egress, against a FAKED
LiveKit SDK.

No LiveKit account exists for this project, so the real SDK is replaced (monkeypatched
`transport.rtc` / `transport.api.LiveKitAPI`) with small fakes. That proves the tap's own
logic - hidden subscribe-only grant, audio-only + consent filtering, 16 kHz mono stream
request, rejoin with backoff, leaving an empty room, stop - and the egress request shape. It
is NOT evidence that the real LiveKit SDK/server behaves as faked here: that end-to-end check
is BLOCKED on LiveKit credentials.
"""

from __future__ import annotations

import array
import asyncio
import uuid
from types import SimpleNamespace
from typing import Any

import jwt
import pytest

from services.meeting_service import transport as transport_mod
from services.meeting_service.audio_tap import Backoff, tap_identity
from services.meeting_service.storage import RecordingConfig
from services.meeting_service.transport import LiveKitTransport

KEY, SECRET = "lk-test-key", "lk-test-secret-0123456789abcdef0123456789"


class FakeRemoteAudioTrack:
    pass


class FakeRemoteVideoTrack:
    pass


class FakeAudioStream:
    requested: list[dict[str, Any]] = []

    def __init__(self, frames: list[bytes]) -> None:
        self._frames = frames
        self.closed = False

    @classmethod
    def from_track(cls, *, track: Any, sample_rate: int, num_channels: int) -> FakeAudioStream:
        cls.requested.append({"sample_rate": sample_rate, "num_channels": num_channels})
        return cls(list(track.frames))

    def __aiter__(self) -> FakeAudioStream:
        return self

    async def __anext__(self) -> Any:
        if not self._frames:
            raise StopAsyncIteration
        raw = self._frames.pop(0)
        return SimpleNamespace(frame=SimpleNamespace(data=memoryview(array.array("h", raw))))

    async def aclose(self) -> None:
        self.closed = True


class FakeRoom:
    instances: list[FakeRoom] = []
    connect_plan: list[Exception | None] = []

    def __init__(self) -> None:
        self.handlers: dict[str, Any] = {}
        self.remote_participants: dict[str, Any] = {}
        self.tokens: list[str] = []
        self.disconnected = False
        self.connected = asyncio.Event()
        FakeRoom.instances.append(self)

    def on(self, event: str, callback: Any) -> Any:
        self.handlers[event] = callback
        return callback

    async def connect(self, url: str, token: str, options: Any) -> None:
        self.tokens.append(token)
        outcome = FakeRoom.connect_plan.pop(0) if FakeRoom.connect_plan else None
        if outcome is not None:
            raise outcome
        self.connected.set()

    async def disconnect(self) -> None:
        self.disconnected = True

    # test drivers
    def publish(self, identity: str, track: Any, sid: str) -> None:
        participant = SimpleNamespace(identity=identity)
        self.remote_participants[identity] = participant
        self.handlers["track_subscribed"](track, SimpleNamespace(sid=sid), participant)

    def leave(self, identity: str) -> None:
        participant = self.remote_participants.pop(identity)
        self.handlers["participant_disconnected"](participant)


@pytest.fixture
def fake_rtc(monkeypatch):
    FakeRoom.instances = []
    FakeRoom.connect_plan = []
    FakeAudioStream.requested = []
    fake = SimpleNamespace(
        Room=FakeRoom,
        RoomOptions=lambda **kw: SimpleNamespace(**kw),
        RemoteAudioTrack=FakeRemoteAudioTrack,
        AudioStream=FakeAudioStream,
    )
    monkeypatch.setattr(transport_mod, "rtc", fake)
    return fake


class RecordingSink:
    def __init__(self, identity: str) -> None:
        self.identity = identity
        self.frames: list[bytes] = []
        self.closed = False

    async def send(self, pcm: bytes) -> None:
        self.frames.append(pcm)

    async def aclose(self) -> None:
        self.closed = True


def _audio_track(*frames: list[int]) -> FakeRemoteAudioTrack:
    track = FakeRemoteAudioTrack()
    track.frames = list(frames)  # type: ignore[attr-defined]
    return track


async def _until(predicate, timeout: float = 2.0) -> None:
    async def wait() -> None:
        while not predicate():
            await asyncio.sleep(0.005)

    await asyncio.wait_for(wait(), timeout)


async def test_tap_joins_hidden_forwards_consented_audio_and_leaves_an_empty_room(fake_rtc):
    org, meeting = uuid.uuid4(), uuid.uuid4()
    sinks: list[RecordingSink] = []

    async def factory(*, org_id, meeting_id, participant_identity):
        assert (org_id, meeting_id) == (org, meeting)
        sink = RecordingSink(participant_identity)
        sinks.append(sink)
        return sink

    lk = LiveKitTransport("wss://lk.test", KEY, SECRET, tap_backoff=Backoff(0.01, 0.02))
    tap = await lk.subscribe_room_audio(
        org_id=org,
        meeting_id=meeting,
        room="meeting-x",
        sink_factory=factory,
        allow_identity=lambda identity: identity == "consented-user",
    )
    await _until(lambda: FakeRoom.instances and FakeRoom.instances[0].connected.is_set())
    room = FakeRoom.instances[0]

    claims = jwt.decode(room.tokens[0], SECRET, algorithms=["HS256"])
    assert claims["sub"] == tap_identity(meeting)
    assert claims["video"]["hidden"] is True
    assert claims["video"]["canSubscribe"] is True
    assert claims["video"]["canPublish"] is False
    assert claims["video"]["room"] == "meeting-x"

    room.publish("consented-user", _audio_track([1, 2], [3, 4]), "TR_a")
    room.publish("unconsented-user", _audio_track([9, 9]), "TR_b")
    room.publish("consented-user", FakeRemoteVideoTrack(), "TR_v")  # video: ignored
    await _until(lambda: sinks and sinks[0].closed)

    assert [s.identity for s in sinks] == ["consented-user"]
    assert sinks[0].frames == [b"\x01\x00\x02\x00", b"\x03\x00\x04\x00"]
    assert FakeAudioStream.requested == [{"sample_rate": 16000, "num_channels": 1}]
    assert "unconsented-user" in tap.skipped_identities  # type: ignore[attr-defined]

    room.leave("consented-user")
    room.leave("unconsented-user")
    await _until(lambda: tap.done)
    assert room.disconnected
    assert len(FakeRoom.instances) == 1  # did not rejoin an empty room


async def test_tap_rejoins_with_backoff_and_a_fresh_token_after_failures(fake_rtc):
    FakeRoom.connect_plan = [ConnectionError("down"), ConnectionError("down"), None]

    async def factory(**_kw):
        return RecordingSink("x")

    lk = LiveKitTransport("wss://lk.test", KEY, SECRET, tap_backoff=Backoff(0.001, 0.002))
    tap = await lk.subscribe_room_audio(
        org_id=uuid.uuid4(), meeting_id=uuid.uuid4(), room="r", sink_factory=factory
    )
    await _until(lambda: len(FakeRoom.instances) == 3 and FakeRoom.instances[2].connected.is_set())
    assert all(r.disconnected for r in FakeRoom.instances[:2])
    assert not tap.done

    # A full disconnect from the server side also triggers a rejoin.
    FakeRoom.instances[2].handlers["disconnected"]("SERVER_SHUTDOWN")
    await _until(lambda: len(FakeRoom.instances) == 4 and FakeRoom.instances[3].connected.is_set())

    await tap.stop()
    assert tap.done
    assert FakeRoom.instances[3].disconnected


async def test_stop_ends_a_live_tap_promptly(fake_rtc):
    async def factory(**_kw):
        return RecordingSink("x")

    lk = LiveKitTransport("wss://lk.test", KEY, SECRET)
    tap = await lk.subscribe_room_audio(
        org_id=uuid.uuid4(), meeting_id=uuid.uuid4(), room="r", sink_factory=factory
    )
    await _until(lambda: FakeRoom.instances and FakeRoom.instances[0].connected.is_set())
    await asyncio.wait_for(tap.stop(), 1.0)
    assert tap.done and FakeRoom.instances[0].disconnected
    assert len(FakeRoom.instances) == 1


# --- egress ---------------------------------------------------------------------------------


class FakeEgressService:
    def __init__(self) -> None:
        self.started: list[Any] = []
        self.stopped: list[Any] = []

    async def start_room_composite_egress(self, req: Any) -> Any:
        self.started.append(req)
        return SimpleNamespace(egress_id="EG_real123")

    async def stop_egress(self, req: Any) -> Any:
        self.stopped.append(req)
        return SimpleNamespace(egress_id=req.egress_id)


@pytest.fixture
def fake_lkapi(monkeypatch):
    egress = FakeEgressService()
    closed: list[bool] = []

    class FakeLiveKitAPI:
        def __init__(self, url, key, secret, timeout=None):
            self.egress = egress

        async def aclose(self) -> None:
            closed.append(True)

    monkeypatch.setattr(transport_mod.api, "LiveKitAPI", FakeLiveKitAPI)
    return SimpleNamespace(egress=egress, closed=closed)


_RECORDING = RecordingConfig(
    enabled=True,
    endpoint_url="https://acct.r2.cloudflarestorage.test",
    access_key_id="AKIA_TEST",
    secret_access_key="test-secret",
    force_path_style=True,
)


async def test_start_egress_writes_room_composite_to_the_tenant_prefix(fake_lkapi):
    org, meeting = uuid.uuid4(), uuid.uuid4()
    lk = LiveKitTransport("wss://lk.test", KEY, SECRET, recording=_RECORDING)

    egress_id = await lk.start_egress(org_id=org, meeting_id=meeting, room="meeting-x")

    assert egress_id == "EG_real123"
    req = fake_lkapi.egress.started[0]
    assert req.room_name == "meeting-x"
    out = req.file_outputs[0]
    assert out.filepath == f"{org}/{meeting}/{{time}}.mp4"
    assert out.s3.bucket == "nexus-recordings"
    assert out.s3.endpoint == "https://acct.r2.cloudflarestorage.test"
    assert out.s3.force_path_style is True
    assert fake_lkapi.closed == [True]


async def test_stop_egress(fake_lkapi):
    lk = LiveKitTransport("wss://lk.test", KEY, SECRET, recording=_RECORDING)
    await lk.stop_egress(egress_id="EG_1")
    assert fake_lkapi.egress.stopped[0].egress_id == "EG_1"


async def test_start_egress_refuses_without_storage_credentials(fake_lkapi):
    lk = LiveKitTransport("wss://lk.test", KEY, SECRET, recording=RecordingConfig(enabled=True))
    with pytest.raises(RuntimeError):
        await lk.start_egress(org_id=uuid.uuid4(), meeting_id=uuid.uuid4(), room="r")
    assert fake_lkapi.egress.started == []


def test_recording_config_from_env_defaults_off_and_hides_secrets():
    assert RecordingConfig.from_env({}).enabled is False
    cfg = RecordingConfig.from_env(
        {
            "RECORDING_ENABLED": "true",
            "S3_ACCESS_KEY_ID": "AKIA_TEST",
            "S3_SECRET_ACCESS_KEY": "shh-test",
            "RECORDING_AUDIO_ONLY": "1",
        }
    )
    assert cfg.enabled and cfg.s3_configured and cfg.audio_only
    assert cfg.bucket == "nexus-recordings"
    assert "shh-test" not in repr(cfg) and "AKIA_TEST" not in repr(cfg)
