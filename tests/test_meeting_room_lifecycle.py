"""Room-start/finish orchestration (`room_lifecycle`), the bucket lifecycle helper, and the
webhook's egress filename handling - all with fakes (FakeTransport, a fake session factory, a
fake S3 client). No Postgres, LiveKit, transcription-service or S3 needed.

The SQL in `load_consent_gate` and the webhook's room_started/room_finished DB path need a
real Postgres and are covered by `tests/test_livekit_webhook.py`-style integration tests only
when the testcontainers stack is up.
"""

from __future__ import annotations

import json
import uuid
from types import SimpleNamespace
from typing import Any

import pytest

from services.meeting_service import bucket_lifecycle
from services.meeting_service.audio_tap import TapManager
from services.meeting_service.room_lifecycle import (
    ConsentGate,
    RoomStarted,
    on_room_finished,
    on_room_started,
)
from services.meeting_service.storage import RecordingConfig
from services.meeting_service.transport import FakeTransport
from services.meeting_service.webhook import _uploaded_filename


class FakeSession:
    def __init__(self, log: list[tuple[str, dict[str, Any]]]) -> None:
        self._log = log

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    def begin(self) -> FakeSession:
        return self

    async def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> None:
        self._log.append((str(stmt), dict(params or {})))


class FakeSessionFactory:
    def __init__(self) -> None:
        self.log: list[tuple[str, dict[str, Any]]] = []

    def __call__(self) -> FakeSession:
        return FakeSession(self.log)


class Sink:
    def __init__(self, store: dict[str, list[bytes]], identity: str) -> None:
        self.store, self.identity = store, identity
        store[identity] = []

    async def send(self, pcm: bytes) -> None:
        self.store[self.identity].append(pcm)

    async def aclose(self) -> None:
        pass


def _factory(store: dict[str, list[bytes]]):
    async def factory(*, org_id, meeting_id, participant_identity):
        return Sink(store, participant_identity)

    return factory


OPEN_GATE = ConsentGate(
    consent_required=False, consented_identities=frozenset(), unconsented_count=3
)
RECORDING_ON = RecordingConfig(enabled=True, access_key_id="AKIA_TEST", secret_access_key="t")


def _started(gate: ConsentGate) -> RoomStarted:
    return RoomStarted(org_id=uuid.uuid4(), meeting_id=uuid.uuid4(), room="meeting-r", gate=gate)


async def test_room_started_taps_audio_and_records_when_allowed():
    transport = FakeTransport(scripted_audio={"alice": [b"a1", b"a2"], "bob": [b"b1"]})
    sessions, taps, store = FakeSessionFactory(), TapManager(), {}
    started = _started(OPEN_GATE)

    await on_room_started(
        sessions,
        transport,
        started,
        taps=taps,
        recording=RECORDING_ON,
        sink_factory=_factory(store),
    )
    await transport.taps[0].wait()

    assert store == {"alice": [b"a1", b"a2"], "bob": [b"b1"]}
    assert [c.method for c in transport.calls] == ["subscribe_room_audio", "start_egress"]
    sql, params = sessions.log[-1]
    assert "UPDATE meeting_rooms SET egress_id" in sql
    assert params["egress_id"] == f"fake-egress-{started.org_id}-{started.meeting_id}"
    assert params["org"] == str(started.org_id)


async def test_consent_required_forwards_only_consented_participants_and_skips_recording():
    gate = ConsentGate(
        consent_required=True, consented_identities=frozenset({"alice"}), unconsented_count=1
    )
    transport = FakeTransport(scripted_audio={"alice": [b"a"], "bob": [b"b"]})
    sessions, store = FakeSessionFactory(), {}

    await on_room_started(
        sessions,
        transport,
        _started(gate),
        taps=TapManager(),
        recording=RECORDING_ON,
        sink_factory=_factory(store),
    )
    await transport.taps[0].wait()

    assert store == {"alice": [b"a"]}
    assert "start_egress" not in [c.method for c in transport.calls]
    assert sessions.log == []


async def test_consent_required_with_nobody_consented_starts_nothing():
    gate = ConsentGate(consent_required=True, consented_identities=frozenset(), unconsented_count=2)
    transport = FakeTransport()
    await on_room_started(
        FakeSessionFactory(),
        transport,
        _started(gate),
        taps=TapManager(),
        recording=RECORDING_ON,
        sink_factory=_factory({}),
    )
    assert transport.calls == []


async def test_full_consent_allows_recording():
    gate = ConsentGate(
        consent_required=True, consented_identities=frozenset({"a", "b"}), unconsented_count=0
    )
    assert gate.recording_allowed() and gate.tap_allowed() and not gate.allows_identity("c")


async def test_recording_off_by_default_and_tap_off_without_token():
    transport = FakeTransport()
    await on_room_started(
        FakeSessionFactory(),
        transport,
        _started(OPEN_GATE),
        taps=TapManager(),
        tap_config=None,  # INTERNAL_AUDIO_TOKEN unset
        recording=RecordingConfig(),  # RECORDING_ENABLED unset
    )
    assert transport.calls == []


async def test_recording_enabled_without_s3_credentials_does_not_start_egress():
    transport = FakeTransport()
    await on_room_started(
        FakeSessionFactory(),
        transport,
        _started(OPEN_GATE),
        taps=TapManager(),
        recording=RecordingConfig(enabled=True),
    )
    assert "start_egress" not in [c.method for c in transport.calls]


async def test_egress_failure_does_not_raise_or_stop_the_tap():
    transport = FakeTransport(start_egress_error=ConnectionError("livekit down"))
    taps = TapManager()
    started = _started(OPEN_GATE)
    await on_room_started(
        FakeSessionFactory(),
        transport,
        started,
        taps=taps,
        recording=RECORDING_ON,
        sink_factory=_factory({}),
    )
    assert [c.method for c in transport.calls] == ["subscribe_room_audio", "start_egress"]


async def test_duplicate_room_started_does_not_open_a_second_tap():
    transport = FakeTransport(scripted_audio={"alice": []})
    taps = TapManager()
    started = _started(OPEN_GATE)

    # Keep the first tap "live" by never letting it finish.
    async def slow_factory(*, org_id, meeting_id, participant_identity):
        import asyncio

        await asyncio.Event().wait()

    for _ in range(2):
        await on_room_started(
            FakeSessionFactory(),
            transport,
            started,
            taps=taps,
            recording=RecordingConfig(),
            sink_factory=slow_factory,
        )
    assert [c.method for c in transport.calls] == ["subscribe_room_audio"]
    await taps.stop_all()


async def test_room_finished_stops_tap_and_egress():
    transport = FakeTransport(scripted_audio={"alice": []})
    taps = TapManager()
    started = _started(OPEN_GATE)

    async def slow_factory(**_kw):
        import asyncio

        await asyncio.Event().wait()

    await on_room_started(
        FakeSessionFactory(),
        transport,
        started,
        taps=taps,
        recording=RecordingConfig(),
        sink_factory=slow_factory,
    )
    assert taps.is_running(org_id=started.org_id, room=started.room)

    await on_room_finished(
        transport, org_id=started.org_id, room=started.room, egress_id="EG_1", taps=taps
    )

    assert not taps.is_running(org_id=started.org_id, room=started.room)
    assert transport.taps[0].stopped
    assert transport.calls[-1].method == "stop_egress"
    assert transport.calls[-1].kwargs == {"egress_id": "EG_1"}


# --- webhook egress filename ----------------------------------------------------------------


def test_uploaded_filename_prefers_file_results_then_legacy_file():
    info = SimpleNamespace(file_results=[SimpleNamespace(filename="o/m/2026.mp4")])
    assert _uploaded_filename(info) == "o/m/2026.mp4"
    legacy = SimpleNamespace(file_results=[], file=SimpleNamespace(filename="o/m/x.ogg"))
    assert _uploaded_filename(legacy) == "o/m/x.ogg"
    assert _uploaded_filename(SimpleNamespace(egress_id="EG")) is None


# --- bucket lifecycle -----------------------------------------------------------------------


def test_lifecycle_rules_match_canon():
    a, b = uuid.UUID(int=1), uuid.UUID(int=2)
    rules = bucket_lifecycle.build_lifecycle_rules(tenant_retention_days={b: 90, a: 365})

    base = rules[0]
    assert base["Filter"] == {"Prefix": ""}
    assert base["Transitions"] == [{"Days": 30, "StorageClass": "STANDARD_IA"}]
    assert "Expiration" not in base  # a catch-all expiry would cap every tenant
    assert [r["Filter"]["Prefix"] for r in rules[1:]] == [f"{a}/", f"{b}/"]
    assert [r["Expiration"]["Days"] for r in rules[1:]] == [365, 90]


def test_lifecycle_rules_validate_inputs():
    with pytest.raises(ValueError):
        bucket_lifecycle.build_lifecycle_rules(tenant_retention_days={uuid.uuid4(): 0})
    too_many = {uuid.uuid4(): 30 for _ in range(1000)}
    with pytest.raises(ValueError):
        bucket_lifecycle.build_lifecycle_rules(tenant_retention_days=too_many)
    no_cold = bucket_lifecycle.build_lifecycle_rules(
        tenant_retention_days={}, cold_storage_class=None
    )
    assert "Transitions" not in no_cold[0]


def test_apply_lifecycle_puts_the_whole_configuration():
    calls: list[dict[str, Any]] = []
    client = SimpleNamespace(put_bucket_lifecycle_configuration=lambda **kw: calls.append(kw))
    rules = bucket_lifecycle.build_lifecycle_rules(tenant_retention_days={})
    bucket_lifecycle.apply_lifecycle(client, bucket="nexus-recordings", rules=rules)
    assert calls == [{"Bucket": "nexus-recordings", "LifecycleConfiguration": {"Rules": rules}}]


def test_cli_dry_run_prints_rules(tmp_path, capsys, monkeypatch):
    org = uuid.uuid4()
    path = tmp_path / "tenants.json"
    path.write_text(json.dumps({str(org): None}))
    monkeypatch.delenv("RECORDING_BUCKET", raising=False)
    assert bucket_lifecycle.main(["--tenant-retention", str(path), "--dry-run"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["Bucket"] == "nexus-recordings"
    assert out["Rules"][1]["Expiration"]["Days"] == 365


def test_make_s3_client_targets_the_configured_endpoint():
    client = bucket_lifecycle.make_s3_client(
        RecordingConfig(
            endpoint_url="http://localhost:9000",
            region="us-east-1",
            access_key_id="minio-test",
            secret_access_key="minio-test-secret",
            force_path_style=True,
        )
    )
    assert client.meta.endpoint_url == "http://localhost:9000"
