"""Recording playback (polish wave 3), meeting-service's share.

1. Not connected: with `RECORDING_ENABLED` unset the route says `not_enabled`; with the flag on
   and no bucket credentials, `not_enabled` + `storage_not_configured`.
2. Participants only: another tenant's meeting is 404, a non-participant 403.
3. A consent refusal is remembered and reported as `none` + `consent_missing`.
4. The egress webhooks drive a segment: `egress_started` (no room metadata; the tenant comes
   from the object path) -> `recording`, `room_finished` -> `processing`, `egress_ended` ->
   `ready` with the uploaded key. A late `egress_updated` cannot undo `ready`. The route then
   answers a presigned URL per ready segment, placed on the meeting's clock, and audits it.
5. Off the record: two egresses give two segments with a gap; a failed egress is `failed`.
6. Internal routes accept only core-api with its secret: pause stops the running egress, resume
   starts a new segment when the call is live, erase deletes the meeting's objects and rows.
7. A shared clip's video is only ever the clip's own object: `not_in_recording` across a gap,
   `renderer_not_installed` without ffmpeg, `ready` (presigned, audited) once it exists.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.meeting_service import clips as clips_mod
from services.meeting_service import recordings, routes, webhook
from services.meeting_service.recording_objects import FakeRecordingObjects, get_recording_objects
from services.meeting_service.transport import FakeTransport
from tests.conftest import APP_URL
from tests.test_chat_api import (
    PRINCIPAL,
    Tenant,
    _create_test_app,
    _sql,
    add_participant,
    seed_meeting,
    seed_tenant,
    sign_in,
)
from tests.test_my_tasks import add_user

FAKE = FakeTransport()
CORE_SECRET = "c" * 40
WORKFLOW_SECRET = "w" * 40
T0 = datetime(2026, 11, 10, 15, 0, tzinfo=UTC)


class Objects:
    current: FakeRecordingObjects | None = None


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("meeting-service-recording-test")
    app.include_router(routes.router)
    app.include_router(webhook.router)
    app.include_router(recordings.router)
    app.include_router(recordings.internal_router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }
    app.dependency_overrides[routes.get_transport] = lambda: FAKE
    app.dependency_overrides[get_recording_objects] = lambda: Objects.current
    with TestClient(app) as c:
        yield c
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    FAKE.calls.clear()
    FAKE.verify_webhook_result = None
    FAKE.verify_webhook_error = None
    Objects.current = FakeRecordingObjects()
    monkeypatch.setenv("RECORDING_ENABLED", "true")
    monkeypatch.setenv("S3_ACCESS_KEY_ID", "test-key")
    monkeypatch.setenv("S3_SECRET_ACCESS_KEY", "test-secret")
    monkeypatch.setenv(
        "MYTHEUS_MEETING_CALLER_TOKENS",
        f"core-api={CORE_SECRET},workflow-service={WORKFLOW_SECRET}",
    )


def room_meeting(*, status: str = "ended") -> tuple[Tenant, Tenant, uuid.UUID, str]:
    host = seed_tenant()
    member = add_user(host)
    m = seed_meeting(host.org_id)
    _sql("UPDATE meetings SET status = %(s)s WHERE id = %(m)s", {"s": status, "m": str(m)})
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id, role_in_meeting) "
        "VALUES (%(m)s, %(u)s, %(o)s, 'host')",
        {"m": str(m), "u": str(host.user_id), "o": str(host.org_id)},
    )
    add_participant(m, host.org_id, member.user_id)
    room = f"meeting-{m}"
    _sql(
        "INSERT INTO meeting_rooms (org_id, meeting_id, livekit_room, status, started_at) "
        "VALUES (%(o)s, %(m)s, %(r)s, %(st)s, %(t)s)",
        {
            "o": str(host.org_id),
            "m": str(m),
            "r": room,
            "st": "active" if status == "live" else "ended",
            "t": T0,
        },
    )
    return host, member, m, room


def _ns(at: datetime) -> int:
    return int(at.timestamp() * 1_000_000_000)


def egress_evt(
    event: str,
    org_id: uuid.UUID,
    m: uuid.UUID,
    room: str,
    egress_id: str,
    *,
    status: int,
    started: datetime | None = None,
    filename: str | None = None,
    duration_ms: int | None = None,
) -> SimpleNamespace:
    files = []
    if filename:
        files.append(
            SimpleNamespace(
                filename=filename,
                started_at=_ns(started) if started else 0,
                ended_at=_ns(started + timedelta(milliseconds=duration_ms or 0)) if started else 0,
                duration=(duration_ms or 0) * 1_000_000,
                size=12345,
            )
        )
    info = SimpleNamespace(
        egress_id=egress_id,
        room_name=room,
        status=status,
        error="" if status != 4 else "encoder crashed",
        started_at=_ns(started) if started else 0,
        ended_at=0,
        file_results=files,
        file=None,
        room_composite=SimpleNamespace(
            file_outputs=[SimpleNamespace(filepath=f"{org_id}/{m}/{{time}}.mp4")]
        ),
    )
    # LiveKit's egress webhooks carry no room: only egress_info.
    return SimpleNamespace(event=event, room=None, participant=None, egress_info=info)


def deliver(client, evt) -> None:
    FAKE.verify_webhook_result = evt
    res = client.post("/livekit/webhook", content=b"{}", headers={"Authorization": "sig"})
    assert res.status_code == 200, res.text


def internal(client, path: str, org_id: uuid.UUID, body=None, *, caller="core-api"):
    secret = CORE_SECRET if caller == "core-api" else WORKFLOW_SECRET
    return client.post(
        path,
        json=body,
        headers={
            "X-Mytheus-Service": caller,
            "X-Mytheus-Service-Token": secret,
            "X-Mytheus-Org": str(org_id),
        },
    )


def segments(m: uuid.UUID) -> list[dict[str, Any]]:
    return _sql(
        "SELECT egress_id, status, object_key, duration_ms FROM meeting_recordings "
        "WHERE meeting_id = %(m)s ORDER BY started_at",
        {"m": str(m)},
    )


def audits(org_id: uuid.UUID, action: str) -> list[dict[str, Any]]:
    return _sql(
        "SELECT actor_id, metadata FROM audit_logs WHERE org_id = %(o)s AND action = %(a)s",
        {"o": str(org_id), "a": action},
    )


def test_not_connected_states(client, monkeypatch):
    host, _, m, _ = room_meeting()
    sign_in(host)
    monkeypatch.delenv("RECORDING_ENABLED")
    res = client.get(f"/api/meetings/{m}/recording")
    assert res.status_code == 200, res.text
    assert res.json()["status"] == "not_enabled" and res.json()["reason"] is None
    monkeypatch.setenv("RECORDING_ENABLED", "true")
    Objects.current = None
    body = client.get(f"/api/meetings/{m}/recording").json()
    assert body["status"] == "not_enabled" and body["reason"] == "storage_not_configured"
    Objects.current = FakeRecordingObjects()
    body = client.get(f"/api/meetings/{m}/recording").json()
    assert body["status"] == "none" and body["reason"] == "not_recorded"


def test_participants_only(client):
    host, _, m, _ = room_meeting()
    outsider = add_user(host)
    sign_in(outsider)
    res = client.get(f"/api/meetings/{m}/recording")
    assert res.status_code == 403 and res.json()["code"] == "NOT_A_PARTICIPANT"
    sign_in(seed_tenant())
    assert client.get(f"/api/meetings/{m}/recording").status_code == 404


def test_consent_refusal_is_reported(client):
    host, _, m, _ = room_meeting()
    _sql(
        "UPDATE meeting_rooms SET recording_skip_reason = 'consent_missing' "
        "WHERE meeting_id = %(m)s",
        {"m": str(m)},
    )
    sign_in(host)
    body = client.get(f"/api/meetings/{m}/recording").json()
    assert body == {**body, "status": "none", "reason": "consent_missing", "segments": []}


def test_egress_lifecycle_to_a_playable_presigned_segment(client):
    host, member, m, room = room_meeting(status="live")
    org = host.org_id
    deliver(client, egress_evt("egress_started", org, m, room, "EG_1", status=0))
    sign_in(member)
    body = client.get(f"/api/meetings/{m}/recording").json()
    assert body["status"] == "recording" and body["segments"][0]["url"] is None

    # The room closes: the segment is finishing its upload.
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from services.meeting_service.room_lifecycle import on_room_finished

    async def finish() -> None:
        engine = create_async_engine(APP_URL)
        try:
            await on_room_finished(
                FAKE,
                org_id=org,
                room=room,
                egress_id="EG_1",
                run_tap=False,
                session_factory=async_sessionmaker(engine, expire_on_commit=False),
                meeting_id=m,
            )
        finally:
            await engine.dispose()

    asyncio.run(finish())
    assert segments(m)[0]["status"] == "processing"
    assert client.get(f"/api/meetings/{m}/recording").json()["status"] == "processing"

    key = f"{org}/{m}/2026-11-10T150005.mp4"
    started = T0 + timedelta(seconds=5)
    deliver(
        client,
        egress_evt(
            "egress_ended",
            org,
            m,
            room,
            "EG_1",
            status=3,
            started=started,
            filename=key,
            duration_ms=60_000,
        ),
    )
    # A late update cannot move it back.
    deliver(client, egress_evt("egress_updated", org, m, room, "EG_1", status=1))
    [seg] = segments(m)
    assert seg["status"] == "ready" and seg["object_key"] == key and seg["duration_ms"] == 60_000

    body = client.get(f"/api/meetings/{m}/recording").json()
    assert body["status"] == "ready" and body["duration_ms"] == 60_000
    [s] = body["segments"]
    assert s["offset_ms"] == 5000 and s["media_type"] == "video/mp4"
    assert s["url"].startswith(f"https://recordings.test/{key}?X-Amz-Expires=600")
    assert body["url_expires_at"] is not None
    [row] = audits(org, "recording.playback_issued")
    assert str(row["actor_id"]) == str(member.user_id) and row["metadata"]["segments"] == 1


def test_off_record_gap_two_segments_and_a_failure(client):
    host, _, m, room = room_meeting()
    org = host.org_id
    deliver(
        client,
        egress_evt(
            "egress_ended",
            org,
            m,
            room,
            "EG_a",
            status=3,
            started=T0,
            filename=f"{org}/{m}/a.mp4",
            duration_ms=120_000,
        ),
    )
    deliver(
        client,
        egress_evt(
            "egress_ended",
            org,
            m,
            room,
            "EG_b",
            status=3,
            started=T0 + timedelta(minutes=5),
            filename=f"{org}/{m}/b.mp4",
            duration_ms=60_000,
        ),
    )
    deliver(client, egress_evt("egress_ended", org, m, room, "EG_c", status=4))
    sign_in(host)
    body = client.get(f"/api/meetings/{m}/recording").json()
    assert body["status"] == "ready"
    by_status = [(s["status"], s["offset_ms"]) for s in body["segments"]]
    assert ("ready", 0) in by_status and ("ready", 300_000) in by_status
    assert any(st == "failed" for st, _ in by_status)
    assert body["duration_ms"] == 180_000


def test_a_key_outside_the_meeting_prefix_is_never_ready(client):
    host, _, m, room = room_meeting()
    other = uuid.uuid4()
    deliver(
        client,
        egress_evt(
            "egress_ended",
            host.org_id,
            m,
            room,
            "EG_x",
            status=3,
            started=T0,
            filename=f"{host.org_id}/{other}/x.mp4",
            duration_ms=1000,
        ),
    )
    assert segments(m)[0]["status"] == "failed"


def test_internal_routes_accept_only_core_api(client):
    host, _, m, _ = room_meeting(status="live")
    res = internal(
        client, f"/internal/meetings/{m}/recording/pause", host.org_id, caller="workflow-service"
    )
    assert res.status_code == 401
    res = client.post(f"/internal/meetings/{m}/recording/pause")
    assert res.status_code == 401


def test_pause_and_resume_around_off_the_record(client):
    host, _, m, room = room_meeting(status="live")
    org = host.org_id
    deliver(client, egress_evt("egress_started", org, m, room, "EG_live", status=1))
    res = internal(client, f"/internal/meetings/{m}/recording/pause", org)
    assert res.status_code == 200 and res.json()["changed"] is True
    assert ("stop_egress", "EG_live") in [(c.method, c.kwargs.get("egress_id")) for c in FAKE.calls]
    assert segments(m)[0]["status"] == "processing"
    # Pausing again is a no-op.
    assert (
        internal(client, f"/internal/meetings/{m}/recording/pause", org).json()["changed"] is False
    )

    res = internal(client, f"/internal/meetings/{m}/recording/resume", org)
    assert res.status_code == 200 and res.json()["changed"] is True, res.text
    assert [s["status"] for s in segments(m)] == ["processing", "recording"]
    # Already recording: resume again does nothing.
    assert (
        internal(client, f"/internal/meetings/{m}/recording/resume", org).json()["changed"] is False
    )


def test_resume_does_nothing_when_recording_is_off(client, monkeypatch):
    host, _, m, _ = room_meeting(status="live")
    monkeypatch.delenv("RECORDING_ENABLED")
    res = internal(client, f"/internal/meetings/{m}/recording/resume", host.org_id)
    assert res.json()["changed"] is False and segments(m) == []


def test_erase_deletes_objects_and_rows(client):
    host, _, m, room = room_meeting()
    org = host.org_id
    objects = Objects.current
    assert objects is not None
    objects.objects[f"{org}/{m}/a.mp4"] = b"v"
    objects.objects[f"{org}/{m}/clips/c.mp4"] = b"c"
    objects.objects[f"{org}/{uuid.uuid4()}/keep.mp4"] = b"k"
    deliver(
        client,
        egress_evt(
            "egress_ended",
            org,
            m,
            room,
            "EG_e",
            status=3,
            started=T0,
            filename=f"{org}/{m}/a.mp4",
            duration_ms=1000,
        ),
    )
    _sql("UPDATE meetings SET deleted_at = now() WHERE id = %(m)s", {"m": str(m)})
    res = internal(client, f"/internal/meetings/{m}/recording/erase", org)
    assert res.status_code == 200, res.text
    assert res.json() == {
        "meeting_id": str(m),
        "status": "erased",
        "objects_deleted": 2,
        "segments_deleted": 1,
    }
    assert len(objects.objects) == 1 and segments(m) == []
    room_row = _sql(
        "SELECT recording_ref, egress_id FROM meeting_rooms WHERE meeting_id = %(m)s", {"m": str(m)}
    )[0]
    assert room_row == {"recording_ref": None, "egress_id": None}
    assert audits(org, "recording.erased")
    Objects.current = None
    again = internal(client, f"/internal/meetings/{m}/recording/erase", org).json()
    assert again["status"] == "not_connected" and again["segments_deleted"] == 0


def test_clip_video_is_only_ever_the_clips_own_object(client, monkeypatch):
    host, _, m, room = room_meeting()
    org = host.org_id
    clip_id = uuid.uuid4()
    path = f"/internal/meetings/{m}/clips/{clip_id}/video"
    assert internal(client, path, org, {"start_ms": 0, "end_ms": 1000}).json()["reason"] == (
        "no_recording"
    )
    deliver(
        client,
        egress_evt(
            "egress_ended",
            org,
            m,
            room,
            "EG_v",
            status=3,
            started=T0,
            filename=f"{org}/{m}/v.mp4",
            duration_ms=120_000,
        ),
    )
    across = internal(client, path, org, {"start_ms": 100_000, "end_ms": 130_000}).json()
    assert across == {**across, "status": "unavailable", "reason": "not_in_recording"}

    monkeypatch.setattr(clips_mod, "ffmpeg_path", lambda: None)
    res = internal(client, path, org, {"start_ms": 10_000, "end_ms": 20_000}).json()
    assert res["status"] == "unavailable" and res["reason"] == "renderer_not_installed"

    objects = Objects.current
    assert objects is not None
    key = clips_mod.clip_key(org_id=org, meeting_id=m, clip_id=clip_id, media_type="video/mp4")
    objects.objects[key] = b"clip"
    ready = internal(client, path, org, {"start_ms": 10_000, "end_ms": 20_000}).json()
    assert ready["status"] == "ready" and ready["url"].startswith(f"https://recordings.test/{key}")
    assert objects.presigned == [key]  # never the recording itself
    assert audits(org, "recording.clip_video_issued")

    Objects.current = None
    assert (
        internal(client, path, org, {"start_ms": 10_000, "end_ms": 20_000}).json()["reason"]
        == "not_connected"
    )


def test_clip_render_is_claimed_once(client, monkeypatch):
    host, _, m, room = room_meeting()
    org = host.org_id
    deliver(
        client,
        egress_evt(
            "egress_ended",
            org,
            m,
            room,
            "EG_r",
            status=3,
            started=T0,
            filename=f"{org}/{m}/r.mp4",
            duration_ms=120_000,
        ),
    )
    monkeypatch.setattr(clips_mod, "ffmpeg_path", lambda: "/usr/bin/false")
    rendered: list[dict[str, Any]] = []

    async def fake_render(objects, **kwargs):
        rendered.append(kwargs)
        return False

    monkeypatch.setattr(clips_mod, "render_clip", fake_render)
    claims = iter([True, False])

    async def fake_claim(org_id, clip_id):
        return next(claims)

    monkeypatch.setattr(clips_mod, "claim_render", fake_claim)
    clip_id = uuid.uuid4()
    path = f"/internal/meetings/{m}/clips/{clip_id}/video"
    first = internal(client, path, org, {"start_ms": 30_000, "end_ms": 45_000}).json()
    assert first["status"] == "rendering"
    assert rendered and rendered[0]["start_s"] == 30.0 and rendered[0]["duration_s"] == 15.0
    assert rendered[0]["source_url"].startswith(f"https://recordings.test/{org}/{m}/r.mp4")
