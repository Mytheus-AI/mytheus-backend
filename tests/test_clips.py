"""Clips and clip shares (polish wave 3, Recording playback), core-api's share.

1. A participant makes a clip; every participant lists it; the creator, the host or an admin
   deletes it (anyone else gets 404). At most 10 minutes; end after start (422).
2. Non-participants 403, another tenant 404; clips are isolated by RLS.
3. A clip cannot cross a stretch that was off the record (409 `off_record_overlap`), a running
   one included.
4. Sharing: the public page shows the span's transcript lines with speakers by name (never by
   email), times from the clip's start, and video `not_included` unless the sharer chose it;
   with `include_video` the page carries meeting-service's clip video. The link is listed with
   the meeting's links (`kind: meeting_clip`). Deleting the clip takes the page down.
5. Confidential meetings: only the host shares a clip, and must confirm.
6. Erasure: a meeting erasure deletes its clips and asks meeting-service to erase the recording
   (`not_connected` without the caller secret); a person erasure deletes the clips they made.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.core_api import recording_client
from services.core_api.routes import clips, erasure, off_record, share_links
from tests.conftest import APP_URL
from tests.test_bff_meeting_screens import transcript_table  # noqa: F401 - fixture
from tests.test_chat_api import PRINCIPAL, Tenant, _create_test_app, _sql, sign_in
from tests.test_my_tasks import add_user
from tests.test_share_links import _rename, meeting_with


@pytest.fixture(autouse=True)
def _key(monkeypatch):
    monkeypatch.setenv("SHARE_LINK_SIGNING_KEY", "s" * 40)
    monkeypatch.setenv("APP_BASE_URL", "https://app.example")
    monkeypatch.delenv("MYTHEUS_MEETING_CALLER_TOKENS", raising=False)


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-clips-test")
    app.include_router(clips.router)
    app.include_router(share_links.router)
    app.include_router(share_links.public_router)
    app.include_router(clips.public_router)
    app.include_router(erasure.router)
    app.include_router(off_record.router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }
    with TestClient(app) as c:
        yield c
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


def line(t: Tenant, m: uuid.UUID, speaker: str, start: int, end: int, words: str) -> None:
    _sql(
        "INSERT INTO transcript_chunks (org_id, meeting_id, speaker_id, start_ms, end_ms, text, "
        "is_final, confidence) VALUES (%(o)s, %(m)s, %(s)s, %(a)s, %(b)s, %(t)s, true, 0.9)",
        {"o": str(t.org_id), "m": str(m), "s": speaker, "a": start, "b": end, "t": words},
    )


def make_clip(client, m: uuid.UUID, start: int = 10_000, end: int = 40_000, title="Pricing call"):
    return client.post(
        f"/api/v1/meetings/{m}/clips", json={"title": title, "start_ms": start, "end_ms": end}
    )


def test_participants_make_list_and_delete_clips(client):
    host = add_host()
    member = add_user(host)
    other = add_user(host)
    m = meeting_with(host, member, other)
    sign_in(member)
    res = make_clip(client, m, title="  The   pricing call ")
    assert res.status_code == 201, res.text
    clip = res.json()
    assert clip["title"] == "The pricing call" and clip["duration_ms"] == 30_000
    assert clip["can_delete"] is True

    sign_in(other)
    listed = client.get(f"/api/v1/meetings/{m}/clips").json()
    assert [c["id"] for c in listed["items"]] == [clip["id"]]
    assert listed["items"][0]["can_delete"] is False and listed["max_duration_ms"] == 600_000
    assert client.delete(f"/api/v1/clips/{clip['id']}").status_code == 404
    sign_in(host)  # the host may delete anyone's clip in their meeting
    assert client.delete(f"/api/v1/clips/{clip['id']}").status_code == 204
    assert client.get(f"/api/v1/meetings/{m}/clips").json()["items"] == []
    actions = {
        r["action"]
        for r in _sql("SELECT action FROM audit_logs WHERE org_id = %(o)s", {"o": str(host.org_id)})
    }
    assert {"clip.created", "clip.deleted"} <= actions


def add_host() -> Tenant:
    from tests.test_chat_api import seed_tenant

    return seed_tenant()


def test_span_rules(client):
    host = add_host()
    m = meeting_with(host)
    sign_in(host)
    assert make_clip(client, m, 0, 600_001).status_code == 422
    assert make_clip(client, m, 5000, 5000).status_code == 422
    assert make_clip(client, m, 0, 600_000).status_code == 201
    blank = client.post(
        f"/api/v1/meetings/{m}/clips", json={"title": "  ", "start_ms": 0, "end_ms": 9}
    )
    assert blank.status_code == 422


def test_access_and_isolation(client):
    from tests.test_chat_api import seed_tenant

    host = add_host()
    outsider = add_user(host)
    m = meeting_with(host)
    sign_in(host)
    clip = make_clip(client, m).json()
    sign_in(outsider)
    assert make_clip(client, m).status_code == 403
    assert client.get(f"/api/v1/meetings/{m}/clips").status_code == 403
    stranger = seed_tenant()
    sign_in(stranger)
    assert client.get(f"/api/v1/meetings/{m}/clips").status_code == 404
    assert client.delete(f"/api/v1/clips/{clip['id']}").status_code == 404
    assert client.post(f"/api/v1/clips/{clip['id']}/share-links").status_code == 404


def test_a_clip_cannot_cross_off_the_record(client):
    host = add_host()
    m = meeting_with(host)
    _sql(
        "INSERT INTO meeting_off_record_segments (org_id, meeting_id, started_at, ended_at, "
        "start_ms, end_ms) VALUES (%(o)s, %(m)s, now(), now(), 60000, 120000)",
        {"o": str(host.org_id), "m": str(m)},
    )
    sign_in(host)
    res = make_clip(client, m, 50_000, 70_000)
    assert res.status_code == 409 and res.json()["reason"] == "off_record_overlap"
    assert make_clip(client, m, 20_000, 60_000).status_code == 201
    assert make_clip(client, m, 120_000, 150_000).status_code == 201
    # A segment still running blocks everything after its start.
    _sql(
        "INSERT INTO meeting_off_record_segments (org_id, meeting_id, started_at, start_ms) "
        "VALUES (%(o)s, %(m)s, now(), 300000)",
        {"o": str(host.org_id), "m": str(m)},
    )
    assert make_clip(client, m, 400_000, 410_000).status_code == 409


@pytest.mark.usefixtures("transcript_table")
def test_clip_share_shows_lines_and_video_only_when_chosen(client, monkeypatch):
    host = add_host()
    _rename(host, "maya.patel@acme.com")
    member = add_user(host)
    _rename(member, "sam.lee@acme.com")
    m = meeting_with(host, member)
    line(host, m, str(host.user_id), 9_000, 12_000, "We should raise the price.")
    line(host, m, str(member.user_id), 15_000, 20_000, "Forty dollars works.")
    line(host, m, "spk_9", 22_000, 24_000, "Agreed.")
    line(host, m, str(host.user_id), 50_000, 52_000, "Outside the clip.")
    sign_in(member)
    clip = make_clip(client, m, 10_000, 40_000).json()
    res = client.post(f"/api/v1/clips/{clip['id']}/share-links", json={"expires_in_days": 7})
    assert res.status_code == 201, res.text
    link = res.json()
    assert link["clip_id"] == clip["id"] and link["include_video"] is False
    assert link["url"] == f"https://app.example/s/{link['token']}"

    # The generic share read points the page at the clip route.
    generic = client.get(f"/api/v1/public/shares/{link['token']}")
    assert generic.status_code == 404 and generic.json()["reason"] == "clip_share"

    page = client.get(f"/api/v1/public/clips/{link['token']}", headers={"Authorization": ""})
    assert page.status_code == 200, page.text
    assert page.headers["cache-control"] == "no-store"
    body = page.json()
    shared = body["clip"]
    assert body["view_count"] == 1 and body["shared_by_name"] == "Sam Lee"
    assert shared["title"] == "Pricing call" and shared["duration_ms"] == 30_000
    assert [(ln["speaker_name"], ln["offset_ms"], ln["text"]) for ln in shared["lines"]] == [
        ("Maya Patel", 0, "We should raise the price."),
        ("Sam Lee", 5000, "Forty dollars works."),
        (None, 12000, "Agreed."),
    ]
    assert shared["video"]["status"] == "not_included" and "@" not in page.text

    calls: list[tuple[Any, ...]] = []

    async def fake_video(org_id, meeting_id, clip_id, start_ms, end_ms):
        calls.append((meeting_id, clip_id, start_ms, end_ms))
        return {
            "status": "ready",
            "url": "https://recordings.test/clip.mp4?sig",
            "media_type": "video/mp4",
            "expires_at": "2026-11-11T10:00:00Z",
        }

    monkeypatch.setattr(recording_client, "clip_video", fake_video)
    with_video = client.post(
        f"/api/v1/clips/{clip['id']}/share-links", json={"include_video": True}
    ).json()
    assert with_video["include_video"] is True
    video = client.get(f"/api/v1/public/clips/{with_video['token']}").json()["clip"]["video"]
    assert video["status"] == "ready" and video["url"] == "https://recordings.test/clip.mp4?sig"
    assert (uuid.UUID(str(m)), uuid.UUID(clip["id"]), 10_000, 40_000) in [
        (uuid.UUID(str(a)), uuid.UUID(str(b)), c, d) for a, b, c, d in calls
    ]

    # Clip links are listed under their clip, not with the meeting's recap links.
    assert client.get(f"/api/v1/meetings/{m}/share-links").json()["items"] == []
    [listed] = client.get(f"/api/v1/meetings/{m}/clips").json()["items"]
    assert {ln["include_video"] for ln in listed["share_links"]} == {True, False}
    assert all(ln["can_revoke"] for ln in listed["share_links"])

    # Without meeting-service, the page still shows the words and says the video is unavailable.
    monkeypatch.undo()
    monkeypatch.setenv("SHARE_LINK_SIGNING_KEY", "s" * 40)
    video = client.get(f"/api/v1/public/clips/{with_video['token']}").json()["clip"]["video"]
    assert video["status"] == "unavailable" and video["reason"] == "not_connected"

    # Revoking works like any share link; deleting the clip takes every page down.
    assert client.delete(f"/api/v1/share-links/{link['id']}").status_code == 204
    gone = client.get(f"/api/v1/public/clips/{link['token']}")
    assert gone.status_code == 410 and gone.json()["reason"] == "revoked"
    assert client.delete(f"/api/v1/clips/{clip['id']}").status_code == 204
    assert client.get(f"/api/v1/public/clips/{with_video['token']}").status_code == 404


def test_confidential_clips_are_shared_only_by_the_host(client):
    host = add_host()
    guest = add_user(host)
    m = meeting_with(host, guest, confidential=True)
    sign_in(guest)
    clip = make_clip(client, m).json()
    res = client.post(f"/api/v1/clips/{clip['id']}/share-links")
    assert res.status_code == 403 and res.json()["reason"] == "confidential_host_only"
    sign_in(host)
    res = client.post(f"/api/v1/clips/{clip['id']}/share-links")
    assert res.status_code == 409 and res.json()["reason"] == "confidential_confirmation_required"
    ok = client.post(f"/api/v1/clips/{clip['id']}/share-links", json={"confirm_confidential": True})
    assert ok.status_code == 201


def test_erasure_removes_clips_and_asks_for_the_recording(client, monkeypatch):
    from tests.test_chat_api import seed_tenant

    t = seed_tenant("admin")
    host = add_user(t)
    m = meeting_with(host)
    _sql("UPDATE meetings SET status = 'ended' WHERE id = %(m)s", {"m": str(m)})
    sign_in(host)
    clip = make_clip(client, m).json()
    link = client.post(f"/api/v1/clips/{clip['id']}/share-links").json()
    sign_in(t)
    res = client.post(
        "/api/v1/erasures",
        json={"kind": "meeting", "target_id": str(m), "confirm": "Pricing review"},
    )
    assert res.status_code == 200, res.text
    out = res.json()
    assert out["erased"]["clips"] == 1
    assert out["recording"] == {"status": "not_connected", "objects_deleted": 0}
    assert _sql("SELECT 1 FROM meeting_clips WHERE meeting_id = %(m)s", {"m": str(m)}) == []
    assert client.get(f"/api/v1/public/clips/{link['token']}").status_code == 404

    async def fake_erase(org_id, meeting_id):
        return "erased", 3

    monkeypatch.setattr(recording_client, "erase", fake_erase)
    again = client.post(
        "/api/v1/erasures",
        json={"kind": "meeting", "target_id": str(m), "confirm": "Pricing review"},
    ).json()
    assert again["recording"] == {"status": "erased", "objects_deleted": 3}
    assert again["erased"]["recording_objects"] == 3


def test_person_erasure_deletes_their_clips(client):
    from tests.test_chat_api import seed_tenant

    t = seed_tenant("admin")
    person = add_user(t)
    _rename(person, "dana@example.com")
    m = meeting_with(person)
    sign_in(person)
    make_clip(client, m)
    sign_in(t)
    res = client.post(
        "/api/v1/erasures",
        json={"kind": "person", "target_id": str(person.user_id), "confirm": "dana@example.com"},
    )
    assert res.status_code == 200, res.text
    assert res.json()["erased"]["clips"] == 1 and res.json()["recording"] is None


def test_off_the_record_pauses_and_resumes_the_recording(client, monkeypatch):
    from tests.test_off_record import FakeRedis

    monkeypatch.setattr(off_record, "get_redis", lambda: FakeRedis())
    calls: list[str] = []

    async def pause(org_id, meeting_id):
        calls.append(f"pause:{meeting_id}")

    async def resume(org_id, meeting_id):
        calls.append(f"resume:{meeting_id}")

    monkeypatch.setattr(recording_client, "pause", pause)
    monkeypatch.setattr(recording_client, "resume", resume)
    host = add_host()
    m = meeting_with(host)
    _sql("UPDATE meetings SET status = 'live' WHERE id = %(m)s", {"m": str(m)})
    sign_in(host)
    assert client.post(f"/api/v1/meetings/{m}/off-record").status_code == 200
    assert client.post(f"/api/v1/meetings/{m}/off-record").status_code == 200  # idempotent
    assert client.post(f"/api/v1/meetings/{m}/off-record/end").status_code == 200
    assert calls == [f"pause:{m}", f"resume:{m}"]
