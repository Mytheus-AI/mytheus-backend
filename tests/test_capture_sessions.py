"""In-person capture (`core-api` `/api/v1/capture/*`, wave 3), against a real Postgres.

Covered: not connected (no secret / no URL / an http URL) is 503 `capture_not_configured` and
`GET /status` says so; consent is required (422 `CONSENT_REQUIRED`, nothing created); a start
creates a live `in_person` meeting hosted by the caller, audits `capture.started` with the
consent time, emits `meeting-events` `created`, and returns a token transcription-service's
verifier accepts (same claims and HS256 signature as mytheus-ui's `signCaptureToken`, 15
minutes); refresh is the participant's only and refuses an ended meeting; end marks the meeting
ended with its real start and end, idempotently; another tenant's meeting and a Mytheus room are
404; the gateway routes `/api/v1/capture` to core-api.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.api_gateway.edge import route_for
from services.core_api.routes import capture
from tests.conftest import APP_URL
from tests.test_chat_api import (
    PRINCIPAL,
    _create_test_app,
    _sql,
    seed_meeting,
    seed_tenant,
    sign_in,
)

pytestmark = pytest.mark.usefixtures("migrated_db")

SECRET = "s" * 40
INGEST = "wss://api.example.test/capture/audio"


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-capture-test")
    app.include_router(capture.router)
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


@pytest.fixture
def configured(monkeypatch):
    monkeypatch.setenv("CAPTURE_TOKEN_SECRET", SECRET)
    monkeypatch.setenv("CAPTURE_INGEST_URL", INGEST)


def _b64d(part: str) -> bytes:
    return base64.urlsafe_b64decode(part + "=" * (-len(part) % 4))


def verify(token: str, secret: str = SECRET) -> dict[str, Any]:
    """transcription-service's checks (`capture_auth.verify_capture_token`), minus `src`."""
    header, body, sig = token.split(".")
    assert json.loads(_b64d(header)) == {"alg": "HS256", "typ": "JWT"}
    expected = hmac.new(secret.encode(), f"{header}.{body}".encode(), hashlib.sha256).digest()
    assert hmac.compare_digest(expected, _b64d(sig))
    claims: dict[str, Any] = json.loads(_b64d(body))
    assert claims["typ"] == "capture"
    assert claims["iss"] == "mytheus-ui"
    assert claims["aud"] == "transcription-service"
    assert claims["exp"] - claims["iat"] == 15 * 60
    return claims


def test_the_gateway_routes_capture_to_core_api():
    assert route_for("/api/v1/capture/sessions") == "core-api"


@pytest.mark.parametrize(
    "env",
    [
        {},
        {"CAPTURE_TOKEN_SECRET": "short", "CAPTURE_INGEST_URL": INGEST},
        {"CAPTURE_TOKEN_SECRET": SECRET},
        {"CAPTURE_TOKEN_SECRET": SECRET, "CAPTURE_INGEST_URL": "http://api.example.test/x"},
    ],
)
def test_not_connected_without_a_secret_and_a_wss_url(client, monkeypatch, env):
    monkeypatch.delenv("CAPTURE_TOKEN_SECRET", raising=False)
    monkeypatch.delenv("CAPTURE_INGEST_URL", raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    me = seed_tenant()
    sign_in(me)
    assert client.get("/api/v1/capture/status").json() == {"configured": False}
    res = client.post("/api/v1/capture/sessions", json={"consent": True})
    assert res.status_code == 503
    assert res.json()["reason"] == "capture_not_configured"
    assert _sql("SELECT 1 FROM meetings WHERE org_id = %(o)s", {"o": str(me.org_id)}) == []


def test_consent_is_required(client, configured):
    me = seed_tenant()
    sign_in(me)
    assert client.get("/api/v1/capture/status").json() == {"configured": True}
    res = client.post("/api/v1/capture/sessions", json={"consent": False})
    assert res.status_code == 422 and res.json()["code"] == "CONSENT_REQUIRED"
    old = (datetime.now(UTC) - timedelta(days=2)).isoformat()
    res = client.post("/api/v1/capture/sessions", json={"consent": True, "consent_at": old})
    assert res.status_code == 422
    assert _sql("SELECT 1 FROM meetings WHERE org_id = %(o)s", {"o": str(me.org_id)}) == []


def test_start_creates_a_live_in_person_meeting_and_a_valid_token(client, configured):
    me = seed_tenant()
    sign_in(me)
    res = client.post(
        "/api/v1/capture/sessions", json={"consent": True, "title": "  Design\u0007 review  "}
    )
    assert res.status_code == 201, res.text
    assert res.headers["cache-control"] == "no-store"
    out = res.json()
    assert out["title"] == "Design review"
    assert out["ingest_url"] == INGEST
    assert (out["sample_rate"], out["encoding"], out["source"]) == (16000, "pcm_s16le", "in_person")
    claims = verify(out["capture_token"])
    assert claims["sub"] == str(me.user_id)
    assert claims["org"] == str(me.org_id)
    assert claims["mid"] == out["meeting_id"]
    assert claims["src"] == "in_person"
    assert claims["consent_at"] == out["consent_at"]
    assert claims["exp"] == out["expires_at"]

    meeting = _sql(
        "SELECT status, source, created_by FROM meetings WHERE id = %(m)s",
        {"m": out["meeting_id"]},
    )
    assert meeting == [{"status": "live", "source": "in_person", "created_by": str(me.user_id)}]
    host = _sql(
        "SELECT role_in_meeting FROM meeting_participants WHERE meeting_id = %(m)s "
        "AND user_id = %(u)s",
        {"m": out["meeting_id"], "u": str(me.user_id)},
    )
    assert host == [{"role_in_meeting": "host"}]
    audits = _sql(
        "SELECT metadata FROM audit_logs WHERE action = 'capture.started' AND target_id = %(m)s",
        {"m": out["meeting_id"]},
    )
    assert audits[0]["metadata"]["consent_at"] == out["consent_at"]
    events = _sql(
        "SELECT payload FROM outbox WHERE org_id = %(o)s AND topic = 'meeting-events'",
        {"o": str(me.org_id)},
    )
    assert sorted(e["payload"]["event_type"] for e in events) == ["created", "updated"]


def test_refresh_and_end(client, configured):
    me = seed_tenant()
    sign_in(me)
    started = client.post("/api/v1/capture/sessions", json={"consent": True}).json()
    mid = started["meeting_id"]
    assert started["title"] == "In-person meeting"
    refreshed = client.post(
        f"/api/v1/capture/sessions/{mid}/refresh", json={"consent_at": started["consent_at"]}
    )
    assert refreshed.status_code == 200, refreshed.text
    assert verify(refreshed.json()["capture_token"])["mid"] == mid

    ended = client.post(f"/api/v1/capture/sessions/{mid}/end")
    assert ended.status_code == 200 and ended.json() == {"meeting_id": mid, "status": "ended"}
    row = _sql(
        "SELECT status, scheduled_start, scheduled_end FROM meetings WHERE id = %(m)s", {"m": mid}
    )[0]
    assert row["status"] == "ended"
    assert row["scheduled_start"] < row["scheduled_end"]
    assert client.post(f"/api/v1/capture/sessions/{mid}/end").status_code == 200
    late = client.post(
        f"/api/v1/capture/sessions/{mid}/refresh", json={"consent_at": started["consent_at"]}
    )
    assert late.status_code == 409


def test_other_meetings_are_not_found(client, configured):
    me = seed_tenant()
    other = seed_tenant()
    sign_in(other)
    theirs = client.post("/api/v1/capture/sessions", json={"consent": True}).json()["meeting_id"]
    sign_in(me)
    room = seed_meeting(me.org_id)
    now = datetime.now(UTC).isoformat()
    for mid in (theirs, str(room), str(uuid.uuid4())):
        res = client.post(f"/api/v1/capture/sessions/{mid}/refresh", json={"consent_at": now})
        assert res.status_code == 404
        assert client.post(f"/api/v1/capture/sessions/{mid}/end").status_code == 404


def test_meetings_publish_in_person_without_widening_the_source_enum():
    """The published `source` enum is frozen (oasdiff): an in-person meeting reads as `mytheus`
    with `recorded_in_person: true`; every other source reads as before."""
    from services.core_api.schemas import Meeting

    now = datetime.now(UTC)
    row = {
        "id": uuid.uuid4(),
        "title": "Standup",
        "status": "live",
        "scheduled_start": None,
        "scheduled_end": None,
        "livekit_room": None,
        "created_by": None,
        "created_at": now,
        "updated_at": now,
    }
    in_person = Meeting(**row, source="in_person")
    assert (in_person.source, in_person.recorded_in_person) == ("mytheus", True)
    for source in ("mytheus", "meet", "teams", "zoom"):
        m = Meeting(**row, source=source)
        assert (m.source, m.recorded_in_person) == (source, False)
    schema = Meeting.model_json_schema(mode="serialization")["properties"]["source"]
    assert schema["enum"] == ["mytheus", "meet", "teams", "zoom"]
