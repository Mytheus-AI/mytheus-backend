"""Off the record (`core-api` `routes/off_record.py`) and the consent receipt (`meeting-service`
`GET /api/meetings/{id}/consent-receipt`), against a real Postgres.

Off the record:
- only the host or the creator starts and ends it (403 for another participant, 403
  NOT_A_PARTICIPANT for a non-participant, 404 across tenants);
- starting writes one segment, sets the Redis flag `nexus:{org}:off-record:{meeting}` and
  broadcasts `{"type": "off-record"}` on the meeting's fan-out channel; a second start answers
  the running segment and opens no other;
- `requested_by` must be a participant; the flag write failing is a 503 with nothing stored;
- ending closes the segment (duration, end_ms), deletes the flag, broadcasts; ending the meeting
  closes a running segment too;
- GET shows every segment and whether the caller may toggle; rows are isolated by RLS.

Consent receipt: the caller's own row only, with the clauses of the version they agreed to.
"""

from __future__ import annotations

import json
import os
import uuid
from typing import Any

import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.core_api.routes import meetings as meetings_routes
from services.core_api.routes import off_record
from services.meeting_service import routes as meeting_routes
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


class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, tuple[str, int | None]] = {}
        self.published: list[tuple[str, dict[str, Any]]] = []
        self.fail = False

    async def set(self, key: str, value: str, ex: int | None = None) -> bool:
        if self.fail:
            raise ConnectionError("redis down")
        self.store[key] = (value, ex)
        return True

    async def delete(self, key: str) -> int:
        if self.fail:
            raise ConnectionError("redis down")
        return 1 if self.store.pop(key, None) is not None else 0

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, json.loads(message)))
        return 1


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-off-record-test")
    app.include_router(off_record.router)
    app.include_router(meetings_routes.router)
    app.include_router(meeting_routes.router)
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
def redis(monkeypatch) -> FakeRedis:
    fake = FakeRedis()
    monkeypatch.setattr(off_record, "get_redis", lambda: fake)
    return fake


def meeting(*, status: str = "live") -> tuple[Tenant, Tenant, uuid.UUID]:
    host = seed_tenant()
    member = add_user(host)
    m = seed_meeting(host.org_id)
    _sql(
        "UPDATE meetings SET title = 'Pricing review', status = %(s)s WHERE id = %(m)s",
        {"s": status, "m": str(m)},
    )
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id, role_in_meeting) "
        "VALUES (%(m)s, %(u)s, %(o)s, 'host')",
        {"m": str(m), "u": str(host.user_id), "o": str(host.org_id)},
    )
    add_participant(m, host.org_id, member.user_id)
    return host, member, m


def segments(m: uuid.UUID) -> list[dict[str, Any]]:
    return _sql(
        "SELECT started_by, requested_by, ended_at, ended_by FROM meeting_off_record_segments "
        "WHERE meeting_id = %(m)s ORDER BY started_at",
        {"m": str(m)},
    )


def test_host_starts_sets_flag_and_broadcasts(client, redis):
    host, member, m = meeting()
    sign_in(host)
    res = client.post(
        f"/api/v1/meetings/{m}/off-record", json={"requested_by": str(member.user_id)}
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["active"] is True and body["can_toggle"] is True
    assert body["current"]["requested_by"] == str(member.user_id)
    key = f"nexus:{host.org_id}:off-record:{m}"
    value, ttl = redis.store[key]
    assert json.loads(value)["segment_id"] == body["current"]["id"]
    assert ttl == off_record.FLAG_TTL_S
    channel, message = redis.published[-1]
    assert channel == f"nexus:{host.org_id}:pubsub:meeting:{m}"
    assert message["type"] == "off-record" and message["meetingId"] == str(m)
    assert message["payload"]["active"] is True
    assert message["payload"]["requestedBy"] == str(member.user_id)

    again = client.post(f"/api/v1/meetings/{m}/off-record")
    assert again.status_code == 200
    assert again.json()["current"]["id"] == body["current"]["id"]
    assert len(segments(m)) == 1
    assert len(redis.published) == 1  # nothing new happened, nothing announced
    audit = _sql(
        "SELECT action FROM audit_logs WHERE target_id = %(m)s AND action LIKE 'meeting.off%%'",
        {"m": str(m)},
    )
    assert [a["action"] for a in audit] == ["meeting.off_record_started"]


def test_end_closes_clears_and_broadcasts(client, redis):
    host, _member, m = meeting()
    sign_in(host)
    assert client.post(f"/api/v1/meetings/{m}/off-record").status_code == 200
    res = client.post(f"/api/v1/meetings/{m}/off-record/end")
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["active"] is False and body["current"] is None
    assert len(body["segments"]) == 1
    seg = body["segments"][0]
    assert seg["ended_at"] is not None and seg["ended_by"] == str(host.user_id)
    assert seg["duration_ms"] is not None and seg["duration_ms"] >= 0
    assert f"nexus:{host.org_id}:off-record:{m}" not in redis.store
    assert redis.published[-1][1]["payload"]["active"] is False
    # ending again is a no-op
    assert client.post(f"/api/v1/meetings/{m}/off-record/end").json()["active"] is False
    assert len(redis.published) == 2


def test_only_host_or_creator_toggles(client, redis):
    host, member, m = meeting()
    sign_in(member)
    res = client.post(f"/api/v1/meetings/{m}/off-record")
    assert res.status_code == 403 and res.json()["code"] == "FORBIDDEN_ROLE"
    assert client.post(f"/api/v1/meetings/{m}/off-record/end").status_code == 403
    state = client.get(f"/api/v1/meetings/{m}/off-record").json()
    assert state["can_toggle"] is False and state["active"] is False
    assert redis.store == {}
    # the creator who is not the host steers too
    _sql(
        "UPDATE meetings SET created_by = %(u)s WHERE id = %(m)s",
        {"u": str(member.user_id), "m": str(m)},
    )
    assert client.post(f"/api/v1/meetings/{m}/off-record").status_code == 200


def test_non_participant_and_other_tenant(client, redis):
    host, _member, m = meeting()
    outsider = add_user(host)
    sign_in(outsider)
    res = client.get(f"/api/v1/meetings/{m}/off-record")
    assert res.status_code == 403 and res.json()["code"] == "NOT_A_PARTICIPANT"
    other = seed_tenant()
    sign_in(other)
    assert client.get(f"/api/v1/meetings/{m}/off-record").status_code == 404
    assert client.post(f"/api/v1/meetings/{m}/off-record").status_code == 404


def test_requested_by_must_be_a_participant(client, redis):
    host, _member, m = meeting()
    sign_in(host)
    res = client.post(f"/api/v1/meetings/{m}/off-record", json={"requested_by": str(uuid.uuid4())})
    assert res.status_code == 422
    assert res.json()["errors"][0]["loc"] == ["body", "requested_by"]
    assert segments(m) == []


def test_flag_write_failure_stores_nothing(client, redis):
    host, _member, m = meeting()
    sign_in(host)
    redis.fail = True
    res = client.post(f"/api/v1/meetings/{m}/off-record")
    assert res.status_code == 503 and res.json()["code"] == "DEPENDENCY_UNAVAILABLE"
    assert segments(m) == []
    assert redis.published == []


def test_ended_meeting_cannot_go_off_record(client, redis):
    host, _member, m = meeting(status="ended")
    sign_in(host)
    assert client.post(f"/api/v1/meetings/{m}/off-record").status_code == 409


def test_ending_the_meeting_closes_a_running_segment(client, redis):
    host, _member, m = meeting()
    sign_in(host)
    assert client.post(f"/api/v1/meetings/{m}/off-record").status_code == 200
    res = client.patch(f"/api/v1/meetings/{m}", json={"status": "ended"})
    assert res.status_code == 200, res.text
    rows = segments(m)
    assert len(rows) == 1 and rows[0]["ended_at"] is not None and rows[0]["ended_by"] is None
    assert f"nexus:{host.org_id}:off-record:{m}" not in redis.store


def test_segments_are_isolated_by_rls(client, redis):
    host, _member, m = meeting()
    sign_in(host)
    assert client.post(f"/api/v1/meetings/{m}/off-record").status_code == 200
    import psycopg2

    conn = psycopg2.connect(APP_URL.replace("+asyncpg", ""))
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM meeting_off_record_segments")  # no tenant: none
            assert cur.fetchone()[0] == 0
            cur.execute(
                "SELECT set_config('app.current_org', %s, false)", (str(seed_tenant().org_id),)
            )
            cur.execute(
                "SELECT count(*) FROM meeting_off_record_segments WHERE meeting_id = %s", (str(m),)
            )
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT set_config('app.current_org', %s, false)", (str(host.org_id),))
            cur.execute(
                "SELECT count(*) FROM meeting_off_record_segments WHERE meeting_id = %s", (str(m),)
            )
            assert cur.fetchone()[0] == 1
    finally:
        conn.close()


# --- consent receipt ----------------------------------------------------------------------


def test_receipt_shows_own_agreement_with_its_clauses(client, monkeypatch):
    monkeypatch.delenv("RECORDING_ENABLED", raising=False)
    host, member, m = meeting()
    _sql(
        "UPDATE meeting_participants SET consented_at = now(), "
        "consent_version = 'room-notice-2026-11-11.2' WHERE meeting_id = %(m)s AND user_id = %(u)s",
        {"m": str(m), "u": str(member.user_id)},
    )
    sign_in(member)
    res = client.get(f"/api/meetings/{m}/consent-receipt")
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["consented"] is True and body["consent_version"] == "room-notice-2026-11-11.2"
    assert body["meeting_title"] == "Pricing review"
    keys = [c["key"] for c in body["clauses"]]
    assert keys[0] == "transcript" and "off_record" in keys and keys[-1] == "agreement"
    av = next(c for c in body["clauses"] if c["key"] == "audio_video")
    assert av["title"] == "Audio and video are not recorded"
    assert body["current_version"] == "room-notice-2026-11-12"

    # the host's own row: not agreed yet, so no clauses to show
    sign_in(host)
    mine = client.get(f"/api/meetings/{m}/consent-receipt").json()
    assert mine["consented"] is False and mine["clauses"] is None


def test_receipt_older_version_and_access(client):
    host, member, m = meeting()
    _sql(
        "UPDATE meeting_participants SET consented_at = now(), "
        "consent_version = 'room-notice-2026-11-11' WHERE meeting_id = %(m)s AND user_id = %(u)s",
        {"m": str(m), "u": str(member.user_id)},
    )
    sign_in(member)
    body = client.get(f"/api/meetings/{m}/consent-receipt").json()
    assert "off_record" not in [c["key"] for c in body["clauses"]]  # the words they saw
    outsider = add_user(host)
    sign_in(outsider)
    assert client.get(f"/api/meetings/{m}/consent-receipt").status_code == 403
    sign_in(seed_tenant())
    assert client.get(f"/api/meetings/{m}/consent-receipt").status_code == 404


def test_myte_notices_keep_older_words_verbatim():
    """ADR-0042: the 2026-11-12 notices say "Myte"; receipts for older versions keep the words
    that were on screen."""
    from services.meeting_service.consent_notices import CURRENT_VERSION, clauses_for

    def notes(version: str) -> str:
        found = clauses_for(version, False)
        assert found is not None
        return next(c.title for c in found if c.key == "agent_notes")

    assert notes("room-notice-2026-11-11.2") == "Your agent's notes are yours"
    assert notes("room-notice-2026-11-12") == "Your Myte's notes are yours"
    assert notes("room-notice-2026-11-12-mobile") == "Your Myte's notes are yours"
    assert CURRENT_VERSION == "room-notice-2026-11-12"
    web = clauses_for("room-notice-2026-11-12", False) or []
    assert "every Myte pauses" in next(c.body for c in web if c.key == "off_record")
    mobile = clauses_for("room-notice-2026-11-12-mobile", False) or []
    assert "off_record" not in [c.key for c in mobile]
