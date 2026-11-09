"""core-api's internal room-state route: meeting-service reports a call live or ended.

Real Postgres as `mytheus_app`; the caller is authenticated with a test secret.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import psycopg2
import psycopg2.extras
import pytest
from starlette.testclient import TestClient

from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.core_api.routes import internal_meetings
from tests.conftest import APP_URL, BOOTSTRAP_URL

pytestmark = pytest.mark.usefixtures("migrated_db")

SECRET = "s" * 40


def _sql(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    conn = psycopg2.connect(BOOTSTRAP_URL.replace("+asyncpg", ""))
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or {})
            return [dict(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


@pytest.fixture(scope="module")
def client():
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ[internal_meetings.TOKENS_ENV] = f"meeting-service={SECRET}"
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = create_app("core-api-internal-test")
    app.include_router(internal_meetings.router)
    with TestClient(app) as c:
        yield c
    os.environ.pop(internal_meetings.TOKENS_ENV, None)
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


def _meeting(status: str = "scheduled") -> tuple[uuid.UUID, uuid.UUID]:
    org_id, meeting_id = uuid.uuid4(), uuid.uuid4()
    _sql(
        "INSERT INTO organizations (id, name, slug, clerk_org_id) VALUES (%(o)s,'O',%(s)s,%(c)s)",
        {"o": str(org_id), "s": f"rs-{org_id}", "c": f"org_{org_id.hex[:16]}"},
    )
    _sql(
        "INSERT INTO meetings (id, org_id, title, status) VALUES (%(m)s, %(o)s, 'M', %(st)s)",
        {"m": str(meeting_id), "o": str(org_id), "st": status},
    )
    return org_id, meeting_id


def _post(client: TestClient, org_id: uuid.UUID, meeting_id: uuid.UUID, state: str, **h: str):
    headers = {
        "X-Mytheus-Service": "meeting-service",
        "X-Mytheus-Service-Token": SECRET,
        "X-Mytheus-Org": str(org_id),
        **h,
    }
    return client.post(
        f"/internal/meetings/{meeting_id}/room-state", json={"state": state}, headers=headers
    )


def _status(meeting_id: uuid.UUID) -> str:
    return _sql("SELECT status FROM meetings WHERE id = %(m)s", {"m": str(meeting_id)})[0]["status"]


def test_a_started_call_makes_the_meeting_live(client) -> None:
    org_id, meeting_id = _meeting()
    r = _post(client, org_id, meeting_id, "live")
    assert r.status_code == 200, r.text
    assert r.json()["changed"] is True
    assert _status(meeting_id) == "live"


def test_a_closed_room_ends_the_meeting_with_its_event_and_audit(client) -> None:
    org_id, meeting_id = _meeting("live")
    r = _post(client, org_id, meeting_id, "ended")
    assert r.status_code == 200
    assert _status(meeting_id) == "ended"
    events = _sql(
        "SELECT payload FROM outbox WHERE org_id = %(o)s AND topic = 'meeting-events'",
        {"o": str(org_id)},
    )
    assert [e["payload"]["event_type"] for e in events] == ["ended"]
    assert _sql(
        "SELECT actor_type FROM audit_logs WHERE target_id = %(m)s", {"m": str(meeting_id)}
    ) == [{"actor_type": "system"}]


def test_an_ended_meeting_is_never_revived(client) -> None:
    org_id, meeting_id = _meeting("ended")
    r = _post(client, org_id, meeting_id, "live")
    assert r.status_code == 200
    assert r.json() == {"meeting_id": str(meeting_id), "status": "ended", "changed": False}
    assert _status(meeting_id) == "ended"


def test_wrong_secret_or_unknown_caller_is_refused(client) -> None:
    org_id, meeting_id = _meeting()
    assert (
        _post(
            client, org_id, meeting_id, "live", **{"X-Mytheus-Service-Token": "x" * 40}
        ).status_code
        == 401
    )
    assert (
        _post(
            client, org_id, meeting_id, "live", **{"X-Mytheus-Service": "rag-service"}
        ).status_code
        == 401
    )
    assert _status(meeting_id) == "scheduled"


def test_another_tenants_meeting_is_404(client) -> None:
    org_a, _ = _meeting()
    _, meeting_b = _meeting()
    assert _post(client, org_a, meeting_b, "ended").status_code == 404
    assert _status(meeting_b) == "scheduled"
