"""core-api's internal retrieval scope for rag-service (mytheus-docs ADR-0033 D1, D3).

Real Postgres as `mytheus_app`; rag-service is authenticated with a test secret.
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
from services.core_api.routes import internal_meetings, internal_retrieval
from tests.conftest import APP_URL, BOOTSTRAP_URL

pytestmark = pytest.mark.usefixtures("migrated_db")

RAG_SECRET = "r" * 40
MEETING_SECRET = "m" * 40


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
    os.environ[internal_meetings.TOKENS_ENV] = (
        f"meeting-service={MEETING_SECRET},rag-service={RAG_SECRET}"
    )
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = create_app("core-api-retrieval-test")
    app.include_router(internal_retrieval.router)
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


def _org() -> uuid.UUID:
    org_id = uuid.uuid4()
    _sql(
        "INSERT INTO organizations (id, name, slug, clerk_org_id) VALUES (%(o)s,'O',%(s)s,%(c)s)",
        {"o": str(org_id), "s": f"rt-{org_id}", "c": f"org_{org_id.hex[:16]}"},
    )
    return org_id


def _user(org_id: uuid.UUID, email: str) -> uuid.UUID:
    uid = uuid.uuid4()
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) VALUES (%(u)s,%(o)s,%(c)s,%(e)s)",
        {"u": str(uid), "o": str(org_id), "c": f"user_{uid.hex[:12]}", "e": email},
    )
    return uid


def _meeting(org_id: uuid.UUID, title: str, *participants: uuid.UUID) -> uuid.UUID:
    mid = uuid.uuid4()
    _sql(
        "INSERT INTO meetings (id, org_id, title) VALUES (%(m)s, %(o)s, %(t)s)",
        {"m": str(mid), "o": str(org_id), "t": title},
    )
    for uid in participants:
        _sql(
            "INSERT INTO meeting_participants (meeting_id, user_id, org_id) "
            "VALUES (%(m)s, %(u)s, %(o)s)",
            {"m": str(mid), "u": str(uid), "o": str(org_id)},
        )
    return mid


def _decision(org_id: uuid.UUID, meeting_id: uuid.UUID, content: str, status: str) -> uuid.UUID:
    did = uuid.uuid4()
    _sql(
        "INSERT INTO decisions (id, org_id, meeting_id, content, confidence, status) "
        "VALUES (%(d)s, %(o)s, %(m)s, %(c)s, 0.9, %(s)s)",
        {"d": str(did), "o": str(org_id), "m": str(meeting_id), "c": content, "s": status},
    )
    return did


def _action(org_id: uuid.UUID, meeting_id: uuid.UUID, content: str, status: str) -> uuid.UUID:
    aid = uuid.uuid4()
    _sql(
        "INSERT INTO action_items (id, org_id, meeting_id, content, confidence, status) "
        "VALUES (%(a)s, %(o)s, %(m)s, %(c)s, 0.9, %(s)s)",
        {"a": str(aid), "o": str(org_id), "m": str(meeting_id), "c": content, "s": status},
    )
    return aid


def _headers(org_id: uuid.UUID, service: str = "rag-service", secret: str = RAG_SECRET):
    return {
        "X-Mytheus-Service": service,
        "X-Mytheus-Service-Token": secret,
        "X-Mytheus-Org": str(org_id),
    }


def test_scope_lists_only_the_askers_meetings(client) -> None:
    org = _org()
    priya, sam = _user(org, "priya@example.com"), _user(org, "sam@example.com")
    shared = _meeting(org, "Pricing review", priya, sam)
    private = _meeting(org, "Sam 1:1", sam)

    r = client.get(
        "/internal/retrieval/scope", params={"user_id": str(priya)}, headers=_headers(org)
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["user_id"] == str(priya)
    assert [m["id"] for m in body["meetings"]] == [str(shared)]
    assert body["meetings"][0]["title"] == "Pricing review"
    assert {p["email"] for p in body["people"]} == {"priya@example.com", "sam@example.com"}
    assert str(private) not in r.text


def test_scope_resolves_a_clerk_user(client) -> None:
    org = _org()
    uid = _user(org, "a@example.com")
    clerk = _sql("SELECT clerk_user_id FROM users WHERE id = %(u)s", {"u": str(uid)})[0]
    r = client.get(
        "/internal/retrieval/scope",
        params={"clerk_user_id": clerk["clerk_user_id"]},
        headers=_headers(org),
    )
    assert r.status_code == 200
    assert r.json()["user_id"] == str(uid)


def test_scope_needs_exactly_one_identity(client) -> None:
    org = _org()
    uid = _user(org, "b@example.com")
    assert client.get("/internal/retrieval/scope", headers=_headers(org)).status_code == 422
    both = {"user_id": str(uid), "clerk_user_id": "user_x"}
    r = client.get("/internal/retrieval/scope", params=both, headers=_headers(org))
    assert r.status_code == 422


def test_another_tenants_user_is_not_found(client) -> None:
    org_a, org_b = _org(), _org()
    uid_b = _user(org_b, "b@example.com")
    r = client.get(
        "/internal/retrieval/scope", params={"user_id": str(uid_b)}, headers=_headers(org_a)
    )
    assert r.status_code == 404


def test_records_return_only_confirmed_rows_in_the_askers_meetings(client) -> None:
    org = _org()
    priya, sam = _user(org, "p@example.com"), _user(org, "s@example.com")
    shared = _meeting(org, "Launch", priya, sam)
    private = _meeting(org, "Sam only", sam)
    confirmed = _decision(org, shared, "Ship on the 2nd", "confirmed")
    proposed = _decision(org, shared, "Maybe Friday", "proposed")
    hidden = _decision(org, private, "Sam's plan", "confirmed")
    done = _action(org, shared, "Write the release notes", "done")
    dismissed = _action(org, shared, "Ignore this", "dismissed")

    r = client.post(
        "/internal/retrieval/records",
        json={
            "user_id": str(priya),
            "decision_ids": [str(confirmed), str(proposed), str(hidden)],
            "action_ids": [str(done), str(dismissed)],
        },
        headers=_headers(org),
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert [d["id"] for d in body["decisions"]] == [str(confirmed)]
    assert body["decisions"][0]["content"] == "Ship on the 2nd"
    assert [a["id"] for a in body["actions"]] == [str(done)]


def test_wrong_secret_or_wrong_caller_is_refused(client) -> None:
    org = _org()
    uid = _user(org, "c@example.com")
    params = {"user_id": str(uid)}
    bad = client.get(
        "/internal/retrieval/scope", params=params, headers=_headers(org, secret="x" * 40)
    )
    assert bad.status_code == 401
    # meeting-service holds a valid core-api secret but is not allowed on retrieval routes.
    other = client.get(
        "/internal/retrieval/scope",
        params=params,
        headers=_headers(org, "meeting-service", MEETING_SECRET),
    )
    assert other.status_code == 401


def test_rag_service_cannot_report_room_state(client) -> None:
    org = _org()
    mid = _meeting(org, "M")
    r = client.post(
        f"/internal/meetings/{mid}/room-state", json={"state": "live"}, headers=_headers(org)
    )
    assert r.status_code == 401
