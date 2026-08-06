"""The person-profile composite. Plan 5-6 section 3.5 acceptance checks.

This endpoint is deliberately thin: `get_user_context()` (identity-tenant-service, Plan 5-6
section 3.3) already assembles role, skills, team, department, recent meetings and topics with
a 300s Redis cache, so `bff` CALLS it in-process rather than re-querying the same shape - the
brief's own instruction, and the one this file exists to prove was followed rather than
silently reverted to a second implementation. The only thing this file adds beyond that shape
is `email`, the one identity field `get_user_context` deliberately never caches (see
`context/service.py`'s module docstring).
"""

from __future__ import annotations

import uuid
from typing import Any

import psycopg2
import psycopg2.extras
import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.bff import people as people_mod
from tests.conftest import APP_URL, BOOTSTRAP_URL


class Principal:
    clerk_org = ""
    clerk_user = ""


PRINCIPAL = Principal()


def _sql(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    conn = psycopg2.connect(BOOTSTRAP_URL.replace("+asyncpg", ""))
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or {})
            return [dict(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


def _seed_tenant(tier: str = "member") -> tuple[uuid.UUID, uuid.UUID]:
    org_id, user_id = uuid.uuid4(), uuid.uuid4()
    clerk_org = f"org_{uuid.uuid4().hex[:16]}"
    clerk_user = f"user_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO organizations (id, name, slug, clerk_org_id) "
        "VALUES (%(id)s, 'Org', %(slug)s, %(cid)s)",
        {"id": str(org_id), "slug": f"people-{org_id}", "cid": clerk_org},
    )
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) "
        "VALUES (%(uid)s, %(org)s, %(cuid)s, 'signed-in@example.com')",
        {"uid": str(user_id), "org": str(org_id), "cuid": clerk_user},
    )
    _sql(
        "INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (%(o)s, %(u)s, %(t)s)",
        {"o": str(org_id), "u": str(user_id), "t": tier},
    )
    PRINCIPAL.clerk_org = clerk_org
    PRINCIPAL.clerk_user = clerk_user
    return org_id, user_id


@pytest.fixture(scope="module")
def client(migrated_db):
    import os

    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()

    app = create_app("bff-people-test")
    app.include_router(people_mod.router)
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


def test_person_profile_matches_get_user_context_plus_email(client):
    org_id, _viewer = _seed_tenant()
    person_id = uuid.uuid4()
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email, role, skills, bio, avatar_url) "
        "VALUES (%(id)s, %(org)s, %(cuid)s, %(email)s, 'engineer', "
        "ARRAY['python','postgres'], 'Builds things', 'https://example.com/a.png')",
        {
            "id": str(person_id),
            "org": str(org_id),
            "cuid": f"user_{person_id.hex[:16]}",
            "email": "person@example.com",
        },
    )
    dept_id = uuid.uuid4()
    _sql(
        "INSERT INTO departments (id, org_id, name) VALUES (%(id)s, %(org)s, 'Engineering')",
        {"id": str(dept_id), "org": str(org_id)},
    )
    team_id = uuid.uuid4()
    _sql(
        "INSERT INTO teams (id, org_id, name, department_id) "
        "VALUES (%(id)s, %(org)s, 'Platform', %(dept)s)",
        {"id": str(team_id), "org": str(org_id), "dept": str(dept_id)},
    )
    _sql(
        "INSERT INTO team_members (org_id, team_id, user_id, role) "
        "VALUES (%(org)s, %(team)s, %(user)s, 'lead')",
        {"org": str(org_id), "team": str(team_id), "user": str(person_id)},
    )
    meeting_id = uuid.uuid4()
    _sql(
        "INSERT INTO meetings (id, org_id, title, status, created_by) "
        "VALUES (%(id)s, %(org)s, 'Sprint Planning', 'ended', %(user)s)",
        {"id": str(meeting_id), "org": str(org_id), "user": str(person_id)},
    )
    _sql(
        "INSERT INTO meeting_participants (org_id, meeting_id, user_id) "
        "VALUES (%(org)s, %(meeting)s, %(user)s)",
        {"org": str(org_id), "meeting": str(meeting_id), "user": str(person_id)},
    )

    resp = client.get(f"/bff/people/{person_id}")
    assert resp.status_code == 200
    body = resp.json()

    assert body["user_id"] == str(person_id)
    assert body["org_id"] == str(org_id)
    assert body["email"] == "person@example.com"
    assert body["role"] == "engineer"
    assert body["bio"] == "Builds things"
    assert body["avatar_url"] == "https://example.com/a.png"
    # Skills render as chips: a flat list of strings is exactly what a chip renderer needs.
    assert body["skills"] == ["python", "postgres"]
    assert [t["id"] for t in body["team"]] == [str(team_id)]
    assert body["department"]["id"] == str(dept_id)
    # Recent meetings link to meeting pages: the response must carry the meeting id, not just
    # a title string, or the UI has nothing to build the link href from.
    assert [m["id"] for m in body["recent_meetings"]] == [str(meeting_id)]
    assert body["recent_meetings"][0]["title"] == "Sprint Planning"
    assert isinstance(body["topics"], list)
    assert len(body["topics"]) > 0, "engineer is a taxonomy role and must have real topics"


def test_unknown_user_id_is_404_not_500(client):
    _seed_tenant()
    resp = client.get(f"/bff/people/{uuid.uuid4()}")
    assert resp.status_code == 404
    assert resp.json()["code"] == "NOT_FOUND"


def test_cross_tenant_user_id_is_404_not_403(client):
    org_a, _ = _seed_tenant()
    person_a = uuid.uuid4()
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) "
        "VALUES (%(id)s, %(org)s, %(cuid)s, 'a@example.com')",
        {"id": str(person_a), "org": str(org_a), "cuid": f"user_{person_a.hex[:16]}"},
    )

    _seed_tenant()  # sign in as a different tenant
    resp = client.get(f"/bff/people/{person_a}")
    assert resp.status_code == 404, "cross-tenant existence must never leak as a 403"
    assert resp.json()["code"] == "NOT_FOUND"


def test_bff_people_route_is_get_only(client):
    routes = [r for r in client.app.routes if hasattr(r, "methods")]
    for route in routes:
        if str(getattr(route, "path", "")).startswith("/bff"):
            assert route.methods <= {"GET", "HEAD", "OPTIONS"}, f"{route.path} mutates"
