"""The dashboard composite. Plan section 3.9 acceptance checks."""

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
from services.bff import dashboard as dashboard_mod
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


def _seed_tenant(tier: str = "member"):
    org_id, user_id = uuid.uuid4(), uuid.uuid4()
    clerk_org = f"org_{uuid.uuid4().hex[:16]}"
    clerk_user = f"user_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO organizations (id, name, slug, clerk_org_id) "
        "VALUES (%(id)s, 'Org', %(slug)s, %(cid)s)",
        {"id": str(org_id), "slug": f"bff-{org_id}", "cid": clerk_org},
    )
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) "
        "VALUES (%(uid)s, %(org)s, %(cuid)s, 'u@example.com')",
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

    app = create_app("bff-test")
    app.include_router(dashboard_mod.router)
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


def test_composite_shape_with_the_real_meetings_arm(client):
    """Plan 3.9 acceptance 1: real meetings, stable empty arms for the rest."""
    org_id, user_id = _seed_tenant()
    _sql(
        "INSERT INTO meetings (org_id, title, status, scheduled_start, created_by) "
        "VALUES (%(o)s, 'Standup', 'scheduled', now() + interval '1 day', %(u)s)",
        {"o": str(org_id), "u": str(user_id)},
    )

    body = client.get("/bff/dashboard").json()
    assert set(body) == {"upcoming_meetings", "action_items", "worklogs", "decisions"}

    assert body["upcoming_meetings"]["status"] == "ok"
    assert [m["title"] for m in body["upcoming_meetings"]["items"]] == ["Standup"]

    for arm in ("action_items", "worklogs", "decisions"):
        # `empty`, not `degraded`: these read models genuinely have nothing yet, which is a
        # different empty state from "we could not load this".
        assert body[arm]["status"] == "empty", arm
        assert body[arm]["items"] == []


def test_arms_are_org_scoped(client):
    """One tenant's dashboard never shows another's meetings."""
    org_a, user_a = _seed_tenant()
    _sql(
        "INSERT INTO meetings (org_id, title, status, created_by) "
        "VALUES (%(o)s, 'A-only', 'scheduled', %(u)s)",
        {"o": str(org_a), "u": str(user_a)},
    )

    _seed_tenant()  # signs in as a different tenant
    body = client.get("/bff/dashboard").json()
    assert body["upcoming_meetings"]["items"] == []
    assert body["upcoming_meetings"]["status"] == "empty"


def test_ended_meetings_are_not_upcoming(client):
    org_id, user_id = _seed_tenant()
    _sql(
        "INSERT INTO meetings (org_id, title, status, created_by) "
        "VALUES (%(o)s, 'Finished', 'ended', %(u)s)",
        {"o": str(org_id), "u": str(user_id)},
    )
    body = client.get("/bff/dashboard").json()
    assert body["upcoming_meetings"]["items"] == []


def test_undated_meetings_sort_after_dated_ones(client):
    """NULLS LAST. Default ASC puts NULLs first and they would monopolise the panel."""
    org_id, user_id = _seed_tenant()
    _sql(
        "INSERT INTO meetings (org_id, title, status, created_by) "
        "VALUES (%(o)s, 'No date', 'scheduled', %(u)s)",
        {"o": str(org_id), "u": str(user_id)},
    )
    _sql(
        "INSERT INTO meetings (org_id, title, status, scheduled_start, created_by) "
        "VALUES (%(o)s, 'Tomorrow', 'scheduled', now() + interval '1 day', %(u)s)",
        {"o": str(org_id), "u": str(user_id)},
    )
    titles = [m["title"] for m in client.get("/bff/dashboard").json()["upcoming_meetings"]["items"]]
    assert titles == ["Tomorrow", "No date"]


def test_a_slow_arm_degrades_and_does_not_500(client, monkeypatch):
    """Plan 3.9 acceptance 3. The screen survives one broken panel."""
    _seed_tenant()
    monkeypatch.setattr(dashboard_mod, "ARM_TIMEOUT_SECONDS", 0.01)

    import asyncio

    original = dashboard_mod._arm

    async def slow_arm(name, load):
        if name == "upcoming_meetings":

            async def _hang():
                await asyncio.sleep(5)
                return []

            return await original(name, _hang)
        return await original(name, load)

    monkeypatch.setattr(dashboard_mod, "_arm", slow_arm)

    resp = client.get("/bff/dashboard")
    assert resp.status_code == 200, "a slow arm must not fail the screen"
    body = resp.json()
    assert body["upcoming_meetings"]["status"] == "degraded"
    assert body["upcoming_meetings"]["items"] == []
    # The other panels still resolved, which is the entire point of degrading per arm.
    assert body["decisions"]["status"] == "empty"


def test_a_failing_arm_degrades_and_does_not_500(client, monkeypatch):
    """A raising arm is handled identically to a slow one: show the rest of the screen."""
    _seed_tenant()
    original = dashboard_mod._arm

    async def broken_arm(name, load):
        if name == "upcoming_meetings":

            async def _boom():
                raise RuntimeError("core-api is down")

            return await original(name, _boom)
        return await original(name, load)

    monkeypatch.setattr(dashboard_mod, "_arm", broken_arm)

    body = client.get("/bff/dashboard").json()
    assert body["upcoming_meetings"]["status"] == "degraded"
    assert body["worklogs"]["status"] == "empty"


def test_bff_exposes_no_mutating_routes(client):
    """Plan 3.9 acceptance 2. A write path here would make bff a second writer."""
    routes = [r for r in client.app.routes if hasattr(r, "methods")]
    for route in routes:
        if str(getattr(route, "path", "")).startswith("/bff"):
            assert route.methods <= {"GET", "HEAD", "OPTIONS"}, f"{route.path} mutates"
