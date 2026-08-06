"""The org-chart composite. Plan 5-6 section 3.5 acceptance checks.

An org chart is a tree, and trees invite two mistakes this file is built to catch:

- An N+1 query per node. `test_query_count_stays_constant_as_the_tree_grows` proves the query
  count is flat (3 statements) whether the tenant has one team or several, rather than trusting
  that it "looks fine" against the seeder's small fixture.
- A recursive walk that follows `parent_id`/`department_id`/`team_id` without re-scoping each
  hop, which is how you walk out of a tenant. `test_chart_is_tenant_scoped_for_two_orgs_with_
  sibling_shaped_hierarchies` proves two orgs with IDENTICALLY NAMED department/team/member
  hierarchies never cross - the assertion is by id, not name, because two same-named nodes in
  different tenants is exactly the case a name-only assertion would miss.
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
from services.bff import org_chart as org_chart_mod
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
        {"id": str(org_id), "slug": f"orgchart-{org_id}", "cid": clerk_org},
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


def _department(org_id: uuid.UUID, name: str, parent_id: uuid.UUID | None = None) -> uuid.UUID:
    dept_id = uuid.uuid4()
    _sql(
        "INSERT INTO departments (id, org_id, name, parent_id) "
        "VALUES (%(id)s, %(org)s, %(name)s, %(parent)s)",
        {
            "id": str(dept_id),
            "org": str(org_id),
            "name": name,
            "parent": str(parent_id) if parent_id else None,
        },
    )
    return dept_id


def _team(org_id: uuid.UUID, name: str, department_id: uuid.UUID | None = None) -> uuid.UUID:
    team_id = uuid.uuid4()
    _sql(
        "INSERT INTO teams (id, org_id, name, department_id) "
        "VALUES (%(id)s, %(org)s, %(name)s, %(dept)s)",
        {
            "id": str(team_id),
            "org": str(org_id),
            "name": name,
            "dept": str(department_id) if department_id else None,
        },
    )
    return team_id


def _member(
    org_id: uuid.UUID,
    team_id: uuid.UUID,
    email: str,
    role: str = "engineer",
    role_in_team: str = "member",
) -> uuid.UUID:
    user_id = uuid.uuid4()
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email, role) "
        "VALUES (%(id)s, %(org)s, %(cuid)s, %(email)s, %(role)s)",
        {
            "id": str(user_id),
            "org": str(org_id),
            "cuid": f"user_{user_id.hex[:16]}",
            "email": email,
            "role": role,
        },
    )
    _sql(
        "INSERT INTO team_members (org_id, team_id, user_id, role) "
        "VALUES (%(org)s, %(team)s, %(user)s, %(role)s)",
        {"org": str(org_id), "team": str(team_id), "user": str(user_id), "role": role_in_team},
    )
    return user_id


@pytest.fixture(scope="module")
def client(migrated_db):
    import os

    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()

    app = create_app("bff-org-chart-test")
    app.include_router(org_chart_mod.router)
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


def test_chart_shape_is_a_department_rooted_tree_with_teams_and_members(client):
    org_id, _ = _seed_tenant()
    dept_id = _department(org_id, "Engineering")
    team_id = _team(org_id, "Platform", dept_id)
    _member(org_id, team_id, "alice@example.com", role="engineer", role_in_team="lead")
    _member(org_id, team_id, "bob@example.com", role="engineer", role_in_team="member")

    body = client.get("/bff/org/chart").json()
    assert body["status"] == "ok"
    assert len(body["departments"]) == 1
    dept = body["departments"][0]
    assert dept["id"] == str(dept_id)
    assert dept["name"] == "Engineering"
    assert dept["children"] == []
    assert len(dept["teams"]) == 1
    team = dept["teams"][0]
    assert team["id"] == str(team_id)
    emails = [m["email"] for m in team["members"]]
    assert emails == ["alice@example.com", "bob@example.com"]
    roles_in_team = {m["email"]: m["role_in_team"] for m in team["members"]}
    assert roles_in_team == {"alice@example.com": "lead", "bob@example.com": "member"}
    assert body["unassigned_teams"] == []


def test_chart_nests_child_departments_under_their_parent(client):
    org_id, _ = _seed_tenant()
    parent_id = _department(org_id, "Product")
    child_id = _department(org_id, "Design", parent_id=parent_id)
    _team(org_id, "Design Systems", child_id)

    body = client.get("/bff/org/chart").json()
    assert len(body["departments"]) == 1, "child departments must nest, not appear as roots"
    parent = body["departments"][0]
    assert parent["id"] == str(parent_id)
    assert len(parent["children"]) == 1
    child = parent["children"][0]
    assert child["id"] == str(child_id)
    assert child["teams"][0]["name"] == "Design Systems"


def test_teams_without_a_department_appear_in_unassigned_teams_not_dropped(client):
    org_id, _ = _seed_tenant()
    team_id = _team(org_id, "Roaming Squad", department_id=None)

    body = client.get("/bff/org/chart").json()
    assert body["departments"] == []
    assert [t["id"] for t in body["unassigned_teams"]] == [str(team_id)]


def test_chart_is_empty_status_for_a_tenant_with_no_hierarchy_yet(client):
    _seed_tenant()
    body = client.get("/bff/org/chart").json()
    assert body["status"] == "empty"
    assert body["departments"] == []
    assert body["unassigned_teams"] == []


def test_chart_is_tenant_scoped_for_two_orgs_with_sibling_shaped_hierarchies(client):
    """Same names on purpose. A name-only assertion would pass even if org A leaked org B's
    node under a shared label; asserting on ids is the only proof that means anything."""
    org_a, _ = _seed_tenant()
    dept_a = _department(org_a, "Engineering")
    team_a = _team(org_a, "Platform", dept_a)
    user_a = _member(org_a, team_a, "shared-label@example.com")

    org_b, _ = _seed_tenant()  # signs in as org B for the next call
    dept_b = _department(org_b, "Engineering")
    team_b = _team(org_b, "Platform", dept_b)
    user_b = _member(org_b, team_b, "shared-label@example.com")

    body = client.get("/bff/org/chart").json()  # signed in as org B (last _seed_tenant call)
    assert len(body["departments"]) == 1
    dept = body["departments"][0]
    assert dept["id"] == str(dept_b)
    assert dept["id"] != str(dept_a)
    team = dept["teams"][0]
    assert team["id"] == str(team_b)
    assert team["id"] != str(team_a)
    member_ids = {m["user_id"] for m in team["members"]}
    # The positive claim: org B's own member is present.
    assert member_ids == {str(user_b)}
    # The negative claim, which is the one that actually proves isolation: org A's user id
    # (same email label, different tenant) must never appear in org B's chart.
    assert str(user_a) not in member_ids


def test_query_count_stays_constant_as_the_tree_grows(client, monkeypatch):
    """Proves the flat-query-count claim rather than asserting it from reading the code: a
    tenant with one team and one with several must cost the same number of SQL statements.

    Counts ONLY the three `_build_tree` statements, not the request's auth/tenancy overhead
    (`resolve_org_id`, the principal lookup, `set_rls` on each of the two sessions
    `tenant_context`/`tenant_session` open) - that overhead is constant per request by a
    different mechanism entirely and is not what this test is about. Filtering by identity
    against the module's own `text()` objects, rather than by string-matching, is exact: no
    other statement in the codebase can accidentally match.
    """
    from sqlalchemy.ext.asyncio import AsyncSession

    from services.bff import org_chart as target

    tracked = {target._SELECT_DEPARTMENTS, target._SELECT_TEAMS, target._SELECT_MEMBERS}
    calls = {"n": 0}
    original_execute = AsyncSession.execute

    async def counting_execute(self, statement, *args, **kwargs):  # type: ignore[no-untyped-def]
        if statement in tracked:
            calls["n"] += 1
        return await original_execute(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "execute", counting_execute)

    org_id, _ = _seed_tenant()
    dept_id = _department(org_id, "Engineering")
    team_id = _team(org_id, "Platform", dept_id)
    _member(org_id, team_id, "solo@example.com")

    calls["n"] = 0
    client.get("/bff/org/chart")
    small_count = calls["n"]
    assert small_count > 0

    # Grow the tree: a second department, two more teams, five more members.
    dept2_id = _department(org_id, "Product", parent_id=dept_id)
    for i in range(2):
        t = _team(org_id, f"Squad {i}", dept2_id)
        for j in range(3):
            _member(org_id, t, f"user{i}-{j}@example.com")

    calls["n"] = 0
    client.get("/bff/org/chart")
    large_count = calls["n"]

    assert large_count == small_count, (
        f"query count grew from {small_count} to {large_count} as the tree grew - "
        "this is the N+1 the brief warned about"
    )
    assert (
        large_count <= 3
    ), f"expected exactly 3 statements (departments/teams/members), got {large_count}"


def test_a_failing_build_degrades_and_does_not_500(client, monkeypatch):
    async def broken_build(session):
        raise RuntimeError("core-api tables unreachable")

    monkeypatch.setattr(org_chart_mod, "_build_tree", broken_build)
    _seed_tenant()

    resp = client.get("/bff/org/chart")
    assert resp.status_code == 200, "a failed build must not fail the screen"
    body = resp.json()
    assert body["status"] == "degraded"
    assert body["departments"] == []
    assert body["unassigned_teams"] == []


def test_a_slow_build_times_out_and_degrades(client, monkeypatch):
    import asyncio

    monkeypatch.setattr(org_chart_mod, "ARM_TIMEOUT_SECONDS", 0.01)

    async def slow_build(session):
        await asyncio.sleep(5)
        return org_chart_mod._ChartData(departments=[], unassigned_teams=[])

    monkeypatch.setattr(org_chart_mod, "_build_tree", slow_build)
    _seed_tenant()

    resp = client.get("/bff/org/chart")
    assert resp.status_code == 200
    assert resp.json()["status"] == "degraded"


def test_bff_org_chart_route_is_get_only(client):
    routes = [r for r in client.app.routes if hasattr(r, "methods")]
    for route in routes:
        if str(getattr(route, "path", "")).startswith("/bff"):
            assert route.methods <= {"GET", "HEAD", "OPTIONS"}, f"{route.path} mutates"
