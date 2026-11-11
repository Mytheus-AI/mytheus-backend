"""Team-membership writes in `core-api`, and the cache eviction they must trigger.

Closes the product gap recorded as status-overview item 20: `team_members` had no write route
anywhere in the program, so the Plan 5-6 seeder was its only writer. It also completes the
"on `team_members` changes" half of Plan 5-6 section 3.3's cache-invalidation rule, which
`invalidate_user_context`'s docstring records as unwired for want of a write path to wire it
into.

The eviction tests are deliberately BEHAVIOURAL rather than key-level: they warm the cache
through `GET /context/{user_id}`, change the membership through `core-api`, and read the
context again, asserting the new membership is visible. Asserting "the Redis key is gone"
would pass just as happily against an implementation that deleted the wrong key or wrote to a
different tenant's namespace. Reading the context back is the property the plan actually
states, and it fails if any link in the chain is wrong.

Two routers are mounted on one app because the write lives in `core-api` and the cache lives
in `identity-tenant-service`. That split is the point of the test: `team_members` is
core-api-owned, so identity must not write it, and the cache is identity-owned, so the
eviction has to cross the boundary explicitly. The structural choices (one module-scoped
TestClient, synchronous psycopg2 for assertions) follow `test_meetings_api.py` for the same
event-loop reasons documented there.
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
from services.core_api.routes import org_hierarchy
from services.identity_tenant_service.routes import context as context_routes
from tests.conftest import APP_URL, BOOTSTRAP_URL


class Principal:
    """The signed-in caller. Mutated between tests to switch tenants on the one client."""

    clerk_org = ""
    clerk_user = ""


PRINCIPAL = Principal()


@pytest.fixture(scope="module")
def client(migrated_db):
    """One app, one event loop, engine pointed at the request-path role."""
    import os

    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()

    app = create_app("team-members-test")
    app.include_router(org_hierarchy.router)
    app.include_router(context_routes.router)
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


def _sql(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Synchronous query as the schema owner. Test scaffolding only."""
    conn = psycopg2.connect(BOOTSTRAP_URL.replace("+asyncpg", ""))
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or {})
            return [dict(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


class Tenant:
    def __init__(self, org_id, clerk_org, user_id, clerk_user, tier):
        self.org_id = org_id
        self.clerk_org = clerk_org
        self.user_id = user_id
        self.clerk_user = clerk_user
        self.tier = tier


def seed_tenant(tier: str = "admin", role: str = "engineer") -> Tenant:
    org_id, user_id = uuid.uuid4(), uuid.uuid4()
    clerk_org = f"org_{uuid.uuid4().hex[:16]}"
    clerk_user = f"user_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO organizations (id, name, slug, clerk_org_id) "
        "VALUES (%(id)s, 'Org', %(slug)s, %(cid)s)",
        {"id": str(org_id), "slug": f"tm-{org_id}", "cid": clerk_org},
    )
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email, role) "
        "VALUES (%(uid)s, %(org)s, %(cuid)s, 'u@example.com', %(role)s)",
        {"uid": str(user_id), "org": str(org_id), "cuid": clerk_user, "role": role},
    )
    if tier:
        _sql(
            "INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (%(o)s, %(u)s, %(t)s)",
            {"o": str(org_id), "u": str(user_id), "t": tier},
        )
    return Tenant(org_id, clerk_org, user_id, clerk_user, tier)


def add_user(tenant: Tenant, role: str = "engineer") -> uuid.UUID:
    """A second user inside an existing tenant, so membership has someone to move around."""
    user_id = uuid.uuid4()
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email, role) "
        "VALUES (%(uid)s, %(org)s, %(cuid)s, 'other@example.com', %(role)s)",
        {
            "uid": str(user_id),
            "org": str(tenant.org_id),
            "cuid": f"user_{uuid.uuid4().hex[:16]}",
            "role": role,
        },
    )
    return user_id


def sign_in(tenant: Tenant) -> None:
    PRINCIPAL.clerk_org = tenant.clerk_org
    PRINCIPAL.clerk_user = tenant.clerk_user


def make_team(client: TestClient, name: str = "Platform") -> uuid.UUID:
    resp = client.post("/api/v1/teams", json={"name": name})
    assert resp.status_code == 201, resp.text
    return uuid.UUID(resp.json()["id"])


@pytest.fixture
def tenant_a():
    t = seed_tenant(tier="admin")
    sign_in(t)
    return t


# --- the write path -------------------------------------------------------------------------


def test_adding_a_member_persists_the_row_with_its_tenant_and_role(client, tenant_a):
    """The row carries `org_id` denormalized, which is what its RLS policy is written over."""
    team_id = make_team(client)
    member = add_user(tenant_a)

    resp = client.put(f"/api/v1/teams/{team_id}/members/{member}", json={"role": "lead"})

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["team_id"] == str(team_id)
    assert body["user_id"] == str(member)
    assert body["role"] == "lead"

    rows = _sql(
        "SELECT org_id, role FROM team_members WHERE team_id = %(t)s AND user_id = %(u)s",
        {"t": str(team_id), "u": str(member)},
    )
    assert len(rows) == 1
    assert str(rows[0]["org_id"]) == str(tenant_a.org_id)
    assert rows[0]["role"] == "lead"


def test_adding_the_same_member_twice_updates_the_role_rather_than_duplicating(client, tenant_a):
    """PUT is the right verb precisely because the second call must not be an error.

    `(team_id, user_id)` is the primary key, so a plain INSERT would raise on the second call
    and a caller correcting somebody's role would have to delete first.
    """
    team_id = make_team(client)
    member = add_user(tenant_a)

    first = client.put(f"/api/v1/teams/{team_id}/members/{member}", json={"role": "member"})
    second = client.put(f"/api/v1/teams/{team_id}/members/{member}", json={"role": "lead"})

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert second.json()["role"] == "lead"

    rows = _sql(
        "SELECT role FROM team_members WHERE team_id = %(t)s AND user_id = %(u)s",
        {"t": str(team_id), "u": str(member)},
    )
    assert len(rows) == 1, "the primary key must not be duplicated"
    assert rows[0]["role"] == "lead"


def test_removing_a_member_deletes_the_row(client, tenant_a):
    team_id = make_team(client)
    member = add_user(tenant_a)
    client.put(f"/api/v1/teams/{team_id}/members/{member}", json={"role": "member"})

    resp = client.delete(f"/api/v1/teams/{team_id}/members/{member}")

    assert resp.status_code == 204, resp.text
    assert (
        _sql(
            "SELECT 1 FROM team_members WHERE team_id = %(t)s AND user_id = %(u)s",
            {"t": str(team_id), "u": str(member)},
        )
        == []
    )


def test_listing_members_returns_the_team_roster(client, tenant_a):
    team_id = make_team(client)
    one, two = add_user(tenant_a), add_user(tenant_a)
    client.put(f"/api/v1/teams/{team_id}/members/{one}", json={"role": "lead"})
    client.put(f"/api/v1/teams/{team_id}/members/{two}", json={"role": "member"})

    resp = client.get(f"/api/v1/teams/{team_id}/members")

    assert resp.status_code == 200, resp.text
    returned = {m["user_id"]: m["role"] for m in resp.json()}
    assert returned == {str(one): "lead", str(two): "member"}


# --- the cache eviction this task exists to wire --------------------------------------------


def test_adding_a_member_evicts_the_cached_user_context(client, tenant_a):
    """Plan 5-6 section 3.3: "on `team_members` changes, DEL nexus:{org_id}:user_ctx:{user_id}".

    Without the eviction the second read is served from the entry warmed by the first and
    reports no teams for five minutes, which is the stale window the plan's rule exists to
    close.
    """
    team_id = make_team(client, name="Platform")
    member = add_user(tenant_a)

    warmed = client.get(f"/context/{member}")
    assert warmed.status_code == 200, warmed.text
    assert warmed.json()["team"] == [], "precondition: not yet on a team"

    client.put(f"/api/v1/teams/{team_id}/members/{member}", json={"role": "member"})

    after = client.get(f"/context/{member}")
    assert after.status_code == 200, after.text
    assert [t["name"] for t in after.json()["team"]] == ["Platform"]


def test_removing_a_member_evicts_the_cached_user_context(client, tenant_a):
    """The removal half. A cache that only evicts on add leaves a removed member still
    reading as a member, which is the direction that matters for access decisions."""
    team_id = make_team(client, name="Platform")
    member = add_user(tenant_a)
    client.put(f"/api/v1/teams/{team_id}/members/{member}", json={"role": "member"})

    warmed = client.get(f"/context/{member}")
    assert warmed.status_code == 200, warmed.text
    assert [t["name"] for t in warmed.json()["team"]] == ["Platform"], "precondition: on a team"

    client.delete(f"/api/v1/teams/{team_id}/members/{member}")

    after = client.get(f"/context/{member}")
    assert after.status_code == 200, after.text
    assert after.json()["team"] == []


# --- authorization --------------------------------------------------------------------------


def test_a_member_tier_caller_cannot_change_membership(client, tenant_a):
    """Team structure is who-reports-to-whom, the same reason creating a team requires admin."""
    team_id = make_team(client)
    member = add_user(tenant_a)

    downgraded = seed_tenant(tier="member")
    sign_in(downgraded)
    try:
        resp = client.put(f"/api/v1/teams/{team_id}/members/{member}", json={"role": "lead"})
        assert resp.status_code == 403
        assert resp.json()["code"] == "FORBIDDEN_ROLE"
    finally:
        sign_in(tenant_a)

    assert (
        _sql(
            "SELECT 1 FROM team_members WHERE team_id = %(t)s AND user_id = %(u)s",
            {"t": str(team_id), "u": str(member)},
        )
        == []
    ), "a denied request must write nothing"


# --- tenant isolation -----------------------------------------------------------------------


def test_a_user_from_another_tenant_cannot_be_added(client, tenant_a):
    """The FK to `users` cannot resolve under RLS, so cross-tenant membership is
    unrepresentable rather than merely refused. It must not surface as a 500.

    The legitimate PUT first is a positive control, not decoration: `404` is also what an
    absent route returns, so without it this test passes against no implementation at all.
    """
    team_id = make_team(client)
    outsider = seed_tenant(tier="admin")
    sign_in(tenant_a)

    insider = add_user(tenant_a)
    control = client.put(f"/api/v1/teams/{team_id}/members/{insider}", json={"role": "member"})
    assert control.status_code == 200, "the route must exist for the 404 below to mean anything"

    resp = client.put(
        f"/api/v1/teams/{team_id}/members/{outsider.user_id}", json={"role": "member"}
    )

    assert resp.status_code == 404, resp.text
    assert resp.json()["code"] == "NOT_FOUND"
    assert (
        _sql(
            "SELECT 1 FROM team_members WHERE user_id = %(u)s",
            {"u": str(outsider.user_id)},
        )
        == []
    )


def test_another_tenants_team_is_not_writable(client, tenant_a):
    """Existence of another tenant's team is confidential: 404, never 403."""
    victim = seed_tenant(tier="admin")
    sign_in(victim)
    victim_team = make_team(client, name="Victim Team")

    sign_in(tenant_a)
    own_team = make_team(client, name="Own Team")
    member = add_user(tenant_a)
    control = client.put(f"/api/v1/teams/{own_team}/members/{member}", json={"role": "member"})
    assert control.status_code == 200, "positive control: the route must exist"

    resp = client.put(f"/api/v1/teams/{victim_team}/members/{member}", json={"role": "member"})

    assert resp.status_code == 404, resp.text
    assert resp.json()["code"] == "NOT_FOUND"


def test_removing_someone_who_is_not_a_member_is_404(client, tenant_a):
    """A no-op DELETE reporting 204 would tell an admin the removal happened when the id was
    simply wrong."""
    team_id = make_team(client)
    stranger = add_user(tenant_a)

    real = add_user(tenant_a)
    client.put(f"/api/v1/teams/{team_id}/members/{real}", json={"role": "member"})
    control = client.delete(f"/api/v1/teams/{team_id}/members/{real}")
    assert control.status_code == 204, "positive control: the route must exist"

    resp = client.delete(f"/api/v1/teams/{team_id}/members/{stranger}")

    assert resp.status_code == 404
    assert resp.json()["code"] == "NOT_FOUND"


# --- audit ----------------------------------------------------------------------------------


def test_both_writes_are_audited_with_the_acting_admin(client, tenant_a):
    """Membership decides what a user can see downstream, so who changed it is a compliance
    fact, not debug logging."""
    team_id = make_team(client)
    member = add_user(tenant_a)

    client.put(f"/api/v1/teams/{team_id}/members/{member}", json={"role": "lead"})
    client.delete(f"/api/v1/teams/{team_id}/members/{member}")

    rows = _sql(
        "SELECT action, actor_id, target_type, metadata FROM audit_logs "
        "WHERE target_id = %(t)s AND org_id = %(o)s AND action LIKE 'team.member_%%' "
        "ORDER BY occurred_at",
        {"t": str(team_id), "o": str(tenant_a.org_id)},
    )
    actions = [r["action"] for r in rows]
    assert actions == ["team.member_added", "team.member_removed"], rows
    assert all(str(r["actor_id"]) == str(tenant_a.user_id) for r in rows)
    assert all(r["target_type"] == "team" for r in rows)
    assert rows[0]["metadata"]["user_id"] == str(member)


# --- gap 31: body-supplied department ids are foreign keys too ------------------------------
#
# Postgres RI checks bypass RLS, so `teams.department_id` and `departments.parent_id` resolved
# another tenant's department and the insert returned 201. The visibility check in
# `services/core_api/visibility.py` is the enforcement; these tests fail if it is removed.


def _make_department(client: TestClient, name: str, parent: uuid.UUID | None = None) -> Any:
    body: dict[str, Any] = {"name": name}
    if parent is not None:
        body["parent_id"] = str(parent)
    return client.post("/api/v1/departments", json=body)


def test_a_team_cannot_be_filed_under_another_tenants_department(client, tenant_a):
    victim = seed_tenant(tier="admin")
    sign_in(victim)
    victim_dept = uuid.UUID(_make_department(client, "Victim Dept").json()["id"])

    sign_in(tenant_a)
    own_dept = uuid.UUID(_make_department(client, "Own Dept").json()["id"])
    control = client.post("/api/v1/teams", json={"name": "Ok", "department_id": str(own_dept)})
    assert control.status_code == 201, "positive control: an own-tenant department is accepted"

    resp = client.post("/api/v1/teams", json={"name": "Cross", "department_id": str(victim_dept)})
    assert resp.status_code == 404, resp.text
    assert resp.json()["code"] == "NOT_FOUND"
    assert _sql("SELECT 1 FROM teams WHERE department_id = %(d)s", {"d": str(victim_dept)}) == []


def test_a_department_cannot_be_nested_under_another_tenants_department(client, tenant_a):
    victim = seed_tenant(tier="admin")
    sign_in(victim)
    victim_dept = uuid.UUID(_make_department(client, "Victim Root").json()["id"])

    sign_in(tenant_a)
    own_root = uuid.UUID(_make_department(client, "Own Root").json()["id"])
    control = _make_department(client, "Own Child", parent=own_root)
    assert control.status_code == 201, "positive control: an own-tenant parent is accepted"

    resp = _make_department(client, "Cross Child", parent=victim_dept)
    assert resp.status_code == 404, resp.text
    assert resp.json()["code"] == "NOT_FOUND"
    assert _sql("SELECT 1 FROM departments WHERE parent_id = %(p)s", {"p": str(victim_dept)}) == []
