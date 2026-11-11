"""People structure: describing, editing, moving and deleting departments and teams, and moving
people between teams (`core-api`, `services/core_api/routes/org_hierarchy.py`).

What it proves, beyond the happy paths:
- a department delete never deletes a team: its teams move to "No department" and its child
  departments move up one level;
- a team delete removes its memberships and nothing else;
- every new write is admin-only (403 for a member), and every id of another tenant, in the path
  or in the body, is a 404 (existence is tenant-confidential);
- the description reaches what agents read: the member's `user_ctx` (behaviourally, through
  `GET /context/{user_id}`, like `test_team_members_api.py`) and the company brain
  (`GetOrgContext.departments` / `.teams`), and a structure write evicts the cached brain.

Reuses `test_team_members_api.py`'s tenant scaffolding and its module client shape.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest
from redis.asyncio import from_url as redis_from_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.bff import org_chart as org_chart_routes
from services.core_api.routes import org_hierarchy
from services.identity_tenant_service import grpc_server as grpc_server_mod
from services.identity_tenant_service.brain import context as brain_context
from services.identity_tenant_service.routes import context as context_routes
from tests.conftest import APP_URL
from tests.test_team_members_api import PRINCIPAL, _sql, add_user, seed_tenant, sign_in

D = "/api/v1/departments"
T = "/api/v1/teams"


def _redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://localhost:6390")


@pytest.fixture(autouse=True)
def _fresh_redis_per_call(monkeypatch: pytest.MonkeyPatch) -> None:
    """One Redis client per call: the TestClient's loop and the test's loop must not share a
    pool (see `test_company_brain.py`)."""
    monkeypatch.setattr(
        brain_context, "get_redis", lambda: redis_from_url(_redis_url(), decode_responses=False)
    )


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()

    app = create_app("org-structure-test")
    app.include_router(org_hierarchy.router)
    app.include_router(context_routes.router)
    app.include_router(org_chart_routes.router)
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
def admin():
    t = seed_tenant(tier="admin")
    sign_in(t)
    return t


def _dept(client: TestClient, name: str, **extra: Any) -> dict[str, Any]:
    resp = client.post(D, json={"name": name, **extra})
    assert resp.status_code == 201, resp.text
    return resp.json()  # type: ignore[no-any-return]


def _team(client: TestClient, name: str, **extra: Any) -> dict[str, Any]:
    resp = client.post(T, json={"name": name, **extra})
    assert resp.status_code == 201, resp.text
    return resp.json()  # type: ignore[no-any-return]


def _listed(client: TestClient, path: str) -> dict[str, dict[str, Any]]:
    resp = client.get(path)
    assert resp.status_code == 200, resp.text
    return {r["id"]: r for r in resp.json()}


# --- descriptions -----------------------------------------------------------------------------


def test_create_with_description_and_read_it_back(client, admin):
    eng = _dept(client, "Engineering", description="  Builds and runs the product.  ")
    team = _team(
        client, "Platform", department_id=eng["id"], description="Owns CI, deploys and infra."
    )
    assert eng["description"] == "Builds and runs the product."
    assert team["description"] == "Owns CI, deploys and infra."
    assert _listed(client, D)[eng["id"]]["description"] == "Builds and runs the product."
    assert _listed(client, T)[team["id"]]["description"] == "Owns CI, deploys and infra."

    blank = _team(client, "Blank", description="   ")
    assert blank["description"] is None


@pytest.mark.parametrize("path", [D, T])
def test_description_over_1000_characters_is_422(client, admin, path):
    resp = client.post(path, json={"name": "X", "description": "a" * 1001})
    assert resp.status_code == 422, resp.text
    assert client.post(path, json={"name": "X", "description": "a" * 1000}).status_code == 201


def test_the_database_enforces_the_limit_too(client, admin):
    team = _team(client, "Limit")
    with pytest.raises(Exception, match="ck_teams_description_length"):
        _sql(
            "UPDATE teams SET description = repeat('a', 1001) WHERE id = %(id)s",
            {"id": team["id"]},
        )


# --- departments ------------------------------------------------------------------------------


def test_patch_department_changes_only_what_is_sent(client, admin):
    dep = _dept(client, "Sales", description="Closes deals.")
    resp = client.patch(f"{D}/{dep['id']}", json={"name": "Revenue"})
    assert resp.status_code == 200, resp.text
    assert (resp.json()["name"], resp.json()["description"]) == ("Revenue", "Closes deals.")

    resp = client.patch(f"{D}/{dep['id']}", json={"description": "Finds and closes deals."})
    assert (resp.json()["name"], resp.json()["description"]) == (
        "Revenue",
        "Finds and closes deals.",
    )

    resp = client.patch(f"{D}/{dep['id']}", json={"description": ""})
    assert resp.json()["description"] is None

    assert client.patch(f"{D}/{dep['id']}", json={"name": "  "}).status_code == 422
    assert client.patch(f"{D}/{dep['id']}", json={"name": None}).status_code == 422


def test_a_department_cannot_be_moved_under_itself_or_a_descendant(client, admin):
    top = _dept(client, "Top")
    mid = _dept(client, "Mid", parent_id=top["id"])
    leaf = _dept(client, "Leaf", parent_id=mid["id"])
    for parent in (top["id"], mid["id"], leaf["id"]):
        resp = client.patch(f"{D}/{top['id']}", json={"parent_id": parent})
        assert resp.status_code == 422, resp.text
    ok = client.patch(f"{D}/{leaf['id']}", json={"parent_id": top["id"]})
    assert ok.status_code == 200 and ok.json()["parent_id"] == top["id"]
    ok = client.patch(f"{D}/{leaf['id']}", json={"parent_id": None})
    assert ok.status_code == 200 and ok.json()["parent_id"] is None


def test_deleting_a_department_moves_its_teams_to_no_department(client, admin):
    parent = _dept(client, "Company")
    dep = _dept(client, "Ops", parent_id=parent["id"])
    child = _dept(client, "Support", parent_id=dep["id"])
    team = _team(client, "Oncall", department_id=dep["id"])
    member = add_user(admin)
    assert (
        client.put(f"{T}/{team['id']}/members/{member}", json={"role": "lead"}).status_code == 200
    )

    resp = client.delete(f"{D}/{dep['id']}")
    assert resp.status_code == 204, resp.text

    departments = _listed(client, D)
    assert dep["id"] not in departments
    assert departments[child["id"]]["parent_id"] == parent["id"]  # moved up one level
    teams = _listed(client, T)
    assert teams[team["id"]]["department_id"] is None  # never deleted
    roster = client.get(f"{T}/{team['id']}/members").json()
    assert [(r["user_id"], r["role"]) for r in roster] == [(str(member), "lead")]

    assert client.delete(f"{D}/{dep['id']}").status_code == 404
    assert client.patch(f"{D}/{dep['id']}", json={"name": "Back"}).status_code == 404


# --- teams -----------------------------------------------------------------------------------


def test_patch_team_moves_it_between_departments(client, admin):
    a = _dept(client, "A")
    b = _dept(client, "B")
    team = _team(client, "Mover", department_id=a["id"], description="Moves things.")

    resp = client.patch(f"{T}/{team['id']}", json={"department_id": b["id"]})
    assert resp.status_code == 200, resp.text
    assert resp.json()["department_id"] == b["id"]
    assert resp.json()["description"] == "Moves things."

    resp = client.patch(f"{T}/{team['id']}", json={"department_id": None, "name": "Loose"})
    assert (resp.json()["department_id"], resp.json()["name"]) == (None, "Loose")

    resp = client.patch(f"{T}/{team['id']}", json={"department_id": str(uuid.uuid4())})
    assert resp.status_code == 404


def test_deleting_a_team_removes_its_memberships_only(client, admin):
    team = _team(client, "Gone")
    other = _team(client, "Stays")
    member = add_user(admin)
    client.put(f"{T}/{team['id']}/members/{member}", json={"role": "member"})
    client.put(f"{T}/{other['id']}/members/{member}", json={"role": "member"})

    assert client.delete(f"{T}/{team['id']}").status_code == 204
    assert team["id"] not in _listed(client, T)
    assert client.get(f"{T}/{team['id']}/members").status_code == 404
    assert not _sql("SELECT 1 FROM team_members WHERE team_id = %(t)s", {"t": team["id"]})
    assert len(client.get(f"{T}/{other['id']}/members").json()) == 1
    assert client.delete(f"{T}/{team['id']}").status_code == 404


# --- moving people -------------------------------------------------------------------------


def test_move_member_is_one_step_and_keeps_the_label(client, admin):
    a = _team(client, "From")
    b = _team(client, "To")
    member = add_user(admin)
    client.put(f"{T}/{a['id']}/members/{member}", json={"role": "lead"})

    resp = client.post(f"{T}/{a['id']}/members/{member}/move", json={"to_team_id": b["id"]})
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"team_id": b["id"], "user_id": str(member), "role": "lead"}
    assert client.get(f"{T}/{a['id']}/members").json() == []
    assert len(client.get(f"{T}/{b['id']}/members").json()) == 1

    resp = client.post(
        f"{T}/{b['id']}/members/{member}/move", json={"to_team_id": a["id"], "role": "member"}
    )
    assert resp.json()["role"] == "member"

    rows = _sql(
        "SELECT action FROM audit_logs WHERE org_id = %(o)s AND action = 'team.member_moved'",
        {"o": str(admin.org_id)},
    )
    assert len(rows) == 2


def test_move_member_errors(client, admin):
    a = _team(client, "E1")
    b = _team(client, "E2")
    member = add_user(admin)
    path = f"{T}/{a['id']}/members/{member}/move"
    assert client.post(path, json={"to_team_id": b["id"]}).status_code == 404  # not on it
    client.put(f"{T}/{a['id']}/members/{member}", json={})
    assert client.post(path, json={"to_team_id": a["id"]}).status_code == 422  # same team
    assert client.post(path, json={"to_team_id": str(uuid.uuid4())}).status_code == 404
    assert len(client.get(f"{T}/{a['id']}/members").json()) == 1  # nothing half-done


# --- RBAC and tenancy ---------------------------------------------------------------------


def test_member_tier_can_read_but_not_write(client, admin):
    dep = _dept(client, "Readable")
    team = _team(client, "ReadableTeam")
    member_id, clerk_user = uuid.uuid4(), f"user_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email, role) "
        "VALUES (%(u)s, %(o)s, %(c)s, 'm@example.com', 'engineer')",
        {"u": str(member_id), "o": str(admin.org_id), "c": clerk_user},
    )
    _sql(
        "INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (%(o)s, %(u)s, 'member')",
        {"o": str(admin.org_id), "u": str(member_id)},
    )
    PRINCIPAL.clerk_user = clerk_user

    assert client.get(D).status_code == 200
    assert client.get(T).status_code == 200
    writes = [
        client.patch(f"{D}/{dep['id']}", json={"name": "No"}),
        client.delete(f"{D}/{dep['id']}"),
        client.patch(f"{T}/{team['id']}", json={"name": "No"}),
        client.delete(f"{T}/{team['id']}"),
        client.post(f"{T}/{team['id']}/members/{member_id}/move", json={"to_team_id": team["id"]}),
    ]
    assert [w.status_code for w in writes] == [403] * len(writes)


def test_another_tenants_structure_is_404_everywhere(client, admin):
    dep = _dept(client, "Mine")
    team = _team(client, "MyTeam", department_id=dep["id"])
    person = add_user(admin)
    client.put(f"{T}/{team['id']}/members/{person}", json={})

    intruder = seed_tenant(tier="admin")
    sign_in(intruder)
    their_team = _team(client, "Theirs")
    their_dept = _dept(client, "TheirDept")
    attempts = [
        client.patch(f"{D}/{dep['id']}", json={"name": "Hijack"}),
        client.delete(f"{D}/{dep['id']}"),
        client.patch(f"{T}/{team['id']}", json={"description": "Hijack"}),
        client.delete(f"{T}/{team['id']}"),
        client.post(
            f"{T}/{team['id']}/members/{person}/move", json={"to_team_id": their_team["id"]}
        ),
        client.patch(f"{T}/{their_team['id']}", json={"department_id": dep["id"]}),
        client.patch(f"{D}/{their_dept['id']}", json={"parent_id": dep["id"]}),
    ]
    assert [a.status_code for a in attempts] == [404] * len(attempts)

    sign_in(admin)
    assert (
        client.post(
            f"{T}/{team['id']}/members/{person}/move", json={"to_team_id": their_team["id"]}
        ).status_code
        == 404
    )
    still = _listed(client, T)[team["id"]]
    assert (still["name"], still["description"], still["department_id"]) == (
        "MyTeam",
        None,
        dep["id"],
    )


# --- what agents see -----------------------------------------------------------------------


def test_member_context_carries_team_and_department_descriptions(client, admin):
    dep = _dept(client, "Product", description="Decides what we build.")
    team = _team(client, "Growth", department_id=dep["id"], description="Runs onboarding.")
    member = add_user(admin)
    client.put(f"{T}/{team['id']}/members/{member}", json={"role": "member"})

    warmed = client.get(f"/context/{member}").json()
    assert warmed["team"][0]["description"] == "Runs onboarding."
    assert warmed["department"]["description"] == "Decides what we build."

    client.patch(f"{T}/{team['id']}", json={"description": "Owns activation."})
    client.patch(f"{D}/{dep['id']}", json={"description": "Owns the roadmap."})
    after = client.get(f"/context/{member}").json()  # evicted, not a stale cache hit
    assert after["team"][0]["description"] == "Owns activation."
    assert after["department"]["description"] == "Owns the roadmap."


def test_org_chart_carries_descriptions(client, admin):
    dep = _dept(client, "Design", description="Makes it make sense.")
    _team(client, "Brand", department_id=dep["id"], description="Owns the look.")
    chart = client.get("/bff/org/chart").json()
    node = next(d for d in chart["departments"] if d["id"] == dep["id"])
    assert node["description"] == "Makes it make sense."
    assert node["teams"][0]["description"] == "Owns the look."


async def _get_org_context(org_id: Any) -> Any:
    import grpc

    import identity_pb2
    import identity_pb2_grpc

    engine = create_async_engine(APP_URL)
    server = grpc.aio.server()
    identity_pb2_grpc.add_IdentityServiceServicer_to_server(
        grpc_server_mod.IdentityService(async_sessionmaker(engine, expire_on_commit=False)), server
    )
    port = server.add_insecure_port("[::]:0")
    await server.start()
    try:
        async with grpc.aio.insecure_channel(f"localhost:{port}") as channel:
            return await identity_pb2_grpc.IdentityServiceStub(channel).GetOrgContext(
                identity_pb2.GetOrgContextRequest(), metadata=(("org-id", str(org_id)),)
            )
    finally:
        await server.stop(None)
        await engine.dispose()


async def _cached(key: str) -> bytes | None:
    r = redis_from_url(_redis_url(), decode_responses=False)
    try:
        return await r.get(key)  # type: ignore[no-any-return]
    finally:
        await r.aclose()


async def test_company_brain_carries_the_structure_and_is_evicted_by_writes(client, admin):
    eng = _dept(client, "Engineering", description="Builds the product.")
    sub = _dept(client, "Infra", parent_id=eng["id"])
    team = _team(client, "Platform", department_id=eng["id"], description="Owns deploys.")
    loose = _team(client, "Loose")
    member = add_user(admin)
    client.put(f"{T}/{team['id']}/members/{member}", json={"role": "lead"})

    other = seed_tenant(tier="admin")
    sign_in(other)
    _team(client, "Secret team", description="Never leaks.")
    sign_in(admin)

    resp = await _get_org_context(admin.org_id)
    assert [(d.id, d.name, d.description, d.parent_id) for d in resp.departments] == [
        (eng["id"], "Engineering", "Builds the product.", ""),
        (sub["id"], "Infra", "", eng["id"]),
    ]
    assert [(t.id, t.name, t.description, t.department_id, t.member_count) for t in resp.teams] == [
        (loose["id"], "Loose", "", "", 0),
        (team["id"], "Platform", "Owns deploys.", eng["id"], 1),
    ]

    key = f"nexus:{admin.org_id}:org_ctx"
    writes = [
        lambda: client.patch(f"{T}/{team['id']}", json={"description": "Owns CI."}),
        lambda: client.patch(f"{D}/{eng['id']}", json={"name": "Eng"}),
        lambda: client.post(T, json={"name": "New"}),
        lambda: client.post(D, json={"name": "NewDept"}),
        lambda: client.put(f"{T}/{loose['id']}/members/{member}", json={}),
        lambda: client.post(
            f"{T}/{loose['id']}/members/{member}/move", json={"to_team_id": team["id"]}
        ),
        lambda: client.delete(f"{D}/{sub['id']}"),
        lambda: client.delete(f"{T}/{loose['id']}"),
    ]
    for write in writes:
        await _get_org_context(admin.org_id)
        assert await _cached(key) is not None
        out = write()
        assert out.status_code in (200, 201, 204), out.text
        assert await _cached(key) is None, out.request.url

    resp = await _get_org_context(admin.org_id)
    assert [d.name for d in resp.departments] == ["Eng", "NewDept"]
    assert {t.name: t.description for t in resp.teams} == {"New": "", "Platform": "Owns CI."}


def test_a_second_eviction_runs_after_the_commit(client, admin, monkeypatch):
    """The in-transaction eviction can be undone by a reader that re-caches the pre-commit
    state; the `after_commit` hook evicts again once the write is durable."""
    import time

    team = _team(client, "Hooked")
    member = add_user(admin)
    client.put(f"{T}/{team['id']}/members/{member}", json={})
    calls: list[tuple[uuid.UUID, list[uuid.UUID]]] = []
    real = org_hierarchy._evict

    async def spy(org_id: uuid.UUID, users: Any) -> None:
        calls.append((org_id, sorted(users, key=str)))
        await real(org_id, users)

    monkeypatch.setattr(org_hierarchy, "_evict", spy)
    assert client.patch(f"{T}/{team['id']}", json={"name": "Hooked2"}).status_code == 200
    deadline = time.monotonic() + 5
    while len(calls) < 2 and time.monotonic() < deadline:
        time.sleep(0.05)
    assert calls == [(admin.org_id, [member]), (admin.org_id, [member])]
