"""The A2A PDP and grant store: ADR-0013, plan 10-13 section 3.2, P10 contract section 3.

- `authorize()` / gRPC `Authorize`: deny by default (no rows = deny for every combination),
  explicit deny wins, unknown values and a bad or disagreeing org deny.
- `agent_authz_scopes`: RLS isolation for the role the application connects as, fail closed
  with no `app.current_org`.
- `/api/v1/agent-authz-scopes`: owner/admin only (a member is 403, the project's convention for
  an admin route), 404 for another tenant's grant, one audit row per changed grant, the template
  adopts every role x query class at the scope and every narrower one without overriding an
  explicit deny, and every change deletes the tenant's cached decisions and nobody else's.
"""

from __future__ import annotations

import itertools
import os
import uuid
from typing import Any

import grpc
import pytest
import redis as redis_sync
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.testclient import TestClient

import identity_pb2
import identity_pb2_grpc
from libs.shared import auth as auth_mod
from libs.shared import cache as cache_mod
from libs.shared import db as db_mod
from libs.shared.tenancy import set_rls
from services.identity_tenant_service.a2a import routes as a2a_routes
from services.identity_tenant_service.a2a.pdp import (
    GRANTEE_ROLES,
    QUERY_CLASSES,
    TARGET_SCOPES,
    authorize,
    decide,
)
from services.identity_tenant_service.grpc_server import IdentityService
from tests.conftest import APP_URL
from tests.test_chat_api import PRINCIPAL, Tenant, _create_test_app, _sql, seed_tenant, sign_in
from tests.test_suggestions import seed_user

pytestmark = pytest.mark.usefixtures("migrated_db")


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    cache_mod.get_redis.cache_clear()
    app = _create_test_app("identity-a2a-test")
    app.include_router(a2a_routes.router)
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
    cache_mod.get_redis.cache_clear()


def _redis() -> redis_sync.Redis:
    return redis_sync.Redis.from_url(os.environ["REDIS_URL"])


def grant_rows(org_id: uuid.UUID) -> list[dict[str, Any]]:
    return _sql(
        "SELECT grantee_role, query_class, target_scope, effect, deleted_at "
        "FROM agent_authz_scopes WHERE org_id = %(o)s",
        {"o": str(org_id)},
    )


def seed_grant(org_id: uuid.UUID, role: str, qclass: str, scope: str, effect: str) -> uuid.UUID:
    gid = uuid.uuid4()
    _sql(
        "INSERT INTO agent_authz_scopes (id, org_id, grantee_role, query_class, target_scope, "
        "effect, created_by) VALUES (%(i)s, %(o)s, %(r)s, %(q)s, %(s)s, %(e)s, %(by)s)",
        {
            "i": str(gid),
            "o": str(org_id),
            "r": role,
            "q": qclass,
            "s": scope,
            "e": effect,
            "by": str(uuid.uuid4()),
        },
    )
    return gid


def audit_actions(org_id: uuid.UUID) -> list[str]:
    return [
        r["action"]
        for r in _sql(
            "SELECT action FROM audit_logs WHERE org_id = %(o)s AND action LIKE 'a2a.grant.%%' "
            "ORDER BY occurred_at",
            {"o": str(org_id)},
        )
    ]


async def pdp(org_id: uuid.UUID, role: str, qclass: str, scope: str) -> str:
    engine = create_async_engine(APP_URL)
    try:
        async with async_sessionmaker(engine)() as session, session.begin():
            return await authorize(session, org_id, role, qclass, scope)
    finally:
        await engine.dispose()


# --- the PDP rule ----------------------------------------------------------------------------


def test_the_pure_rule_is_deny_by_default_and_deny_wins():
    assert decide([]) == "deny"
    assert decide(["allow"]) == "allow"
    assert decide(["deny"]) == "deny"
    assert decide(["allow", "deny"]) == "deny"
    assert decide(["deny", "allow", "allow"]) == "deny"


async def test_a_tenant_with_no_rows_denies_every_combination():
    t = seed_tenant("admin")
    for role, qclass, scope in itertools.product(GRANTEE_ROLES, QUERY_CLASSES, TARGET_SCOPES):
        assert await pdp(t.org_id, role, qclass, scope) == "deny", (role, qclass, scope)


async def test_an_allow_row_allows_exactly_its_tuple_and_a_deny_row_denies():
    t = seed_tenant("admin")
    seed_grant(t.org_id, "member", "meeting_context", "same_team", "allow")
    seed_grant(t.org_id, "member", "decision_lineage", "same_team", "deny")
    assert await pdp(t.org_id, "member", "meeting_context", "same_team") == "allow"
    assert await pdp(t.org_id, "member", "decision_lineage", "same_team") == "deny"
    assert await pdp(t.org_id, "member", "meeting_context", "department") == "deny"
    assert await pdp(t.org_id, "admin", "meeting_context", "same_team") == "deny"
    # another tenant's allow never answers for this one
    other = seed_tenant("admin")
    assert await pdp(other.org_id, "member", "meeting_context", "same_team") == "deny"


async def test_unknown_values_and_revoked_rows_deny():
    t = seed_tenant("admin")
    gid = seed_grant(t.org_id, "member", "meeting_context", "org", "allow")
    assert await pdp(t.org_id, "guest", "meeting_context", "org") == "deny"
    assert await pdp(t.org_id, "member", "everything", "org") == "deny"
    assert await pdp(t.org_id, "member", "meeting_context", "galaxy") == "deny"
    _sql("UPDATE agent_authz_scopes SET deleted_at = now() WHERE id = %(i)s", {"i": str(gid)})
    assert await pdp(t.org_id, "member", "meeting_context", "org") == "deny"


# --- gRPC Authorize --------------------------------------------------------------------------


async def test_grpc_authorize_is_deny_by_default_and_fails_closed_on_bad_input():
    t = seed_tenant("admin")
    engine = create_async_engine(APP_URL)
    server = grpc.aio.server()
    identity_pb2_grpc.add_IdentityServiceServicer_to_server(
        IdentityService(async_sessionmaker(engine, expire_on_commit=False)), server
    )
    port = server.add_insecure_port("[::]:0")
    await server.start()

    def req(org: str, role: str = "member", qclass: str = "action_status", scope: str = "org"):
        return identity_pb2.AuthorizeRequest(
            org_id=org, grantee_role=role, query_class=qclass, target_scope=scope
        )

    try:
        async with grpc.aio.insecure_channel(f"localhost:{port}") as channel:
            stub = identity_pb2_grpc.IdentityServiceStub(channel)
            org = str(t.org_id)
            assert (await stub.Authorize(req(org))).decision == "deny"  # no rows
            seed_grant(t.org_id, "member", "action_status", "org", "allow")
            assert (await stub.Authorize(req(org))).decision == "allow"
            assert (await stub.Authorize(req(org, role="owner"))).decision == "deny"
            assert (await stub.Authorize(req(org, scope="nowhere"))).decision == "deny"
            assert (await stub.Authorize(req(""))).decision == "deny"
            assert (await stub.Authorize(req("not-a-uuid"))).decision == "deny"
            assert (await stub.Authorize(req(str(uuid.uuid4())))).decision == "deny"
            # metadata naming a different tenant than the body: deny
            spoofed = await stub.Authorize(req(org), metadata=(("org-id", str(uuid.uuid4())),))
            assert spoofed.decision == "deny"
            agreeing = await stub.Authorize(req(org), metadata=(("org-id", org),))
            assert agreeing.decision == "allow"
            # explicit deny on the same tuple (an admin tightening it) wins
            _sql(
                "UPDATE agent_authz_scopes SET effect = 'deny' WHERE org_id = %(o)s",
                {"o": org},
            )
            assert (await stub.Authorize(req(org))).decision == "deny"
    finally:
        await server.stop(None)
        await engine.dispose()


# --- RLS -------------------------------------------------------------------------------------


async def test_rls_isolates_agent_authz_scopes_and_fails_closed():
    a, b = seed_tenant("admin"), seed_tenant("admin")
    seed_grant(a.org_id, "member", "meeting_context", "self", "allow")
    seed_grant(b.org_id, "member", "meeting_context", "self", "allow")
    engine = create_async_engine(APP_URL)
    try:
        async with engine.connect() as conn, conn.begin():
            await set_rls(conn, a.org_id)  # type: ignore[arg-type]
            orgs = {r[0] for r in await conn.execute(text("SELECT org_id FROM agent_authz_scopes"))}
            assert orgs == {a.org_id}
            # A cannot write a row for B either (WITH CHECK)
            with pytest.raises(Exception, match="row-level security"):
                await conn.execute(
                    text(
                        "INSERT INTO agent_authz_scopes (org_id, grantee_role, query_class, "
                        "target_scope, created_by) VALUES (:o, 'member', 'action_status', "
                        "'org', :by)"
                    ),
                    {"o": str(b.org_id), "by": str(uuid.uuid4())},
                )
        async with engine.connect() as conn, conn.begin():
            rows = (await conn.execute(text("SELECT count(*) FROM agent_authz_scopes"))).scalar()
            assert rows == 0  # no app.current_org: nothing
    finally:
        await engine.dispose()


# --- REST: RBAC ------------------------------------------------------------------------------


def test_a_member_is_403_on_every_grant_route(client):
    admin = seed_tenant("admin")
    gid = seed_grant(admin.org_id, "member", "meeting_context", "self", "allow")
    member = seed_user(admin, "member")
    sign_in(member)
    body = {"grantee_role": "member", "query_class": "meeting_context", "target_scope": "org"}
    assert client.get("/api/v1/agent-authz-scopes").status_code == 403
    assert client.post("/api/v1/agent-authz-scopes", json=body).status_code == 403
    assert client.delete(f"/api/v1/agent-authz-scopes/{gid}").status_code == 403
    resp = client.post("/api/v1/agent-authz-scopes/template", json={"target_scope": "org"})
    assert resp.status_code == 403
    assert [r["deleted_at"] for r in grant_rows(admin.org_id)] == [None]
    assert len(grant_rows(admin.org_id)) == 1


def test_owner_and_admin_can_manage_and_another_tenant_is_404(client):
    owner = seed_tenant("owner")
    sign_in(owner)
    body = {"grantee_role": "member", "query_class": "decision_lineage", "target_scope": "org"}
    resp = client.post("/api/v1/agent-authz-scopes", json=body)
    assert resp.status_code == 200, resp.text
    gid = resp.json()["id"]
    assert resp.json()["effect"] == "allow"
    assert [g["id"] for g in client.get("/api/v1/agent-authz-scopes").json()["items"]] == [gid]

    stranger = seed_tenant("admin")
    sign_in(stranger)
    assert client.get("/api/v1/agent-authz-scopes").json()["items"] == []
    assert client.delete(f"/api/v1/agent-authz-scopes/{gid}").status_code == 404
    assert client.delete(f"/api/v1/agent-authz-scopes/{uuid.uuid4()}").status_code == 404

    sign_in(owner)
    assert client.delete(f"/api/v1/agent-authz-scopes/{gid}").status_code == 204
    assert client.delete(f"/api/v1/agent-authz-scopes/{gid}").status_code == 404  # already gone
    assert client.get("/api/v1/agent-authz-scopes").json()["items"] == []
    assert audit_actions(owner.org_id) == ["a2a.grant.create", "a2a.grant.revoke"]


def test_bad_values_are_422(client):
    admin = seed_tenant("admin")
    sign_in(admin)
    bad = {"grantee_role": "guest", "query_class": "meeting_context", "target_scope": "org"}
    assert client.post("/api/v1/agent-authz-scopes", json=bad).status_code == 422
    resp = client.post("/api/v1/agent-authz-scopes/template", json={"target_scope": "self"})
    assert resp.status_code == 422
    assert grant_rows(admin.org_id) == []


# --- REST: upsert, deny, template ------------------------------------------------------------


async def test_upsert_reactivates_tightens_and_audits_only_changes(client):
    admin = seed_tenant("admin")
    sign_in(admin)
    body = {"grantee_role": "admin", "query_class": "action_status", "target_scope": "department"}
    first = client.post("/api/v1/agent-authz-scopes", json=body).json()
    again = client.post("/api/v1/agent-authz-scopes", json=body).json()
    assert again["id"] == first["id"]  # idempotent, no audit for a no-op
    assert await pdp(admin.org_id, "admin", "action_status", "department") == "allow"

    tightened = client.post("/api/v1/agent-authz-scopes", json={**body, "effect": "deny"}).json()
    assert tightened["id"] == first["id"] and tightened["effect"] == "deny"
    assert await pdp(admin.org_id, "admin", "action_status", "department") == "deny"

    assert client.delete(f"/api/v1/agent-authz-scopes/{first['id']}").status_code == 204
    revived = client.post("/api/v1/agent-authz-scopes", json=body).json()
    assert revived["id"] == first["id"] and revived["effect"] == "allow"
    assert await pdp(admin.org_id, "admin", "action_status", "department") == "allow"
    assert audit_actions(admin.org_id) == [
        "a2a.grant.create",
        "a2a.grant.create",
        "a2a.grant.revoke",
        "a2a.grant.create",
    ]


async def test_template_grants_every_role_and_class_at_and_below_the_scope(client):
    admin = seed_tenant("admin")
    sign_in(admin)
    resp = client.post("/api/v1/agent-authz-scopes/template", json={"target_scope": "same_team"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["created"] == 18  # {self, same_team} x 3 roles x 3 classes
    assert len(body["items"]) == 18
    assert {i["target_scope"] for i in body["items"]} == {"self", "same_team"}
    assert {i["effect"] for i in body["items"]} == {"allow"}
    assert await pdp(admin.org_id, "owner", "decision_lineage", "same_team") == "allow"
    assert await pdp(admin.org_id, "member", "meeting_context", "department") == "deny"
    assert audit_actions(admin.org_id) == ["a2a.grant.create"] * 18

    # widening adds only the new rows; repeating it adds nothing
    wider = client.post("/api/v1/agent-authz-scopes/template", json={"target_scope": "org"}).json()
    assert wider["created"] == 18 and len(wider["items"]) == 36
    again = client.post("/api/v1/agent-authz-scopes/template", json={"target_scope": "org"}).json()
    assert again["created"] == 0
    assert len(audit_actions(admin.org_id)) == 36


async def test_the_template_never_overrides_an_explicit_deny(client):
    admin = seed_tenant("admin")
    seed_grant(admin.org_id, "member", "decision_lineage", "same_team", "deny")
    sign_in(admin)
    body = client.post("/api/v1/agent-authz-scopes/template", json={"target_scope": "org"}).json()
    assert body["created"] == 35  # 36 minus the one denied tuple
    assert await pdp(admin.org_id, "member", "decision_lineage", "same_team") == "deny"
    assert await pdp(admin.org_id, "member", "decision_lineage", "department") == "allow"


# --- decision cache invalidation -------------------------------------------------------------


def _cache_keys(t: Tenant) -> list[str]:
    return [
        f"nexus:{t.org_id}:a2a:grant:member:meeting_context:same_team",
        f"nexus:{t.org_id}:a2a:grant:admin:action_status:org",
    ]


def test_every_change_clears_this_tenants_cached_decisions_only(client):
    admin, neighbour = seed_tenant("admin"), seed_tenant("admin")
    r = _redis()
    unrelated = f"nexus:{admin.org_id}:user_ctx:{admin.user_id}"
    sign_in(admin)
    body = {"grantee_role": "member", "query_class": "meeting_context", "target_scope": "same_team"}

    def prime() -> None:
        for key in _cache_keys(admin) + _cache_keys(neighbour):
            r.set(key, "deny", ex=60)
        r.set(unrelated, "x", ex=60)

    prime()
    gid = client.post("/api/v1/agent-authz-scopes", json=body).json()["id"]
    assert [r.exists(k) for k in _cache_keys(admin)] == [0, 0]

    prime()
    assert client.delete(f"/api/v1/agent-authz-scopes/{gid}").status_code == 204
    assert [r.exists(k) for k in _cache_keys(admin)] == [0, 0]  # revoke clears the cache

    prime()
    client.post("/api/v1/agent-authz-scopes/template", json={"target_scope": "department"})
    assert [r.exists(k) for k in _cache_keys(admin)] == [0, 0]

    # the neighbour's decisions and this tenant's other keys are untouched
    assert [r.exists(k) for k in _cache_keys(neighbour)] == [1, 1]
    assert r.exists(unrelated) == 1
    r.delete(*_cache_keys(neighbour), unrelated)
