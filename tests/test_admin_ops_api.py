"""admin-ops-service: `/admin/tenant/*` and `/admin/ops/*` (P12 contract section 2, plan 3.13).

The real admin-ops routers run against the real Postgres as `mytheus_app`; the identity calls
they make go in-process (httpx `ASGITransport`) to the real identity-tenant-service routers
(`/rbac/grants`, `/tenant-settings`, `/internal/tenant-status`), so identity's own RBAC rules,
audit rows and outbox rows are what is asserted. Only `verify_token` is overridden (no live
Clerk); everything after it (org resolution, `app.current_org`, RLS, tiers) is real.

Covered:
- tenant-admin routes are owner/admin only (a member is 403 and the denial is audited) and every
  write lands in `audit_logs` (identity's rows for members/settings, this service's for support
  grants and export requests);
- member tier changes reuse identity's rules (an admin cannot mint an owner);
- the data-export request records `admin-events` `erasure_requested` (`kind: export`), 202
  pending;
- RLS on `support_grants` and `feature_flags`, and `ops_audit_log` is append-only;
- ops routes deny non-staff (deny by default when `MYTHEUS_STAFF_USER_IDS` is unset) and every
  call, reads and denials included, is in `ops_audit_log`;
- suspend flips `organizations.status` through identity (its `tenant-events` and audit row) and
  emits `admin-events` `tenant_suspended`; reinstate flips it back;
- a flag change emits `admin-events` `flag_changed`;
- the cost dashboard touches only aggregate tables.
"""

from __future__ import annotations

import os
import re
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import psycopg2
import psycopg2.extras
import pytest
from fastapi import FastAPI
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import create_async_engine
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import authz as authz_mod
from libs.shared import cache as cache_mod
from libs.shared import db as db_mod
from libs.shared.errors import add_exception_handlers
from services.admin_ops_service import common, ops_routes, tenant_routes
from services.identity_tenant_service.routes import internal_status, rbac, tenant_settings
from tests.conftest import APP_URL, BOOTSTRAP_URL

pytestmark = pytest.mark.usefixtures("migrated_db")

SECRET = "i" * 40


class Principal:
    clerk_org = ""
    clerk_user = ""


PRINCIPAL = Principal()


def _claims() -> dict[str, str]:
    return {"sub": PRINCIPAL.clerk_user, "org_id": PRINCIPAL.clerk_org}


def _sql(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    conn = psycopg2.connect(BOOTSTRAP_URL.replace("+asyncpg", ""))
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or {})
            return [dict(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


class Tenant:
    def __init__(self, org_id: uuid.UUID, clerk_org: str) -> None:
        self.org_id = org_id
        self.clerk_org = clerk_org

    def add_user(self, tier: str | None, email: str | None = None) -> User:
        user_id = uuid.uuid4()
        clerk_user = f"user_{uuid.uuid4().hex[:16]}"
        _sql(
            "INSERT INTO users (id, org_id, clerk_user_id, email) VALUES (%(u)s,%(o)s,%(c)s,%(e)s)",
            {
                "u": str(user_id),
                "o": str(self.org_id),
                "c": clerk_user,
                "e": email or f"{user_id.hex[:8]}@example.com",
            },
        )
        if tier:
            _sql(
                "INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (%(o)s, %(u)s, %(t)s)",
                {"o": str(self.org_id), "u": str(user_id), "t": tier},
            )
        return User(self, user_id, clerk_user)


class User:
    def __init__(self, tenant: Tenant, user_id: uuid.UUID, clerk_user: str) -> None:
        self.tenant = tenant
        self.org_id = tenant.org_id
        self.user_id = user_id
        self.clerk_user = clerk_user

    def sign_in(self) -> User:
        PRINCIPAL.clerk_org = self.tenant.clerk_org
        PRINCIPAL.clerk_user = self.clerk_user
        return self


def new_tenant(name: str = "Org") -> Tenant:
    org_id = uuid.uuid4()
    clerk_org = f"org_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO organizations (id, name, slug, clerk_org_id) VALUES (%(i)s,%(n)s,%(s)s,%(c)s)",
        {"i": str(org_id), "n": name, "s": f"ao-{org_id}", "c": clerk_org},
    )
    return Tenant(org_id, clerk_org)


def audit_rows(org_id: uuid.UUID, action: str) -> list[dict[str, Any]]:
    return _sql(
        "SELECT actor_id, actor_type, target_type, target_id, metadata FROM audit_logs "
        "WHERE org_id = %(o)s AND action = %(a)s ORDER BY occurred_at",
        {"o": str(org_id), "a": action},
    )


def outbox_rows(org_id: uuid.UUID, topic: str) -> list[dict[str, Any]]:
    return _sql(
        "SELECT idempotency_key, payload FROM outbox WHERE org_id = %(o)s AND topic = %(t)s "
        "ORDER BY id",
        {"o": str(org_id), "t": topic},
    )


def ops_rows(staff_user_id: uuid.UUID | None, action: str | None = None) -> list[dict[str, Any]]:
    where = "staff_user_id = %(s)s" if staff_user_id else "staff_user_id IS NULL"
    if action:
        where += " AND action = %(a)s"
    rows = _sql(
        f"SELECT action, target_org_id, details FROM ops_audit_log WHERE {where} "
        "ORDER BY created_at",
        {"s": str(staff_user_id) if staff_user_id else None, "a": action},
    )
    for row in rows:
        if row["target_org_id"] is not None:
            row["target_org_id"] = uuid.UUID(str(row["target_org_id"]))
    return rows


@pytest.fixture(scope="module")
def apps(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ[common.IDENTITY_TOKENS_ENV] = f"admin-ops-service={SECRET}"
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    cache_mod.get_redis.cache_clear()

    identity = FastAPI()
    add_exception_handlers(identity)
    for router in (rbac.router, tenant_settings.router, internal_status.router):
        identity.include_router(router)
    identity.dependency_overrides[auth_mod.verify_token] = _claims

    admin = FastAPI()
    add_exception_handlers(admin)
    admin.include_router(tenant_routes.router)
    admin.include_router(ops_routes.router)
    admin.dependency_overrides[auth_mod.verify_token] = _claims
    admin.dependency_overrides[common.identity_transport] = lambda: httpx.ASGITransport(
        app=identity
    )
    # Identity is also mounted under the same test app, so direct calls to it share one event
    # loop (and the one cached engine's connections) with the admin calls.
    admin.mount("/_identity", identity)
    with TestClient(admin) as client:
        yield client

    os.environ.pop(common.IDENTITY_TOKENS_ENV, None)
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    cache_mod.get_redis.cache_clear()


@pytest.fixture
def client(apps):
    return apps


@pytest.fixture
def staff(monkeypatch):
    """A Mytheus staff member in the Mytheus org, configured in MYTHEUS_STAFF_USER_IDS."""
    user = new_tenant("Mytheus").add_user("member")
    monkeypatch.setenv(common.STAFF_ENV, f"{uuid.uuid4()}, not-a-uuid, {user.user_id}")
    return user.sign_in()


# --- tenant admin ------------------------------------------------------------------------------


def test_tenant_admin_routes_are_owner_or_admin_only_and_denials_are_audited(client):
    t = new_tenant()
    member = t.add_user("member").sign_in()
    grant = str(uuid.uuid4())
    calls = [
        ("get", "/admin/tenant/members", None),
        ("patch", f"/admin/tenant/members/{member.user_id}", {"tier": "admin"}),
        ("delete", f"/admin/tenant/members/{member.user_id}", None),
        ("get", "/admin/tenant/settings", None),
        ("patch", "/admin/tenant/settings", {"pii_redaction_enabled": False}),
        ("get", "/admin/tenant/support-grants", None),
        (
            "post",
            "/admin/tenant/support-grants",
            {"staff_user_id": grant, "expires_at": "2099-01-01T00:00:00Z"},
        ),
        ("delete", f"/admin/tenant/support-grants/{grant}", None),
        ("post", "/admin/tenant/data-export", {}),
        ("get", "/admin/tenant/data-export", None),
    ]
    for method, path, body in calls:
        kwargs = {"json": body} if body is not None else {}
        resp = client.request(method.upper(), path, **kwargs)
        assert resp.status_code == 403, (path, resp.text)
        assert resp.json()["code"] == "FORBIDDEN_ROLE"
    denials = audit_rows(t.org_id, authz_mod.ACTION_AUTHZ_DENY)
    assert len(denials) == len(calls)
    assert all(d["metadata"]["required"] == "admin" for d in denials)

    t.add_user("owner").sign_in()
    assert client.get("/admin/tenant/members").status_code == 200
    t.add_user("admin").sign_in()
    assert client.get("/admin/tenant/support-grants").status_code == 200


def test_member_tier_changes_reuse_identity_rules_and_are_audited(client):
    t = new_tenant()
    admin = t.add_user("admin", "admin@example.com")
    member = t.add_user("member", "member@example.com")
    stranger = new_tenant().add_user("member")
    admin.sign_in()

    listed = client.get("/admin/tenant/members").json()["items"]
    assert {(m["email"], m["tier"]) for m in listed} == {
        ("admin@example.com", "admin"),
        ("member@example.com", "member"),
    }

    resp = client.patch(f"/admin/tenant/members/{member.user_id}", json={"tier": "admin"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["tier"] == "admin" and resp.json()["email"] == "member@example.com"
    granted = audit_rows(t.org_id, "rbac.granted")
    assert len(granted) == 1 and granted[0]["metadata"] == {"from": "member", "to": "admin"}
    assert str(granted[0]["actor_id"]) == str(admin.user_id)

    # identity's no-escalation rule, passed through unchanged
    escalate = client.patch(f"/admin/tenant/members/{member.user_id}", json={"tier": "owner"})
    assert escalate.status_code == 403 and escalate.json()["code"] == "FORBIDDEN_ROLE"
    # another tenant's user is invisible: 404, not 403
    foreign = client.patch(f"/admin/tenant/members/{stranger.user_id}", json={"tier": "member"})
    assert foreign.status_code == 404

    t.add_user("owner").sign_in()
    assert client.delete(f"/admin/tenant/members/{member.user_id}").status_code == 204
    assert _sql("SELECT 1 FROM rbac_grants WHERE user_id = %(u)s", {"u": str(member.user_id)}) == []
    assert len(audit_rows(t.org_id, "rbac.revoked")) == 1


def test_settings_go_through_identity_and_are_audited(client):
    t = new_tenant()
    t.add_user("admin").sign_in()
    before = client.get("/admin/tenant/settings")
    assert before.status_code == 200, before.text
    assert before.json()["retention_days_transcripts"] == 365

    change = {
        "retention_days_transcripts": 30,
        "pii_redaction_enabled": False,
        "agent_confidence_overrides": {"DecisionAgent": {"surface": 0.8, "silent": 0.5}},
    }
    resp = client.patch("/admin/tenant/settings", json=change)
    assert resp.status_code == 200, resp.text
    assert resp.json() == {**change, "retention_days_worklogs": 365}
    assert client.get("/admin/tenant/settings").json() == resp.json()
    row = _sql(
        "SELECT retention_days_transcripts, agent_confidence_overrides FROM tenant_settings "
        "WHERE org_id = %(o)s",
        {"o": str(t.org_id)},
    )[0]
    assert row["retention_days_transcripts"] == 30
    assert row["agent_confidence_overrides"]["DecisionAgent"]["silent"] == 0.5
    audited = audit_rows(t.org_id, tenant_settings.ACTION_TENANT_SETTINGS_UPDATED)
    assert len(audited) == 1 and audited[0]["metadata"]["retention_days_transcripts"] == 30

    bad = {"agent_confidence_overrides": {"DecisionAgent": {"surface": 0.3, "silent": 0.6}}}
    assert client.patch("/admin/tenant/settings", json=bad).status_code == 422
    assert client.patch("/admin/tenant/settings", json={}).status_code == 422


def test_support_grants_are_time_boxed_staff_only_and_audited(client, monkeypatch):
    staff_id = uuid.uuid4()
    monkeypatch.setenv(common.STAFF_ENV, str(staff_id))
    t = new_tenant()
    admin = t.add_user("admin").sign_in()
    soon = (datetime.now(UTC) + timedelta(days=2)).isoformat()

    not_staff = {"staff_user_id": str(uuid.uuid4()), "expires_at": soon}
    assert client.post("/admin/tenant/support-grants", json=not_staff).status_code == 422
    too_long = (datetime.now(UTC) + timedelta(days=31)).isoformat()
    long_body = {"staff_user_id": str(staff_id), "expires_at": too_long}
    assert client.post("/admin/tenant/support-grants", json=long_body).status_code == 422
    past = {"staff_user_id": str(staff_id), "expires_at": "2020-01-01T00:00:00Z"}
    assert client.post("/admin/tenant/support-grants", json=past).status_code == 422

    body = {"staff_user_id": str(staff_id), "scope": "impersonate", "expires_at": soon}
    created = client.post("/admin/tenant/support-grants", json=body)
    assert created.status_code == 201, created.text
    grant = created.json()
    assert grant["granted_by"] == str(admin.user_id) and grant["revoked_at"] is None
    assert [g["id"] for g in client.get("/admin/tenant/support-grants").json()["items"]] == [
        grant["id"]
    ]
    assert len(audit_rows(t.org_id, tenant_routes.ACTION_SUPPORT_GRANT_CREATED)) == 1

    # another tenant cannot see or revoke it
    new_tenant().add_user("admin").sign_in()
    assert client.get("/admin/tenant/support-grants").json()["items"] == []
    assert client.delete(f"/admin/tenant/support-grants/{grant['id']}").status_code == 404

    admin.sign_in()
    assert client.delete(f"/admin/tenant/support-grants/{grant['id']}").status_code == 204
    assert client.delete(f"/admin/tenant/support-grants/{grant['id']}").status_code == 204
    assert len(audit_rows(t.org_id, tenant_routes.ACTION_SUPPORT_GRANT_REVOKED)) == 1
    listed = client.get("/admin/tenant/support-grants").json()["items"]
    assert listed[0]["revoked_at"] is not None


def test_data_export_request_records_an_admin_event_and_is_pending(client):
    t = new_tenant()
    admin = t.add_user("owner").sign_in()
    resp = client.post("/admin/tenant/data-export", json={})
    assert resp.status_code == 202, resp.text
    export = resp.json()
    assert export["status"] == "pending" and export["manifest_url"] is None

    audited = audit_rows(t.org_id, tenant_routes.ACTION_DATA_EXPORT_REQUESTED)
    assert len(audited) == 1 and audited[0]["target_id"] == export["id"]
    events = outbox_rows(t.org_id, "admin-events")
    assert len(events) == 1
    payload = events[0]["payload"]
    assert payload["event_type"] == "erasure_requested"
    assert payload["target_id"] == export["id"] and payload["actor_id"] == str(admin.user_id)
    assert '"kind": "export"' in payload["detail_json"]
    assert payload["header"]["org_id"] == str(t.org_id)

    listed = client.get("/admin/tenant/data-export").json()["items"]
    assert [(e["id"], e["status"]) for e in listed] == [(export["id"], "pending")]


# --- RLS and grants on the new tables ----------------------------------------------------------


async def test_support_grants_and_feature_flags_are_tenant_isolated():
    a, b = new_tenant(), new_tenant()
    for t in (a, b):
        _sql(
            "INSERT INTO support_grants (org_id, granted_by, staff_user_id, scope, expires_at) "
            "VALUES (%(o)s, %(g)s, %(s)s, 'read_only', now() + interval '1 day')",
            {"o": str(t.org_id), "g": str(uuid.uuid4()), "s": str(uuid.uuid4())},
        )
        _sql(
            "INSERT INTO feature_flags (key, org_id, enabled) VALUES (%(k)s, %(o)s, true)",
            {"k": f"flag-{t.org_id.hex[:8]}", "o": str(t.org_id)},
        )
    global_key = f"global-{uuid.uuid4().hex[:8]}"
    _sql("INSERT INTO feature_flags (key, enabled) VALUES (%(k)s, true)", {"k": global_key})

    engine = create_async_engine(APP_URL)
    try:
        async with engine.connect() as conn:
            # no tenant bound: nothing tenant-scoped, globals only
            assert (await conn.execute(text("SELECT org_id FROM support_grants"))).all() == []
            orgs = (await conn.execute(text("SELECT org_id FROM feature_flags"))).scalars().all()
            assert set(orgs) == {None}
        async with engine.begin() as conn:
            await conn.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(a.org_id)}
            )
            grants = (await conn.execute(text("SELECT org_id FROM support_grants"))).scalars()
            assert set(grants.all()) == {a.org_id}
            flags = (await conn.execute(text("SELECT key, org_id FROM feature_flags"))).all()
            assert {o for _, o in flags} == {a.org_id, None}
            assert global_key in {k for k, _ in flags}
            # a tenant session can neither write another tenant's row nor a global one
            for org in (str(b.org_id), None):
                with pytest.raises(Exception, match="row-level security"):
                    async with conn.begin_nested():
                        await conn.execute(
                            text("INSERT INTO feature_flags (key, org_id) VALUES ('x', :o)"),
                            {"o": org},
                        )
        async with engine.begin() as conn:
            await conn.execute(text("SELECT set_config('app.ops_console', 'on', true)"))
            orgs = set((await conn.execute(text("SELECT org_id FROM feature_flags"))).scalars())
            assert {a.org_id, b.org_id, None} <= orgs
    finally:
        await engine.dispose()


async def test_ops_audit_log_is_append_only_for_the_app_role():
    engine = create_async_engine(APP_URL)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("INSERT INTO ops_audit_log (action) VALUES ('probe')"))
        for statement in (
            "UPDATE ops_audit_log SET action = 'x'",
            "DELETE FROM ops_audit_log",
        ):
            with pytest.raises(Exception, match="permission denied"):
                async with engine.begin() as conn:
                    await conn.execute(text(statement))
    finally:
        await engine.dispose()


# --- internal ops ------------------------------------------------------------------------------


OPS_CALLS: list[tuple[str, str, dict[str, Any] | None]] = [
    ("GET", "/admin/ops/tenants", None),
    ("POST", "/admin/ops/tenants/suspend", {"org_id": str(uuid.uuid4()), "reason": "x"}),
    ("POST", "/admin/ops/tenants/reinstate", {"org_id": str(uuid.uuid4())}),
    ("GET", "/admin/ops/flags", None),
    ("POST", "/admin/ops/flags", {"key": "probe"}),
    ("PATCH", f"/admin/ops/flags/{uuid.uuid4()}", {"enabled": True}),
    ("DELETE", f"/admin/ops/flags/{uuid.uuid4()}", None),
    ("GET", "/admin/ops/costs", None),
    ("POST", "/admin/ops/audit/search", {"org_id": str(uuid.uuid4())}),
]


def test_ops_routes_deny_non_staff_and_log_every_denial(client, monkeypatch):
    monkeypatch.delenv(common.STAFF_ENV, raising=False)
    owner = new_tenant().add_user("owner").sign_in()  # a tenant owner is still not staff
    for method, path, body in OPS_CALLS:
        kwargs = {"json": body} if body is not None else {}
        resp = client.request(method, path, **kwargs)
        assert resp.status_code == 403, (path, resp.text)
    denied = ops_rows(owner.user_id, "ops.denied")
    assert [d["details"]["path"] for d in denied] == [p for _, p, _ in OPS_CALLS]

    assert client.get("/admin/ops/whoami").json() == {"staff": False}
    assert ops_rows(owner.user_id, "ops.whoami")[0]["details"]["staff"] is False

    # deny by default: configured staff of someone else does not make this caller staff
    monkeypatch.setenv(common.STAFF_ENV, str(uuid.uuid4()))
    assert client.get("/admin/ops/tenants").status_code == 403


def test_staff_reads_are_logged_too(client, staff):
    target = new_tenant("Target")
    target.add_user("owner")
    target.add_user("member")
    assert client.get("/admin/ops/whoami").json() == {"staff": True}

    tenants = client.get("/admin/ops/tenants")
    assert tenants.status_code == 200, tenants.text
    row = next(i for i in tenants.json()["items"] if i["org_id"] == str(target.org_id))
    assert row["name"] == "Target" and row["member_count"] == 2 and row["status"] == "active"

    # keyset paging, newest first: the target (created last) leads page one, never repeats
    first = client.get("/admin/ops/tenants", params={"limit": 1}).json()
    assert [i["org_id"] for i in first["items"]] == [str(target.org_id)]
    second = client.get("/admin/ops/tenants", params={"limit": 1, "cursor": first["next_cursor"]})
    assert second.json()["items"][0]["org_id"] != str(target.org_id)
    assert client.get("/admin/ops/tenants", params={"cursor": "!!"}).status_code == 400

    assert client.get("/admin/ops/flags").status_code == 200
    assert client.get("/admin/ops/costs").status_code == 200
    search = client.post("/admin/ops/audit/search", json={"org_id": str(target.org_id)})
    assert search.status_code == 200, search.text

    actions = [r["action"] for r in ops_rows(staff.user_id)]
    assert actions == [
        "ops.whoami",
        "tenants.list",
        "tenants.list",
        "tenants.list",
        "tenants.list",
        "flags.list",
        "costs.read",
        "audit.read",
    ]
    assert ops_rows(staff.user_id, "audit.read")[0]["target_org_id"] == target.org_id


def test_suspend_flips_status_through_identity_and_emits_admin_events(client, staff):
    target = new_tenant()
    tenant_user = target.add_user("owner")
    suspend = {"org_id": str(target.org_id), "reason": "unpaid invoices"}

    resp = client.post("/admin/ops/tenants/suspend", json=suspend)
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"org_id": str(target.org_id), "status": "suspended"}
    status_of = "SELECT status FROM organizations WHERE id = %(o)s"
    assert _sql(status_of, {"o": str(target.org_id)})[0]["status"] == "suspended"

    # identity: the sole tenant-events producer, and the tenant's own audit trail
    tenant_events = outbox_rows(target.org_id, "tenant-events")
    assert [e["payload"]["event_type"] for e in tenant_events] == ["suspended"]
    audited = audit_rows(target.org_id, internal_status.ACTION_TENANT_SUSPENDED)
    assert len(audited) == 1 and audited[0]["actor_type"] == "system"
    assert audited[0]["metadata"]["staff_user_id"] == str(staff.user_id)
    # admin-ops: the admin-events record and the ops log
    admin_events = outbox_rows(target.org_id, "admin-events")
    assert [e["payload"]["event_type"] for e in admin_events] == ["tenant_suspended"]
    assert admin_events[0]["payload"]["actor_id"] == str(staff.user_id)
    logged = ops_rows(staff.user_id, "tenant.suspend")
    assert logged[0]["target_org_id"] == target.org_id
    assert logged[0]["details"]["outcome"] == "changed"

    # the suspended tenant is locked out of its own console
    tenant_user.sign_in()
    assert client.get("/admin/tenant/members").status_code in (401, 403, 404)
    staff.sign_in()

    # repeating it changes nothing and emits nothing more
    assert client.post("/admin/ops/tenants/suspend", json=suspend).status_code == 200
    assert len(outbox_rows(target.org_id, "admin-events")) == 1
    assert len(outbox_rows(target.org_id, "tenant-events")) == 1

    back = client.post("/admin/ops/tenants/reinstate", json={"org_id": str(target.org_id)})
    assert back.status_code == 200 and back.json()["status"] == "active"
    assert _sql(status_of, {"o": str(target.org_id)})[0]["status"] == "active"
    assert len(audit_rows(target.org_id, internal_status.ACTION_TENANT_REINSTATED)) == 1
    assert ops_rows(staff.user_id, "tenant.reinstate")[0]["details"]["outcome"] == "changed"

    own = {"org_id": str(staff.org_id), "reason": "oops"}
    assert client.post("/admin/ops/tenants/suspend", json=own).status_code == 409
    unknown = {"org_id": str(uuid.uuid4()), "reason": "x"}
    assert client.post("/admin/ops/tenants/suspend", json=unknown).status_code == 404
    outcomes = [r["details"]["outcome"] for r in ops_rows(staff.user_id, "tenant.suspend")]
    assert outcomes == ["changed", "unchanged", "refused_own_org", "failed"]


def test_identity_tenant_status_route_requires_the_admin_ops_token(client):
    target = new_tenant()
    good = {
        "X-Mytheus-Service": "admin-ops-service",
        "X-Mytheus-Service-Token": SECRET,
        "X-Mytheus-Org": str(target.org_id),
    }
    body = {"status": "suspended"}
    route = "/_identity/internal/tenant-status"
    assert client.post(route, json=body).status_code == 401
    bad = {**good, "X-Mytheus-Service-Token": "x" * 40}
    assert client.post(route, json=body, headers=bad).status_code == 401
    other = {**good, "X-Mytheus-Service": "workflow-service"}
    assert client.post(route, json=body, headers=other).status_code == 401
    ok = client.post(route, json={"status": "active"}, headers=good)
    assert ok.status_code == 200 and ok.json()["changed"] is False


def test_flag_changes_emit_admin_events(client, staff):
    target = new_tenant()
    key = f"proactive.{uuid.uuid4().hex[:8]}"
    glob = client.post("/admin/ops/flags", json={"key": key, "enabled": True})
    assert glob.status_code == 201, glob.text
    scoped = client.post(
        "/admin/ops/flags",
        json={"key": key, "org_id": str(target.org_id), "rules": {"pct": 10}},
    )
    assert scoped.status_code == 201, scoped.text
    assert client.post("/admin/ops/flags", json={"key": key}).status_code == 409
    unknown = {"key": key, "org_id": str(uuid.uuid4())}
    assert client.post("/admin/ops/flags", json=unknown).status_code == 404

    flag_id = scoped.json()["id"]
    patched = client.patch(f"/admin/ops/flags/{flag_id}", json={"enabled": True})
    assert patched.status_code == 200 and patched.json()["enabled"] is True
    assert patched.json()["updated_by"] == str(staff.user_id)
    assert client.delete(f"/admin/ops/flags/{flag_id}").status_code == 204
    assert client.delete(f"/admin/ops/flags/{flag_id}").status_code == 404

    scoped_events = outbox_rows(target.org_id, "admin-events")
    assert [e["payload"]["event_type"] for e in scoped_events] == ["flag_changed"] * 3
    ops = [e["payload"]["detail_json"] for e in scoped_events]
    assert '"op": "created"' in ops[0] and '"op": "updated"' in ops[1]
    assert '"op": "deleted"' in ops[2]
    # a global flag's event is keyed to the staff member's own org (no org of its own)
    global_events = outbox_rows(staff.org_id, "admin-events")
    assert len(global_events) == 1 and global_events[0]["payload"]["target_id"] == glob.json()["id"]
    listed = {f["id"] for f in client.get("/admin/ops/flags").json()["items"]}
    assert glob.json()["id"] in listed and flag_id not in listed
    actions = [r["action"] for r in ops_rows(staff.user_id) if r["action"].startswith("flag.")]
    assert actions == ["flag.create", "flag.create", "flag.update", "flag.delete", "flag.delete"]


_TABLE_REF = re.compile(r"\b(?:FROM|JOIN|INTO|UPDATE)\s+([a-z_][a-z0-9_]*)", re.IGNORECASE)


def test_cost_dashboard_reads_only_aggregate_tables(client, staff):
    target = new_tenant("Costly")
    period = ops_routes.current_period()
    _sql(
        "INSERT INTO usage_aggregates (org_id, dimension, billing_period, quantity) "
        "VALUES (%(o)s, 'agent_token', %(p)s, 12345)",
        {"o": str(target.org_id), "p": period},
    )
    _sql(
        "INSERT INTO agent_budgets (org_id, period, token_quota, hard_cap) "
        "VALUES (%(o)s, %(p)s, 200000, 250000)",
        {"o": str(target.org_id), "p": period},
    )
    # a tenant-content row that must never be read by the dashboard
    _sql(
        "INSERT INTO meetings (org_id, title) VALUES (%(o)s, 'secret strategy')",
        {"o": str(target.org_id)},
    )

    statements: list[str] = []
    engine = db_mod.get_engine()

    def capture(conn, cursor, statement, parameters, context, executemany):  # noqa: ANN001
        statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", capture)
    try:
        resp = client.get("/admin/ops/costs")
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture)
    assert resp.status_code == 200, resp.text
    report = resp.json()
    assert report["period"] == period and report["dlq"] is None and report["consumer_lag"] is None
    row = next(t for t in report["tenants"] if t["org_id"] == str(target.org_id))
    assert row == {
        "org_id": str(target.org_id),
        "name": "Costly",
        "tokens_consumed": 12345,
        "token_quota": 200000,
    }

    # The request's own authentication (who is calling: the staff member's own org and user
    # row) is the same for every route; everything else is what the dashboard read.
    def auth_chain(statement: str) -> bool:
        return "clerk_user_id" in statement or "clerk_org_id" in statement

    touched = {
        table.lower()
        for statement in statements
        if not auth_chain(statement)
        for table in _TABLE_REF.findall(statement)
    }
    assert any("clerk_user_id" in s for s in statements)
    assert statements, "the listener saw nothing; the assertion below would be vacuous"
    assert touched <= ops_routes.COST_TABLES | {"ops_audit_log"}, touched
    assert {"usage_aggregates", "agent_budgets"} <= touched


def test_audit_search_reads_one_tenant_pages_and_is_logged(client, staff):
    target, other = new_tenant(), new_tenant()
    for i in range(3):
        _sql(
            "INSERT INTO audit_logs (org_id, actor_type, action, occurred_at) "
            "VALUES (%(o)s, 'system', %(a)s, now() - %(i)s * interval '1 minute')",
            {"o": str(target.org_id), "a": f"probe.{i}", "i": i},
        )
    _sql(
        "INSERT INTO audit_logs (org_id, actor_type, action) VALUES (%(o)s, 'system', 'other')",
        {"o": str(other.org_id)},
    )
    first = client.post("/admin/ops/audit/search", json={"org_id": str(target.org_id), "limit": 2})
    assert first.status_code == 200, first.text
    page = first.json()
    assert [i["action"] for i in page["items"]] == ["probe.0", "probe.1"]
    assert all(i["org_id"] == str(target.org_id) for i in page["items"])
    second = client.post(
        "/admin/ops/audit/search",
        json={"org_id": str(target.org_id), "limit": 2, "cursor": page["next_cursor"]},
    ).json()
    assert [i["action"] for i in second["items"]] == ["probe.2"] and second["next_cursor"] is None
    bad = {"org_id": str(target.org_id), "cursor": "!!"}
    assert client.post("/admin/ops/audit/search", json=bad).status_code == 400
    reads = ops_rows(staff.user_id, "audit.read")
    assert [r["target_org_id"] for r in reads] == [target.org_id] * 3
    assert reads[-1]["details"] == {"outcome": "bad_cursor"}
