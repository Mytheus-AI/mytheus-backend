# ruff: noqa: F811  (pytest fixtures imported from test_admin_ops_api are parameters by name)
"""The superadmin console (ADR-0046): overview, workspace detail, health, staff, move requests
and support sessions, plus the `act`-claim gate in `tenant_context` and the audit tagging.

Runs on the same real-Postgres fixture as `test_admin_ops_api.py` (only `verify_token` and the
two outbound transports, Clerk and the health probes, are faked).
"""

from __future__ import annotations

import uuid
from typing import Any

import httpx
import pytest

from libs.shared import clerk_admin
from services.admin_ops_service import console_routes, ops_routes
from tests.test_admin_ops_api import (  # noqa: F401  (fixtures are used by name)
    PRINCIPAL,
    _sql,
    apps,
    audit_rows,
    client,
    new_tenant,
    ops_rows,
    staff,
)

pytestmark = pytest.mark.usefixtures("migrated_db")


def grant(org_id: uuid.UUID, staff_user_id: uuid.UUID, scope: str = "impersonate") -> str:
    rows = _sql(
        "INSERT INTO support_grants (org_id, granted_by, staff_user_id, scope, expires_at) "
        "VALUES (%(o)s, %(g)s, %(s)s, %(scope)s, now() + interval '1 day') RETURNING id",
        {"o": str(org_id), "g": str(uuid.uuid4()), "s": str(staff_user_id), "scope": scope},
    )
    return str(rows[0]["id"])


@pytest.fixture
def fake_clerk(monkeypatch):
    calls: list[dict[str, Any]] = []
    revoked: list[str] = []

    async def create(clerk_user_id: str, actor: str, *, expires_in_seconds: int):
        calls.append({"user": clerk_user_id, "actor": actor, "seconds": expires_in_seconds})
        return clerk_admin.ActorToken(id=f"act_{len(calls)}", url="https://clerk.test/ticket")

    async def revoke(token_id: str) -> bool:
        revoked.append(token_id)
        return True

    monkeypatch.setattr(clerk_admin, "create_actor_token", create)
    monkeypatch.setattr(clerk_admin, "revoke_actor_token", revoke)
    return calls, revoked


def test_console_routes_deny_non_staff(client, monkeypatch):
    monkeypatch.delenv("MYTHEUS_STAFF_USER_IDS", raising=False)
    outsider = new_tenant().add_user("owner").sign_in()
    target = str(new_tenant().org_id)
    for method, path, body in (
        ("GET", "/admin/ops/overview", None),
        ("GET", "/admin/ops/health", None),
        ("GET", "/admin/ops/staff", None),
        ("POST", "/admin/ops/tenants/detail", {"org_id": target}),
        ("POST", "/admin/ops/support/members", {"org_id": target}),
    ):
        assert client.request(method, path, json=body).status_code == 403, path
    assert len(ops_rows(outsider.user_id, "ops.denied")) == 5


def test_overview_counts_every_workspace_and_is_logged(client, staff):
    t = new_tenant("Busy")
    t.add_user("owner")
    t.add_user("member")
    for _ in range(3):
        _sql("INSERT INTO meetings (org_id, title) VALUES (%(o)s, 'x')", {"o": str(t.org_id)})
    _sql(
        "INSERT INTO usage_aggregates (org_id, dimension, billing_period, quantity) "
        "VALUES (%(o)s, 'agent_token', %(p)s, 5000)",
        {"o": str(t.org_id), "p": ops_routes.current_period()},
    )

    resp = client.get("/admin/ops/overview")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    totals = body["totals"]
    expected_tenants = _sql("SELECT count(*) AS n FROM organizations WHERE deleted_at IS NULL")
    assert totals["tenants"] == expected_tenants[0]["n"]
    assert totals["meetings_7d"] >= 3 and totals["members"] >= 2
    assert len(body["signups_by_week"]) == 12 and len(body["meetings_by_week"]) == 12
    assert body["meetings_by_week"][-1]["count"] >= 3
    assert body["signups_by_week"][-1]["count"] >= 1
    assert any(r["org_id"] == str(t.org_id) for r in body["token_leaders"])
    assert len(body["ops_activity_by_day"]) == 14
    assert ops_rows(staff.user_id, "overview.read")


def test_rollup_runs_as_the_app_role_and_restores_its_binding(client, staff):
    """`ops_tenant_rollup` is SECURITY INVOKER: run as `mytheus_app` it still sees each
    workspace only through RLS, and it puts the caller's binding back, so a later statement in
    the same transaction does not run as the last workspace visited."""
    import psycopg2

    from tests.conftest import APP_URL

    mine = new_tenant()
    other = new_tenant()
    _sql("INSERT INTO meetings (org_id, title) VALUES (%(o)s, 'x')", {"o": str(other.org_id)})
    conn = psycopg2.connect(APP_URL.replace("+asyncpg", ""))
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT set_config('app.current_org', %s, true)", (str(mine.org_id),))
            cur.execute(
                "SELECT meetings_total FROM ops_tenant_rollup(2, '2026-11') WHERE org_id = %s",
                (str(other.org_id),),
            )
            assert cur.fetchone()[0] == 1
            cur.execute("SELECT current_setting('app.current_org', true)")
            assert cur.fetchone()[0] == str(mine.org_id)
            cur.execute("SELECT count(*) FROM meetings")
            assert cur.fetchone()[0] == 0  # still bound to `mine`, which has none
    finally:
        conn.close()


def test_tenant_detail_shows_the_control_plane_picture(client, staff):
    t = new_tenant("Detail")
    owner = t.add_user("owner")
    _sql(
        "INSERT INTO data_residency (org_id, region, db_cell) VALUES (%(o)s, 'us', 'pool-us-1')",
        {"o": str(t.org_id)},
    )
    _sql(
        "INSERT INTO meetings (org_id, title) VALUES (%(o)s, 'private title')", {"o": str(t.org_id)}
    )
    grant(t.org_id, staff.user_id, "read_only")

    resp = client.post("/admin/ops/tenants/detail", json={"org_id": str(t.org_id)})
    assert resp.status_code == 200, resp.text
    d = resp.json()
    assert d["name"] == "Detail" and d["members"] == 1 and d["meetings_total"] == 1
    assert d["residency"]["db_cell"] == "pool-us-1"
    assert d["grants"][0]["scope"] == "read_only" and d["grants"][0]["active"] is True
    assert len(d["meetings_by_week"]) == 12
    assert "private title" not in resp.text
    assert owner.user_id  # the member exists, but no member data is in the detail
    assert ops_rows(staff.user_id, "tenant.detail")[-1]["target_org_id"] == t.org_id

    missing = client.post("/admin/ops/tenants/detail", json={"org_id": str(uuid.uuid4())})
    assert missing.status_code == 404


def test_health_probes_services_and_reports_the_outbox(client, staff):
    t = new_tenant("Backlog")
    _sql(
        "INSERT INTO outbox (org_id, topic, partition_key, idempotency_key, schema_subject, "
        "payload, attempts) VALUES (%(o)s, 'admin-events', %(o)s, %(k)s, 'admin-events-value', "
        "'{}'::jsonb, 3)",
        {"o": str(t.org_id), "k": f"k-{uuid.uuid4()}"},
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "core-api":
            return httpx.Response(503)
        return httpx.Response(200, json={"status": "ok"})

    client.app.dependency_overrides[console_routes.probe_transport] = lambda: httpx.MockTransport(
        handler
    )
    try:
        resp = client.get("/admin/ops/health")
    finally:
        client.app.dependency_overrides.pop(console_routes.probe_transport, None)
    assert resp.status_code == 200, resp.text
    h = resp.json()
    by_name = {p["service"]: p for p in h["services"]}
    assert by_name["admin-ops-service"]["status"] == "up"
    assert by_name["core-api"]["status"] == "down" and by_name["bff"]["status"] == "up"
    assert h["outbox_pending"] >= 1 and h["outbox_max_attempts"] >= 3
    assert any(r["org_id"] == str(t.org_id) for r in h["outbox_tenants"])
    assert h["dlq"] is None and h["consumer_lag"] is None
    assert ops_rows(staff.user_id, "health.read")[-1]["details"]["down"] == ["core-api"]


def test_staff_report_resolves_emails_from_whoami(client, staff):
    assert client.get("/admin/ops/whoami").json() == {"staff": True}
    resp = client.get("/admin/ops/staff")
    assert resp.status_code == 200, resp.text
    report = resp.json()
    me = next(s for s in report["staff"] if s["user_id"] == str(staff.user_id))
    assert me["email"] and me["org_name"] == "Mytheus" and me["actions_30d"] >= 1
    # the random allowlisted id that never signed in has no email yet
    assert any(s["email"] is None for s in report["staff"])
    assert len(report["activity_by_day"]) == 14 and report["recent"]


def test_move_requests_one_open_at_a_time_and_cancel(client, staff):
    t = new_tenant("Mover")
    body = {"org_id": str(t.org_id), "to_region": "eu", "reason": "customer asked for EU"}
    first = client.post("/admin/ops/tenants/migrate", json=body)
    assert first.status_code == 201, first.text
    m = first.json()
    assert (m["from_region"], m["from_cell"], m["to_cell"], m["status"]) == (
        "us",
        "pool-us-1",
        "pool-eu-1",
        "requested",
    )
    assert client.post("/admin/ops/tenants/migrate", json=body).status_code == 409
    same = {**body, "to_region": "us"}
    assert client.post("/admin/ops/tenants/migrate", json=same).status_code == 409

    cancel = {"org_id": str(t.org_id), "migration_id": m["id"]}
    assert client.post("/admin/ops/tenants/migrations/cancel", json=cancel).json()["status"] == (
        "cancelled"
    )
    assert client.post("/admin/ops/tenants/migrations/cancel", json=cancel).status_code == 404
    assert client.post("/admin/ops/tenants/migrate", json=body).status_code == 201
    actions = [r["action"] for r in ops_rows(staff.user_id) if r["action"].startswith("tenant.")]
    assert actions.count("tenant.migrate") == 4 and "tenant.migrate.cancel" in actions


def test_support_needs_an_impersonate_grant(client, staff, fake_clerk):
    t = new_tenant("Customer")
    member = t.add_user("member")
    target = {"org_id": str(t.org_id)}
    grant(t.org_id, staff.user_id, "read_only")
    assert client.post("/admin/ops/support/members", json=target).status_code == 403
    start = {**target, "user_id": str(member.user_id), "reason": "reproduce the bug"}
    assert client.post("/admin/ops/support/sessions", json=start).status_code == 403
    assert fake_clerk[0] == []
    outcomes = [r["details"].get("outcome") for r in ops_rows(staff.user_id)]
    assert outcomes.count("no_impersonate_grant") == 2


def test_support_session_lifecycle_is_enforced_and_tagged(client, staff, fake_clerk):
    calls, revoked = fake_clerk
    t = new_tenant("Customer")
    admin = t.add_user("admin")
    grant(t.org_id, staff.user_id)

    members = client.post("/admin/ops/support/members", json={"org_id": str(t.org_id)})
    assert members.status_code == 200, members.text
    assert [m["user_id"] for m in members.json()["items"]] == [str(admin.user_id)]

    # staff accounts cannot be acted as
    staff_start = {
        "org_id": str(t.org_id),
        "user_id": str(staff.user_id),
        "reason": "should not work",
    }
    assert client.post("/admin/ops/support/sessions", json=staff_start).status_code == 404

    start = client.post(
        "/admin/ops/support/sessions",
        json={
            "org_id": str(t.org_id),
            "user_id": str(admin.user_id),
            "reason": "rep",
            "minutes": 15,
        },
    )
    assert start.status_code == 422  # reason is at least 5 characters
    start = client.post(
        "/admin/ops/support/sessions",
        json={
            "org_id": str(t.org_id),
            "user_id": str(admin.user_id),
            "reason": "reproduce the bug",
            "minutes": 15,
        },
    )
    assert start.status_code == 201, start.text
    session = start.json()["session"]
    assert start.json()["sign_in_url"] == "https://clerk.test/ticket"
    assert calls[0]["user"] == admin.clerk_user and calls[0]["actor"] == staff.clerk_user
    assert 800 <= calls[0]["seconds"] <= 900
    started = audit_rows(t.org_id, "support_session.started")
    assert started[0]["actor_type"] == "system"
    assert started[0]["metadata"]["staff_user_id"] == str(staff.user_id)

    # As the member, through Clerk impersonation: honoured while the session is open...
    admin.sign_in(acting_as_staff=staff.clerk_user)
    assert client.get("/admin/tenant/support-grants").status_code == 200
    export = client.post("/admin/tenant/data-export")
    assert export.status_code == 202, export.text
    tagged = audit_rows(t.org_id, "data_export.requested")
    assert tagged[-1]["actor_type"] == "system"
    assert tagged[-1]["metadata"]["support_session"] == {
        "id": session["id"],
        "staff_user_id": str(staff.user_id),
    }
    # ...support cannot grant itself more support...
    more = client.post(
        "/admin/tenant/support-grants",
        json={
            "staff_user_id": str(staff.user_id),
            "scope": "impersonate",
            "expires_at": "2099-01-01T00:00:00Z",
        },
    )
    assert more.status_code == 403
    # ...the workspace sees the session...
    listed = client.get("/admin/tenant/support-sessions").json()["items"]
    assert listed[0]["id"] == session["id"] and listed[0]["open"] is True
    # ...and a different actor, or no session at all, is refused.
    admin.sign_in(acting_as_staff="user_somebody_else")
    assert client.get("/admin/tenant/support-grants").status_code == 401

    staff.sign_in()
    end = client.post(
        "/admin/ops/support/sessions/end",
        json={"org_id": str(t.org_id), "session_id": session["id"]},
    )
    assert end.status_code == 200 and end.json()["open"] is False
    assert revoked == ["act_1"]
    assert audit_rows(t.org_id, "support_session.ended")[0]["actor_type"] == "system"

    admin.sign_in(acting_as_staff=staff.clerk_user)
    assert client.get("/admin/tenant/support-grants").status_code == 401
    admin.sign_in()  # the member's own session is unaffected
    assert client.get("/admin/tenant/support-grants").status_code == 200
    PRINCIPAL.act = None


def test_revoking_the_grant_ends_an_open_session(client, staff, fake_clerk):
    t = new_tenant("Customer")
    admin = t.add_user("admin")
    grant_id = grant(t.org_id, staff.user_id)
    started = client.post(
        "/admin/ops/support/sessions",
        json={"org_id": str(t.org_id), "user_id": str(admin.user_id), "reason": "check billing"},
    )
    assert started.status_code == 201, started.text
    admin.sign_in(acting_as_staff=staff.clerk_user)
    assert client.get("/admin/tenant/support-grants").status_code == 200
    _sql("UPDATE support_grants SET revoked_at = now() WHERE id = %(g)s", {"g": grant_id})
    assert client.get("/admin/tenant/support-grants").status_code == 401
    PRINCIPAL.act = None


def test_clerk_outage_starts_nothing(client, staff, monkeypatch):
    t = new_tenant("Customer")
    member = t.add_user("member")
    grant(t.org_id, staff.user_id)

    async def down(*args: Any, **kwargs: Any) -> clerk_admin.ActorToken:
        raise clerk_admin.ClerkUnavailable("no key")

    monkeypatch.setattr(clerk_admin, "create_actor_token", down)
    resp = client.post(
        "/admin/ops/support/sessions",
        json={"org_id": str(t.org_id), "user_id": str(member.user_id), "reason": "reproduce it"},
    )
    assert resp.status_code == 503
    rows = _sql(
        "SELECT count(*) AS n FROM support_sessions WHERE org_id = %(o)s", {"o": str(t.org_id)}
    )
    assert rows[0]["n"] == 0
