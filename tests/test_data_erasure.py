"""Data erasure: admin-ops `POST/GET /admin/tenant/erasures` driving core-api
`POST /api/v1/erasures` and identity's token revokes, end to end against the real Postgres as
`mytheus_app`.

admin-ops reaches core-api and identity in-process (httpx `ASGITransport`) with the caller's own
token, as in production; only `verify_token` is overridden. Covered:

- admin-only (a member is 403 at both admin-ops and core-api) and the console route is the one
  api-gateway routes;
- a person erasure deletes their notes, promises, agent questions, chat, status requests and
  speaker attribution, keeps confirmed decisions and tasks with the person removed, revokes
  their share links and their access tokens, and touches nobody else's rows;
- a meeting erasure deletes the meeting's content and never-confirmed candidates, keeps
  confirmed records, revokes its share links and hides the meeting; a live meeting is refused;
- the typed confirmation is checked server-side (mismatch is 422 and changes nothing), and an
  admin cannot erase their own data;
- the request is audited (`data_erasure.applied` by core-api, `data_erasure.completed` here,
  without the person's email) and emits `admin-events` `erasure_requested` for the target;
- a repeat is a no-op that reports zero, and another tenant's ids are 404 (RLS).
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import cache as cache_mod
from libs.shared import db as db_mod
from libs.shared.errors import add_exception_handlers
from services.admin_ops_service import common, tenant_routes
from services.api_gateway.edge import route_for
from services.core_api.routes import erasure
from services.identity_tenant_service.routes import api_tokens
from tests.conftest import APP_URL
from tests.test_admin_ops_api import (
    PRINCIPAL,
    _sql,
    audit_rows,
    new_tenant,
    outbox_rows,
)

pytestmark = pytest.mark.usefixtures("migrated_db")


def _claims() -> dict[str, str]:
    return {"sub": PRINCIPAL.clerk_user, "org_id": PRINCIPAL.clerk_org}


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    cache_mod.get_redis.cache_clear()

    identity = FastAPI()
    add_exception_handlers(identity)
    identity.include_router(api_tokens.router)
    identity.dependency_overrides[auth_mod.verify_token] = _claims

    core = FastAPI()
    add_exception_handlers(core)
    core.include_router(erasure.router)
    core.dependency_overrides[auth_mod.verify_token] = _claims

    admin = FastAPI()
    add_exception_handlers(admin)
    admin.include_router(tenant_routes.router)
    admin.dependency_overrides[auth_mod.verify_token] = _claims
    admin.dependency_overrides[common.identity_transport] = lambda: httpx.ASGITransport(
        app=identity
    )
    admin.dependency_overrides[common.core_transport] = lambda: httpx.ASGITransport(app=core)
    admin.mount("/_core", core)
    with TestClient(admin, headers={"Authorization": "Bearer test"}) as c:
        yield c

    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    cache_mod.get_redis.cache_clear()


def _meeting(org: uuid.UUID, title: str | None = "Pricing review", status: str = "ended") -> str:
    mid = str(uuid.uuid4())
    _sql(
        "INSERT INTO meetings (id, org_id, title, status) VALUES (%(i)s, %(o)s, %(t)s, %(s)s)",
        {"i": mid, "o": str(org), "t": title, "s": status},
    )
    return mid


def _seed_person_data(org: uuid.UUID, user: uuid.UUID, meeting: str) -> dict[str, str]:
    o, u, m = str(org), str(user), meeting
    p = {"o": o, "u": u, "m": m, "s": f"spk_{u[:8]}"}
    _sql(
        "INSERT INTO worklogs (org_id, meeting_id, user_id, window_id, content) "
        "VALUES (%(o)s, %(m)s, %(u)s, 'w1', 'my note')",
        p,
    )
    _sql(
        "INSERT INTO commitments (org_id, user_id, meeting_id, content, source_window_id) "
        "VALUES (%(o)s, %(u)s, %(m)s, 'send the deck', 'w1')",
        p,
    )
    _sql(
        "INSERT INTO agent_suggestions (org_id, user_id, meeting_id, question, priority) "
        "VALUES (%(o)s, %(u)s, %(m)s, 'ask about pricing?', 2)",
        p,
    )
    _sql(
        "INSERT INTO chat_messages (org_id, meeting_id, user_id, content) "
        "VALUES (%(o)s, %(m)s, %(u)s, 'hello')",
        p,
    )
    _sql(
        "INSERT INTO meeting_speakers (org_id, meeting_id, speaker_id, user_id, assignment) "
        "VALUES (%(o)s, %(m)s, %(s)s, %(u)s, 'manual')",
        p,
    )
    decision = _sql(
        "INSERT INTO decisions (org_id, meeting_id, content, confidence, status, confirmed_by) "
        "VALUES (%(o)s, %(m)s, 'Ship in May', 0.9, 'confirmed', %(u)s) RETURNING id",
        p,
    )[0]["id"]
    action = _sql(
        "INSERT INTO action_items (org_id, meeting_id, content, confidence, status, assignee_id, "
        "confirmed_by) VALUES (%(o)s, %(m)s, 'Draft the plan', 0.9, 'confirmed', %(u)s, %(u)s) "
        "RETURNING id",
        p,
    )[0]["id"]
    _sql(
        "INSERT INTO share_links (org_id, kind, meeting_id, created_by, expires_at) "
        "VALUES (%(o)s, 'meeting_recap', %(m)s, %(u)s, now() + interval '7 days')",
        p,
    )
    _sql(
        "INSERT INTO api_tokens (org_id, user_id, name, token_hash, token_hint, scope, "
        "expires_at) VALUES (%(o)s, %(u)s, 'CLI', "
        "encode(sha256(random()::text::bytea), 'hex'), 'abcd', 'read', "
        "now() + interval '30 days')",
        p,
    )
    return {"decision": str(decision), "action": str(action)}


def _count(table: str, where: str, params: dict[str, Any]) -> int:
    return int(_sql(f"SELECT count(*) AS n FROM {table} WHERE {where}", params)[0]["n"])


def test_console_route_is_routed_and_core_leg_is_not():
    assert route_for("/admin/tenant/erasures") == "admin-ops-service"
    # The core-api leg is reached only through admin-ops, so every erasure is fully recorded.
    assert route_for("/api/v1/erasures") is None


def test_erasure_is_admin_only(client):
    t = new_tenant()
    target = t.add_user("member")
    t.add_user("member").sign_in()
    body = {"kind": "person", "target_id": str(target.user_id), "confirm": "x"}
    assert client.post("/admin/tenant/erasures", json=body).status_code == 403
    assert client.get("/admin/tenant/erasures").status_code == 403
    assert client.post("/_core/api/v1/erasures", json=body).status_code == 403


def test_person_erasure_removes_their_data_keeps_org_records_and_is_recorded(client):
    t = new_tenant()
    target = t.add_user("member", email="Dana@Example.com")
    bystander = t.add_user("member")
    admin = t.add_user("admin").sign_in()
    meeting = _meeting(t.org_id)
    ids = _seed_person_data(t.org_id, target.user_id, meeting)
    _seed_person_data(t.org_id, bystander.user_id, meeting)

    body = {"kind": "person", "target_id": str(target.user_id), "confirm": "dana@example.com "}
    res = client.post("/admin/tenant/erasures", json=body)
    assert res.status_code == 200, res.text
    out = res.json()
    assert out["label"] == "Dana@Example.com"
    assert out["erased"] == {
        "notes": 1,
        "promises": 1,
        "agent_questions": 1,
        "chat_messages": 1,
        "status_requests": 0,
        "speaker_attributions": 1,
        "agent_memory": 0,
        "agent_feedback": 0,
        "agent_calibration": 0,
        "clips": 0,
        "date_proposals": 0,
        "capture_preferences": 0,
    }
    assert out["anonymized"]["decisions"] == 1
    assert out["anonymized"]["actions_unassigned"] == 1
    assert out["revoked_share_links"] == 1
    assert out["revoked_tokens"] == 1

    u = {"u": str(target.user_id)}
    for table in ("worklogs", "commitments", "agent_suggestions", "chat_messages"):
        assert _count(table, "user_id = %(u)s", u) == 0
    assert _count("meeting_speakers", "user_id = %(u)s", u) == 0
    decision = _sql(
        "SELECT status, confirmed_by FROM decisions WHERE id = %(d)s", {"d": ids["decision"]}
    )
    assert decision == [{"status": "confirmed", "confirmed_by": None}]
    action = _sql(
        "SELECT assignee_id, confirmed_by FROM action_items WHERE id = %(a)s", {"a": ids["action"]}
    )
    assert action == [{"assignee_id": None, "confirmed_by": None}]
    assert _count("share_links", "created_by = %(u)s AND revoked_at IS NULL", u) == 0
    assert _count("api_tokens", "user_id = %(u)s AND revoked_at IS NULL", u) == 0

    # The bystander's rows are untouched.
    b = {"u": str(bystander.user_id)}
    assert _count("worklogs", "user_id = %(u)s", b) == 1
    assert _count("api_tokens", "user_id = %(u)s AND revoked_at IS NULL", b) == 1

    completed = audit_rows(t.org_id, "data_erasure.completed")
    assert len(completed) == 1
    assert completed[0]["actor_id"] == str(admin.user_id)
    assert "dana@example.com" not in str(completed[0]["metadata"]).lower()
    assert len(audit_rows(t.org_id, "data_erasure.applied")) == 1
    assert len(audit_rows(t.org_id, "api_token.revoked")) == 1
    events = [
        r["payload"]
        for r in outbox_rows(t.org_id, "admin-events")
        if r["payload"].get("event_type") == "erasure_requested"
    ]
    assert len(events) == 1
    assert events[0]["target_type"] == "person"
    assert events[0]["target_id"] == str(target.user_id)

    listed = client.get("/admin/tenant/erasures").json()["items"]
    assert [(i["kind"], i["label"], i["revoked_tokens"]) for i in listed] == [
        ("person", "Dana@Example.com", 1)
    ]

    # A repeat is a no-op that reports zero.
    again = client.post("/admin/tenant/erasures", json=body).json()
    assert sum(again["erased"].values()) == 0
    assert again["revoked_tokens"] == 0


def test_confirmation_mismatch_and_self_erasure_change_nothing(client):
    t = new_tenant()
    target = t.add_user("member", email="sam@example.com")
    admin = t.add_user("admin").sign_in()
    meeting = _meeting(t.org_id)
    _seed_person_data(t.org_id, target.user_id, meeting)

    wrong = {"kind": "person", "target_id": str(target.user_id), "confirm": "sam@example.org"}
    res = client.post("/admin/tenant/erasures", json=wrong)
    assert res.status_code == 422
    assert res.json().get("reason") == "confirmation_mismatch"
    assert _count("worklogs", "user_id = %(u)s", {"u": str(target.user_id)}) == 1
    assert audit_rows(t.org_id, "data_erasure.completed") == []

    admin_email = _sql("SELECT email FROM users WHERE id = %(u)s", {"u": str(admin.user_id)})
    own = {"kind": "person", "target_id": str(admin.user_id), "confirm": admin_email[0]["email"]}
    res = client.post("/admin/tenant/erasures", json=own)
    assert res.status_code == 422
    assert res.json().get("reason") == "self"


def test_meeting_erasure_keeps_confirmed_records_and_hides_the_meeting(client):
    t = new_tenant()
    person = t.add_user("member")
    t.add_user("owner").sign_in()
    meeting = _meeting(t.org_id, title=None)
    other = _meeting(t.org_id, title="Other")
    ids = _seed_person_data(t.org_id, person.user_id, meeting)
    _seed_person_data(t.org_id, person.user_id, other)
    proposed = _sql(
        "INSERT INTO decisions (org_id, meeting_id, content, confidence) "
        "VALUES (%(o)s, %(m)s, 'maybe', 0.5) RETURNING id",
        {"o": str(t.org_id), "m": meeting},
    )[0]["id"]

    body = {"kind": "meeting", "target_id": meeting, "confirm": "untitled meeting"}
    res = client.post("/admin/tenant/erasures", json=body)
    assert res.status_code == 200, res.text
    out = res.json()
    assert out["label"] == "Untitled meeting"
    assert out["erased"]["notes"] == 1
    assert out["erased"]["unconfirmed_decisions"] == 1
    assert out["anonymized"] == {"meeting_hidden": 1}
    assert out["revoked_tokens"] == 0

    m = {"m": meeting}
    for table in ("worklogs", "commitments", "agent_suggestions", "chat_messages"):
        assert _count(table, "meeting_id = %(m)s", m) == 0
    assert _count("decisions", "id = %(d)s", {"d": str(proposed)}) == 0
    assert _count("decisions", "id = %(d)s", {"d": ids["decision"]}) == 1
    assert _count("action_items", "id = %(a)s", {"a": ids["action"]}) == 1
    assert _count("meetings", "id = %(m)s AND deleted_at IS NOT NULL", m) == 1
    assert _count("share_links", "meeting_id = %(m)s AND revoked_at IS NULL", m) == 0
    # The other meeting and the person's tokens are untouched.
    assert _count("worklogs", "meeting_id = %(m)s", {"m": other}) == 1
    assert (
        _count("api_tokens", "user_id = %(u)s AND revoked_at IS NULL", {"u": str(person.user_id)})
        == 2
    )

    listed = client.get("/admin/tenant/erasures").json()["items"]
    assert [(i["kind"], i["label"]) for i in listed] == [("meeting", "Untitled meeting")]


def test_live_meeting_is_refused(client):
    t = new_tenant()
    t.add_user("admin").sign_in()
    meeting = _meeting(t.org_id, title="Standup", status="live")
    res = client.post(
        "/admin/tenant/erasures",
        json={"kind": "meeting", "target_id": meeting, "confirm": "Standup"},
    )
    assert res.status_code == 409
    assert res.json().get("reason") == "meeting_live"


def test_another_tenants_target_is_not_found(client):
    a = new_tenant()
    b = new_tenant()
    victim = b.add_user("member", email="v@example.com")
    meeting_b = _meeting(b.org_id, title="B only")
    a.add_user("admin").sign_in()
    res = client.post(
        "/admin/tenant/erasures",
        json={"kind": "person", "target_id": str(victim.user_id), "confirm": "v@example.com"},
    )
    assert res.status_code == 404
    res = client.post(
        "/admin/tenant/erasures",
        json={"kind": "meeting", "target_id": meeting_b, "confirm": "B only"},
    )
    assert res.status_code == 404
    assert _count("meetings", "id = %(m)s AND deleted_at IS NULL", {"m": meeting_b}) == 1


def test_person_erasure_clears_their_promise_ledger_traces(client):
    """Promise ledger: the date proposals they made or received and their capture preferences
    go; a colleague's promise to them no longer names them."""
    t = new_tenant()
    target = t.add_user("member", email="erin@example.com")
    colleague = t.add_user("member")
    t.add_user("admin").sign_in()
    mine = _sql(
        "INSERT INTO commitments (org_id, user_id, content, channel, source_window_id, "
        "recipient_user_id) VALUES (%(o)s, %(u)s, 'Send Erin the deck', 'manual', 'm1', %(r)s) "
        "RETURNING id",
        {"o": str(t.org_id), "u": str(colleague.user_id), "r": str(target.user_id)},
    )[0]["id"]
    _sql(
        "INSERT INTO promise_date_proposals (org_id, commitment_id, proposer_id, requester_id, "
        "content, proposed_due) VALUES (%(o)s, %(c)s, %(p)s, %(r)s, 'x', '2026-12-01')",
        {"o": str(t.org_id), "c": mine, "p": str(colleague.user_id), "r": str(target.user_id)},
    )
    _sql(
        "INSERT INTO promise_capture_preferences (org_id, user_id, slack_enabled) "
        "VALUES (%(o)s, %(u)s, false)",
        {"o": str(t.org_id), "u": str(target.user_id)},
    )
    body = {"kind": "person", "target_id": str(target.user_id), "confirm": "erin@example.com"}
    out = client.post("/admin/tenant/erasures", json=body).json()
    assert out["erased"]["date_proposals"] == 1 and out["erased"]["capture_preferences"] == 1
    assert out["anonymized"]["promise_recipients"] == 1
    row = _sql("SELECT recipient_user_id FROM commitments WHERE id = %(c)s", {"c": mine})[0]
    assert row["recipient_user_id"] is None
