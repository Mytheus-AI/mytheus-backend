"""`POST /api/v1/actions` (Phase 4): a person creates a task directly.

1. The default assignee is the caller; the row is `confirmed` with `confirmed_by` = caller and
   no meeting; exactly one `actions` outbox row (`action:{id}:confirmed`, nil meeting uuid) and
   one `action.created` audit row commit with it; it shows in My Tasks.
2. With a meeting: the caller must be a participant (403), the meeting visible (404); the
   outbox row carries the meeting id.
3. An assignee from another tenant (or nobody) is 404; a colleague is fine.
4. `Idempotency-Key` replays the first response and creates nothing new.
5. Validation: blank content, unknown fields, over-long content are 422.
"""

from __future__ import annotations

import os
import uuid

import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.core_api.routes import action_create, my_tasks
from tests.conftest import APP_URL
from tests.test_chat_api import (
    PRINCIPAL,
    _create_test_app,
    _sql,
    add_participant,
    seed_meeting,
    seed_tenant,
    sign_in,
)
from tests.test_my_tasks import add_user

NIL = "00000000-0000-0000-0000-000000000000"


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-action-create-test")
    app.include_router(action_create.router)
    app.include_router(my_tasks.router)
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


def _outbox(action_id: str) -> list[dict]:
    return _sql(
        "SELECT topic, payload, idempotency_key FROM outbox WHERE idempotency_key = %(k)s",
        {"k": f"action:{action_id}:confirmed"},
    )


def test_a_created_task_is_confirmed_mine_and_emitted_once(client):
    me = seed_tenant()
    sign_in(me)
    res = client.post(
        "/api/v1/actions", json={"content": "  Send the deck  ", "due_date": "2026-12-01"}
    )
    assert res.status_code == 201, res.text
    body = res.json()
    assert body["content"] == "Send the deck" and body["status"] == "confirmed"
    assert body["assignee_id"] == str(me.user_id) and body["meeting_id"] is None
    [row] = _sql(
        "SELECT status, confirmed_by, meeting_id, confidence FROM action_items WHERE id = %(i)s",
        {"i": body["id"]},
    )
    assert str(row["confirmed_by"]) == str(me.user_id) and row["meeting_id"] is None
    [event] = _outbox(body["id"])
    assert event["topic"] == "actions"
    payload = event["payload"]
    assert payload["meeting_id"] == NIL and payload["status"] == "confirmed"
    assert payload["assignee_id"] == str(me.user_id) and payload["due_date"] is not None
    audits = _sql(
        "SELECT metadata FROM audit_logs WHERE action = 'action.created' AND target_id = %(t)s",
        {"t": body["id"]},
    )
    assert len(audits) == 1 and audits[0]["metadata"]["via"] == "session"
    mine = client.get("/api/v1/actions/mine").json()["items"]
    assert [i["id"] for i in mine] == [body["id"]] and mine[0]["meeting_title"] is None
    # The legacy list promises a meeting, so it leaves the task out; done still works on it.
    legacy = client.get("/api/v1/actions", params={"assignee_id": "me"}).json()["items"]
    assert legacy == []
    done = client.post(f"/api/v1/actions/{body['id']}/done")
    assert done.status_code == 200 and done.json()["status"] == "done"
    assert done.json()["meeting_id"] == NIL and done.json()["meeting_title"] == ""


def test_with_a_meeting_the_caller_must_be_a_participant(client):
    me = seed_tenant()
    sign_in(me)
    m = seed_meeting(me.org_id)
    res = client.post("/api/v1/actions", json={"content": "x", "meeting_id": str(m)})
    assert res.status_code == 403 and res.json()["code"] == "NOT_A_PARTICIPANT"
    add_participant(m, me.org_id, me.user_id)
    res = client.post("/api/v1/actions", json={"content": "x", "meeting_id": str(m)})
    assert res.status_code == 201 and res.json()["meeting_title"] == "M"
    assert _outbox(res.json()["id"])[0]["payload"]["meeting_id"] == str(m)
    other = seed_tenant()
    foreign = seed_meeting(other.org_id)
    res = client.post("/api/v1/actions", json={"content": "x", "meeting_id": str(foreign)})
    assert res.status_code == 404


def test_assignee_must_be_in_this_workspace(client):
    me = seed_tenant()
    colleague = add_user(me)
    stranger = seed_tenant()
    sign_in(me)
    res = client.post(
        "/api/v1/actions", json={"content": "x", "assignee_id": str(stranger.user_id)}
    )
    assert res.status_code == 404
    res = client.post("/api/v1/actions", json={"content": "x", "assignee_id": str(uuid.uuid4())})
    assert res.status_code == 404
    res = client.post(
        "/api/v1/actions", json={"content": "x", "assignee_id": str(colleague.user_id)}
    )
    assert res.status_code == 201 and res.json()["assignee_id"] == str(colleague.user_id)


def test_idempotency_key_creates_once(client):
    me = seed_tenant()
    sign_in(me)
    headers = {"Idempotency-Key": f"k-{uuid.uuid4()}"}
    first = client.post("/api/v1/actions", json={"content": "once"}, headers=headers)
    second = client.post("/api/v1/actions", json={"content": "once"}, headers=headers)
    assert first.status_code == 201 and second.status_code == 201
    assert first.json()["id"] == second.json()["id"]
    rows = _sql(
        "SELECT 1 FROM action_items WHERE org_id = %(o)s AND content = 'once'",
        {"o": str(me.org_id)},
    )
    assert len(rows) == 1


@pytest.mark.parametrize(
    "body",
    [{"content": "   "}, {"content": "x" * 501}, {"content": "x", "status": "proposed"}, {}],
)
def test_validation(client, body):
    sign_in(seed_tenant())
    assert client.post("/api/v1/actions", json=body).status_code == 422
