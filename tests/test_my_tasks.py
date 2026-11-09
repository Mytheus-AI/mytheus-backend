"""My Tasks (`core-api`), against a real Postgres. Plan 7-9 sections 3.6 step 6 and 3.8 step 2;
ADR-0031 D3.

`GET /api/v1/actions?assignee_id=me&status=confirmed`:
1. the caller sees only their own confirmed tasks, soonest due first, undated last, each with
   its meeting's title; `proposed` / `dismissed` candidates and deleted rows never appear;
2. `assignee_id` accepts only `me` (anything else, or omitting it, is a 422);
3. RLS: tenant B sees none of tenant A's tasks, even with the same assignee id in the query.

`POST /api/v1/actions/{id}/done`:
4. the assignee moves confirmed -> done (audited), and it moves from the open list to the done
   list; repeating it is a 200 with no second audit row;
5. a different member is 403, an admin may close it, a proposed candidate is 409, another
   tenant's id is 404;
6. nothing is written to the outbox: progress is not a `decisions`/`actions` fact.

`action_items` belongs to the Plan 8 confirmation work (Part A), which creates it by migration.
Until that migration exists in this database, the module fixture creates a stand-in with the
plan's DDL, the canonical RLS policy and grants, and drops it afterwards. Once Part A's
migration lands the fixture finds the real table and uses it unchanged.
"""

from __future__ import annotations

import uuid

import pytest

from services.core_api.routes import my_tasks
from tests.conftest import APP_URL
from tests.test_chat_api import (
    PRINCIPAL,
    Tenant,
    _create_test_app,
    _sql,
    seed_meeting,
    seed_tenant,
    sign_in,
)

_RLS = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"
_STAND_IN = f"""
CREATE TABLE action_items (
  id           uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
  org_id       uuid        NOT NULL REFERENCES organizations(id),
  meeting_id   uuid        NOT NULL REFERENCES meetings(id),
  content      text        NOT NULL,
  assignee_id  uuid REFERENCES users(id),
  due_date     date,
  status       text        NOT NULL DEFAULT 'proposed',
  confidence   real        NOT NULL,
  confirmed_by uuid REFERENCES users(id),
  created_at   timestamptz NOT NULL DEFAULT now(),
  deleted_at   timestamptz
);
CREATE INDEX action_items_assignee_idx ON action_items (org_id, assignee_id, status);
ALTER TABLE action_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE action_items FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON action_items USING ({_RLS}) WITH CHECK ({_RLS});
GRANT SELECT, INSERT, UPDATE, DELETE ON action_items TO mytheus_app, mytheus_admin;
"""


@pytest.fixture(scope="module")
def action_items_table(migrated_db):
    exists = _sql("SELECT to_regclass('public.action_items') IS NOT NULL AS e")[0]["e"]
    if not exists:
        _sql(_STAND_IN)
    yield
    if not exists:
        _sql("DROP TABLE IF EXISTS action_items")


@pytest.fixture(scope="module")
def client(action_items_table):
    import os

    from starlette.testclient import TestClient

    from libs.shared import auth as auth_mod
    from libs.shared import db as db_mod

    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-my-tasks-test")
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


def add_user(t: Tenant, tier: str = "member") -> Tenant:
    user_id = uuid.uuid4()
    clerk_user = f"user_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) "
        "VALUES (%(u)s, %(o)s, %(c)s, 'v@example.com')",
        {"u": str(user_id), "o": str(t.org_id), "c": clerk_user},
    )
    _sql(
        "INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (%(o)s, %(u)s, %(t)s)",
        {"o": str(t.org_id), "u": str(user_id), "t": tier},
    )
    return Tenant(t.org_id, t.clerk_org, user_id, clerk_user)


def task(
    t: Tenant,
    meeting: uuid.UUID,
    assignee: uuid.UUID,
    content: str,
    *,
    status: str = "confirmed",
    due: str | None = None,
    deleted: bool = False,
) -> uuid.UUID:
    row = _sql(
        "INSERT INTO action_items (org_id, meeting_id, content, assignee_id, due_date, status, "
        "confidence, deleted_at) VALUES (%(o)s, %(m)s, %(c)s, %(a)s, %(d)s, %(s)s, 0.8, "
        "CASE WHEN %(x)s THEN now() END) RETURNING id",
        {
            "o": str(t.org_id),
            "m": str(meeting),
            "c": content,
            "a": str(assignee),
            "d": due,
            "s": status,
            "x": deleted,
        },
    )
    return uuid.UUID(str(row[0]["id"]))


def _audits(action_id: uuid.UUID) -> int:
    return len(
        _sql(
            "SELECT 1 FROM audit_logs WHERE action = 'action.completed' AND target_id = %(t)s",
            {"t": str(action_id)},
        )
    )


def test_my_open_tasks_are_mine_confirmed_and_ordered_by_due_date(client):
    me = seed_tenant()
    other = add_user(me)
    sign_in(me)
    m = seed_meeting(me.org_id)
    later = task(me, m, me.user_id, "later", due="2026-12-01")
    undated = task(me, m, me.user_id, "undated")
    soon = task(me, m, me.user_id, "soon", due="2026-11-10")
    task(me, m, me.user_id, "candidate", status="proposed", due="2026-11-01")
    task(me, m, me.user_id, "dismissed", status="dismissed")
    task(me, m, me.user_id, "gone", due="2026-11-01", deleted=True)
    task(me, m, other.user_id, "someone else's", due="2026-11-01")

    res = client.get("/api/v1/actions", params={"assignee_id": "me", "status": "confirmed"})
    assert res.status_code == 200
    items = res.json()["items"]
    assert [i["id"] for i in items] == [str(soon), str(later), str(undated)]
    assert {i["meeting_title"] for i in items} == {"M"}
    assert all(i["status"] == "confirmed" for i in items)


def test_assignee_must_be_me(client):
    sign_in(seed_tenant())
    assert client.get("/api/v1/actions").status_code == 422
    other = str(uuid.uuid4())
    assert client.get("/api/v1/actions", params={"assignee_id": other}).status_code == 422
    bad = {"assignee_id": "me", "status": "proposed"}
    assert client.get("/api/v1/actions", params=bad).status_code == 422


def test_rls_hides_another_tenants_tasks(client):
    a = seed_tenant()
    task(a, seed_meeting(a.org_id), a.user_id, "a's task")
    b = seed_tenant()
    sign_in(b)
    assert client.get("/api/v1/actions", params={"assignee_id": "me"}).json()["items"] == []
    # Even the row id is invisible to B.
    sign_in(a)
    [item] = client.get("/api/v1/actions", params={"assignee_id": "me"}).json()["items"]
    sign_in(b)
    assert client.post(f"/api/v1/actions/{item['id']}/done").status_code == 404


def test_mark_done_moves_a_task_and_is_idempotent(client):
    me = seed_tenant()
    sign_in(me)
    m = seed_meeting(me.org_id)
    t = task(me, m, me.user_id, "write the page", due="2026-11-12")
    outbox_before = _sql("SELECT count(*) AS n FROM outbox")[0]["n"]

    res = client.post(f"/api/v1/actions/{t}/done")
    assert res.status_code == 200 and res.json()["status"] == "done"
    assert _audits(t) == 1
    assert client.get("/api/v1/actions", params={"assignee_id": "me"}).json()["items"] == []
    done = client.get("/api/v1/actions", params={"assignee_id": "me", "status": "done"})
    assert [i["id"] for i in done.json()["items"]] == [str(t)]

    again = client.post(f"/api/v1/actions/{t}/done")
    assert again.status_code == 200 and again.json()["status"] == "done"
    assert _audits(t) == 1
    # Progress is not a decisions/actions fact: nothing was enqueued.
    assert _sql("SELECT count(*) AS n FROM outbox")[0]["n"] == outbox_before


def test_who_may_mark_done_and_what(client):
    owner = seed_tenant()
    member = add_user(owner)
    admin = add_user(owner, tier="admin")
    m = seed_meeting(owner.org_id)
    mine = task(owner, m, owner.user_id, "owner's")
    candidate = task(owner, m, owner.user_id, "candidate", status="proposed")

    sign_in(member)
    res = client.post(f"/api/v1/actions/{mine}/done")
    assert res.status_code == 403 and res.json()["code"] == "FORBIDDEN_ROLE"

    sign_in(admin)
    assert client.post(f"/api/v1/actions/{mine}/done").status_code == 200

    sign_in(owner)
    res = client.post(f"/api/v1/actions/{candidate}/done")
    assert res.status_code == 409
    assert client.post(f"/api/v1/actions/{uuid.uuid4()}/done").status_code == 404
