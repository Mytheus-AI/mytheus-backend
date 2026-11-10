"""ADR-0039 Part B in core-api: meeting types, confidential meetings, agenda goals, the drafted
agenda, "send your agent instead", promises, status requests, and the internal routes.

Over HTTP as `mytheus_app` (real RLS, real transactions; only `verify_token` stubbed), and the
`agent-results` consumer against the real database:

- RLS isolation and fail-closed for `meeting_templates`, `commitments`, `action_status_requests`.
- B1: members list, owner/admin write (403 for a member), the 50 cap (422), validation, 404
  across tenants.
- B2: a template's `confidential` is the default; carry-forward, the drafted agenda, the internal
  agenda's carry-forward and the related-attendees hop never draw on a confidential meeting for
  someone who was not in it.
- B3: `goal_id` on create and on the agenda PUT.
- B4: drafted agenda contents and order, the 20 cap, 422s, nothing saved.
- B5: self only, participant only (403), 409 once ended; `proxy_questions` only to the person and
  the host; the internal agenda's `proxies`.
- B6: commitments from PersonalListenerAgent records, idempotent, private; PATCH owner only.
- C4: `suggested_owner_user_id` becomes the candidate's assignee when it has none.
- B7: internal creation (idempotent, confirmed + assigned only), list/answer by the assignee only,
  expiry when the meeting ends, `latest_status` in carry-forward and in the internal agenda.
"""

from __future__ import annotations

import json
import os
import uuid
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.tenancy import set_rls
from services.core_api.routes import (
    commitments,
    internal_agents,
    internal_lifecycle,
    internal_meetings,
    meeting_templates,
    meetings,
    status_requests,
)
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
from tests.test_suggestions import seed_user
from tests.test_worklogs import apply, result

AGENT_SECRET = "a" * 40
WORKFLOW_SECRET = "w" * 40


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ[internal_meetings.TOKENS_ENV] = (
        f"agent-runtime-service={AGENT_SECRET},workflow-service={WORKFLOW_SECRET}"
    )
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-brain-test")
    for module in (
        meetings,
        meeting_templates,
        commitments,
        status_requests,
        internal_agents,
        internal_lifecycle,
    ):
        app.include_router(module.router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }
    with TestClient(app) as c:
        yield c
    os.environ.pop(internal_meetings.TOKENS_ENV, None)
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


def _loc(resp: Any) -> list[list[str]]:
    assert resp.status_code == 422, resp.text
    return [e["loc"] for e in resp.json().get("errors", [])]


def create(client: TestClient, **body: Any) -> dict[str, Any]:
    resp = client.post("/api/v1/meetings", json={"title": "Weekly", **body})
    assert resp.status_code == 201, resp.text
    return resp.json()


def _decision(org: uuid.UUID, meeting: str, content: str) -> None:
    _sql(
        "INSERT INTO decisions (org_id, meeting_id, content, confidence, status) "
        "VALUES (%(o)s, %(m)s, %(c)s, 0.9, 'confirmed')",
        {"o": str(org), "m": meeting, "c": content},
    )


def _action(
    org: uuid.UUID, meeting: str, content: str, status: str = "confirmed", assignee: Any = None
) -> str:
    return str(
        _sql(
            "INSERT INTO action_items (org_id, meeting_id, content, confidence, status, "
            "assignee_id) VALUES (%(o)s, %(m)s, %(c)s, 0.9, %(s)s, %(a)s) RETURNING id",
            {
                "o": str(org),
                "m": meeting,
                "c": content,
                "s": status,
                "a": str(assignee) if assignee else None,
            },
        )[0]["id"]
    )


def _headers(org_id: uuid.UUID, service: str = "agent-runtime-service") -> dict[str, str]:
    secret = AGENT_SECRET if service == "agent-runtime-service" else WORKFLOW_SECRET
    return {
        "X-Mytheus-Service": service,
        "X-Mytheus-Service-Token": secret,
        "X-Mytheus-Org": str(org_id),
    }


# --- RLS -------------------------------------------------------------------------------------


def _seed_rows(t: Tenant) -> None:
    m = str(seed_meeting(t.org_id))
    a = _action(t.org_id, m, "x", assignee=t.user_id)
    _sql(
        "INSERT INTO meeting_templates (org_id, name) VALUES (%(o)s, 'Standup')",
        {"o": str(t.org_id)},
    )
    _sql(
        "INSERT INTO commitments (org_id, user_id, meeting_id, content, source_window_id) "
        "VALUES (%(o)s, %(u)s, %(m)s, 'Send the deck', 'w0')",
        {"o": str(t.org_id), "u": str(t.user_id), "m": m},
    )
    _sql(
        "INSERT INTO action_status_requests (org_id, action_id, user_id, meeting_id) "
        "VALUES (%(o)s, %(a)s, %(u)s, %(m)s)",
        {"o": str(t.org_id), "a": a, "u": str(t.user_id), "m": m},
    )


@pytest.mark.parametrize("table", ["meeting_templates", "commitments", "action_status_requests"])
async def test_rls_isolates_and_fails_closed(migrated_db, table):
    a, b = seed_tenant(), seed_tenant()
    _seed_rows(a)
    _seed_rows(b)
    engine = create_async_engine(APP_URL)
    try:
        async with engine.connect() as conn, conn.begin():
            await set_rls(conn, a.org_id)  # type: ignore[arg-type]
            orgs = {r[0] for r in await conn.execute(text(f"SELECT org_id FROM {table}"))}
            assert orgs == {a.org_id}
        async with engine.connect() as conn, conn.begin():
            assert (await conn.execute(text(f"SELECT count(*) FROM {table}"))).scalar() == 0
        async with engine.connect() as conn:
            await conn.begin()
            await set_rls(conn, a.org_id)  # type: ignore[arg-type]
            with pytest.raises(DBAPIError):
                await conn.execute(
                    text("INSERT INTO meeting_templates (org_id, name) VALUES (:o, 'Sneaky')"),
                    {"o": str(b.org_id)},
                )
            await conn.rollback()
    finally:
        await engine.dispose()


# --- B1: meeting types ---------------------------------------------------------------------------


def test_meeting_types_are_member_readable_and_admin_writable(client):
    admin = seed_tenant("admin")
    member = seed_user(admin)
    sign_in(admin)
    body = {
        "name": "  Weekly sync ",
        "description": "Team sync",
        "default_duration_minutes": 45,
        "agenda": [{"title": "Wins", "duration_minutes": 5}, {"title": "Blockers"}],
        "confidential": True,
    }
    resp = client.post("/api/v1/meeting-templates", json=body)
    assert resp.status_code == 201, resp.text
    tpl = resp.json()
    assert tpl["name"] == "Weekly sync" and tpl["confidential"] is True
    assert tpl["agenda"] == [
        {"title": "Wins", "duration_minutes": 5},
        {"title": "Blockers", "duration_minutes": None},
    ]
    assert tpl["created_by"] == str(admin.user_id)

    sign_in(member)
    assert [t["id"] for t in client.get("/api/v1/meeting-templates").json()["items"]] == [tpl["id"]]
    assert client.post("/api/v1/meeting-templates", json={"name": "x"}).status_code == 403
    url = f"/api/v1/meeting-templates/{tpl['id']}"
    assert client.patch(url, json={"name": "y"}).status_code == 403
    assert client.delete(url).status_code == 403

    sign_in(admin)
    resp = client.patch(url, json={"description": None, "default_duration_minutes": 30})
    assert resp.status_code == 200, resp.text
    assert resp.json()["description"] is None and resp.json()["default_duration_minutes"] == 30
    assert _loc(client.patch(url, json={"name": None})) == [["body"]]
    assert client.delete(url).status_code == 204
    assert client.delete(url).status_code == 404


def test_meeting_type_limits(client):
    admin = seed_tenant("owner")
    sign_in(admin)
    assert ["body", "default_duration_minutes"] in _loc(
        client.post("/api/v1/meeting-templates", json={"name": "x", "default_duration_minutes": 4})
    )
    assert ["body", "name"] in _loc(client.post("/api/v1/meeting-templates", json={"name": ""}))
    assert ["body", "agenda"] in _loc(
        client.post(
            "/api/v1/meeting-templates",
            json={"name": "x", "agenda": [{"title": f"{i}"} for i in range(21)]},
        )
    )
    _sql(
        "INSERT INTO meeting_templates (org_id, name) "
        "SELECT %(o)s, 'T' || g FROM generate_series(1, 50) g",
        {"o": str(admin.org_id)},
    )
    assert _loc(client.post("/api/v1/meeting-templates", json={"name": "51st"})) == [
        ["body", "name"]
    ]


def test_another_tenants_meeting_type_is_invisible(client):
    other = seed_tenant("admin")
    sign_in(other)
    tpl = client.post("/api/v1/meeting-templates", json={"name": "Theirs"}).json()
    me = seed_tenant("admin")
    sign_in(me)
    assert client.get("/api/v1/meeting-templates").json()["items"] == []
    assert (
        client.patch(f"/api/v1/meeting-templates/{tpl['id']}", json={"name": "x"}).status_code
        == 404
    )
    assert client.delete(f"/api/v1/meeting-templates/{tpl['id']}").status_code == 404
    assert _loc(client.post("/api/v1/meetings", json={"title": "x", "template_id": tpl["id"]})) == [
        ["body", "template_id"]
    ]


# --- B2/B3: confidential, template, goals -------------------------------------------------


def test_template_confidential_is_the_default_and_goals_are_stored(client):
    admin = seed_tenant("admin")
    sign_in(admin)
    tpl = client.post(
        "/api/v1/meeting-templates", json={"name": "Board", "confidential": True}
    ).json()
    goal = str(uuid.uuid4())
    m = create(client, template_id=tpl["id"], agenda=[{"title": "Plan", "goal_id": goal}])
    assert (m["confidential"], m["template_id"]) == (True, tpl["id"])
    assert m["agenda"][0]["goal_id"] == goal
    assert create(client, template_id=tpl["id"], confidential=False)["confidential"] is False
    assert create(client)["confidential"] is False

    item = m["agenda"][0]["id"]
    goal2 = str(uuid.uuid4())
    resp = client.put(
        f"/api/v1/meetings/{m['id']}/agenda",
        json={"items": [{"id": item, "title": "Plan", "goal_id": goal2}, {"title": "New"}]},
    )
    assert resp.status_code == 200, resp.text
    assert [i["goal_id"] for i in resp.json()] == [goal2, None]
    listed = {x["id"]: x for x in client.get("/api/v1/meetings").json()["items"]}
    assert listed[m["id"]]["confidential"] is True


# --- B4: drafted agenda --------------------------------------------------------------------------


def test_drafted_agenda_order_and_contents(client):
    owner = seed_tenant("admin")
    bob = seed_user(owner)
    sign_in(owner)
    tpl = client.post(
        "/api/v1/meeting-templates",
        json={"name": "Sync", "agenda": [{"title": "Wins", "duration_minutes": 5}]},
    ).json()
    goal = str(uuid.uuid4())
    prev = create(
        client,
        participant_ids=[str(bob.user_id)],
        agenda=[
            {"title": "Budget", "owner_user_id": str(bob.user_id), "goal_id": goal},
            {"title": "Hiring", "duration_minutes": 10},
            {"title": "Roadmap"},
        ],
    )
    budget, hiring, _roadmap = (i["id"] for i in prev["agenda"])
    _sql("UPDATE meeting_agenda_items SET status = 'covered' WHERE id = %(i)s", {"i": hiring})
    _sql("UPDATE meeting_agenda_items SET status = 'skipped' WHERE id = %(i)s", {"i": budget})
    _action(owner.org_id, prev["id"], "Draft the plan", assignee=bob.user_id)
    _action(owner.org_id, prev["id"], "Already done", status="done")
    _action(owner.org_id, prev["id"], "x" * 300)

    before = _sql("SELECT count(*) AS n FROM meeting_agenda_items")
    resp = client.post(
        "/api/v1/meetings/agenda-draft",
        json={"previous_meeting_id": prev["id"], "template_id": tpl["id"]},
    )
    assert resp.status_code == 200, resp.text
    items = resp.json()["items"]
    assert [(i["source"], i["title"]) for i in items[:4]] == [
        ("template", "Wins"),
        ("unfinished", "Budget"),
        ("unfinished", "Roadmap"),
        ("open_action", "Status: Draft the plan"),
    ]
    assert items[0]["duration_minutes"] == 5 and items[0]["owner_user_id"] is None
    assert items[1]["owner_user_id"] == str(bob.user_id) and items[1]["goal_id"] == goal
    assert items[3]["owner_user_id"] == str(bob.user_id)
    assert len(items) == 5 and len(items[4]["title"]) == 200 and items[4]["title"].endswith("...")
    assert _sql("SELECT count(*) AS n FROM meeting_agenda_items") == before, "nothing is saved"
    # Deterministic.
    again = client.post(
        "/api/v1/meetings/agenda-draft",
        json={"previous_meeting_id": prev["id"], "template_id": tpl["id"]},
    )
    assert again.json() == resp.json()

    assert client.post("/api/v1/meetings/agenda-draft", json={}).json() == {
        "items": [],
        "open_questions": [],
    }
    assert _loc(
        client.post(
            "/api/v1/meetings/agenda-draft", json={"previous_meeting_id": str(uuid.uuid4())}
        )
    ) == [["body", "previous_meeting_id"]]
    assert _loc(
        client.post("/api/v1/meetings/agenda-draft", json={"template_id": str(uuid.uuid4())})
    ) == [["body", "template_id"]]


def test_drafted_agenda_is_capped_and_skips_a_confidential_meeting_i_was_not_in(client):
    owner = seed_tenant()
    outsider = seed_user(owner)
    sign_in(owner)
    prev = create(client, agenda=[{"title": f"Item {i}"} for i in range(15)])
    for i in range(10):
        _action(owner.org_id, prev["id"], f"Action {i}")
    items = client.post(
        "/api/v1/meetings/agenda-draft", json={"previous_meeting_id": prev["id"]}
    ).json()["items"]
    assert len(items) == 20 and items[-1]["title"] == "Status: Action 4"

    secret = create(client, confidential=True, agenda=[{"title": "Layoffs"}])
    sign_in(outsider)
    resp = client.post("/api/v1/meetings/agenda-draft", json={"previous_meeting_id": secret["id"]})
    assert resp.status_code == 200 and resp.json() == {"items": [], "open_questions": []}


# --- B5: send your agent instead -------------------------------------------------------------


def test_attendance_is_self_only_and_questions_are_private(client):
    host = seed_tenant()
    bob = seed_user(host)
    carol = seed_user(host)
    stranger = seed_user(host)
    sign_in(host)
    m = create(client, participant_ids=[str(bob.user_id), str(carol.user_id)])
    url = f"/api/v1/meetings/{m['id']}/attendance"

    sign_in(bob)
    resp = client.put(
        url,
        json={
            "attendance": "agent",
            "proxy_update": "  Shipped the API; blocked on design.  ",
            "proxy_questions": ["When is the freeze?", "", "When is the freeze?"],
        },
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "user_id": str(bob.user_id),
        "role_in_meeting": "attendee",
        "attendance": "agent",
        "proxy_update": "Shipped the API; blocked on design.",
        "proxy_questions": ["When is the freeze?"],
    }

    def bobs_row(viewer: Tenant) -> dict[str, Any]:
        sign_in(viewer)
        got = client.get(f"/api/v1/meetings/{m['id']}").json()
        return next(p for p in got["participants"] if p["user_id"] == str(bob.user_id))

    assert bobs_row(bob)["proxy_questions"] == ["When is the freeze?"]
    assert bobs_row(host)["proxy_questions"] == ["When is the freeze?"]
    carols_view = bobs_row(carol)
    assert carols_view["attendance"] == "agent"
    assert carols_view["proxy_update"] == "Shipped the API; blocked on design."
    assert carols_view["proxy_questions"] is None
    listed = {x["id"]: x for x in client.get("/api/v1/meetings").json()["items"]}
    bob_listed = next(
        p for p in listed[m["id"]]["participants"] if p["user_id"] == str(bob.user_id)
    )
    assert bob_listed["proxy_questions"] is None and bob_listed["attendance"] == "agent"

    sign_in(stranger)
    assert client.put(url, json={"attendance": "agent"}).status_code == 403
    sign_in(bob)
    assert ["body", "proxy_questions"] in _loc(
        client.put(url, json={"attendance": "agent", "proxy_questions": ["q"] * 6})
    )
    assert ["body", "proxy_update"] in _loc(
        client.put(url, json={"attendance": "agent", "proxy_update": "x" * 2001})
    )
    assert ["body", "attendance"] in _loc(client.put(url, json={"attendance": "remote"}))
    back = client.put(url, json={"attendance": "live"}).json()
    assert (back["attendance"], back["proxy_update"], back["proxy_questions"]) == ("live", None, [])

    _sql("UPDATE meetings SET status = 'ended' WHERE id = %(m)s", {"m": m["id"]})
    assert client.put(url, json={"attendance": "agent"}).status_code == 409

    sign_in(seed_tenant())
    assert client.put(url, json={"attendance": "agent"}).status_code == 404


# --- carry-forward and the confidential rule ---------------------------------------------------


def test_carry_forward_hides_a_confidential_previous_meeting_from_outsiders(client):
    owner = seed_tenant()
    bob = seed_user(owner)
    outsider = seed_user(owner)
    sign_in(owner)
    secret = create(client, confidential=True, participant_ids=[str(bob.user_id)])
    _decision(owner.org_id, secret["id"], "Acquire Acme")
    m = create(
        client,
        previous_meeting_id=secret["id"],
        participant_ids=[str(bob.user_id), str(outsider.user_id)],
    )
    url = f"/api/v1/meetings/{m['id']}/carry-forward"
    assert [d["content"] for d in client.get(url).json()["decisions"]] == ["Acquire Acme"]
    sign_in(outsider)
    assert client.get(url).json() == {
        "previous_meeting": None,
        "decisions": [],
        "open_actions": [],
        "saved_questions": [],
        "open_items": [],
        "unanswered_questions": [],
    }


def test_internal_agenda_carries_brain_fields_and_respects_confidential(client):
    owner = seed_tenant()
    bob = seed_user(owner)
    outsider = seed_user(owner)
    sign_in(owner)
    earlier = create(client, participant_ids=[str(bob.user_id)])
    _decision(owner.org_id, earlier["id"], "Ship in Q1")
    a1 = _action(owner.org_id, earlier["id"], "Draft the plan", assignee=bob.user_id)
    _action(owner.org_id, earlier["id"], "Book venue")
    goal = str(uuid.uuid4())
    m = create(
        client,
        title="Planning",
        previous_meeting_id=earlier["id"],
        participant_ids=[str(bob.user_id)],
        agenda=[{"title": "A", "owner_user_id": str(bob.user_id), "goal_id": goal}],
    )
    sign_in(bob)
    client.put(
        f"/api/v1/meetings/{m['id']}/attendance",
        json={"attendance": "agent", "proxy_update": "On track", "proxy_questions": ["Budget?"]},
    )
    # Bob answers a status request about his action.
    req = client.post(
        f"/internal/meetings/{m['id']}/status-requests",
        headers=_headers(owner.org_id, "workflow-service"),
    ).json()["requests"]
    assert [r["action_id"] for r in req] == [a1]
    client.post(f"/api/v1/status-requests/{req[0]['id']}/answer", json={"status_text": "80% done"})

    body = client.get(f"/internal/meetings/{m['id']}/agenda", headers=_headers(owner.org_id)).json()
    assert (body["title"], body["confidential"]) == ("Planning", False)
    assert body["items"][0]["owner_user_id"] == str(bob.user_id)
    assert body["items"][0]["goal_id"] == goal
    assert body["proxies"] == [
        {"user_id": str(bob.user_id), "proxy_update": "On track", "proxy_questions": ["Budget?"]}
    ]
    assert body["carry_forward"] == {
        "decisions": ["Ship in Q1"],
        "open_actions": ["Draft the plan (latest status: 80% done)", "Book venue"],
        "open_items": [],
        "questions": [],
    }

    # A confidential previous meeting: carry-forward only when everyone here was in it.
    _sql("UPDATE meetings SET confidential = true WHERE id = %(m)s", {"m": earlier["id"]})
    body = client.get(f"/internal/meetings/{m['id']}/agenda", headers=_headers(owner.org_id)).json()
    assert body["carry_forward"]["decisions"] == ["Ship in Q1"]
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id) "
        "VALUES (%(m)s, %(u)s, %(o)s)",
        {"m": m["id"], "u": str(outsider.user_id), "o": str(owner.org_id)},
    )
    body = client.get(f"/internal/meetings/{m['id']}/agenda", headers=_headers(owner.org_id)).json()
    assert body["carry_forward"] == {
        "decisions": [],
        "open_actions": [],
        "open_items": [],
        "questions": [],
    }


def test_related_attendees_never_hop_through_a_confidential_meeting(client):
    owner = seed_tenant()
    peer = seed_user(owner)
    secret_peer = seed_user(owner)
    sign_in(owner)
    open_m = create(client, participant_ids=[str(peer.user_id)])
    secret_m = create(client, confidential=True, participant_ids=[str(secret_peer.user_id)])
    for mid in (open_m["id"], secret_m["id"]):
        _sql("UPDATE meetings SET status = 'ended' WHERE id = %(m)s", {"m": mid})
    now = create(client)
    resp = client.get(
        f"/internal/meetings/{now['id']}/related-attendees",
        params={"user_id": str(owner.user_id)},
        headers=_headers(owner.org_id),
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["user_ids"] == [str(peer.user_id)]


# --- B6: promises -----------------------------------------------------------------------------


def pl_record(
    t: Tenant, meeting_id: Any, commitments_: Any, *, window: str = "w0", key: str | None = None
) -> dict[str, Any]:
    value = result(t, uuid.UUID(str(meeting_id)), 0.9, window=window, key=key)
    payload = {"surfaced": True}
    if commitments_ is not None:
        payload["commitments"] = commitments_
    value["payload_json"] = json.dumps(payload)
    return value


def stored_commitments(meeting_id: Any) -> list[dict[str, Any]]:
    return _sql(
        "SELECT user_id, content, due_hint, status, source_window_id FROM commitments "
        "WHERE meeting_id = %(m)s ORDER BY content",
        {"m": str(meeting_id)},
    )


async def test_commitments_are_persisted_idempotently_and_privately(client):
    me = seed_tenant()
    bob = seed_user(me)
    m = seed_meeting(me.org_id)
    entries = [
        {"content": "  I'll send the deck  ", "due_hint": "by Friday"},
        {"content": "Book the room", "due_hint": None},
        {"content": ""},
        "not an object",
        {"content": "x" * 600, "due_hint": "y" * 150},
    ]
    value = pl_record(me, m, entries)
    assert await apply(value) is True
    rows = stored_commitments(m)
    assert [(r["content"][:20], r["due_hint"]) for r in rows] == [
        ("Book the room", None),
        ("I'll send the deck", "by Friday"),
        ("x" * 20, "y" * 100),
    ]
    assert {r["user_id"] for r in rows} == {str(me.user_id)}
    assert {r["source_window_id"] for r in rows} == {f"{m}:w0"}
    assert len(next(r for r in rows if r["content"].startswith("x"))["content"]) == 500

    assert await apply(value) is False  # redelivery
    replay = pl_record(me, m, entries, key=f"replay-{uuid.uuid4()}")
    assert await apply(replay) is True  # the note is new, the commitments are not
    assert len(stored_commitments(m)) == 3

    # At most 5 per record; a record without commitments, or with a bad payload, still applies.
    many = [{"content": f"Promise {i}"} for i in range(8)]
    await apply(pl_record(me, m, many, window="w1"))
    assert len(stored_commitments(m)) == 8
    await apply(pl_record(me, m, None, window="w2"))
    bad = pl_record(me, m, None, window="w3")
    bad["payload_json"] = "not json"
    assert await apply(bad) is True  # the note still lands
    assert len(stored_commitments(m)) == 8

    sign_in(me)
    items = client.get("/api/v1/commitments").json()["items"]
    assert len(items) == 8 and items[0]["meeting_title"] == "M"
    cid = items[0]["id"]
    sign_in(bob)
    assert client.get("/api/v1/commitments").json()["items"] == []
    assert client.patch(f"/api/v1/commitments/{cid}", json={"status": "done"}).status_code == 404

    sign_in(me)
    done = client.patch(f"/api/v1/commitments/{cid}", json={"status": "done"}).json()
    assert done["status"] == "done" and done["done_at"] is not None
    assert [c["id"] for c in client.get("/api/v1/commitments?status=done").json()["items"]] == [cid]
    reopened = client.patch(f"/api/v1/commitments/{cid}", json={"status": "open"}).json()
    assert reopened["status"] == "open" and reopened["done_at"] is None
    dismissed = client.patch(f"/api/v1/commitments/{cid}", json={"status": "dismissed"}).json()
    assert dismissed["status"] == "dismissed" and dismissed["done_at"] is None
    assert ["body", "status"] in _loc(
        client.patch(f"/api/v1/commitments/{cid}", json={"status": "finished"})
    )
    assert ["query", "status"] in _loc(client.get("/api/v1/commitments?status=nope"))


# --- C4: suggested owner ------------------------------------------------------------------


async def test_suggested_owner_becomes_the_assignee_when_the_agent_named_none(migrated_db):
    me = seed_tenant()
    owner = seed_user(me)
    named = seed_user(me)
    foreign = seed_tenant()
    m = seed_meeting(me.org_id)
    cands = [
        {"id": str(uuid.uuid4()), "content": "A", "confidence": 0.9,
         "suggested_owner_user_id": str(owner.user_id)},
        {"id": str(uuid.uuid4()), "content": "B", "confidence": 0.9,
         "assignee_id": str(named.user_id), "suggested_owner_user_id": str(owner.user_id)},
        {"id": str(uuid.uuid4()), "content": "C", "confidence": 0.9,
         "suggested_owner_user_id": str(foreign.user_id)},
    ]  # fmt: skip
    value = result(me, m, 0.0, agent="ActionAgent", kind="action_candidate")
    value["payload_json"] = json.dumps({"candidates": cands})
    assert await apply(value) is True
    rows = {
        r["content"]: r["assignee_id"]
        for r in _sql(
            "SELECT content, assignee_id FROM action_items WHERE meeting_id = %(m)s",
            {"m": str(m)},
        )
    }
    assert rows == {"A": str(owner.user_id), "B": str(named.user_id), "C": None}


# --- B7: status requests ---------------------------------------------------------------------


def test_status_requests_are_created_once_and_answered_by_the_assignee(client):
    owner = seed_tenant()
    bob = seed_user(owner)
    carol = seed_user(owner)
    sign_in(owner)
    earlier = create(client, participant_ids=[str(bob.user_id), str(carol.user_id)])
    a_bob = _action(owner.org_id, earlier["id"], "Draft the plan", assignee=bob.user_id)
    a_carol = _action(owner.org_id, earlier["id"], "Book venue", assignee=carol.user_id)
    _action(owner.org_id, earlier["id"], "Nobody's", assignee=None)
    _action(owner.org_id, earlier["id"], "Done one", status="done", assignee=bob.user_id)
    _action(owner.org_id, earlier["id"], "Proposed", status="proposed", assignee=bob.user_id)
    m = create(
        client,
        title="Follow-up",
        previous_meeting_id=earlier["id"],
        scheduled_start="2026-12-01T10:00:00Z",
    )
    url = f"/internal/meetings/{m['id']}/status-requests"

    assert client.post(url, headers=_headers(owner.org_id)).status_code == 401  # wrong caller
    first = client.post(url, headers=_headers(owner.org_id, "workflow-service"))
    assert first.status_code == 200, first.text
    reqs = first.json()["requests"]
    assert [(r["action_id"], r["user_id"], r["action_content"]) for r in reqs] == [
        (a_bob, str(bob.user_id), "Draft the plan"),
        (a_carol, str(carol.user_id), "Book venue"),
    ]
    again = client.post(url, headers=_headers(owner.org_id, "workflow-service")).json()
    assert again == first.json()
    assert (
        _sql(
            "SELECT count(*) AS n FROM action_status_requests WHERE meeting_id = %(m)s",
            {"m": m["id"]},
        )[0]["n"]
        == 2
    )
    unlinked = create(client)
    assert client.post(
        f"/internal/meetings/{unlinked['id']}/status-requests",
        headers=_headers(owner.org_id, "workflow-service"),
    ).json() == {"requests": []}
    assert (
        client.post(url, headers=_headers(seed_tenant().org_id, "workflow-service")).status_code
        == 404
    )

    sign_in(bob)
    mine = client.get("/api/v1/status-requests?state=pending").json()["items"]
    assert [(r["action_content"], r["meeting_title"]) for r in mine] == [
        ("Draft the plan", "Follow-up")
    ]
    assert mine[0]["meeting_scheduled_start"].startswith("2026-12-01T10:00:00")
    assert mine[0]["action_title"] == "Draft the plan"
    rid = mine[0]["id"]
    carol_rid = next(r["id"] for r in reqs if r["user_id"] == str(carol.user_id))
    assert (
        client.post(
            f"/api/v1/status-requests/{carol_rid}/answer", json={"status_text": "x"}
        ).status_code
        == 404
    )
    assert ["body", "status_text"] in _loc(
        client.post(f"/api/v1/status-requests/{rid}/answer", json={"status_text": "x" * 501})
    )
    answered = client.post(
        f"/api/v1/status-requests/{rid}/answer", json={"status_text": "Half done"}
    ).json()
    assert answered["state"] == "answered" and answered["status_text"] == "Half done"
    assert client.get("/api/v1/status-requests?state=pending").json()["items"] == []

    sign_in(owner)
    cf = client.get(f"/api/v1/meetings/{m['id']}/carry-forward").json()
    latest = {a["title"]: a["latest_status"] for a in cf["open_actions"]}
    assert latest["Book venue"] is None
    assert latest["Draft the plan"]["text"] == "Half done"
    assert latest["Draft the plan"]["answered_at"] is not None

    # Ending the meeting expires what is still pending; an expired request cannot be answered.
    assert client.patch(f"/api/v1/meetings/{m['id']}", json={"status": "ended"}).status_code == 200
    sign_in(carol)
    [expired] = client.get("/api/v1/status-requests").json()["items"]
    assert expired["state"] == "expired"
    assert (
        client.post(
            f"/api/v1/status-requests/{carol_rid}/answer", json={"status_text": "late"}
        ).status_code
        == 409
    )
    assert client.post(url, headers=_headers(owner.org_id, "workflow-service")).json() == {
        "requests": []
    }


def test_reminders_read_for_the_workflow(client):
    owner = seed_tenant()
    bob = seed_user(owner)
    sign_in(owner)
    earlier = create(client)
    m = create(
        client,
        title="Plan",
        previous_meeting_id=earlier["id"],
        participant_ids=[str(bob.user_id)],
        scheduled_start="2026-12-01T10:00:00Z",
    )
    sign_in(bob)
    client.put(f"/api/v1/meetings/{m['id']}/attendance", json={"attendance": "agent"})
    url = f"/internal/meetings/{m['id']}/reminders"
    body = client.get(url, headers=_headers(owner.org_id, "workflow-service")).json()
    assert body["title"] == "Plan" and body["status"] == "scheduled"
    assert body["previous_meeting_id"] == earlier["id"] and body["confidential"] is False
    assert body["scheduled_start"].startswith("2026-12-01T10:00:00")
    assert sorted((p["user_id"], p["attendance"]) for p in body["participants"]) == sorted(
        [(str(owner.user_id), "live"), (str(bob.user_id), "agent")]
    )
    assert client.get(url, headers=_headers(owner.org_id)).status_code == 401
    assert (
        client.get(url, headers=_headers(seed_tenant().org_id, "workflow-service")).status_code
        == 404
    )


# --- notification-service: ADR-0039 C7 alert types --------------------------------------------


@pytest.mark.parametrize("alert_type", ["already_decided", "topic_watch"])
async def test_new_proactive_alert_types_become_in_app_rows(migrated_db, alert_type):
    from tests.test_notifications import alert_obj, alert_result
    from tests.test_notifications import apply as notify

    t = seed_tenant()
    watcher = seed_user(t)
    m = seed_meeting(t.org_id)  # the watcher is not in it: topic_watch reaches outsiders
    alert = alert_obj(alert_type=alert_type, priority=3, link=f"/meetings/{m}")
    assert await notify("agent-results", alert_result(t, m, alert, recipient=watcher.user_id))
    [row] = _sql(
        "SELECT alert_type, link, source FROM notification_log WHERE user_id = %(u)s",
        {"u": str(watcher.user_id)},
    )
    assert row == {"alert_type": alert_type, "link": f"/meetings/{m}", "source": "agent_alert"}
