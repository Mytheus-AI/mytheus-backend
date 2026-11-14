"""Ask Myte v2 (ADR-0049 R2, F1, F4, F5): core-api `/api/v1/myte/*`.

Against a real Postgres (and Redis for `Idempotency-Key`):

- the migration: the three tables exist with RLS ENABLE + FORCE and the `tenant_isolation` policy,
  a session with no `app.current_org` sees nothing, and the CHECKs refuse what the routes refuse;
- R2: `proposal_states` merges into `payload.proposal_states` (alone or with feedback), keeps the
  rest of the payload, overwrites a key, caps the total at 20, and is Myte-messages and owner only;
- F1 peer questions: ask, list by box and status, get, answer once, decline, withdraw; the asker
  and the recipient see it, a third person and another workspace get 404; the 20-pending cap;
  asking yourself, someone outside the workspace, or from another person's thread; the
  notifications to the recipient and back to the asker; `Idempotency-Key`;
- F4 memories: create (trimmed, deduplicated), list, delete, the 100 cap, owner only;
- F5 routines: create with `next_run_at`, patch (timing recomputes, re-enable recomputes, weekly
  needs a weekday), due, ran, delete, the 10 cap, owner only;
- erasure deletes all three for the erased person, and only theirs.

The scheduler (`myte_schedule.next_run`, across timezones and DST), the pure update rules and
body validation at the bottom run without a database.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg2
import pytest
from pydantic import ValidationError
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.core_api import myte_schedule
from services.core_api.routes import erasure, myte, myte_memory, myte_peers
from tests.conftest import APP_URL, _role_url
from tests.test_chat_api import PRINCIPAL, Tenant, _create_test_app, _sql, seed_tenant, sign_in
from tests.test_my_tasks import add_user

BASE = "/api/v1/myte"


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-myte-v2-test")
    app.include_router(myte.router)
    app.include_router(myte_peers.router)
    app.include_router(myte_memory.router)
    app.include_router(erasure.router)
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


def named(t: Tenant, name: str, agent: str | None = None, seed: str | None = None) -> Tenant:
    _sql(
        "UPDATE users SET display_name = %(n)s, agent_name = %(a)s, agent_avatar_seed = %(s)s "
        "WHERE id = %(u)s",
        {"n": name, "a": agent, "s": seed, "u": str(t.user_id)},
    )
    return t


def _thread(client: TestClient, t: Tenant) -> str:
    sign_in(t)
    res = client.post(f"{BASE}/threads", json={"title": "Pricing"})
    assert res.status_code == 201, res.text
    return str(res.json()["id"])


def _answer_message(client: TestClient, t: Tenant, payload: dict[str, Any] | None = None) -> dict:
    tid = _thread(client, t)
    res = client.post(
        f"{BASE}/threads/{tid}/messages",
        json={
            "messages": [
                {"role": "user", "content": "Add a task?"},
                {"role": "myte", "content": "Shall I?", "payload": payload},
            ]
        },
    )
    assert res.status_code == 201, res.text
    return res.json()["items"]


def notifications(prefix: str) -> list[dict[str, Any]]:
    return [
        r["payload"]
        for r in _sql(
            "SELECT payload FROM outbox WHERE topic = 'notifications' "
            "AND payload->>'source_event_id' LIKE %(p)s ORDER BY id",
            {"p": f"{prefix}%"},
        )
    ]


# --- the migration ------------------------------------------------------------------------------

TABLES = ("myte_memories", "myte_peer_questions", "myte_routines")


def test_migration_creates_three_tables_with_forced_rls(migrated_db):
    rows = _sql(
        "SELECT c.relname, c.relrowsecurity, c.relforcerowsecurity, "
        "(SELECT count(*) FROM pg_policies p WHERE p.tablename = c.relname "
        " AND p.policyname = 'tenant_isolation') AS policies "
        "FROM pg_class c WHERE c.relname = ANY(%(t)s) ORDER BY 1",
        {"t": list(TABLES)},
    )
    assert rows == [
        {"relname": t, "relrowsecurity": True, "relforcerowsecurity": True, "policies": 1}
        for t in TABLES
    ]
    indexes = {r["indexname"] for r in _sql("SELECT indexname FROM pg_indexes")}
    assert {
        "ix_myte_peer_questions_inbox",
        "ix_myte_peer_questions_sent",
        "ix_myte_memories_owner",
        "ix_myte_routines_due",
    } <= indexes


def test_without_app_current_org_the_request_role_sees_nothing(client):
    t = seed_tenant()
    other = add_user(t)
    sign_in(t)
    assert client.post(f"{BASE}/memories", json={"text": "I like brevity"}).status_code == 201
    res = client.post(
        f"{BASE}/routines",
        json={"question": "What changed?", "cadence": "daily", "hour": 9, "timezone": "UTC"},
    )
    assert res.status_code == 201
    res = client.post(
        f"{BASE}/peer-questions", json={"to_user_id": str(other.user_id), "question": "Q?"}
    )
    assert res.status_code == 201
    conn = psycopg2.connect(_role_url("mytheus_app").replace("+asyncpg", ""))
    try:
        with conn.cursor() as cur:
            for table in TABLES:
                cur.execute(f"SELECT count(*) FROM {table}")
                assert cur.fetchone() == (0,), table
    finally:
        conn.close()


def test_the_database_refuses_what_the_routes_refuse(client):
    t = seed_tenant()
    p = {"o": str(t.org_id), "u": str(t.user_id)}
    with pytest.raises(psycopg2.errors.CheckViolation):
        _sql(
            "INSERT INTO myte_peer_questions (org_id, from_user_id, to_user_id, question) "
            "VALUES (%(o)s, %(u)s, %(u)s, 'self?')",
            p,
        )
    with pytest.raises(psycopg2.errors.CheckViolation):
        _sql("INSERT INTO myte_memories (org_id, user_id, text) VALUES (%(o)s, %(u)s, '')", p)
    with pytest.raises(psycopg2.errors.CheckViolation):
        _sql(
            "INSERT INTO myte_routines (org_id, user_id, question, cadence, hour, timezone, "
            "next_run_at) VALUES (%(o)s, %(u)s, 'q', 'weekly', 9, 'UTC', now())",
            p,
        )
    with pytest.raises(psycopg2.errors.CheckViolation):
        _sql(
            "INSERT INTO myte_routines (org_id, user_id, question, cadence, hour, timezone, "
            "next_run_at) VALUES (%(o)s, %(u)s, 'q', 'daily', 24, 'UTC', now())",
            p,
        )


# --- R2: proposal states ------------------------------------------------------------------------


def test_proposal_states_merge_into_the_payload(client):
    t = seed_tenant()
    _, answer = _answer_message(client, t, {"cited": [1], "proposals": [{"id": "p1"}]})
    url = f"{BASE}/messages/{answer['id']}"
    res = client.patch(url, json={"proposal_states": {"p1": "confirmed"}})
    assert res.status_code == 200, res.text
    assert res.json()["payload"] == {
        "cited": [1],
        "proposals": [{"id": "p1"}],
        "proposal_states": {"p1": "confirmed"},
    }
    assert res.json()["feedback"] == 0

    both = client.patch(
        url, json={"feedback": 1, "proposal_states": {"p2": "dismissed", "p1": "dismissed"}}
    ).json()
    assert both["feedback"] == 1
    assert both["payload"]["proposal_states"] == {"p1": "dismissed", "p2": "dismissed"}
    assert both["payload"]["cited"] == [1]

    detail = client.get(f"{BASE}/threads/{answer['thread_id']}").json()
    assert detail["messages"][1]["payload"]["proposal_states"]["p2"] == "dismissed"


def test_proposal_states_start_a_null_payload_and_cap_at_twenty(client):
    t = seed_tenant()
    _, answer = _answer_message(client, t, None)
    url = f"{BASE}/messages/{answer['id']}"
    first = {f"p{n}": "confirmed" for n in range(15)}
    res = client.patch(url, json={"proposal_states": first})
    assert res.status_code == 200 and res.json()["payload"] == {"proposal_states": first}
    more = {f"q{n}": "dismissed" for n in range(6)}
    res = client.patch(url, json={"proposal_states": more})
    assert res.status_code == 422
    assert res.headers["content-type"].startswith("application/problem+json")
    # Overwriting existing keys does not count twice.
    assert client.patch(url, json={"proposal_states": {"p0": "dismissed"}}).status_code == 200
    assert client.patch(url, json={"proposal_states": {"x" * 65: "confirmed"}}).status_code == 422
    assert client.patch(url, json={"proposal_states": {"p1": "maybe"}}).status_code == 422
    assert client.patch(url, json={"proposal_states": {}}).status_code == 422
    assert client.patch(url, json={}).status_code == 422


def test_proposal_states_myte_only_and_owner_only(client):
    t = seed_tenant()
    question, answer = _answer_message(client, t, {})
    body = {"proposal_states": {"p1": "confirmed"}}
    assert client.patch(f"{BASE}/messages/{question['id']}", json=body).status_code == 422
    for intruder in (add_user(t, tier="owner"), seed_tenant("owner")):
        sign_in(intruder)
        assert client.patch(f"{BASE}/messages/{answer['id']}", json=body).status_code == 404
    sign_in(t)
    assert (
        client.get(f"{BASE}/threads/{answer['thread_id']}").json()["messages"][1]["payload"] == {}
    )


# --- F1: peer questions -------------------------------------------------------------------------


def _ask(client: TestClient, asker: Tenant, to: Tenant, **extra: Any) -> dict:
    sign_in(asker)
    res = client.post(
        f"{BASE}/peer-questions",
        json={"to_user_id": str(to.user_id), "question": "  What did the client say?  ", **extra},
    )
    assert res.status_code == 201, res.text
    return res.json()


def test_ask_answer_and_both_sides_see_it(client):
    asker = named(seed_tenant(), "Maya Patel")
    samar = named(add_user(asker), "Samar Khan", agent="Pip", seed="pip-7")
    tid = _thread(client, asker)
    q = _ask(client, asker, samar, thread_id=tid)
    assert q["question"] == "What did the client say?"
    assert q["status"] == "pending" and q["answer"] is None and q["answered_at"] is None
    assert q["thread_id"] == tid
    assert q["from_user"] == {
        "id": str(asker.user_id),
        "name": "Maya Patel",
        "agent_name": "Maya's Myte",
        "agent_avatar_seed": None,
    }
    assert q["to_user"] == {
        "id": str(samar.user_id),
        "name": "Samar Khan",
        "agent_name": "Pip",
        "agent_avatar_seed": "pip-7",
    }
    [note] = notifications(f"mytepeer:{q['id']}")
    assert note["user_id"] == str(samar.user_id) and note["channel"] == "in_app"
    assert note["title"] == "Maya Patel asked your Myte a question"
    assert note["body"] == "What did the client say?"
    assert note["link"] == f"/inbox?peer_question={q['id']}"

    sent = client.get(f"{BASE}/peer-questions", params={"box": "sent"}).json()["items"]
    assert [i["id"] for i in sent] == [q["id"]]
    assert client.get(f"{BASE}/peer-questions").json()["items"] == []

    sign_in(samar)
    inbox = client.get(f"{BASE}/peer-questions", params={"status": "pending"}).json()["items"]
    assert [i["id"] for i in inbox] == [q["id"]]
    assert inbox[0]["thread_id"] is None, "the asker's thread is not the recipient's business"
    res = client.post(
        f"{BASE}/peer-questions/{q['id']}/answer", json={"answer": "  They want annual.  "}
    )
    assert res.status_code == 200, res.text
    done = res.json()
    assert done["status"] == "answered" and done["answer"] == "They want annual."
    assert done["answered_at"] is not None
    again = client.post(f"{BASE}/peer-questions/{q['id']}/answer", json={"answer": "x"})
    assert again.status_code == 409
    assert client.post(f"{BASE}/peer-questions/{q['id']}/decline", json={}).status_code == 409

    [back] = notifications(f"mytepeer:{q['id']}:answered")
    assert back["user_id"] == str(asker.user_id)
    assert back["title"] == "Pip (Samar Khan's Myte) answered your question"
    assert back["body"] == "They want annual." and back["link"] == f"/ask?thread={tid}"

    sign_in(asker)
    seen = client.get(f"{BASE}/peer-questions/{q['id']}").json()
    assert seen["status"] == "answered" and seen["answer"] == "They want annual."
    assert seen["thread_id"] == tid
    assert (
        client.get(f"{BASE}/peer-questions", params={"box": "sent", "status": "pending"}).json()[
            "items"
        ]
        == []
    )


def test_decline_and_withdraw(client):
    asker = named(seed_tenant(), "Maya Patel")
    samar = named(add_user(asker), "Samar Khan")
    q = _ask(client, asker, samar)
    assert (
        client.post(f"{BASE}/peer-questions/{q['id']}/answer", json={"answer": "x"}).status_code
        == 409
    )
    assert client.post(f"{BASE}/peer-questions/{q['id']}/decline", json={}).status_code == 409

    sign_in(samar)
    assert client.delete(f"{BASE}/peer-questions/{q['id']}").status_code == 409
    res = client.post(f"{BASE}/peer-questions/{q['id']}/decline", json={"reason": " Ask Raj "})
    assert res.status_code == 200, res.text
    assert res.json()["status"] == "declined" and res.json()["decline_reason"] == "Ask Raj"
    assert res.json()["answered_at"] is not None
    [back] = notifications(f"mytepeer:{q['id']}:declined")
    assert back["user_id"] == str(asker.user_id) and back["body"] == "Ask Raj"
    assert back["link"] == "/ask"

    sign_in(asker)
    assert client.delete(f"{BASE}/peer-questions/{q['id']}").status_code == 409
    pending = _ask(client, asker, samar)
    assert client.delete(f"{BASE}/peer-questions/{pending['id']}").status_code == 204
    assert client.get(f"{BASE}/peer-questions/{pending['id']}").status_code == 404


def test_a_third_person_or_workspace_gets_404(client):
    asker = seed_tenant()
    samar = add_user(asker)
    q = _ask(client, asker, samar)
    for intruder in (add_user(asker, tier="owner"), seed_tenant("owner")):
        sign_in(intruder)
        qid = q["id"]
        assert client.get(f"{BASE}/peer-questions/{qid}").status_code == 404
        res = client.post(f"{BASE}/peer-questions/{qid}/answer", json={"answer": "x"})
        assert res.status_code == 404
        assert res.headers["content-type"].startswith("application/problem+json")
        assert client.post(f"{BASE}/peer-questions/{qid}/decline", json={}).status_code == 404
        assert client.delete(f"{BASE}/peer-questions/{qid}").status_code == 404
        for box in ("inbox", "sent"):
            listed = client.get(f"{BASE}/peer-questions", params={"box": box}).json()
            assert listed["items"] == []


def test_ask_refuses_self_strangers_foreign_threads_and_bad_text(client):
    asker = seed_tenant()
    samar = add_user(asker)
    stranger = seed_tenant()
    gone = add_user(asker)
    _sql("UPDATE users SET deleted_at = now() WHERE id = %(u)s", {"u": str(gone.user_id)})
    their_thread = _thread(client, samar)
    sign_in(asker)
    url = f"{BASE}/peer-questions"
    assert (
        client.post(url, json={"to_user_id": str(asker.user_id), "question": "Hi"}).status_code
        == 422
    )
    for who in (stranger.user_id, gone.user_id, uuid.uuid4()):
        assert client.post(url, json={"to_user_id": str(who), "question": "Hi"}).status_code == 404
    res = client.post(
        url,
        json={"to_user_id": str(samar.user_id), "question": "Hi", "thread_id": their_thread},
    )
    assert res.status_code == 404
    for bad in ("   ", "x" * 1001):
        res = client.post(url, json={"to_user_id": str(samar.user_id), "question": bad})
        assert res.status_code == 422


def test_at_most_twenty_pending_per_asker(client):
    asker = seed_tenant()
    samar = add_user(asker)
    asked = [_ask(client, asker, samar) for _ in range(20)]
    res = client.post(
        f"{BASE}/peer-questions", json={"to_user_id": str(samar.user_id), "question": "21?"}
    )
    assert res.status_code == 422
    sign_in(samar)
    client.post(f"{BASE}/peer-questions/{asked[0]['id']}/answer", json={"answer": "Done"})
    _ask(client, asker, samar)


def test_idempotency_key_asks_once(client):
    asker = seed_tenant()
    samar = add_user(asker)
    sign_in(asker)
    key = {"Idempotency-Key": f"peer-{uuid.uuid4()}"}
    body = {"to_user_id": str(samar.user_id), "question": "Once?"}
    first = client.post(f"{BASE}/peer-questions", json=body, headers=key)
    again = client.post(f"{BASE}/peer-questions", json=body, headers=key)
    assert first.status_code == again.status_code == 201
    assert again.headers.get("X-Idempotent-Replayed") == "true"
    assert again.json() == first.json()
    stored = _sql(
        "SELECT count(*) AS n FROM myte_peer_questions WHERE from_user_id = %(u)s",
        {"u": str(asker.user_id)},
    )
    assert stored == [{"n": 1}]
    assert len(notifications(f"mytepeer:{first.json()['id']}")) == 1


# --- F4: memories -------------------------------------------------------------------------------


def test_memories_create_list_dedupe_and_delete(client):
    t = seed_tenant()
    tid = _thread(client, t)
    res = client.post(
        f"{BASE}/memories", json={"text": "  I prefer   short answers ", "source_thread_id": tid}
    )
    assert res.status_code == 201, res.text
    first = res.json()
    assert first["text"] == "I prefer short answers" and first["source_thread_id"] == tid
    same = client.post(f"{BASE}/memories", json={"text": "i prefer short answers"})
    assert same.status_code == 201 and same.json()["id"] == first["id"]
    second = client.post(f"{BASE}/memories", json={"text": "I run the Atlas team"}).json()
    listed = client.get(f"{BASE}/memories").json()["items"]
    assert [m["id"] for m in listed] == [second["id"], first["id"]]
    assert set(listed[0]) == {"id", "text", "source_thread_id", "created_at"}

    assert client.delete(f"{BASE}/memories/{first['id']}").status_code == 204
    assert client.delete(f"{BASE}/memories/{first['id']}").status_code == 404
    for bad in ("   ", "x" * 301):
        assert client.post(f"{BASE}/memories", json={"text": bad}).status_code == 422

    # Deleting the thread keeps the memory and forgets where it came from.
    third = client.post(
        f"{BASE}/memories", json={"text": "Fiscal year starts in April", "source_thread_id": tid}
    ).json()
    assert client.delete(f"{BASE}/threads/{tid}").status_code == 204
    kept = {m["id"]: m for m in client.get(f"{BASE}/memories").json()["items"]}
    assert kept[third["id"]]["source_thread_id"] is None


def test_memories_cap_and_owner_only(client):
    t = seed_tenant()
    colleague = add_user(t, tier="owner")
    their_thread = _thread(client, colleague)
    sign_in(t)
    _sql(
        "INSERT INTO myte_memories (org_id, user_id, text) "
        "SELECT %(o)s, %(u)s, 'fact ' || n FROM generate_series(1, 100) n",
        {"o": str(t.org_id), "u": str(t.user_id)},
    )
    assert client.post(f"{BASE}/memories", json={"text": "one more"}).status_code == 422
    mine = client.get(f"{BASE}/memories").json()["items"]
    assert len(mine) == 100

    for intruder in (colleague, seed_tenant("owner")):
        sign_in(intruder)
        assert client.get(f"{BASE}/memories").json()["items"] == []
        assert client.delete(f"{BASE}/memories/{mine[0]['id']}").status_code == 404
    sign_in(colleague)
    res = client.post(f"{BASE}/memories", json={"text": "x", "source_thread_id": their_thread})
    assert res.status_code == 201
    sign_in(t)
    res = client.post(f"{BASE}/memories", json={"text": "y", "source_thread_id": their_thread})
    assert res.status_code == 404


# --- F5: routines -------------------------------------------------------------------------------


def _routine(client: TestClient, **body: Any) -> dict:
    payload = {
        "question": "What changed in pricing?",
        "cadence": "weekly",
        "weekday": 0,
        "hour": 9,
        "timezone": "Europe/London",
        **body,
    }
    res = client.post(f"{BASE}/routines", json=payload)
    assert res.status_code == 201, res.text
    return res.json()


def test_routines_create_patch_due_ran_and_delete(client):
    t = seed_tenant()
    sign_in(t)
    before = datetime.now(UTC)
    r = _routine(client, scope={"kind": "all", "label": "Everything"})
    assert r["enabled"] is True and r["last_run_at"] is None and r["last_thread_id"] is None
    assert r["scope"]["label"] == "Everything"
    next_at = datetime.fromisoformat(r["next_run_at"])
    assert next_at > before
    local = next_at.astimezone(myte_schedule.zone("Europe/London"))
    assert (local.weekday(), local.hour, local.minute) == (0, 9, 0)
    assert next_at - before <= timedelta(days=7)

    daily = client.patch(f"{BASE}/routines/{r['id']}", json={"cadence": "daily", "hour": 7})
    assert daily.status_code == 200, daily.text
    assert daily.json()["weekday"] is None and daily.json()["hour"] == 7
    assert datetime.fromisoformat(daily.json()["next_run_at"]) <= next_at + timedelta(days=1)
    res = client.patch(f"{BASE}/routines/{r['id']}", json={"cadence": "weekly"})
    assert res.status_code == 422, "weekly needs a weekday"
    res = client.patch(f"{BASE}/routines/{r['id']}", json={"cadence": "weekly", "weekday": 4})
    assert res.status_code == 200 and res.json()["weekday"] == 4
    renamed = client.patch(f"{BASE}/routines/{r['id']}", json={"question": "Pricing news?"})
    assert renamed.json()["next_run_at"] == res.json()["next_run_at"], "text only: same slot"

    # Make it due, then see it listed, run it, and watch it move on.
    _sql(
        "UPDATE myte_routines SET next_run_at = now() - interval '2 days' WHERE id = %(id)s",
        {"id": r["id"]},
    )
    due = client.get(f"{BASE}/routines/due").json()["items"]
    assert [d["id"] for d in due] == [r["id"]]
    tid = _thread(client, t)
    ran = client.post(f"{BASE}/routines/{r['id']}/ran", json={"thread_id": tid})
    assert ran.status_code == 200, ran.text
    assert ran.json()["last_thread_id"] == tid and ran.json()["last_run_at"] is not None
    assert datetime.fromisoformat(ran.json()["next_run_at"]) > datetime.now(UTC)
    assert client.get(f"{BASE}/routines/due").json()["items"] == []

    # Disabled routines are never due; enabling one again starts from the next slot.
    _sql(
        "UPDATE myte_routines SET next_run_at = now() - interval '1 day', enabled = false "
        "WHERE id = %(id)s",
        {"id": r["id"]},
    )
    assert client.get(f"{BASE}/routines/due").json()["items"] == []
    back = client.patch(f"{BASE}/routines/{r['id']}", json={"enabled": True}).json()
    assert datetime.fromisoformat(back["next_run_at"]) > datetime.now(UTC)

    assert [x["id"] for x in client.get(f"{BASE}/routines").json()["items"]] == [r["id"]]
    assert client.delete(f"{BASE}/routines/{r['id']}").status_code == 204
    assert client.delete(f"{BASE}/routines/{r['id']}").status_code == 404


def test_routine_validation(client):
    t = seed_tenant()
    sign_in(t)
    url = f"{BASE}/routines"
    ok = {"question": "Q?", "cadence": "daily", "hour": 9, "timezone": "UTC"}
    for bad in (
        {**ok, "timezone": "Mars/Olympus"},
        {**ok, "timezone": "../etc/passwd"},
        {**ok, "hour": 24},
        {**ok, "cadence": "monthly"},
        {**ok, "cadence": "weekly"},
        {**ok, "weekday": 2},
        {**ok, "cadence": "weekly", "weekday": 7},
        {**ok, "question": "  "},
        {**ok, "question": "x" * 1001},
        {**ok, "scope": {"kind": "planet"}},
    ):
        assert client.post(url, json=bad).status_code == 422, bad
    r = client.post(url, json=ok).json()
    assert client.patch(f"{url}/{r['id']}", json={}).status_code == 422
    assert client.patch(f"{url}/{r['id']}", json={"hour": None}).status_code == 422
    assert client.patch(f"{url}/{r['id']}", json={"timezone": "Nowhere"}).status_code == 422


def test_routines_cap_and_owner_only(client):
    t = seed_tenant()
    sign_in(t)
    made = [_routine(client, cadence="daily", weekday=None) for _ in range(10)]
    res = client.post(
        f"{BASE}/routines",
        json={"question": "11?", "cadence": "daily", "hour": 9, "timezone": "UTC"},
    )
    assert res.status_code == 422
    colleague = add_user(t, tier="owner")
    their_thread = _thread(client, colleague)
    rid = made[0]["id"]
    for intruder in (colleague, seed_tenant("owner")):
        sign_in(intruder)
        assert client.get(f"{BASE}/routines").json()["items"] == []
        assert client.get(f"{BASE}/routines/due").json()["items"] == []
        assert client.patch(f"{BASE}/routines/{rid}", json={"hour": 3}).status_code == 404
        assert client.delete(f"{BASE}/routines/{rid}").status_code == 404
        res = client.post(f"{BASE}/routines/{rid}/ran", json={"thread_id": their_thread})
        assert res.status_code == 404
    sign_in(t)
    res = client.post(f"{BASE}/routines/{rid}/ran", json={"thread_id": their_thread})
    assert res.status_code == 404, "the thread must be the owner's"


def test_routine_idempotency_key_creates_once(client):
    t = seed_tenant()
    sign_in(t)
    key = {"Idempotency-Key": f"routine-{uuid.uuid4()}"}
    body = {"question": "Once?", "cadence": "weekdays", "hour": 8, "timezone": "Asia/Kolkata"}
    first = client.post(f"{BASE}/routines", json=body, headers=key)
    again = client.post(f"{BASE}/routines", json=body, headers=key)
    assert first.status_code == again.status_code == 201
    assert again.json() == first.json()
    assert len(client.get(f"{BASE}/routines").json()["items"]) == 1


# --- erasure ------------------------------------------------------------------------------------


def test_erasure_deletes_their_questions_memories_and_routines(client):
    admin = seed_tenant("owner")
    gone = add_user(admin)
    keep = add_user(admin)
    _ask(client, gone, keep)
    _ask(client, keep, gone)
    between_others = _ask(client, keep, admin)
    for who in (gone, keep):
        sign_in(who)
        client.post(f"{BASE}/memories", json={"text": f"memory of {who.user_id}"})
        _routine(client)

    sign_in(admin)
    res = client.post(
        "/api/v1/erasures",
        json={"kind": "person", "target_id": str(gone.user_id), "confirm": "v@example.com"},
    )
    assert res.status_code == 200, res.text
    erased = res.json()["erased"]
    assert erased["myte_peer_questions"] == 2
    assert erased["myte_memories"] == 1 and erased["myte_routines"] == 1
    left = _sql(
        "SELECT (SELECT count(*) FROM myte_peer_questions WHERE from_user_id = %(g)s "
        " OR to_user_id = %(g)s) AS q, "
        "(SELECT count(*) FROM myte_memories WHERE user_id = %(g)s) AS m, "
        "(SELECT count(*) FROM myte_routines WHERE user_id = %(g)s) AS r, "
        "(SELECT count(*) FROM myte_memories WHERE user_id = %(k)s) AS km, "
        "(SELECT count(*) FROM myte_routines WHERE user_id = %(k)s) AS kr",
        {"g": str(gone.user_id), "k": str(keep.user_id)},
    )
    assert left == [{"q": 0, "m": 0, "r": 0, "km": 1, "kr": 1}]
    sign_in(keep)
    assert client.get(f"{BASE}/peer-questions/{between_others['id']}").status_code == 200


# --- pure: no database --------------------------------------------------------------------------

NY = "America/New_York"


def _utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=UTC)


def test_next_run_daily_is_strictly_after():
    assert myte_schedule.next_run("daily", 9, "UTC", _utc(2026, 11, 14, 8, 59)) == _utc(
        2026, 11, 14, 9
    )
    assert myte_schedule.next_run("daily", 9, "UTC", _utc(2026, 11, 14, 9)) == _utc(
        2026, 11, 15, 9
    ), "exactly on the slot moves to the next one"


def test_next_run_in_a_timezone_ahead_of_utc():
    # 09:00 in Kolkata (UTC+5:30) is 03:30 UTC; at 04:00 UTC today's slot has passed.
    assert myte_schedule.next_run("daily", 9, "Asia/Kolkata", _utc(2026, 11, 14, 4)) == _utc(
        2026, 11, 15, 3, 30
    )
    # Late on the 14th in UTC is already the 15th in Tokyo.
    assert myte_schedule.next_run("daily", 8, "Asia/Tokyo", _utc(2026, 11, 14, 22)) == _utc(
        2026, 11, 14, 23
    )


def test_next_run_weekdays_skips_the_weekend():
    # Friday 2026-11-13 after 9:00 New York (EST, UTC-5) -> Monday 2026-11-16 14:00 UTC.
    assert myte_schedule.next_run("weekdays", 9, NY, _utc(2026, 11, 13, 15)) == _utc(
        2026, 11, 16, 14
    )
    assert myte_schedule.next_run("weekdays", 9, NY, _utc(2026, 11, 16, 13)) == _utc(
        2026, 11, 16, 14
    )


def test_next_run_weekly_uses_monday_zero():
    # Saturday 2026-11-14 -> Monday 2026-11-16 (weekday 0), and Sunday is 6.
    assert myte_schedule.next_run("weekly", 9, "UTC", _utc(2026, 11, 14, 12), 0) == _utc(
        2026, 11, 16, 9
    )
    assert myte_schedule.next_run("weekly", 9, "UTC", _utc(2026, 11, 14, 12), 6) == _utc(
        2026, 11, 15, 9
    )
    # Monday after the slot -> next Monday.
    assert myte_schedule.next_run("weekly", 9, "UTC", _utc(2026, 11, 16, 9, 1), 0) == _utc(
        2026, 11, 23, 9
    )


def test_next_run_follows_daylight_saving():
    # London: 09:00 is 08:00 UTC in summer (BST) and 09:00 UTC in winter (GMT). Clocks go back
    # on Sunday 2026-10-25.
    london = "Europe/London"
    assert myte_schedule.next_run("daily", 9, london, _utc(2026, 10, 24, 9)) == _utc(
        2026, 10, 25, 9
    )
    assert myte_schedule.next_run("daily", 9, london, _utc(2026, 10, 23, 9)) == _utc(
        2026, 10, 24, 8
    )
    # New York springs forward on 2026-03-08: 09:00 is 14:00 UTC before, 13:00 UTC after.
    assert myte_schedule.next_run("daily", 9, NY, _utc(2026, 3, 7, 15)) == _utc(2026, 3, 8, 13)


def test_next_run_in_the_skipped_and_repeated_hours():
    # 02:00 does not exist in New York on 2026-03-08; it runs at 03:00 EDT (07:00 UTC).
    assert myte_schedule.next_run("daily", 2, NY, _utc(2026, 3, 8, 0)) == _utc(2026, 3, 8, 7)
    # 01:00 happens twice on 2026-11-01; it runs once, at the first (EDT, 05:00 UTC).
    first = myte_schedule.next_run("daily", 1, NY, _utc(2026, 11, 1, 0))
    assert first == _utc(2026, 11, 1, 5)
    assert myte_schedule.next_run("daily", 1, NY, first) == _utc(2026, 11, 2, 6)


def test_next_run_rejects_bad_input():
    with pytest.raises(ValueError):
        myte_schedule.next_run("daily", 9, "UTC", datetime(2026, 1, 1))
    with pytest.raises(ValueError):
        myte_schedule.next_run("daily", 24, "UTC", _utc(2026, 1, 1))
    with pytest.raises(ValueError):
        myte_schedule.next_run("weekly", 9, "UTC", _utc(2026, 1, 1))
    for bad in ("", "Mars/Olympus", "/etc/localtime", "../UTC", "x" * 65):
        with pytest.raises(ValueError):
            myte_schedule.zone(bad)


def _current(**over: Any) -> dict[str, Any]:
    return {
        "question": "Q?",
        "scope": None,
        "cadence": "weekly",
        "weekday": 0,
        "hour": 9,
        "timezone": "UTC",
        "enabled": True,
        "next_run_at": _utc(2026, 11, 16, 9),
        **over,
    }


def test_apply_update_rules():
    now = _utc(2026, 11, 14, 12)
    upd = myte_memory.MyteRoutineUpdate
    same = myte_memory.apply_update(_current(), upd(question="New?"), now)
    assert same["question"] == "New?" and same["next_run_at"] == _utc(2026, 11, 16, 9)
    daily = myte_memory.apply_update(_current(), upd(cadence="daily"), now)
    assert daily["weekday"] is None and daily["next_run_at"] == _utc(2026, 11, 15, 9)
    friday = myte_memory.apply_update(_current(), upd(weekday=4), now)
    assert friday["next_run_at"] == _utc(2026, 11, 20, 9)
    with pytest.raises(ValueError):
        myte_memory.apply_update(
            _current(cadence="daily", weekday=None), upd(cadence="weekly"), now
        )
    stale = _utc(2026, 11, 1, 9)
    off = _current(enabled=False, next_run_at=stale)
    assert myte_memory.apply_update(off, upd(enabled=True), now)["next_run_at"] == _utc(
        2026, 11, 16, 9
    )
    assert myte_memory.apply_update(off, upd(question="x"), now)["next_run_at"] == stale
    cleared = myte_memory.apply_update(_current(scope={"kind": "all"}), upd(scope=None), now)
    assert cleared["scope"] is None


def test_body_validation():
    with pytest.raises(ValidationError):
        myte.MyteFeedback()
    with pytest.raises(ValidationError):
        myte.MyteFeedback(feedback_reason="why")
    with pytest.raises(ValidationError):
        myte.MyteFeedback(proposal_states={f"p{n}": "confirmed" for n in range(21)})
    myte.MyteFeedback(proposal_states={"p": "dismissed"})
    myte.MyteFeedback(feedback=1, feedback_reason="good")
    assert myte_peers.MytePeerQuestionCreate(
        to_user_id=uuid.uuid4(), question=" a\nb "
    ).question == ("a\nb")
    with pytest.raises(ValidationError):
        myte_peers.MytePeerAnswer(answer=" ")
    assert myte_peers.MytePeerDecline(reason="  ").reason is None
    assert myte_memory.MyteMemoryCreate(text=" a  b ").text == "a b"
    with pytest.raises(ValidationError):
        myte_memory.MyteRoutineCreate(
            question="q", cadence="daily", weekday=1, hour=1, timezone="UTC"
        )


def test_merge_proposal_states_keeps_the_rest():
    assert myte.merge_proposal_states(
        {"a": 1, "proposal_states": {"x": "confirmed"}}, {"y": "dismissed"}
    ) == {
        "a": 1,
        "proposal_states": {"x": "confirmed", "y": "dismissed"},
    }
    assert myte.merge_proposal_states({"proposal_states": "junk"}, {"y": "confirmed"}) == {
        "proposal_states": {"y": "confirmed"}
    }


def test_people_fall_back_to_their_first_name():
    row = {
        "fu_id": uuid.uuid4(),
        "fu_email": "samar.khan@acme.com",
        "fu_display_name": None,
        "fu_agent_name": "  ",
        "fu_agent_avatar_seed": "",
    }
    p = myte_peers.person(row, "fu")
    assert p.agent_name.endswith("'s Myte") and p.agent_avatar_seed is None
    assert p.name


def test_routes_are_registered_as_adr_0049():
    paths = {
        (sorted(r.methods)[0], r.path)  # type: ignore[attr-defined]
        for router in (myte_peers.router, myte_memory.router)
        for r in router.routes
    }
    assert paths == {
        ("POST", "/api/v1/myte/peer-questions"),
        ("GET", "/api/v1/myte/peer-questions"),
        ("GET", "/api/v1/myte/peer-questions/{question_id}"),
        ("POST", "/api/v1/myte/peer-questions/{question_id}/answer"),
        ("POST", "/api/v1/myte/peer-questions/{question_id}/decline"),
        ("DELETE", "/api/v1/myte/peer-questions/{question_id}"),
        ("GET", "/api/v1/myte/memories"),
        ("POST", "/api/v1/myte/memories"),
        ("DELETE", "/api/v1/myte/memories/{memory_id}"),
        ("GET", "/api/v1/myte/routines"),
        ("POST", "/api/v1/myte/routines"),
        ("GET", "/api/v1/myte/routines/due"),
        ("PATCH", "/api/v1/myte/routines/{routine_id}"),
        ("DELETE", "/api/v1/myte/routines/{routine_id}"),
        ("POST", "/api/v1/myte/routines/{routine_id}/ran"),
    }
