"""Ask Myte threads (ADR-0047 D7): core-api `/api/v1/myte/*`.

Against a real Postgres (and Redis for `Idempotency-Key`):

- the migration: both tables exist with RLS ENABLE + FORCE and the `tenant_isolation` policy, and
  a session with no `app.current_org` sees nothing (fail closed);
- CRUD: create (title default, trim, scope), get with messages oldest first, rename/pin, delete
  one, delete all (only the caller's);
- owner only: another person in the same workspace, and a person in another workspace, get 404
  on every route, and their lists never show the thread;
- messages: 1 or 2 per save, in order, `last_message_at` bumped; a replayed `Idempotency-Key`
  stores the turn once; a blank question, 3 messages or a payload over 64 KB is a 422;
- feedback: on Myte's answers only; 0 clears the reason;
- the list: pinned first, then newest; keyset pagination with no gaps or repeats; `q` matches a
  title or a message's text, with `%` matching itself; a mangled cursor is 400.

The pure checks at the bottom (title cleaning, the ILIKE escape, the payload limit, body
validation, the cursor) run without a database.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

import psycopg2
import pytest
from pydantic import ValidationError
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.errors import ProblemDetail
from services.core_api.routes import myte
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
    app = _create_test_app("core-api-myte-test")
    app.include_router(myte.router)
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


def _thread(client: TestClient, t: Tenant, **body: object) -> dict:
    sign_in(t)
    res = client.post(f"{BASE}/threads", json=body)
    assert res.status_code == 201, res.text
    return res.json()


def _turn(client: TestClient, thread_id: str, question: str = "What did we decide?") -> list:
    res = client.post(
        f"{BASE}/threads/{thread_id}/messages",
        json={
            "messages": [
                {"role": "user", "content": question},
                {"role": "myte", "content": "We chose Atlas [1].", "payload": {"cited": [1]}},
            ]
        },
    )
    assert res.status_code == 201, res.text
    return res.json()["items"]


# --- the migration ------------------------------------------------------------------------------


def test_migration_creates_both_tables_with_forced_rls(migrated_db):
    rows = _sql(
        "SELECT c.relname, c.relrowsecurity, c.relforcerowsecurity, "
        "(SELECT count(*) FROM pg_policies p WHERE p.tablename = c.relname "
        " AND p.policyname = 'tenant_isolation') AS policies "
        "FROM pg_class c WHERE c.relname IN ('myte_threads', 'myte_messages') ORDER BY 1"
    )
    assert rows == [
        {
            "relname": "myte_messages",
            "relrowsecurity": True,
            "relforcerowsecurity": True,
            "policies": 1,
        },
        {
            "relname": "myte_threads",
            "relrowsecurity": True,
            "relforcerowsecurity": True,
            "policies": 1,
        },
    ]
    indexes = {r["indexname"] for r in _sql("SELECT indexname FROM pg_indexes")}
    assert {"ix_myte_threads_owner_recent", "ix_myte_messages_thread"} <= indexes


def test_without_app_current_org_the_request_role_sees_nothing(client):
    t = seed_tenant()
    _thread(client, t, title="Fail closed")
    conn = psycopg2.connect(_role_url("mytheus_app").replace("+asyncpg", ""))
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM myte_threads")
            assert cur.fetchone() == (0,)
            cur.execute("SELECT count(*) FROM myte_messages")
            assert cur.fetchone() == (0,)
    finally:
        conn.close()


def test_the_database_refuses_a_bad_role_or_feedback(client):
    t = seed_tenant()
    thread = _thread(client, t)
    params = {"o": str(t.org_id), "t": thread["id"], "u": str(t.user_id)}
    with pytest.raises(psycopg2.errors.CheckViolation):
        _sql(
            "INSERT INTO myte_messages (org_id, thread_id, user_id, role, content) "
            "VALUES (%(o)s, %(t)s, %(u)s, 'admin', 'x')",
            params,
        )
    with pytest.raises(psycopg2.errors.CheckViolation):
        _sql(
            "INSERT INTO myte_messages (org_id, thread_id, user_id, role, content, feedback) "
            "VALUES (%(o)s, %(t)s, %(u)s, 'myte', 'x', 2)",
            params,
        )


# --- CRUD ---------------------------------------------------------------------------------------


def test_create_get_rename_pin_and_delete(client):
    t = seed_tenant()
    blank = _thread(client, t, title="   ")
    assert blank["title"] == "New question" and blank["pinned"] is False
    assert blank["scope"] is None
    long = _thread(
        client,
        t,
        title="  Why   did we " + "x" * 300,
        scope={"kind": "meeting", "meeting_id": str(uuid.uuid4())},
    )
    assert long["title"].startswith("Why did we x") and len(long["title"]) == 120
    assert long["scope"]["kind"] == "meeting"

    _turn(client, blank["id"])
    detail = client.get(f"{BASE}/threads/{blank['id']}").json()
    assert [m["role"] for m in detail["messages"]] == ["user", "myte"]
    assert detail["messages"][1]["payload"] == {"cited": [1]}
    assert detail["messages"][1]["feedback"] == 0

    res = client.patch(f"{BASE}/threads/{blank['id']}", json={"title": "Atlas", "pinned": True})
    assert res.status_code == 200, res.text
    assert res.json()["title"] == "Atlas" and res.json()["pinned"] is True
    assert client.patch(f"{BASE}/threads/{blank['id']}", json={}).status_code == 422
    assert client.patch(f"{BASE}/threads/{blank['id']}", json={"pinned": None}).status_code == 422

    assert client.delete(f"{BASE}/threads/{blank['id']}").status_code == 204
    assert client.get(f"{BASE}/threads/{blank['id']}").status_code == 404
    assert client.delete(f"{BASE}/threads/{blank['id']}").status_code == 404
    left = _sql(
        "SELECT count(*) AS n FROM myte_messages WHERE thread_id = %(t)s", {"t": blank["id"]}
    )
    assert left == [{"n": 0}], "messages cascade with their thread"


def test_delete_all_removes_only_the_callers_threads(client):
    t = seed_tenant()
    colleague = add_user(t)
    mine = [_thread(client, t)["id"] for _ in range(3)]
    _turn(client, mine[0])
    theirs = _thread(client, colleague)["id"]

    sign_in(t)
    assert client.delete(f"{BASE}/threads").status_code == 204
    assert client.get(f"{BASE}/threads").json()["items"] == []
    owned = _sql(
        "SELECT count(*) AS n FROM myte_threads WHERE user_id = %(u)s", {"u": str(t.user_id)}
    )
    assert owned == [{"n": 0}]

    sign_in(colleague)
    assert [i["id"] for i in client.get(f"{BASE}/threads").json()["items"]] == [theirs]


# --- owner only ---------------------------------------------------------------------------------


def test_another_person_or_workspace_gets_404_everywhere(client):
    owner = seed_tenant()
    thread = _thread(client, owner, title="Private")
    message = _turn(client, thread["id"])[1]

    colleague = add_user(owner, tier="owner")  # an admin of the same workspace: still a 404
    stranger = seed_tenant("owner")
    for intruder in (colleague, stranger):
        sign_in(intruder)
        tid, mid = thread["id"], message["id"]
        assert client.get(f"{BASE}/threads/{tid}").status_code == 404
        assert client.patch(f"{BASE}/threads/{tid}", json={"pinned": True}).status_code == 404
        res = client.post(
            f"{BASE}/threads/{tid}/messages",
            json={"messages": [{"role": "user", "content": "hi"}]},
        )
        assert res.status_code == 404
        assert res.headers["content-type"].startswith("application/problem+json")
        assert client.patch(f"{BASE}/messages/{mid}", json={"feedback": -1}).status_code == 404
        assert client.delete(f"{BASE}/threads/{tid}").status_code == 404
        assert client.get(f"{BASE}/threads").json()["items"] == []
        assert client.delete(f"{BASE}/threads").status_code == 204

    sign_in(owner)
    still = client.get(f"{BASE}/threads/{thread['id']}").json()
    assert still["title"] == "Private" and still["pinned"] is False
    assert still["messages"][1]["feedback"] == 0


# --- messages -----------------------------------------------------------------------------------


def test_append_keeps_order_and_bumps_last_message_at(client):
    t = seed_tenant()
    thread = _thread(client, t)
    first = _turn(client, thread["id"], "First?")
    second = _turn(client, thread["id"], "Second?")
    at = [datetime.fromisoformat(m["created_at"]) for m in (*first, *second)]
    assert at == sorted(at) and len(set(at)) == 4
    detail = client.get(f"{BASE}/threads/{thread['id']}").json()
    assert [m["content"] for m in detail["messages"]][::2] == ["First?", "Second?"]
    assert detail["last_message_at"] == second[1]["created_at"]
    assert detail["updated_at"] >= detail["created_at"]

    one = client.post(
        f"{BASE}/threads/{thread['id']}/messages",
        json={"messages": [{"role": "myte", "content": "", "payload": {"degraded": True}}]},
    )
    assert one.status_code == 201, "a stopped answer with no text is still saved"


def test_append_validation(client):
    t = seed_tenant()
    tid = _thread(client, t)["id"]
    url = f"{BASE}/threads/{tid}/messages"
    q = {"role": "user", "content": "x"}
    assert client.post(url, json={"messages": []}).status_code == 422
    assert client.post(url, json={"messages": [q, q, q]}).status_code == 422
    assert (
        client.post(url, json={"messages": [{"role": "user", "content": " "}]}).status_code == 422
    )
    assert (
        client.post(url, json={"messages": [{"role": "boss", "content": "x"}]}).status_code == 422
    )
    big = {"role": "myte", "content": "x", "payload": {"blob": "y" * 70_000}}
    res = client.post(url, json={"messages": [big]})
    assert res.status_code == 422
    assert res.headers["content-type"].startswith("application/problem+json")


def test_idempotency_key_stores_a_turn_once(client):
    t = seed_tenant()
    tid = _thread(client, t)["id"]
    key = {"Idempotency-Key": f"myte-{uuid.uuid4()}"}
    body = {"messages": [{"role": "user", "content": "Once?"}, {"role": "myte", "content": "Yes"}]}
    first = client.post(f"{BASE}/threads/{tid}/messages", json=body, headers=key)
    again = client.post(f"{BASE}/threads/{tid}/messages", json=body, headers=key)
    assert first.status_code == again.status_code == 201
    assert again.headers.get("X-Idempotent-Replayed") == "true"
    assert again.json() == first.json()
    stored = _sql("SELECT count(*) AS n FROM myte_messages WHERE thread_id = %(t)s", {"t": tid})
    assert stored == [{"n": 2}]


# --- feedback -----------------------------------------------------------------------------------


def test_feedback_on_answers_only_and_zero_clears_the_reason(client):
    t = seed_tenant()
    question, answer = _turn(client, _thread(client, t)["id"])
    url = f"{BASE}/messages/{answer['id']}"
    res = client.patch(url, json={"feedback": -1, "feedback_reason": "  wrong   meeting "})
    assert res.status_code == 200, res.text
    assert res.json()["feedback"] == -1 and res.json()["feedback_reason"] == "wrong meeting"
    cleared = client.patch(url, json={"feedback": 0, "feedback_reason": "ignored"}).json()
    assert cleared["feedback"] == 0 and cleared["feedback_reason"] is None
    assert client.patch(url, json={"feedback": 2}).status_code == 422
    assert (
        client.patch(f"{BASE}/messages/{question['id']}", json={"feedback": 1}).status_code == 422
    )
    assert client.patch(f"{BASE}/messages/{uuid.uuid4()}", json={"feedback": 1}).status_code == 404


# --- the list -----------------------------------------------------------------------------------


def test_list_orders_pinned_first_paginates_and_searches(client):
    t = seed_tenant()
    ids = [_thread(client, t, title=f"Thread {n}")["id"] for n in range(5)]
    for tid in ids:
        _turn(client, tid, f"question for {tid}")
    client.patch(f"{BASE}/threads/{ids[0]}", json={"pinned": True})

    seen: list[str] = []
    cursor = None
    while True:
        params: dict[str, object] = {"limit": 2}
        if cursor:
            params["cursor"] = cursor
        page = client.get(f"{BASE}/threads", params=params).json()
        seen += [i["id"] for i in page["items"]]
        cursor = page["next_cursor"]
        if not cursor:
            break
    assert seen == [ids[0], ids[4], ids[3], ids[2], ids[1]], "pinned first, then newest"

    by_title = client.get(f"{BASE}/threads", params={"q": "thread 3"}).json()["items"]
    assert [i["id"] for i in by_title] == [ids[3]]
    by_text = client.get(f"{BASE}/threads", params={"q": f"for {ids[2]}"}).json()["items"]
    assert [i["id"] for i in by_text] == [ids[2]]
    assert set(by_text[0]) == {"id", "title", "pinned", "created_at", "last_message_at"}

    percent = _thread(client, t, title="Margin 100% up")["id"]
    hits = client.get(f"{BASE}/threads", params={"q": "100%"}).json()["items"]
    assert [i["id"] for i in hits] == [percent]
    only = client.get(f"{BASE}/threads", params={"q": "%"}).json()["items"]
    assert [i["id"] for i in only] == [percent], "`%` matches itself, not everything"

    assert client.get(f"{BASE}/threads", params={"cursor": "not-a-cursor"}).status_code == 400


# --- pure: no database --------------------------------------------------------------------------


def test_clean_title():
    assert myte.clean_title(None) == "New question"
    assert myte.clean_title("  \n ") == "New question"
    assert myte.clean_title(" a \t b ") == "a b"
    assert len(myte.clean_title("x" * 500)) == 120


def test_like_pattern_escapes_wildcards():
    assert myte.like_pattern("100%_a\\b") == "%100\\%\\_a\\\\b%"


def test_payload_limit_is_64_kb_of_compact_utf8_json():
    ok = {"t": "y" * (65_536 - len('{"t":""}'))}
    assert myte.payload_size(ok) == 65_536
    myte.MyteMessageIn(role="myte", payload=ok)
    with pytest.raises(ValidationError):
        myte.MyteMessageIn(role="myte", payload={"t": "y" * 65_536})


def test_body_validation():
    with pytest.raises(ValidationError):
        myte.MyteMessageIn(role="user", content="   ")
    myte.MyteMessageIn(role="myte", content="")
    with pytest.raises(ValidationError):
        myte.MyteMessagesAppend(messages=[])
    with pytest.raises(ValidationError):
        myte.MyteThreadUpdate()
    with pytest.raises(ValidationError):
        myte.MyteThreadCreate(scope={"kind": "planet"})  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        myte.MyteFeedback(feedback=5)  # type: ignore[arg-type]


def test_cursor_round_trip_and_rejection():
    row = {
        "pinned": True,
        "last_message_at": datetime(2026, 11, 14, tzinfo=UTC),
        "id": uuid.uuid4(),
    }
    assert myte._decode_cursor(myte._encode_cursor(row)) == (
        True,
        row["last_message_at"],
        row["id"],
    )
    for bad in ("zzz", "eyJwIjogMX0="):
        with pytest.raises(ProblemDetail):
            myte._decode_cursor(bad)


def test_routes_are_registered_as_adr_0047_d7():
    paths = {(sorted(r.methods)[0], r.path) for r in myte.router.routes}  # type: ignore[attr-defined]
    assert paths == {
        ("GET", "/api/v1/myte/threads"),
        ("POST", "/api/v1/myte/threads"),
        ("GET", "/api/v1/myte/threads/{thread_id}"),
        ("PATCH", "/api/v1/myte/threads/{thread_id}"),
        ("DELETE", "/api/v1/myte/threads/{thread_id}"),
        ("DELETE", "/api/v1/myte/threads"),
        ("POST", "/api/v1/myte/threads/{thread_id}/messages"),
        ("PATCH", "/api/v1/myte/messages/{message_id}"),
    }
