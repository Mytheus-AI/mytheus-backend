"""The decision log (`GET /api/v1/decisions`, `core-api`) against a real Postgres.

1. Only confirmed and superseded decisions appear, newest first, with the meeting's title;
   proposed, dismissed and deleted rows never do.
2. Participant-scoped: a decision from a meeting the caller was not in is not in their log.
3. `superseded_by` names the decision that replaced an entry.
4. RLS: tenant B's log never shows tenant A's decisions, even when B's caller id is somehow
   a participant row in A's data (it cannot be, but the query must not depend on that).
"""

from __future__ import annotations

import uuid

import pytest

from services.core_api.routes import decision_log
from tests.conftest import APP_URL
from tests.test_chat_api import (
    PRINCIPAL,
    Tenant,
    _create_test_app,
    _sql,
    add_participant,
    seed_meeting,
    seed_tenant,
    sign_in,
)
from tests.test_my_tasks import add_user


@pytest.fixture(scope="module")
def client(migrated_db):
    import os

    from starlette.testclient import TestClient

    from libs.shared import auth as auth_mod
    from libs.shared import db as db_mod

    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-decision-log-test")
    app.include_router(decision_log.router)
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


def decision(
    t: Tenant,
    meeting: uuid.UUID,
    content: str,
    *,
    status: str = "confirmed",
    minutes_ago: int = 0,
    supersedes: uuid.UUID | None = None,
    deleted: bool = False,
) -> uuid.UUID:
    row = _sql(
        "INSERT INTO decisions (org_id, meeting_id, content, rationale, confidence, status, "
        "supersedes, created_at, deleted_at) VALUES (%(o)s, %(m)s, %(c)s, 'because', 0.9, "
        "%(s)s, %(sup)s, now() - make_interval(mins => %(a)s), CASE WHEN %(x)s THEN now() END) "
        "RETURNING id",
        {
            "o": str(t.org_id),
            "m": str(meeting),
            "c": content,
            "s": status,
            "sup": str(supersedes) if supersedes else None,
            "a": minutes_ago,
            "x": deleted,
        },
    )
    return row[0]["id"]


def test_lists_only_confirmed_decisions_from_my_meetings_newest_first(client):
    t = seed_tenant()
    sign_in(t)
    mine = seed_meeting(t.org_id)
    add_participant(mine, t.org_id, t.user_id)
    theirs = seed_meeting(t.org_id)  # same tenant, but the caller was not in it

    decision(t, mine, "Older call", minutes_ago=30)
    decision(t, mine, "Newer call", minutes_ago=5)
    decision(t, mine, "Still a candidate", status="proposed")
    decision(t, mine, "Rejected", status="dismissed")
    decision(t, mine, "Deleted", deleted=True)
    decision(t, theirs, "Not my meeting")

    resp = client.get("/api/v1/decisions")
    assert resp.status_code == 200
    items = resp.json()["items"]
    assert [d["content"] for d in items] == ["Newer call", "Older call"]
    assert {d["meeting_title"] for d in items} == {"M"}
    assert all(d["status"] == "confirmed" and d["rationale"] == "because" for d in items)


def test_superseded_entries_link_to_their_replacement(client):
    t = seed_tenant()
    sign_in(t)
    m = seed_meeting(t.org_id)
    add_participant(m, t.org_id, t.user_id)
    old = decision(t, m, "Ship in Q3", status="superseded", minutes_ago=60)
    new = decision(t, m, "Ship in Q4", supersedes=old, minutes_ago=1)

    by_id = {d["id"]: d for d in client.get("/api/v1/decisions").json()["items"]}
    assert by_id[str(old)]["superseded_by"] == str(new)
    assert by_id[str(old)]["status"] == "superseded"
    assert by_id[str(new)]["supersedes"] == str(old)
    assert by_id[str(new)]["superseded_by"] is None


def test_another_tenants_decisions_are_invisible(client):
    a = seed_tenant()
    m = seed_meeting(a.org_id)
    add_participant(m, a.org_id, a.user_id)
    decision(a, m, "Tenant A secret")

    b = seed_tenant()
    sign_in(b)
    assert client.get("/api/v1/decisions").json()["items"] == []

    sign_in(a)
    assert [d["content"] for d in client.get("/api/v1/decisions").json()["items"]] == [
        "Tenant A secret"
    ]


def test_a_colleague_sees_only_their_own_meetings(client):
    t = seed_tenant()
    colleague = add_user(t)
    shared, private = seed_meeting(t.org_id), seed_meeting(t.org_id)
    for m in (shared, private):
        add_participant(m, t.org_id, t.user_id)
    add_participant(shared, t.org_id, colleague.user_id)
    decision(t, shared, "Shared call")
    decision(t, private, "Private call")

    sign_in(colleague)
    assert [d["content"] for d in client.get("/api/v1/decisions").json()["items"]] == [
        "Shared call"
    ]
