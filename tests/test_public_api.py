"""Public API reads (core-api, Phase 4): search, meeting recap, decision record.

1. Search is participant-scoped: matches from meetings the caller was not in (same tenant) and
   from other tenants never appear; proposed candidates never appear; the caller's own task
   with no meeting does; `%` and `_` are literal; no `q` lists recent meetings only.
2. The recap is readable by participants of a confidential meeting (it is not a share), 403
   for a non-participant, 404 across tenants.
3. The decision record is 404 for a proposed candidate.
"""

from __future__ import annotations

import os

import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.core_api.routes import public_api
from tests.conftest import APP_URL
from tests.test_chat_api import PRINCIPAL, _create_test_app, _sql, seed_tenant, sign_in
from tests.test_my_tasks import add_user
from tests.test_share_links import action, decision, meeting_with


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-public-api-test")
    app.include_router(public_api.router)
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


def test_search_is_participant_scoped(client):
    me = seed_tenant()
    colleague = add_user(me)
    mine = meeting_with(me)
    decision(me, mine, "Move launch to March")
    decision(me, mine, "launch candidate", status="proposed")
    action(me, mine, "Tell sales about the launch")
    not_mine = meeting_with(colleague)
    decision(colleague, not_mine, "Cancel the launch party")
    other = seed_tenant()
    decision(other, meeting_with(other), "Launch in Japan")
    _sql(
        "INSERT INTO action_items (org_id, content, assignee_id, status, confidence) "
        "VALUES (%(o)s, 'Book the launch venue', %(u)s, 'confirmed', 1.0)",
        {"o": str(me.org_id), "u": str(me.user_id)},
    )
    sign_in(me)
    items = client.get("/api/v1/search", params={"q": "LAUNCH"}).json()["items"]
    assert sorted(i["text"] for i in items) == [
        "Book the launch venue",
        "Move launch to March",
        "Tell sales about the launch",
    ]
    assert client.get("/api/v1/search", params={"q": "%%"}).json()["items"] == []
    recent = client.get("/api/v1/search").json()["items"]
    assert [(i["kind"], i["id"]) for i in recent] == [("meeting", str(mine))]
    assert client.get("/api/v1/search", params={"q": "x"}).status_code == 422


def test_recap_and_record_access(client):
    host = seed_tenant()
    guest = add_user(host)
    outsider = add_user(host)
    m = meeting_with(host, guest, confidential=True)
    d = decision(host, m, "Keep it quiet")
    candidate = decision(host, m, "maybe", status="proposed")
    sign_in(guest)
    recap = client.get(f"/api/v1/meetings/{m}/recap")
    assert recap.status_code == 200 and recap.json()["decisions"][0]["content"] == "Keep it quiet"
    assert client.get(f"/api/v1/decisions/{d}/record").status_code == 200
    assert client.get(f"/api/v1/decisions/{candidate}/record").status_code == 404
    sign_in(outsider)
    assert client.get(f"/api/v1/meetings/{m}/recap").status_code == 403
    sign_in(seed_tenant())
    assert client.get(f"/api/v1/meetings/{m}/recap").status_code == 404
