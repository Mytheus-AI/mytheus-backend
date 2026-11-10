"""Share links (Phase 4): create, list, revoke, and the public read.

1. A participant shares a recap; the public read (no auth) returns confirmed decisions and
   actions only, people by name and never by email, and counts the view.
2. A non-participant cannot share (403); another tenant's meeting is 404.
3. Confidential meetings: only the host may share (403 `confidential_host_only` otherwise).
4. Revocation (creator, host or admin; others 404) makes the public read 410 `revoked`; an
   expired link is 410 `expired`; a forged or tampered token is 404.
5. The page is built as the creator sees it: when the creator leaves the meeting the link
   stops working (404); lineage steps from meetings they were not in are withheld.
6. Decision record links: proposed decisions cannot be shared (404); the record carries its
   lineage oldest first.
7. Not configured (no signing key): create is 503 and every read 404.
"""

from __future__ import annotations

import os
import uuid

import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.core_api import share_tokens
from services.core_api.routes import share_links
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


@pytest.fixture(autouse=True)
def _key(monkeypatch):
    monkeypatch.setenv("SHARE_LINK_SIGNING_KEY", "s" * 40)
    monkeypatch.setenv("APP_BASE_URL", "https://app.example")


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-share-links-test")
    app.include_router(share_links.router)
    app.include_router(share_links.public_router)
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


def meeting_with(host: Tenant, *others: Tenant, confidential: bool = False) -> uuid.UUID:
    m = seed_meeting(host.org_id)
    _sql(
        "UPDATE meetings SET title = 'Pricing review', confidential = %(c)s, created_by = %(h)s "
        "WHERE id = %(m)s",
        {"c": confidential, "h": str(host.user_id), "m": str(m)},
    )
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id, role_in_meeting) "
        "VALUES (%(m)s, %(u)s, %(o)s, 'host')",
        {"m": str(m), "u": str(host.user_id), "o": str(host.org_id)},
    )
    for o in others:
        add_participant(m, o.org_id, o.user_id)
    return m


def decision(
    t: Tenant, m: uuid.UUID, content: str, status: str = "confirmed", supersedes=None
) -> uuid.UUID:
    row = _sql(
        "INSERT INTO decisions (org_id, meeting_id, content, confidence, status, supersedes) "
        "VALUES (%(o)s, %(m)s, %(c)s, 0.9, %(s)s, %(p)s) RETURNING id",
        {
            "o": str(t.org_id),
            "m": str(m),
            "c": content,
            "s": status,
            "p": str(supersedes) if supersedes else None,
        },
    )
    return uuid.UUID(str(row[0]["id"]))


def action(t: Tenant, m: uuid.UUID, content: str, status: str = "confirmed") -> None:
    _sql(
        "INSERT INTO action_items (org_id, meeting_id, content, assignee_id, status, confidence) "
        "VALUES (%(o)s, %(m)s, %(c)s, %(a)s, %(s)s, 0.8)",
        {"o": str(t.org_id), "m": str(m), "c": content, "a": str(t.user_id), "s": status},
    )


def _rename(t: Tenant, email: str) -> None:
    _sql("UPDATE users SET email = %(e)s WHERE id = %(u)s", {"e": email, "u": str(t.user_id)})


def test_a_participant_shares_a_recap_and_strangers_read_only_confirmed_facts(client):
    host = seed_tenant()
    _rename(host, "maya.patel@acme.com")
    m = meeting_with(host)
    decision(host, m, "Raise the price to $40")
    decision(host, m, "secret candidate", status="proposed")
    action(host, m, "Email the customers")
    action(host, m, "agent guess", status="proposed")
    sign_in(host)
    res = client.post(f"/api/v1/meetings/{m}/share-links", json={"expires_in_days": 7})
    assert res.status_code == 201, res.text
    link = res.json()
    assert link["url"] == f"https://app.example/s/{link['token']}" and link["state"] == "active"
    assert link["created_by_name"] == "Maya Patel"

    page = client.get(f"/api/v1/public/shares/{link['token']}", headers={"Authorization": ""})
    assert page.status_code == 200, page.text
    assert page.headers["cache-control"] == "no-store"
    body = page.json()
    recap = body["recap"]
    assert body["kind"] == "meeting_recap" and body["decision"] is None
    assert recap["title"] == "Pricing review" and recap["host_name"] == "Maya Patel"
    assert [d["content"] for d in recap["decisions"]] == ["Raise the price to $40"]
    assert [a["content"] for a in recap["actions"]] == ["Email the customers"]
    assert recap["actions"][0]["owner_name"] == "Maya Patel"
    assert "@" not in page.text
    assert body["view_count"] == 1
    assert client.get(f"/api/v1/public/shares/{link['token']}").json()["view_count"] == 2
    [listed] = client.get(f"/api/v1/meetings/{m}/share-links").json()["items"]
    assert listed["view_count"] == 2 and listed["can_revoke"] is True


def test_non_participants_and_other_tenants_cannot_share(client):
    host = seed_tenant()
    outsider = add_user(host)
    m = meeting_with(host)
    sign_in(outsider)
    res = client.post(f"/api/v1/meetings/{m}/share-links")
    assert res.status_code == 403 and res.json()["code"] == "NOT_A_PARTICIPANT"
    assert client.get(f"/api/v1/meetings/{m}/share-links").status_code == 403
    sign_in(seed_tenant())
    assert client.post(f"/api/v1/meetings/{m}/share-links").status_code == 404


def test_confidential_meetings_are_shareable_only_by_the_host(client):
    host = seed_tenant()
    guest = add_user(host)
    m = meeting_with(host, guest, confidential=True)
    sign_in(guest)
    res = client.post(f"/api/v1/meetings/{m}/share-links")
    assert res.status_code == 403 and res.json()["reason"] == "confidential_host_only"
    sign_in(host)
    # The host must confirm publishing a confidential meeting's recap explicitly.
    res = client.post(f"/api/v1/meetings/{m}/share-links")
    assert res.status_code == 409
    assert res.json()["reason"] == "confidential_confirmation_required"
    confirmed = {"confirm_confidential": True}
    assert client.post(f"/api/v1/meetings/{m}/share-links", json=confirmed).status_code == 201


def test_revoke_expire_and_forgery(client):
    host = seed_tenant()
    member = add_user(host)
    other = add_user(host)
    m = meeting_with(host, member, other)
    sign_in(member)
    link = client.post(f"/api/v1/meetings/{m}/share-links").json()
    sign_in(other)
    assert client.delete(f"/api/v1/share-links/{link['id']}").status_code == 404
    sign_in(host)  # the host may revoke anyone's link to their meeting
    assert client.delete(f"/api/v1/share-links/{link['id']}").status_code == 204
    assert client.delete(f"/api/v1/share-links/{link['id']}").status_code == 204
    res = client.get(f"/api/v1/public/shares/{link['token']}")
    assert res.status_code == 410 and res.json()["reason"] == "revoked"

    sign_in(member)
    fresh = client.post(f"/api/v1/meetings/{m}/share-links").json()
    _sql(
        "UPDATE share_links SET created_at = now() - interval '2 days', "
        "expires_at = now() - interval '1 minute' WHERE id = %(i)s",
        {"i": fresh["id"]},
    )
    res = client.get(f"/api/v1/public/shares/{fresh['token']}")
    assert res.status_code == 410 and res.json()["reason"] == "expired"

    payload, sig = link["token"].split(".")
    assert client.get(f"/api/v1/public/shares/{payload}.{'A' * len(sig)}").status_code == 404
    forged = share_tokens.mint(uuid.uuid4(), uuid.uuid4()).split(".")[0] + "." + sig
    assert client.get(f"/api/v1/public/shares/{forged}").status_code == 404
    audits = _sql(
        "SELECT action FROM audit_logs WHERE target_id = %(t)s ORDER BY occurred_at",
        {"t": link["id"]},
    )
    assert [a["action"] for a in audits] == ["share_link.created", "share_link.revoked"]


def test_the_link_follows_the_creators_access(client):
    host = seed_tenant()
    member = add_user(host)
    m = meeting_with(host, member)
    sign_in(member)
    token = client.post(f"/api/v1/meetings/{m}/share-links").json()["token"]
    assert client.get(f"/api/v1/public/shares/{token}").status_code == 200
    _sql(
        "DELETE FROM meeting_participants WHERE meeting_id = %(m)s AND user_id = %(u)s",
        {"m": str(m), "u": str(member.user_id)},
    )
    assert client.get(f"/api/v1/public/shares/{token}").status_code == 404


def test_decision_record_with_lineage_and_withheld_steps(client):
    host = seed_tenant()
    m1 = meeting_with(host)
    elsewhere_host = add_user(host)
    m0 = meeting_with(elsewhere_host)  # host was not in this one
    oldest = decision(elsewhere_host, m0, "Price stays at $30", status="superseded")
    middle = decision(host, m1, "Price goes to $35", status="superseded", supersedes=oldest)
    subject = decision(host, m1, "Price goes to $40", supersedes=middle)
    candidate = decision(host, m1, "maybe $50", status="proposed")
    sign_in(host)
    assert client.post(f"/api/v1/decisions/{candidate}/share-links").status_code == 404
    res = client.post(f"/api/v1/decisions/{subject}/share-links")
    assert res.status_code == 201 and res.json()["kind"] == "decision_record"
    record = client.get(f"/api/v1/public/shares/{res.json()['token']}").json()["decision"]
    assert record["content"] == "Price goes to $40"
    chain = record["lineage"]
    assert [s["relation"] for s in chain] == ["earlier", "earlier", "subject"]
    assert chain[0]["visible"] is False and chain[0]["content"] is None
    assert chain[1]["content"] == "Price goes to $35" and chain[2]["decision_id"] == str(subject)
    assert "Price stays at $30" not in str(record)
    [listed] = client.get(f"/api/v1/decisions/{subject}/share-links").json()["items"]
    assert listed["decision_id"] == str(subject)


def test_not_configured_fails_closed(client, monkeypatch):
    host = seed_tenant()
    m = meeting_with(host)
    sign_in(host)
    token = client.post(f"/api/v1/meetings/{m}/share-links").json()["token"]
    monkeypatch.delenv("SHARE_LINK_SIGNING_KEY")
    monkeypatch.delenv("GATEWAY_SIGNING_KEY", raising=False)
    res = client.post(f"/api/v1/meetings/{m}/share-links")
    assert res.status_code == 503 and res.json()["reason"] == "not_configured"
    assert client.get(f"/api/v1/public/shares/{token}").status_code == 404
