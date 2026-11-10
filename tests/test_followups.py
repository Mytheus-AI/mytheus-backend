"""The meeting follow-up (`core-api` `routes/followups.py`), against a real Postgres.

- GET is for participants (403 otherwise, 404 across tenants) and starts from the defaults;
- PUT is the host's alone; guests are validated, lowercased, de-duplicated, at most 20;
- send is the host's, only once the meeting has ended and while the follow-up is on (409);
- one dispatch: one `notifications` outbox row per participant carrying the confirmed headline
  only (never a proposed candidate), the host's recap share link, the guests mailed through
  notification-service, the result stored and audited; a second send adds no outbox rows;
- notification-service without a caller secret skips the guests (`not_configured`); down is 503;
- a confidential meeting's follow-up still goes out (the host's own act);
- the internal dispatch takes only workflow-service's caller token.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.core_api import notification_client
from services.core_api.notification_client import NotConfigured, NotificationServiceError
from services.core_api.routes import followups, internal_meetings
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

SECRET = "w" * 40


class FakeNotifications:
    def __init__(self, error: Exception | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.error = error

    async def send_email(self, org_id: uuid.UUID, **kwargs: Any) -> dict[str, Any]:
        if self.error is not None:
            raise self.error
        self.calls.append({"org_id": org_id, **kwargs})
        return {
            "configured": True,
            "provider": "resend",
            "results": [
                {"recipient": r, "status": "sent", "detail": None, "replayed": False}
                for r in kwargs["recipients"]
            ],
        }


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ["SHARE_LINK_SIGNING_KEY"] = "k" * 40
    os.environ[internal_meetings.TOKENS_ENV] = f"workflow-service={SECRET}"
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-followups-test")
    app.include_router(followups.router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }
    with TestClient(app) as c:
        yield c
    notification_client.set_notifications(None)
    for key in ("SHARE_LINK_SIGNING_KEY", internal_meetings.TOKENS_ENV):
        os.environ.pop(key, None)
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


@pytest.fixture
def fake() -> FakeNotifications:
    f = FakeNotifications()
    notification_client.set_notifications(f)  # type: ignore[arg-type]
    return f


def meeting_with_host(
    *, ended: bool = True, confidential: bool = False
) -> tuple[Tenant, Tenant, uuid.UUID]:
    host = seed_tenant()
    guest_user = add_user(host)
    m = seed_meeting(host.org_id)
    _sql(
        "UPDATE meetings SET title = 'Pricing review', status = %(s)s, confidential = %(c)s "
        "WHERE id = %(m)s",
        {"s": "ended" if ended else "live", "c": confidential, "m": str(m)},
    )
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id, role_in_meeting) "
        "VALUES (%(m)s, %(u)s, %(o)s, 'host')",
        {"m": str(m), "u": str(host.user_id), "o": str(host.org_id)},
    )
    add_participant(m, host.org_id, guest_user.user_id)
    return host, guest_user, m


def decision(t: Tenant, m: uuid.UUID, content: str, status: str = "confirmed") -> None:
    _sql(
        "INSERT INTO decisions (org_id, meeting_id, content, confidence, status) "
        "VALUES (%(o)s, %(m)s, %(c)s, 0.9, %(s)s)",
        {"o": str(t.org_id), "m": str(m), "c": content, "s": status},
    )


def outbox_rows(m: uuid.UUID) -> list[dict[str, Any]]:
    return _sql(
        "SELECT payload FROM outbox WHERE topic = 'notifications' AND idempotency_key LIKE %(k)s",
        {"k": f"notif:followup:{m}:%"},
    )


def test_get_is_for_participants_and_starts_from_defaults(client):
    host, member, m = meeting_with_host()
    sign_in(member)
    res = client.get(f"/api/v1/meetings/{m}/follow-up")
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["enabled"] is True and body["guest_emails"] == []
    assert body["is_host"] is False and body["can_send"] is False
    assert body["share_link"] is None and body["last_result"] is None
    sign_in(host)
    assert client.get(f"/api/v1/meetings/{m}/follow-up").json()["can_send"] is True
    outsider = add_user(host)
    sign_in(outsider)
    assert client.get(f"/api/v1/meetings/{m}/follow-up").status_code == 403
    sign_in(seed_tenant())
    assert client.get(f"/api/v1/meetings/{m}/follow-up").status_code == 404


def test_put_is_the_hosts_and_validates_guests(client):
    host, member, m = meeting_with_host()
    body = {"enabled": True, "guest_emails": ["Ana@Client.com", "ana@client.com", "b@x.io"]}
    sign_in(member)
    assert client.put(f"/api/v1/meetings/{m}/follow-up", json=body).status_code == 403
    sign_in(host)
    res = client.put(f"/api/v1/meetings/{m}/follow-up", json=body)
    assert res.status_code == 200, res.text
    assert res.json()["guest_emails"] == ["ana@client.com", "b@x.io"]
    bad = {"enabled": True, "guest_emails": ["nope"]}
    assert client.put(f"/api/v1/meetings/{m}/follow-up", json=bad).status_code == 422
    many = {"enabled": True, "guest_emails": [f"g{i}@x.io" for i in range(21)]}
    assert client.put(f"/api/v1/meetings/{m}/follow-up", json=many).status_code == 422
    off = client.put(f"/api/v1/meetings/{m}/follow-up", json={"enabled": False})
    assert off.json()["enabled"] is False and off.json()["can_send"] is False


def test_send_needs_the_host_an_ended_meeting_and_the_follow_up_on(client, fake):
    host, member, live = meeting_with_host(ended=False)
    sign_in(member)
    assert client.post(f"/api/v1/meetings/{live}/follow-up/send").status_code == 403
    sign_in(host)
    assert client.post(f"/api/v1/meetings/{live}/follow-up/send").status_code == 409
    host2, _, ended = meeting_with_host()
    sign_in(host2)
    client.put(f"/api/v1/meetings/{ended}/follow-up", json={"enabled": False})
    assert client.post(f"/api/v1/meetings/{ended}/follow-up/send").status_code == 409
    assert fake.calls == []


def test_dispatch_notifies_participants_mails_guests_and_records(client, fake):
    host, member, m = meeting_with_host()
    decision(host, m, "Raise the base price by 8%")
    decision(host, m, "A proposal nobody confirmed", status="proposed")
    sign_in(host)
    client.put(
        f"/api/v1/meetings/{m}/follow-up",
        json={"enabled": True, "guest_emails": ["ana@client.com"]},
    )
    res = client.post(f"/api/v1/meetings/{m}/follow-up/send")
    assert res.status_code == 200, res.text
    out = res.json()
    assert out["status"] == "sent" and out["participants_notified"] == 2
    assert out["email_configured"] is True
    assert out["guests"] == [{"recipient": "ana@client.com", "status": "sent", "detail": None}]
    assert "/s/" in out["share_url"]

    rows = outbox_rows(m)
    assert {r["payload"]["user_id"] for r in rows} == {str(host.user_id), str(member.user_id)}
    payload = rows[0]["payload"]
    assert payload["title"] == "Recap: Pricing review"
    assert "Raise the base price by 8%" in payload["body"]
    assert "nobody confirmed" not in payload["body"]
    assert payload["link"] == f"/meetings/{m}"

    [call] = fake.calls
    assert call["recipients"] == ["ana@client.com"]
    assert call["idempotency_key"] == f"followup:{m}"
    assert out["share_url"] in call["text"] and out["share_url"] in call["html"]
    assert "nobody confirmed" not in call["html"]

    view = client.get(f"/api/v1/meetings/{m}/follow-up").json()
    assert view["last_result"]["status"] == "sent" and view["last_sent_at"]
    assert view["share_link"]["url"] == out["share_url"]
    audits = _sql(
        "SELECT actor_id FROM audit_logs WHERE action = 'meeting.followup_sent' "
        "AND target_id = %(m)s",
        {"m": str(m)},
    )
    assert [str(a["actor_id"]) for a in audits] == [str(host.user_id)]

    # Sending again reuses the link and adds no outbox rows.
    again = client.post(f"/api/v1/meetings/{m}/follow-up/send").json()
    assert again["share_url"] == out["share_url"]
    assert len(outbox_rows(m)) == 2
    links = _sql("SELECT count(*) AS n FROM share_links WHERE meeting_id = %(m)s", {"m": str(m)})
    assert links[0]["n"] == 1


def test_guests_are_skipped_when_email_is_not_wired_and_503_when_down(client):
    host, _, m = meeting_with_host()
    sign_in(host)
    client.put(
        f"/api/v1/meetings/{m}/follow-up", json={"enabled": True, "guest_emails": ["a@b.io"]}
    )
    notification_client.set_notifications(FakeNotifications(error=NotConfigured(None, "x")))  # type: ignore[arg-type]
    out = client.post(f"/api/v1/meetings/{m}/follow-up/send").json()
    assert out["email_configured"] is False
    assert out["guests"] == [
        {"recipient": "a@b.io", "status": "skipped", "detail": "not_configured"}
    ]
    notification_client.set_notifications(  # type: ignore[arg-type]
        FakeNotifications(error=NotificationServiceError(None, "ConnectError"))
    )
    assert client.post(f"/api/v1/meetings/{m}/follow-up/send").status_code == 503


def test_a_confidential_meetings_follow_up_is_the_hosts_to_send(client, fake):
    host, _, m = meeting_with_host(confidential=True)
    sign_in(host)
    out = client.post(f"/api/v1/meetings/{m}/follow-up/send").json()
    assert out["status"] == "sent" and out["participants_notified"] == 2


def test_internal_dispatch_takes_only_the_workflow_token(client, fake):
    host, _, m = meeting_with_host()
    url = f"/internal/meetings/{m}/follow-up/dispatch"
    assert client.post(url).status_code == 401
    headers = {
        "X-Mytheus-Service": "workflow-service",
        "X-Mytheus-Service-Token": SECRET,
        "X-Mytheus-Org": str(host.org_id),
    }
    res = client.post(url, headers=headers)
    assert res.status_code == 200, res.text
    assert res.json()["status"] == "sent"
    other = seed_tenant()
    headers["X-Mytheus-Org"] = str(other.org_id)
    assert client.post(url, headers=headers).status_code == 404


def test_meeting_followups_rows_are_isolated_by_rls(client):
    import psycopg2

    host, _, m = meeting_with_host()
    sign_in(host)
    client.put(f"/api/v1/meetings/{m}/follow-up", json={"enabled": True, "guest_emails": []})
    conn = psycopg2.connect(APP_URL.replace("+asyncpg", ""))
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM meeting_followups")  # no tenant set: fail closed
            assert cur.fetchone()[0] == 0
            cur.execute(
                "SELECT set_config('app.current_org', %s, false)", (str(seed_tenant().org_id),)
            )
            cur.execute("SELECT count(*) FROM meeting_followups WHERE meeting_id = %s", (str(m),))
            assert cur.fetchone()[0] == 0
    finally:
        conn.close()
