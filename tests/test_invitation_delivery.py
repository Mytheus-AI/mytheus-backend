"""notification-service's side of meeting invitations (ADR-0043).

- Resend gets `reply_to` and the `.ics` as a base64 attachment with its full content type; an
  unset `EMAIL_FROM` falls back to `Mytheus <noreply@mytheus.cloud>`;
- `POST /internal/email/invitations` takes only core-api's caller token, claims each message
  in `email_deliveries` (a replay never resends), retries a transient failure, records a 4xx as
  failed at once, and with no provider skips every message as `not_configured`.
"""

from __future__ import annotations

import base64
import json
import os
import uuid
from typing import Any

import httpx
import pytest
from starlette.testclient import TestClient

from libs.shared import db as db_mod
from services.notification_service import internal, invitations
from services.notification_service.email import (
    DEFAULT_EMAIL_FROM,
    Attachment,
    EmailError,
    ResendEmailSender,
    email_sender_from_env,
)
from services.notification_service.internal_auth import TOKENS_ENV
from tests.conftest import APP_URL
from tests.test_chat_api import _create_test_app, _sql, seed_meeting, seed_tenant

SECRET = "n" * 40
ICS = b"BEGIN:VCALENDAR\r\nVERSION:2.0\r\nEND:VCALENDAR\r\n"


async def test_resend_sends_reply_to_and_the_calendar_attachment():
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"id": "re_1"})

    sender = ResendEmailSender("re_key", DEFAULT_EMAIL_FROM, transport=httpx.MockTransport(handler))
    await sender.send(
        to="raj@acme.test",
        subject="Invitation",
        body="text",
        html="<p>x</p>",
        reply_to="maya@acme.test",
        attachments=[Attachment("invite.ics", "text/calendar; method=REQUEST", ICS)],
    )
    payload = json.loads(seen[0].content)
    assert payload["from"] == "Mytheus <noreply@mytheus.cloud>"
    assert payload["reply_to"] == "maya@acme.test"
    assert payload["attachments"] == [
        {
            "filename": "invite.ics",
            "content": base64.b64encode(ICS).decode(),
            "content_type": "text/calendar; method=REQUEST",
        }
    ]


def test_email_from_defaults_to_the_verified_sender(monkeypatch: pytest.MonkeyPatch):
    for key in ("EMAIL_PROVIDER", "EMAIL_FROM", "SMTP_URL", "SMTP_FROM"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("RESEND_API_KEY", "re_x")
    sender = email_sender_from_env()
    assert isinstance(sender, ResendEmailSender)
    assert "noreply@mytheus.cloud" in repr(sender) and "re_x" not in repr(sender)


class FlakySender:
    provider = "resend"

    def __init__(self, failures: dict[str, list[str]] | None = None) -> None:
        self.failures = failures or {}
        self.sent: list[dict[str, Any]] = []
        self.attempts: dict[str, int] = {}

    async def send(self, **kwargs: Any) -> None:
        to = kwargs["to"]
        self.attempts[to] = self.attempts.get(to, 0) + 1
        queue = self.failures.get(to, [])
        if queue:
            raise EmailError(queue.pop(0))
        self.sent.append(kwargs)


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ[TOKENS_ENV] = f"core-api={SECRET}"
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("notification-invitations-test")
    app.include_router(invitations.router)
    with TestClient(app) as c:
        yield c
    internal.set_sender(None, active=False)
    os.environ.pop(TOKENS_ENV, None)
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(invitations, "RETRY_BASE_S", 0.0)


def _headers(org_id: uuid.UUID, secret: str = SECRET) -> dict[str, str]:
    return {
        "X-Mytheus-Service": "core-api",
        "X-Mytheus-Service-Token": secret,
        "X-Mytheus-Org": str(org_id),
    }


def _body(meeting_id: uuid.UUID, recipients: list[str], tag: str = "1") -> dict[str, Any]:
    return {
        "kind": "meeting_invitation",
        "meeting_id": str(meeting_id),
        "messages": [
            {
                "idempotency_key": f"invite:{meeting_id}:{r}:{tag}",
                "recipient": r,
                "subject": "Invitation: Pricing review",
                "text": "Join",
                "html": "<p>Join</p>",
                "reply_to": "maya@acme.test",
            }
            for r in recipients
        ],
        "attachment": {
            "filename": "invite.ics",
            "content_type": "text/calendar; method=REQUEST",
            "content_base64": base64.b64encode(ICS).decode(),
        },
    }


def test_only_core_api_may_call(client):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    body = _body(m, ["raj@acme.test"])
    assert client.post("/internal/email/invitations", json=body).status_code == 401
    bad = client.post(
        "/internal/email/invitations", json=body, headers=_headers(t.org_id, "x" * 40)
    )
    assert bad.status_code == 401


def test_sends_with_the_attachment_and_a_replay_never_resends(client):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    fake = FlakySender()
    internal.set_sender(fake)  # type: ignore[arg-type]
    body = _body(m, ["raj@acme.test", "priya@acme.test"])
    res = client.post("/internal/email/invitations", json=body, headers=_headers(t.org_id))
    assert res.status_code == 200, res.text
    assert [r["status"] for r in res.json()["results"]] == ["sent", "sent"]
    first = fake.sent[0]
    assert first["reply_to"] == "maya@acme.test"
    [attachment] = first["attachments"]
    assert attachment.content == ICS and attachment.content_type.endswith("method=REQUEST")
    again = client.post("/internal/email/invitations", json=body, headers=_headers(t.org_id))
    assert all(r["replayed"] for r in again.json()["results"]) and len(fake.sent) == 2
    rows = _sql("SELECT kind, status FROM email_deliveries WHERE meeting_id = %(m)s", {"m": str(m)})
    assert rows == [{"kind": "meeting_invitation", "status": "sent"}] * 2


def test_transient_failures_are_retried_and_a_refusal_is_final(client):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    fake = FlakySender(
        {
            "raj@acme.test": ["resend_http_503", "resend_unreachable:ConnectError"],
            "priya@acme.test": ["resend_http_422"],
            "diego@acme.test": ["resend_http_500"] * 5,
        }
    )
    internal.set_sender(fake)  # type: ignore[arg-type]
    res = client.post(
        "/internal/email/invitations",
        json=_body(m, ["raj@acme.test", "priya@acme.test", "diego@acme.test"]),
        headers=_headers(t.org_id),
    )
    results = res.json()["results"]
    assert [r["status"] for r in results] == ["sent", "failed", "failed"]
    assert results[1]["detail"] == "resend_http_422" and results[2]["detail"] == "resend_http_500"
    assert fake.attempts == {
        "raj@acme.test": 3,
        "priya@acme.test": 1,
        "diego@acme.test": invitations.SEND_ATTEMPTS,
    }


def test_not_configured_skips_every_message(client):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    internal.set_sender(None)
    res = client.post(
        "/internal/email/invitations",
        json=_body(m, ["raj@acme.test"], tag="nc"),
        headers=_headers(t.org_id),
    )
    out = res.json()
    assert out["configured"] is False
    assert out["results"][0]["status"] == "skipped"
    assert out["results"][0]["detail"] == "not_configured"


def test_another_tenants_meeting_is_404(client):
    t = seed_tenant()
    other = seed_tenant()
    m = seed_meeting(other.org_id)
    internal.set_sender(FlakySender())  # type: ignore[arg-type]
    res = client.post(
        "/internal/email/invitations", json=_body(m, ["raj@acme.test"]), headers=_headers(t.org_id)
    )
    assert res.status_code == 404
