"""The email channel's provider abstraction and notification-service's internal email route.

Provider (no database):
- Resend posts `{from, to, subject, text, html}` with the bearer key; a 4xx is an `EmailError`
  carrying only the status, never the body;
- selection: explicit `EMAIL_PROVIDER`, else Resend when its key and sender are set, else SMTP,
  else None (`email_configured()` false);
- SMTP attaches the HTML as an alternative part.

`POST /internal/email/send` (real Postgres):
- refused without core-api's caller token;
- sends each recipient once, after the claims commit; a replay never re-sends and reports the
  recorded status with `replayed: true`; a new recipient on the replay is sent;
- not configured: every recipient `skipped` / `not_configured`, `configured: false`;
- a provider failure is `failed`, never a 5xx;
- another tenant's meeting is a 404; RLS keeps `email_deliveries` per tenant.
"""

from __future__ import annotations

import json
import os
import uuid
from email.message import EmailMessage
from typing import Any

import httpx
import pytest
from starlette.testclient import TestClient

from libs.shared import db as db_mod
from services.notification_service import internal
from services.notification_service.email import (
    EmailError,
    ResendEmailSender,
    SmtpEmailSender,
    email_configured,
    email_provider,
    email_sender_from_env,
)
from services.notification_service.internal_auth import TOKENS_ENV
from tests.conftest import APP_URL
from tests.test_chat_api import _create_test_app, _sql, seed_meeting, seed_tenant

SECRET = "c" * 40
_ENV = ("EMAIL_PROVIDER", "RESEND_API_KEY", "EMAIL_FROM", "SMTP_URL", "SMTP_FROM")


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for key in _ENV:
        monkeypatch.delenv(key, raising=False)
    return monkeypatch


# --- provider -----------------------------------------------------------------------------


async def test_resend_request_shape():
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"id": "re_1"})

    sender = ResendEmailSender(
        "re_key", "Mytheus <recap@mytheus.ai>", transport=httpx.MockTransport(handler)
    )
    await sender.send(to="guest@acme.com", subject="Recap", body="text", html="<p>x</p>")
    [request] = seen
    assert request.method == "POST" and str(request.url) == "https://api.resend.com/emails"
    assert request.headers["authorization"] == "Bearer re_key"
    assert json.loads(request.content) == {
        "from": "Mytheus <recap@mytheus.ai>",
        "to": ["guest@acme.com"],
        "subject": "Recap",
        "text": "text",
        "html": "<p>x</p>",
    }
    assert "re_key" not in repr(sender)


async def test_resend_error_carries_the_status_only():
    sender = ResendEmailSender(
        "k",
        "a@b.co",
        transport=httpx.MockTransport(lambda r: httpx.Response(422, json={"message": "secret"})),
    )
    with pytest.raises(EmailError) as err:
        await sender.send(to="g@acme.com", subject="s", body="b")
    assert err.value.reason == "resend_http_422"


def test_provider_selection(clean_env):
    assert email_sender_from_env() is None and not email_configured()
    assert email_provider() is None
    clean_env.setenv("SMTP_URL", "smtp://mail.example.com:2525")
    clean_env.setenv("SMTP_FROM", "a@example.com")
    assert isinstance(email_sender_from_env(), SmtpEmailSender) and email_provider() == "smtp"
    clean_env.setenv("RESEND_API_KEY", "re_x")
    clean_env.setenv("EMAIL_FROM", "b@example.com")
    assert email_provider() == "resend"  # auto: Resend wins when both are set
    clean_env.setenv("EMAIL_PROVIDER", "smtp")
    assert email_provider() == "smtp"
    clean_env.setenv("EMAIL_PROVIDER", "resend")
    clean_env.delenv("RESEND_API_KEY")
    assert email_provider() is None  # explicitly Resend, but Resend is not configured
    clean_env.setenv("EMAIL_PROVIDER", "carrier-pigeon")
    assert email_sender_from_env() is None


async def test_smtp_attaches_html_as_an_alternative(monkeypatch: pytest.MonkeyPatch):
    sent: list[EmailMessage] = []
    sender = SmtpEmailSender("smtp://mail.example.com", "a@example.com")
    monkeypatch.setattr(sender, "_send_sync", sent.append)
    await sender.send(to="g@acme.com", subject="s", body="plain", html="<b>rich</b>")
    [msg] = sent
    assert msg.is_multipart()
    assert [p.get_content_type() for p in msg.iter_parts()] == ["text/plain", "text/html"]


# --- internal route -------------------------------------------------------------------------


class FakeSender:
    provider = "resend"

    def __init__(self, fail_for: str | None = None) -> None:
        self.sent: list[tuple[str, str, str, str | None]] = []
        self.fail_for = fail_for

    async def send(self, *, to: str, subject: str, body: str, html: str | None = None) -> None:
        if to == self.fail_for:
            raise EmailError("resend_http_500")
        self.sent.append((to, subject, body, html))


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ[TOKENS_ENV] = f"core-api={SECRET}"
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("notification-internal-test")
    app.include_router(internal.router)
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


def _headers(org_id: uuid.UUID, secret: str = SECRET) -> dict[str, str]:
    return {
        "X-Mytheus-Service": "core-api",
        "X-Mytheus-Service-Token": secret,
        "X-Mytheus-Org": str(org_id),
    }


def _body(meeting_id: uuid.UUID, recipients: list[str], key: str | None = None) -> dict[str, Any]:
    return {
        "kind": "meeting_followup",
        "meeting_id": str(meeting_id),
        "idempotency_key": key or f"followup:{meeting_id}",
        "recipients": recipients,
        "subject": "Recap: Pricing",
        "text": "Decisions\n- Ship it",
        "html": "<p>Ship it</p>",
    }


def test_refused_without_the_caller_token(client):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    body = _body(m, ["g@acme.com"])
    assert client.post("/internal/email/send", json=body).status_code == 401
    bad = client.post("/internal/email/send", json=body, headers=_headers(t.org_id, "x" * 40))
    assert bad.status_code == 401


def test_sends_once_and_a_replay_never_resends(client):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    fake = FakeSender()
    internal.set_sender(fake)
    res = client.post(
        "/internal/email/send",
        json=_body(m, ["Guest@Acme.com", "guest@acme.com", "b@beta.io"]),
        headers=_headers(t.org_id),
    )
    assert res.status_code == 200, res.text
    out = res.json()
    assert out["configured"] is True and out["provider"] == "resend"
    assert [(r["recipient"], r["status"], r["replayed"]) for r in out["results"]] == [
        ("guest@acme.com", "sent", False),
        ("b@beta.io", "sent", False),
    ]
    assert [s[0] for s in fake.sent] == ["guest@acme.com", "b@beta.io"]
    assert fake.sent[0][3] == "<p>Ship it</p>"

    again = client.post(
        "/internal/email/send",
        json=_body(m, ["guest@acme.com", "new@gamma.dev"]),
        headers=_headers(t.org_id),
    ).json()
    assert [(r["recipient"], r["status"], r["replayed"]) for r in again["results"]] == [
        ("guest@acme.com", "sent", True),
        ("new@gamma.dev", "sent", False),
    ]
    assert [s[0] for s in fake.sent] == ["guest@acme.com", "b@beta.io", "new@gamma.dev"]
    rows = _sql(
        "SELECT recipient, status, provider FROM email_deliveries WHERE meeting_id = %(m)s "
        "ORDER BY created_at",
        {"m": str(m)},
    )
    assert {(r["recipient"], r["status"], r["provider"]) for r in rows} == {
        ("guest@acme.com", "sent", "resend"),
        ("b@beta.io", "sent", "resend"),
        ("new@gamma.dev", "sent", "resend"),
    }


def test_not_configured_skips_every_recipient(client):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    internal.set_sender(None)
    out = client.post(
        "/internal/email/send", json=_body(m, ["g@acme.com"]), headers=_headers(t.org_id)
    ).json()
    assert out["configured"] is False and out["provider"] is None
    assert out["results"] == [
        {
            "recipient": "g@acme.com",
            "status": "skipped",
            "detail": "not_configured",
            "replayed": False,
        }
    ]


def test_a_provider_failure_is_failed_not_a_5xx(client):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    internal.set_sender(FakeSender(fail_for="bad@acme.com"))
    res = client.post(
        "/internal/email/send",
        json=_body(m, ["bad@acme.com", "ok@acme.com"]),
        headers=_headers(t.org_id),
    )
    assert res.status_code == 200
    assert [(r["status"], r["detail"]) for r in res.json()["results"]] == [
        ("failed", "resend_http_500"),
        ("sent", None),
    ]


def test_invalid_recipients_are_a_422(client):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    for bad in (["not-an-email"], [], [f"g{i}@acme.com" for i in range(21)]):
        res = client.post("/internal/email/send", json=_body(m, bad), headers=_headers(t.org_id))
        assert res.status_code == 422, bad


def test_another_tenants_meeting_is_404_and_rows_stay_per_tenant(client):
    a, b = seed_tenant(), seed_tenant()
    m = seed_meeting(a.org_id)
    internal.set_sender(FakeSender())
    res = client.post(
        "/internal/email/send", json=_body(m, ["g@acme.com"]), headers=_headers(b.org_id)
    )
    assert res.status_code == 404
    ok = client.post(
        "/internal/email/send",
        json=_body(m, ["g@acme.com"], key="shared-key"),
        headers=_headers(a.org_id),
    )
    assert ok.status_code == 200
    # Tenant B using the same key on its own meeting is a fresh claim, not A's replay.
    mb = seed_meeting(b.org_id)
    other = client.post(
        "/internal/email/send",
        json=_body(mb, ["g@acme.com"], key="shared-key"),
        headers=_headers(b.org_id),
    ).json()
    assert other["results"][0]["replayed"] is False
