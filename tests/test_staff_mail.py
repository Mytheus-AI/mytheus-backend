"""libs/shared/staff_mail.py: the superadmin heads-up email, with a fake sender."""

from __future__ import annotations

import services.notification_service.email as email_mod
from libs.shared import staff_mail


class FakeSender:
    def __init__(self) -> None:
        self.sent: list[dict[str, str | None]] = []

    async def send(self, *, to, subject, body, html=None, reply_to=None, attachments=None):
        self.sent.append({"to": to, "subject": subject, "body": body, "html": html})


async def test_mails_every_listed_address(monkeypatch):
    fake = FakeSender()
    monkeypatch.setattr(email_mod, "email_sender_from_env", lambda: fake)
    monkeypatch.setenv("STAFF_ALERT_EMAILS", "a@mytheus.cloud, b@mytheus.cloud")
    n = await staff_mail.notify_staff(
        "A new workspace was created", [("Workspace", "Kestrel <Labs>")]
    )
    assert n == 2
    assert [m["to"] for m in fake.sent] == ["a@mytheus.cloud", "b@mytheus.cloud"]
    assert fake.sent[0]["subject"] == "[Mytheus] A new workspace was created"
    assert "Workspace: Kestrel <Labs>" in str(fake.sent[0]["body"])
    assert "Kestrel &lt;Labs&gt;" in str(fake.sent[0]["html"])  # escaped in HTML


async def test_no_provider_sends_nothing_and_does_not_raise(monkeypatch):
    monkeypatch.setattr(email_mod, "email_sender_from_env", lambda: None)
    monkeypatch.setenv("STAFF_ALERT_EMAILS", "a@mytheus.cloud")
    assert await staff_mail.notify_staff("x", []) == 0


async def test_a_failing_provider_never_raises(monkeypatch):
    class Boom(FakeSender):
        async def send(self, **_kw):
            raise RuntimeError("down")

    monkeypatch.setattr(email_mod, "email_sender_from_env", lambda: Boom())
    monkeypatch.setenv("STAFF_ALERT_EMAILS", "a@mytheus.cloud")
    assert await staff_mail.notify_staff("x", [("k", "v")]) == 0
