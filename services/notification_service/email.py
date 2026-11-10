"""The email channel: a small SMTP adapter, off unless configured.

`SMTP_URL` (`smtp://[user:pass@]host[:port]` for STARTTLS-when-offered, or
`smtps://[user:pass@]host[:port]` for implicit TLS) and `SMTP_FROM` (the sender address) turn it
on. With either unset `email_sender_from_env()` returns None, and the consumer records the email
attempt as skipped (`smtp_not_configured`) instead of failing the message. `APP_BASE_URL`
(optional) makes a notification's relative `link` absolute in the email body.

The send is `smtplib` on a worker thread: a blocking client in an async loop would stall every
other partition this consumer serves while one mail server is slow.
"""

from __future__ import annotations

import asyncio
import os
import smtplib
import ssl
from email.message import EmailMessage
from typing import Protocol
from urllib.parse import unquote, urlparse

SMTP_TIMEOUT_S = 15.0


class EmailSender(Protocol):
    async def send(self, *, to: str, subject: str, body: str) -> None: ...


class SmtpEmailSender:
    def __init__(self, url: str, sender: str) -> None:
        parsed = urlparse(url)
        if parsed.scheme not in ("smtp", "smtps") or not parsed.hostname:
            raise ValueError("SMTP_URL must be smtp://host[:port] or smtps://host[:port]")
        self._implicit_tls = parsed.scheme == "smtps"
        self._host = parsed.hostname
        self._port = parsed.port or (465 if self._implicit_tls else 587)
        self._user = unquote(parsed.username) if parsed.username else None
        self._password = unquote(parsed.password) if parsed.password else None
        self._sender = sender

    async def send(self, *, to: str, subject: str, body: str) -> None:
        msg = EmailMessage()
        msg["From"] = self._sender
        msg["To"] = to
        msg["Subject"] = subject
        msg.set_content(body)
        await asyncio.to_thread(self._send_sync, msg)

    def _send_sync(self, msg: EmailMessage) -> None:
        context = ssl.create_default_context()
        client: smtplib.SMTP
        if self._implicit_tls:
            client = smtplib.SMTP_SSL(
                self._host, self._port, timeout=SMTP_TIMEOUT_S, context=context
            )
        else:
            client = smtplib.SMTP(self._host, self._port, timeout=SMTP_TIMEOUT_S)
        with client:
            if not self._implicit_tls:
                client.ehlo()
                if client.has_extn("starttls"):
                    client.starttls(context=context)
                    client.ehlo()
            if self._user and self._password:
                client.login(self._user, self._password)
            client.send_message(msg)


def email_sender_from_env() -> EmailSender | None:
    url, sender = os.environ.get("SMTP_URL", "").strip(), os.environ.get("SMTP_FROM", "").strip()
    if not url or not sender:
        return None
    return SmtpEmailSender(url, sender)


def render_body(body: str, link: str | None) -> str:
    if not link:
        return body
    base = os.environ.get("APP_BASE_URL", "").strip().rstrip("/")
    return f"{body}\n\n{base}{link}" if base else body
