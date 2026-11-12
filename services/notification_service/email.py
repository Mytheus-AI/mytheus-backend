"""The email channel: a provider abstraction, off unless configured.

Two providers, one interface (`EmailSender.send(to, subject, body, html=None, reply_to=None,
attachments=None)`):

- **Resend** (`ResendEmailSender`): `POST https://api.resend.com/emails` with
  `Authorization: Bearer $RESEND_API_KEY`, sender `$EMAIL_FROM`.
- **SMTP** (`SmtpEmailSender`): `SMTP_URL` (`smtp://[user:pass@]host[:port]` for
  STARTTLS-when-offered, or `smtps://[user:pass@]host[:port]` for implicit TLS) and `SMTP_FROM`
  (the sender; `EMAIL_FROM` is accepted too).

`EMAIL_FROM` defaults to `Mytheus <noreply@mytheus.cloud>` (the verified sending domain) when
Resend has a key but no sender was set (ADR-0043). `reply_to` sets `Reply-To` (a meeting
invitation replies to the person who sent it); `attachments` carry e.g. the `.ics` of an
invitation with its full content type (`text/calendar; method=REQUEST`).

Selection. `EMAIL_PROVIDER=resend|smtp` picks one explicitly (and is "not configured" when that
provider's settings are missing); unset, Resend is used when `RESEND_API_KEY` and `EMAIL_FROM`
are set, else SMTP when `SMTP_URL` and a sender are set. With neither, `email_sender_from_env()`
returns None: the consumer records each email attempt as skipped (`smtp_not_configured`) and
the in-app row stays the delivery, and the preferences API reports `email_configured: false`
so the settings screen can say email is not available on this server. Email is never an error
because it is not configured.

`APP_BASE_URL` (optional) makes a notification's relative `link` absolute in the email body.

The SMTP send is `smtplib` on a worker thread: a blocking client in an async loop would stall
every other partition this consumer serves while one mail server is slow. Neither sender logs a
body, a recipient or a provider response.
"""

from __future__ import annotations

import asyncio
import base64
import os
import smtplib
import ssl
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Literal, Protocol
from urllib.parse import unquote, urlparse

import httpx

SMTP_TIMEOUT_S = 15.0
RESEND_URL = "https://api.resend.com/emails"
RESEND_TIMEOUT = httpx.Timeout(15.0, connect=5.0)
PROVIDERS = ("resend", "smtp")
DEFAULT_EMAIL_FROM = "Mytheus <noreply@mytheus.cloud>"
ProviderName = Literal["resend", "smtp"]


class EmailError(Exception):
    """The provider refused or could not be reached. Carries a short reason, never a body."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class Attachment:
    """A file sent with an email. `content_type` is the full MIME type with parameters."""

    filename: str
    content_type: str
    content: bytes


class EmailSender(Protocol):
    async def send(
        self,
        *,
        to: str,
        subject: str,
        body: str,
        html: str | None = None,
        reply_to: str | None = None,
        attachments: list[Attachment] | None = None,
    ) -> None: ...


class SmtpEmailSender:
    provider = "smtp"

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

    async def send(
        self,
        *,
        to: str,
        subject: str,
        body: str,
        html: str | None = None,
        reply_to: str | None = None,
        attachments: list[Attachment] | None = None,
    ) -> None:
        msg = EmailMessage()
        msg["From"] = self._sender
        msg["To"] = to
        msg["Subject"] = subject
        if reply_to:
            msg["Reply-To"] = reply_to
        msg.set_content(body)
        if html:
            msg.add_alternative(html, subtype="html")
        for item in attachments or []:
            maintype, _, rest = item.content_type.partition("/")
            subtype, *params = [p.strip() for p in rest.split(";")]
            if maintype == "text":
                msg.add_attachment(
                    item.content.decode("utf-8"), subtype=subtype, filename=item.filename
                )
            else:
                msg.add_attachment(
                    item.content, maintype=maintype, subtype=subtype, filename=item.filename
                )
            part = list(msg.iter_attachments())[-1]
            for param in params:  # e.g. `method=REQUEST` on text/calendar
                key, _, value = param.partition("=")
                if key and value:
                    part.set_param(key.strip(), value.strip())
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


class ResendEmailSender:
    provider = "resend"

    def __init__(
        self,
        api_key: str,
        sender: str,
        *,
        url: str = RESEND_URL,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not api_key or not sender:
            raise ValueError("Resend needs RESEND_API_KEY and EMAIL_FROM")
        self._api_key = api_key
        self._sender = sender
        self._url = url
        self._transport = transport

    def __repr__(self) -> str:  # never render the key
        return f"ResendEmailSender(sender={self._sender!r}, api_key=<redacted>)"

    async def send(
        self,
        *,
        to: str,
        subject: str,
        body: str,
        html: str | None = None,
        reply_to: str | None = None,
        attachments: list[Attachment] | None = None,
    ) -> None:
        payload: dict[str, object] = {
            "from": self._sender,
            "to": [to],
            "subject": subject,
            "text": body,
        }
        if html:
            payload["html"] = html
        if reply_to:
            payload["reply_to"] = reply_to
        if attachments:
            payload["attachments"] = [
                {
                    "filename": a.filename,
                    "content": base64.b64encode(a.content).decode("ascii"),
                    "content_type": a.content_type,
                }
                for a in attachments
            ]
        try:
            async with httpx.AsyncClient(
                transport=self._transport, timeout=RESEND_TIMEOUT
            ) as client:
                response = await client.post(
                    self._url,
                    json=payload,
                    headers={"Authorization": f"Bearer {self._api_key}"},
                )
        except httpx.HTTPError as exc:
            raise EmailError(f"resend_unreachable:{type(exc).__name__}") from None
        if response.status_code >= 400:
            raise EmailError(f"resend_http_{response.status_code}")


def _env(name: str) -> str:
    return os.environ.get(name, "").strip()


def email_sender_from_env() -> EmailSender | None:
    """The configured provider, or None when email is not configured on this server."""
    choice = _env("EMAIL_PROVIDER").lower()
    if choice and choice not in PROVIDERS:
        return None
    resend_key, email_from = _env("RESEND_API_KEY"), _env("EMAIL_FROM")
    smtp_url, smtp_from = _env("SMTP_URL"), _env("SMTP_FROM") or email_from
    if choice in ("", "resend") and resend_key:
        return ResendEmailSender(resend_key, email_from or DEFAULT_EMAIL_FROM)
    if choice in ("", "smtp") and smtp_url and smtp_from:
        return SmtpEmailSender(smtp_url, smtp_from)
    return None


def provider_name(sender: EmailSender | None) -> ProviderName | None:
    """`resend` or `smtp` for a sender (its `provider` attribute), None for no sender."""
    name = getattr(sender, "provider", None) if sender is not None else None
    if name == "resend":
        return "resend"
    if name == "smtp":
        return "smtp"
    return None


def email_provider() -> ProviderName | None:
    """`resend`, `smtp`, or None when email is not configured."""
    return provider_name(email_sender_from_env())


def email_configured() -> bool:
    return email_sender_from_env() is not None


def render_body(body: str, link: str | None) -> str:
    if not link:
        return body
    base = os.environ.get("APP_BASE_URL", "").strip().rstrip("/")
    return f"{body}\n\n{base}{link}" if base else body
