"""core-api -> notification-service, over the caller-token scheme.

core-api presents its own `core-api=<secret>` entry from `MYTHEUS_NOTIFICATION_CALLER_TOKENS`
(the variable notification-service verifies against), base URL `NOTIFICATION_SERVICE_URL`
(default `http://notification-service:8000`).

- `GET /internal/email/status`: whether notification-service has an email provider, for the
  follow-up composer (`email_status`: True / False, None when it cannot be asked).
- `POST /internal/email/send`: the follow-up email to the external guests a host listed.
  notification-service is the sole external fan-out, so core-api never talks to a mail
  provider itself.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import httpx

SERVICE_NAME = "core-api"
TOKENS_ENV = "MYTHEUS_NOTIFICATION_CALLER_TOKENS"
URL_ENV = "NOTIFICATION_SERVICE_URL"


class NotificationServiceError(Exception):
    def __init__(self, status: int | None, reason: str) -> None:
        super().__init__(f"notification-service call failed: {reason}")
        self.status = status
        self.reason = reason


class NotConfigured(NotificationServiceError):
    """core-api holds no secret for notification-service: guests cannot be emailed."""


def own_secret(raw: str) -> str | None:
    for entry in raw.split(","):
        name, _, secret = entry.strip().partition("=")
        if name.strip() == SERVICE_NAME and secret.strip():
            return secret.strip()
    return None


class NotificationClient:
    def __init__(
        self,
        base_url: str | None = None,
        secret: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base = (
            base_url or os.environ.get(URL_ENV, "http://notification-service:8000")
        ).rstrip("/")
        self._secret = secret if secret is not None else own_secret(os.environ.get(TOKENS_ENV, ""))
        self._transport = transport

    async def email_status(self, org_id: uuid.UUID) -> bool | None:
        """True / False: whether external guests can be emailed. False when core-api holds no
        secret for notification-service (it could never send); None when it cannot be asked."""
        if not self._secret:
            return False
        headers = {
            "X-Mytheus-Service": SERVICE_NAME,
            "X-Mytheus-Service-Token": self._secret,
            "X-Mytheus-Org": str(org_id),
        }
        try:
            async with httpx.AsyncClient(transport=self._transport, timeout=3.0) as client:
                response = await client.get(f"{self._base}/internal/email/status", headers=headers)
        except httpx.HTTPError:
            return None
        if response.status_code != 200:
            return None
        try:
            return bool(response.json().get("configured"))
        except ValueError:
            return None

    async def send_email(
        self,
        org_id: uuid.UUID,
        *,
        meeting_id: uuid.UUID,
        idempotency_key: str,
        recipients: list[str],
        subject: str,
        text: str,
        html: str,
    ) -> dict[str, Any]:
        if not self._secret:
            raise NotConfigured(None, f"{TOKENS_ENV} has no {SERVICE_NAME} entry")
        headers = {
            "X-Mytheus-Service": SERVICE_NAME,
            "X-Mytheus-Service-Token": self._secret,
            "X-Mytheus-Org": str(org_id),
        }
        body = {
            "kind": "meeting_followup",
            "meeting_id": str(meeting_id),
            "idempotency_key": idempotency_key,
            "recipients": recipients,
            "subject": subject,
            "text": text,
            "html": html,
        }
        try:
            async with httpx.AsyncClient(transport=self._transport, timeout=30.0) as client:
                response = await client.post(
                    f"{self._base}/internal/email/send", headers=headers, json=body
                )
        except httpx.HTTPError as exc:
            raise NotificationServiceError(None, type(exc).__name__) from None
        if response.status_code >= 300:
            raise NotificationServiceError(response.status_code, f"HTTP {response.status_code}")
        result: dict[str, Any] = response.json()
        return result

    async def send_user_email(
        self,
        org_id: uuid.UUID,
        *,
        user_id: uuid.UUID,
        kind: str,
        idempotency_key: str,
        subject: str,
        text: str,
        html: str,
    ) -> dict[str, Any]:
        """`POST /internal/email/user`: one email to one user who opted into it (the weekly
        digest, polish wave 3). Same errors as `send_email`."""
        if not self._secret:
            raise NotConfigured(None, f"{TOKENS_ENV} has no {SERVICE_NAME} entry")
        headers = {
            "X-Mytheus-Service": SERVICE_NAME,
            "X-Mytheus-Service-Token": self._secret,
            "X-Mytheus-Org": str(org_id),
        }
        body = {
            "kind": kind,
            "user_id": str(user_id),
            "idempotency_key": idempotency_key,
            "subject": subject,
            "text": text,
            "html": html,
        }
        try:
            async with httpx.AsyncClient(transport=self._transport, timeout=30.0) as client:
                response = await client.post(
                    f"{self._base}/internal/email/user", headers=headers, json=body
                )
        except httpx.HTTPError as exc:
            raise NotificationServiceError(None, type(exc).__name__) from None
        if response.status_code >= 300:
            raise NotificationServiceError(response.status_code, f"HTTP {response.status_code}")
        result: dict[str, Any] = response.json()
        return result

    async def send_invitations(
        self,
        org_id: uuid.UUID,
        *,
        meeting_id: uuid.UUID,
        kind: str,
        messages: list[dict[str, Any]],
        attachment: dict[str, str] | None,
    ) -> dict[str, Any]:
        """`POST /internal/email/invitations` (ADR-0043): a meeting's invitations, update or
        cancellation, one rendered message per recipient and the shared `.ics`. Same errors
        as `send_email`."""
        if not self._secret:
            raise NotConfigured(None, f"{TOKENS_ENV} has no {SERVICE_NAME} entry")
        headers = {
            "X-Mytheus-Service": SERVICE_NAME,
            "X-Mytheus-Service-Token": self._secret,
            "X-Mytheus-Org": str(org_id),
        }
        body: dict[str, Any] = {
            "kind": kind,
            "meeting_id": str(meeting_id),
            "messages": messages,
            "attachment": attachment,
        }
        try:
            # Up to 50 sends with retries: generous, and still bounded.
            async with httpx.AsyncClient(transport=self._transport, timeout=120.0) as client:
                response = await client.post(
                    f"{self._base}/internal/email/invitations", headers=headers, json=body
                )
        except httpx.HTTPError as exc:
            raise NotificationServiceError(None, type(exc).__name__) from None
        if response.status_code >= 300:
            raise NotificationServiceError(response.status_code, f"HTTP {response.status_code}")
        result: dict[str, Any] = response.json()
        return result


_client: NotificationClient | None = None


def get_notifications() -> NotificationClient:
    global _client
    if _client is None:
        _client = NotificationClient()
    return _client


def set_notifications(client: NotificationClient | None) -> None:
    """Tests install a fake here; None restores the real client on next use."""
    global _client
    _client = client
