"""Slack delivery for notification-service (P12 contract section 1).

notification-service is the sole external fan-out, but integration-service owns the Slack
install and its encrypted bot token. So per Slack attempt, after the transaction that claimed
the channel has committed:

1. `GET {INTEGRATION_SERVICE_URL}/internal/slack/token` (caller `notification-service`, its
   secret from `MYTHEUS_INTEGRATION_CALLER_TOKENS`) -> the tenant's bot token. 404 means Slack
   is not connected: the attempt is recorded as `skipped:slack_not_connected`, as before P12.
2. `users.lookupByEmail` (the recipient's Mytheus email) -> Slack user id; unknown ->
   `skipped:slack_user_not_found`.
3. `conversations.open` -> the DM channel, then `chat.postMessage` with Block Kit rendered from
   the notification's title, body (Slack mrkdwn) and link.

The token lives in a local variable for the length of one send. It is never stored, cached,
logged or put in a receipt; failures are recorded by Slack's error code only.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from typing import Any, Protocol

import httpx

SERVICE_NAME = "notification-service"
TOKENS_ENV = "MYTHEUS_INTEGRATION_CALLER_TOKENS"
SLACK_API = "https://slack.com/api"
SKIPPED_NOT_CONNECTED = "skipped:slack_not_connected"
SKIPPED_USER_NOT_FOUND = "skipped:slack_user_not_found"


class SlackSender(Protocol):
    async def send(
        self, *, org_id: uuid.UUID, email: str, title: str, body: str, link: str | None
    ) -> tuple[str, str | None]:
        """-> (NotificationDeliveryStatus symbol, detail)."""
        ...


def own_secret(raw: str) -> str | None:
    for entry in raw.split(","):
        name, _, secret = entry.strip().partition("=")
        if name.strip() == SERVICE_NAME and secret.strip():
            return secret.strip()
    return None


def blocks_for(title: str, body: str, link_url: str | None) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = [
        {"type": "header", "text": {"type": "plain_text", "text": title[:150], "emoji": True}},
        {"type": "section", "text": {"type": "mrkdwn", "text": body[:2900] or title[:150]}},
    ]
    if link_url:
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Open in Mytheus"},
                        "url": link_url,
                    }
                ],
            }
        )
    return blocks


@dataclass
class SlackDelivery:
    integration_url: str
    secret: str
    app_base_url: str
    transport: httpx.AsyncBaseTransport | None = None
    slack_api: str = SLACK_API

    async def _token(self, client: httpx.AsyncClient, org_id: uuid.UUID) -> str | None:
        response = await client.get(
            f"{self.integration_url}/internal/slack/token",
            headers={
                "X-Mytheus-Service": SERVICE_NAME,
                "X-Mytheus-Service-Token": self.secret,
                "X-Mytheus-Org": str(org_id),
            },
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        token = response.json().get("access_token")
        return str(token) if token else None

    async def _slack(
        self, client: httpx.AsyncClient, token: str, method: str, **kwargs: Any
    ) -> dict[str, Any]:
        response = await client.post(
            f"{self.slack_api}/{method}",
            headers={"Authorization": f"Bearer {token}"},
            **kwargs,
        )
        response.raise_for_status()
        body: dict[str, Any] = response.json()
        return body

    async def send(
        self, *, org_id: uuid.UUID, email: str, title: str, body: str, link: str | None
    ) -> tuple[str, str | None]:
        try:
            async with httpx.AsyncClient(transport=self.transport, timeout=10.0) as client:
                token = await self._token(client, org_id)
                if token is None:
                    return "failed", SKIPPED_NOT_CONNECTED
                user = await self._slack(
                    client, token, "users.lookupByEmail", data={"email": email}
                )
                if not user.get("ok"):
                    if user.get("error") == "users_not_found":
                        return "failed", SKIPPED_USER_NOT_FOUND
                    return "failed", f"slack_error:{user.get('error', 'lookup_failed')}"
                user_id = (user.get("user") or {}).get("id")
                opened = await self._slack(
                    client, token, "conversations.open", data={"users": user_id}
                )
                if not opened.get("ok"):
                    return "failed", f"slack_error:{opened.get('error', 'open_failed')}"
                channel = (opened.get("channel") or {}).get("id")
                link_url = f"{self.app_base_url}{link}" if link else None
                posted = await self._slack(
                    client,
                    token,
                    "chat.postMessage",
                    json={
                        "channel": channel,
                        "text": title,
                        "blocks": blocks_for(title, body, link_url),
                    },
                )
                if not posted.get("ok"):
                    return "failed", f"slack_error:{posted.get('error', 'post_failed')}"
        except httpx.HTTPError as exc:
            return "failed", f"slack_error:{type(exc).__name__}"
        return "sent", None


def slack_sender_from_env() -> SlackDelivery | None:
    """None when this service holds no integration-service caller secret: Slack attempts are
    then recorded as skipped, exactly as before P12."""
    secret = own_secret(os.environ.get(TOKENS_ENV, ""))
    if not secret:
        return None
    return SlackDelivery(
        integration_url=os.environ.get(
            "INTEGRATION_SERVICE_URL", "http://integration-service:8000"
        ).rstrip("/"),
        secret=secret,
        app_base_url=os.environ.get("APP_BASE_URL", "http://localhost:3000").rstrip("/"),
    )
