"""In-memory stand-ins for Google, Slack, Atlassian and core-api, shared by the P12 tests.

Nothing in the suite reaches a real provider: integration-service talks to providers only
through `services.integration_service.clients.Clients`, and these fakes replace the whole set.
"""

from __future__ import annotations

import base64
import os
import time
import uuid
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

from services.integration_service.clients import Clients, ProviderError, TokenGrant
from services.integration_service.oauth_state import challenge_for

STATE_SECRET = "s" * 48
CALLER_SECRET_NOTIF = "n" * 40
CALLER_SECRET_WF = "w" * 40
CORE_SECRET_INTEG = "i" * 40
SLACK_SIGNING = "slack-signing-secret-for-tests"
JIRA_SECRET = "jira-client-secret-for-tests-0123456789"

ENV = {
    "ENVIRONMENT": "test",
    "MYTHEUS_KEY_CUSTODY": "local-env",
    "MYTHEUS_KEK_ID": "local-1",
    "MYTHEUS_KEK_BASE64": base64.b64encode(b"k" * 32).decode(),
    "MYTHEUS_BLIND_INDEX_KEY_BASE64": base64.b64encode(b"b" * 32).decode(),
    "OAUTH_STATE_SECRET": STATE_SECRET,
    "OAUTH_REDIRECT_BASE": "https://api.mytheus.test",
    "APP_BASE_URL": "https://app.mytheus.test",
    "GOOGLE_OAUTH_CLIENT_ID": "google-client",
    "GOOGLE_OAUTH_CLIENT_SECRET": "google-secret",
    "SLACK_CLIENT_ID": "slack-client",
    "SLACK_CLIENT_SECRET": "slack-secret",
    "SLACK_SIGNING_SECRET": SLACK_SIGNING,
    "JIRA_CLIENT_ID": "jira-client",
    "JIRA_CLIENT_SECRET": JIRA_SECRET,
    "MYTHEUS_INTEGRATION_CALLER_TOKENS": (
        f"notification-service={CALLER_SECRET_NOTIF},workflow-service={CALLER_SECRET_WF}"
    ),
    "MYTHEUS_CORE_CALLER_TOKENS": f"integration-service={CORE_SECRET_INTEG}",
}


def install_env() -> dict[str, str | None]:
    previous = {k: os.environ.get(k) for k in ENV}
    os.environ.update(ENV)
    from services.integration_service import store

    store.get_cipher.cache_clear()
    return previous


def restore_env(previous: dict[str, str | None]) -> None:
    for k, v in previous.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


SECRET_TOKENS = {
    "google_calendar": ("ya29.google-ACCESS-secret", "1//google-REFRESH-secret"),
    "slack": ("xoxb-slack-ACCESS-secret", None),
    "jira": ("jira-ACCESS-secret-token", "jira-REFRESH-secret-token"),
}
ACCOUNTS = {"google_calendar": "owner@example.com", "slack": "T0TEAM", "jira": "cloud-123"}


class FakeOAuth:
    def __init__(self, provider: str, scopes: tuple[str, ...]) -> None:
        self.provider = provider
        self.scopes = scopes
        self.challenges: dict[str, str] = {}
        # Each exchange returns a fresh provider account unless a test pins one: one account can
        # be live in only one tenant (uq_integration_connections_account).
        self.account: str | None = None
        self.exchanges = 0
        self.refreshes = 0
        self.revoked: list[str] = []

    def authorize_url(self, *, state: str, code_challenge: str, redirect_uri: str) -> str:
        self.challenges[state] = code_challenge
        query = urlencode(
            {"state": state, "code_challenge": code_challenge, "redirect_uri": redirect_uri}
        )
        return f"https://provider.test/{self.provider}/authorize?{query}"

    async def exchange(self, *, code: str, code_verifier: str, redirect_uri: str) -> TokenGrant:
        self.exchanges += 1
        # The provider binds the code to the challenge it saw at authorize time.
        if challenge_for(code_verifier) not in self.challenges.values():
            raise ProviderError(self.provider, "exchange", "invalid_grant", 400)
        access, refresh = SECRET_TOKENS[self.provider]
        return TokenGrant(
            access_token=access,
            refresh_token=refresh,
            expires_in=3600,
            scopes=list(self.scopes),
            external_account_id=self.account or f"{ACCOUNTS[self.provider]}-{uuid.uuid4().hex[:8]}",
            extra={"site_url": "https://nexus.atlassian.net"} if self.provider == "jira" else {},
        )

    async def refresh(self, refresh_token: str) -> TokenGrant:
        self.refreshes += 1
        return TokenGrant(
            access_token=f"refreshed-{self.provider}-ACCESS-secret",
            refresh_token=refresh_token,
            expires_in=3600,
            scopes=list(self.scopes),
        )

    async def revoke(self, token: str) -> None:
        self.revoked.append(token)


class FakeCalendar:
    def __init__(self) -> None:
        self.watches: list[dict[str, Any]] = []
        self.stops: list[str] = []
        self.events: list[dict[str, Any]] = []
        self.list_calls: list[str | None] = []
        self.downloads: list[str] = []
        self.owners: dict[str, list[str]] = {}

    async def watch(
        self, token: str, *, channel_id: str, address: str, channel_token: str
    ) -> dict[str, Any]:
        self.watches.append(
            {"token": token, "channel_id": channel_id, "address": address, "ct": channel_token}
        )
        return {"resource_id": "res-1", "expiration_ms": str(int((time.time() + 6e5) * 1000))}

    async def stop(self, token: str, *, channel_id: str, resource_id: str) -> None:
        self.stops.append(channel_id)

    async def list_events(
        self, token: str, *, sync_token: str | None
    ) -> tuple[list[dict[str, Any]], str | None]:
        self.list_calls.append(sync_token)
        return list(self.events), f"sync-{len(self.list_calls)}"

    async def file_owners(self, token: str, *, file_id: str) -> list[str]:
        return self.owners.get(file_id, [])

    async def download(self, token: str, *, file_id: str, mime_type: str | None) -> bytes:
        self.downloads.append(file_id)
        return b"%PDF-1.4 attachment bytes"


class FakeSlack:
    async def auth_test(self, token: str) -> dict[str, Any]:
        return {"ok": True}


class FakeJira:
    def __init__(self) -> None:
        self.issues: list[dict[str, Any]] = []
        self.created: list[dict[str, Any]] = []
        self.webhooks: list[dict[str, Any]] = []
        self.deleted: list[list[int]] = []
        self.searches = 0
        self.reads: list[str] = []

    async def first_project_key(self, token: str, cloud_id: str) -> str | None:
        return "NEX"

    async def register_webhook(self, token: str, cloud_id: str, *, url: str, jql: str) -> list[int]:
        self.webhooks.append({"url": url, "jql": jql})
        return [101]

    async def refresh_webhooks(self, token: str, cloud_id: str, ids: list[int]) -> str | None:
        return None

    async def delete_webhooks(self, token: str, cloud_id: str, ids: list[int]) -> None:
        self.deleted.append(ids)

    async def search_by_label(self, token: str, cloud_id: str, label: str) -> list[dict[str, Any]]:
        self.searches += 1
        return [i for i in self.issues if label in i["fields"]["labels"]]

    async def create_issue(
        self, token: str, cloud_id: str, fields: dict[str, Any]
    ) -> dict[str, Any]:
        key = f"NEX-{len(self.issues) + 1}"
        issue = {
            "id": str(10000 + len(self.issues)),
            "key": key,
            "fields": {**fields, "status": {"statusCategory": {"key": "new"}}},
        }
        self.issues.append(issue)
        self.created.append(fields)
        return {"id": issue["id"], "key": key}

    async def get_issue(self, token: str, cloud_id: str, key: str) -> dict[str, Any] | None:
        self.reads.append(key)
        return next((i for i in self.issues if i["key"] == key), None)

    def set_status(self, key: str, category: str) -> None:
        for issue in self.issues:
            if issue["key"] == key:
                issue["fields"]["status"] = {"statusCategory": {"key": category}}

    async def account_id_for_email(self, token: str, cloud_id: str, email: str) -> str | None:
        return "acc-1" if email == "assignee@example.com" else None


def fake_clients() -> Clients:
    oauth: dict[str, Any] = {
        "google_calendar": FakeOAuth("google_calendar", ("calendar.readonly",)),
        "slack": FakeOAuth("slack", ("chat:write", "users:read", "users:read.email", "im:write")),
        "jira": FakeOAuth("jira", ("read:jira-work", "write:jira-work")),
    }
    return Clients(
        oauth=oauth,
        calendar=FakeCalendar(),  # type: ignore[arg-type]
        slack=FakeSlack(),  # type: ignore[arg-type]
        jira=FakeJira(),  # type: ignore[arg-type]
    )


class FakeCore:
    def __init__(self, digest: dict[str, Any] | None = None) -> None:
        self.status_calls: list[tuple[uuid.UUID, uuid.UUID, str, str]] = []
        self.digest = digest

    async def external_status(
        self, org_id: uuid.UUID, action_id: uuid.UUID, status: str, external_key: str
    ) -> dict[str, Any]:
        self.status_calls.append((org_id, action_id, status, external_key))
        return {"action_id": str(action_id), "status": status, "changed": True}

    async def slack_digest(self, org_id: uuid.UUID, meeting_id: uuid.UUID) -> dict[str, Any]:
        assert self.digest is not None
        return self.digest


def state_from(url: str) -> str:
    return parse_qs(urlparse(url).query)["state"][0]
