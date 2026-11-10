"""Provider HTTP clients: the only code in integration-service that talks to Google, Slack or
Atlassian.

Each provider is two small classes: an OAuth client (authorize URL, code exchange, refresh,
revoke) and an API client (the handful of REST calls the connector needs). Everything above this
module depends on the `Clients` container, never on httpx directly, so tests swap the whole set
for in-memory fakes (`set_clients`) and nothing in the suite reaches a real provider.

No method logs a request or response body, and `ProviderError` carries only the provider, the
operation and a short machine-readable reason, never a payload: provider error bodies echo
codes and tokens back often enough that the safe default is to keep them out entirely.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.parse import urlencode

import httpx

from services.integration_service import config

_TIMEOUT = httpx.Timeout(10.0, connect=5.0)
MAX_ATTACHMENT_BYTES = 25 * 1024 * 1024


class ProviderError(Exception):
    def __init__(self, provider: str, operation: str, reason: str, status: int | None = None):
        super().__init__(f"{provider} {operation} failed: {reason}")
        self.provider = provider
        self.operation = operation
        self.reason = reason
        self.status = status


@dataclass
class TokenGrant:
    access_token: str
    refresh_token: str | None
    expires_in: int | None
    scopes: list[str]
    external_account_id: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        return (
            f"TokenGrant(scopes={self.scopes!r}, expires_in={self.expires_in!r}, "
            f"external_account_id={self.external_account_id!r}, tokens=<redacted>)"
        )


class OAuthClient(Protocol):
    @property
    def provider(self) -> str: ...

    @property
    def scopes(self) -> tuple[str, ...]: ...

    def authorize_url(self, *, state: str, code_challenge: str, redirect_uri: str) -> str: ...

    async def exchange(self, *, code: str, code_verifier: str, redirect_uri: str) -> TokenGrant: ...

    async def refresh(self, refresh_token: str) -> TokenGrant: ...

    async def revoke(self, token: str) -> None: ...


class _Http:
    def __init__(self, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._transport = transport

    async def request(
        self, provider: str, operation: str, method: str, url: str, **kwargs: Any
    ) -> httpx.Response:
        try:
            async with httpx.AsyncClient(transport=self._transport, timeout=_TIMEOUT) as client:
                response = await client.request(method, url, **kwargs)
        except httpx.HTTPError as exc:
            raise ProviderError(provider, operation, type(exc).__name__) from None
        return response

    async def json(
        self, provider: str, operation: str, method: str, url: str, **kwargs: Any
    ) -> Any:
        response = await self.request(provider, operation, method, url, **kwargs)
        if response.status_code >= 400:
            raise ProviderError(
                provider, operation, f"http_{response.status_code}", response.status_code
            )
        if not response.content:
            return {}
        try:
            return response.json()
        except ValueError:
            raise ProviderError(provider, operation, "invalid_json") from None


def _creds(provider: str) -> config.AppCredentials:
    creds = config.credentials(provider)
    if creds is None:
        raise ProviderError(provider, "configure", "not_configured")
    return creds


# --- Google ----------------------------------------------------------------------------------

GOOGLE_AUTHORIZE = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN = "https://oauth2.googleapis.com/token"
GOOGLE_REVOKE = "https://oauth2.googleapis.com/revoke"
GOOGLE_USERINFO = "https://openidconnect.googleapis.com/v1/userinfo"
GOOGLE_CALENDAR_API = "https://www.googleapis.com/calendar/v3"
GOOGLE_DRIVE_API = "https://www.googleapis.com/drive/v3"


class GoogleOAuth(_Http):
    provider = config.GOOGLE_CALENDAR
    scopes = (
        "openid",
        "email",
        "https://www.googleapis.com/auth/calendar.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    )

    def authorize_url(self, *, state: str, code_challenge: str, redirect_uri: str) -> str:
        query = {
            "client_id": _creds(self.provider).client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": " ".join(self.scopes),
            "access_type": "offline",
            "prompt": "consent",
            "include_granted_scopes": "true",
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
        return f"{GOOGLE_AUTHORIZE}?{urlencode(query)}"

    async def _token(self, operation: str, form: dict[str, str]) -> dict[str, Any]:
        creds = _creds(self.provider)
        form = {**form, "client_id": creds.client_id, "client_secret": creds.client_secret}
        body: dict[str, Any] = await self.json(
            self.provider, operation, "POST", GOOGLE_TOKEN, data=form
        )
        if not body.get("access_token"):
            raise ProviderError(self.provider, operation, "no_access_token")
        return body

    async def exchange(self, *, code: str, code_verifier: str, redirect_uri: str) -> TokenGrant:
        body = await self._token(
            "exchange",
            {
                "code": code,
                "code_verifier": code_verifier,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
        )
        info = await self.json(
            self.provider,
            "userinfo",
            "GET",
            GOOGLE_USERINFO,
            headers={"Authorization": f"Bearer {body['access_token']}"},
        )
        return TokenGrant(
            access_token=body["access_token"],
            refresh_token=body.get("refresh_token"),
            expires_in=body.get("expires_in"),
            scopes=str(body.get("scope", "")).split(),
            external_account_id=info.get("email"),
        )

    async def refresh(self, refresh_token: str) -> TokenGrant:
        body = await self._token(
            "refresh", {"refresh_token": refresh_token, "grant_type": "refresh_token"}
        )
        return TokenGrant(
            access_token=body["access_token"],
            refresh_token=body.get("refresh_token") or refresh_token,
            expires_in=body.get("expires_in"),
            scopes=str(body.get("scope", "")).split(),
        )

    async def revoke(self, token: str) -> None:
        await self.request(self.provider, "revoke", "POST", GOOGLE_REVOKE, data={"token": token})


class GoogleCalendarApi(_Http):
    provider = config.GOOGLE_CALENDAR

    @staticmethod
    def _auth(token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {token}"}

    async def watch(
        self, token: str, *, channel_id: str, address: str, channel_token: str
    ) -> dict[str, Any]:
        """`events.watch` on the primary calendar -> {resource_id, expiration_ms}."""
        body = await self.json(
            self.provider,
            "watch",
            "POST",
            f"{GOOGLE_CALENDAR_API}/calendars/primary/events/watch",
            headers=self._auth(token),
            json={
                "id": channel_id,
                "type": "web_hook",
                "address": address,
                "token": channel_token,
                "params": {"ttl": "604800"},
            },
        )
        return {"resource_id": body.get("resourceId"), "expiration_ms": body.get("expiration")}

    async def stop(self, token: str, *, channel_id: str, resource_id: str) -> None:
        await self.request(
            self.provider,
            "stop",
            "POST",
            f"{GOOGLE_CALENDAR_API}/channels/stop",
            headers=self._auth(token),
            json={"id": channel_id, "resourceId": resource_id},
        )

    async def list_events(
        self, token: str, *, sync_token: str | None
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Incremental `events.list` from `sync_token`, or an initial sync (from yesterday on)
        when it is None. A 410 (sync token expired) raises `ProviderError(reason="sync_gone")`;
        the caller clears the cursor and runs a full sync."""
        events: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            params: dict[str, str] = {"maxResults": "250", "showDeleted": "true"}
            if sync_token:
                params["syncToken"] = sync_token
            else:
                params["timeMin"] = time.strftime(
                    "%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 86400)
                )
            if page_token:
                params["pageToken"] = page_token
            response = await self.request(
                self.provider,
                "list_events",
                "GET",
                f"{GOOGLE_CALENDAR_API}/calendars/primary/events",
                headers=self._auth(token),
                params=params,
            )
            if response.status_code == 410:
                raise ProviderError(self.provider, "list_events", "sync_gone", 410)
            if response.status_code >= 400:
                raise ProviderError(
                    self.provider,
                    "list_events",
                    f"http_{response.status_code}",
                    response.status_code,
                )
            body = response.json()
            events.extend(body.get("items", []))
            page_token = body.get("nextPageToken")
            if not page_token:
                return events, body.get("nextSyncToken")

    async def download(self, token: str, *, file_id: str, mime_type: str | None) -> bytes:
        """Drive bytes for an attachment. Google-native docs are exported as PDF."""
        if mime_type and mime_type.startswith("application/vnd.google-apps."):
            url = f"{GOOGLE_DRIVE_API}/files/{file_id}/export"
            params = {"mimeType": "application/pdf"}
        else:
            url = f"{GOOGLE_DRIVE_API}/files/{file_id}"
            params = {"alt": "media"}
        response = await self.request(
            self.provider, "download", "GET", url, headers=self._auth(token), params=params
        )
        if response.status_code >= 400:
            raise ProviderError(
                self.provider, "download", f"http_{response.status_code}", response.status_code
            )
        if len(response.content) > MAX_ATTACHMENT_BYTES:
            raise ProviderError(self.provider, "download", "too_large")
        return response.content


# --- Slack -----------------------------------------------------------------------------------

SLACK_AUTHORIZE = "https://slack.com/oauth/v2/authorize"
SLACK_API = "https://slack.com/api"


class SlackOAuth(_Http):
    provider = config.SLACK
    scopes = ("chat:write", "users:read", "users:read.email", "im:write")

    def authorize_url(self, *, state: str, code_challenge: str, redirect_uri: str) -> str:
        query = {
            "client_id": _creds(self.provider).client_id,
            "scope": ",".join(self.scopes),
            "redirect_uri": redirect_uri,
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
        return f"{SLACK_AUTHORIZE}?{urlencode(query)}"

    async def _access(self, operation: str, form: dict[str, str]) -> dict[str, Any]:
        creds = _creds(self.provider)
        form = {**form, "client_id": creds.client_id, "client_secret": creds.client_secret}
        body: dict[str, Any] = await self.json(
            self.provider, operation, "POST", f"{SLACK_API}/oauth.v2.access", data=form
        )
        if not body.get("ok") or not body.get("access_token"):
            raise ProviderError(self.provider, operation, str(body.get("error", "not_ok")))
        return body

    async def exchange(self, *, code: str, code_verifier: str, redirect_uri: str) -> TokenGrant:
        body = await self._access(
            "exchange",
            {"code": code, "code_verifier": code_verifier, "redirect_uri": redirect_uri},
        )
        team = body.get("team") or {}
        return TokenGrant(
            access_token=body["access_token"],
            refresh_token=body.get("refresh_token"),
            expires_in=body.get("expires_in"),
            scopes=[s for s in str(body.get("scope", "")).split(",") if s],
            external_account_id=team.get("id"),
            extra={"bot_user_id": body.get("bot_user_id"), "team_name": team.get("name")},
        )

    async def refresh(self, refresh_token: str) -> TokenGrant:
        body = await self._access(
            "refresh", {"grant_type": "refresh_token", "refresh_token": refresh_token}
        )
        return TokenGrant(
            access_token=body["access_token"],
            refresh_token=body.get("refresh_token") or refresh_token,
            expires_in=body.get("expires_in"),
            scopes=[s for s in str(body.get("scope", "")).split(",") if s],
        )

    async def revoke(self, token: str) -> None:
        await self.request(
            self.provider,
            "revoke",
            "POST",
            f"{SLACK_API}/auth.revoke",
            headers={"Authorization": f"Bearer {token}"},
        )


class SlackApi(_Http):
    provider = config.SLACK

    async def auth_test(self, token: str) -> dict[str, Any]:
        body: dict[str, Any] = await self.json(
            self.provider,
            "auth_test",
            "POST",
            f"{SLACK_API}/auth.test",
            headers={"Authorization": f"Bearer {token}"},
        )
        if not body.get("ok"):
            raise ProviderError(self.provider, "auth_test", str(body.get("error", "not_ok")))
        return body


# --- Jira (Atlassian OAuth 2.0 3LO) ----------------------------------------------------------

ATLASSIAN_AUTHORIZE = "https://auth.atlassian.com/authorize"
ATLASSIAN_TOKEN = "https://auth.atlassian.com/oauth/token"
ATLASSIAN_RESOURCES = "https://api.atlassian.com/oauth/token/accessible-resources"
JIRA_API = "https://api.atlassian.com/ex/jira"


class JiraOAuth(_Http):
    provider = config.JIRA
    scopes = (
        "read:jira-work",
        "write:jira-work",
        "read:jira-user",
        "manage:jira-webhook",
        "offline_access",
    )

    def authorize_url(self, *, state: str, code_challenge: str, redirect_uri: str) -> str:
        query = {
            "audience": "api.atlassian.com",
            "client_id": _creds(self.provider).client_id,
            "scope": " ".join(self.scopes),
            "redirect_uri": redirect_uri,
            "state": state,
            "response_type": "code",
            "prompt": "consent",
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
        return f"{ATLASSIAN_AUTHORIZE}?{urlencode(query)}"

    async def _token(self, operation: str, payload: dict[str, str]) -> dict[str, Any]:
        creds = _creds(self.provider)
        payload = {**payload, "client_id": creds.client_id, "client_secret": creds.client_secret}
        body: dict[str, Any] = await self.json(
            self.provider, operation, "POST", ATLASSIAN_TOKEN, json=payload
        )
        if not body.get("access_token"):
            raise ProviderError(self.provider, operation, "no_access_token")
        return body

    async def exchange(self, *, code: str, code_verifier: str, redirect_uri: str) -> TokenGrant:
        body = await self._token(
            "exchange",
            {
                "grant_type": "authorization_code",
                "code": code,
                "code_verifier": code_verifier,
                "redirect_uri": redirect_uri,
            },
        )
        sites = await self.json(
            self.provider,
            "accessible_resources",
            "GET",
            ATLASSIAN_RESOURCES,
            headers={"Authorization": f"Bearer {body['access_token']}"},
        )
        site = next((s for s in sites if isinstance(s, dict) and s.get("id")), None)
        if site is None:
            raise ProviderError(self.provider, "exchange", "no_jira_site")
        return TokenGrant(
            access_token=body["access_token"],
            refresh_token=body.get("refresh_token"),
            expires_in=body.get("expires_in"),
            scopes=str(body.get("scope", "")).split(),
            external_account_id=str(site["id"]),
            extra={"site_url": site.get("url")},
        )

    async def refresh(self, refresh_token: str) -> TokenGrant:
        body = await self._token(
            "refresh", {"grant_type": "refresh_token", "refresh_token": refresh_token}
        )
        return TokenGrant(
            access_token=body["access_token"],
            refresh_token=body.get("refresh_token") or refresh_token,
            expires_in=body.get("expires_in"),
            scopes=str(body.get("scope", "")).split(),
        )

    async def revoke(self, token: str) -> None:
        # Atlassian 3LO has no token revocation endpoint; disconnect deletes the webhooks and
        # our copy of the tokens, and the grant expires (or the admin removes the app).
        return None


class JiraApi(_Http):
    provider = config.JIRA

    @staticmethod
    def _auth(token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    async def _call(
        self, token: str, cloud_id: str, operation: str, method: str, path: str, **kw: Any
    ) -> Any:
        return await self.json(
            self.provider,
            operation,
            method,
            f"{JIRA_API}/{cloud_id}/rest/api/3{path}",
            headers=self._auth(token),
            **kw,
        )

    async def first_project_key(self, token: str, cloud_id: str) -> str | None:
        body = await self._call(
            token, cloud_id, "projects", "GET", "/project/search", params={"maxResults": "1"}
        )
        values = body.get("values") or []
        return str(values[0]["key"]) if values else None

    async def register_webhook(self, token: str, cloud_id: str, *, url: str, jql: str) -> list[int]:
        body = await self._call(
            token,
            cloud_id,
            "register_webhook",
            "POST",
            "/webhook",
            json={"url": url, "webhooks": [{"events": ["jira:issue_updated"], "jqlFilter": jql}]},
        )
        return [
            int(r["createdWebhookId"])
            for r in body.get("webhookRegistrationResult", [])
            if r.get("createdWebhookId") is not None
        ]

    async def refresh_webhooks(self, token: str, cloud_id: str, ids: list[int]) -> str | None:
        body = await self._call(
            token,
            cloud_id,
            "refresh_webhooks",
            "PUT",
            "/webhook/refresh",
            json={"webhookIds": ids},
        )
        expiry = body.get("expirationDate")
        return str(expiry) if expiry is not None else None

    async def delete_webhooks(self, token: str, cloud_id: str, ids: list[int]) -> None:
        await self._call(
            token, cloud_id, "delete_webhooks", "DELETE", "/webhook", json={"webhookIds": ids}
        )

    async def search_by_label(self, token: str, cloud_id: str, label: str) -> list[dict[str, Any]]:
        body = await self._call(
            token,
            cloud_id,
            "search",
            "POST",
            "/search/jql",
            json={"jql": f'labels = "{label}"', "fields": ["status", "labels"], "maxResults": 5},
        )
        return [i for i in body.get("issues", []) if isinstance(i, dict)]

    async def create_issue(
        self, token: str, cloud_id: str, fields: dict[str, Any]
    ) -> dict[str, Any]:
        body: dict[str, Any] = await self._call(
            token, cloud_id, "create_issue", "POST", "/issue", json={"fields": fields}
        )
        if not body.get("key"):
            raise ProviderError(self.provider, "create_issue", "no_key")
        return body

    async def account_id_for_email(self, token: str, cloud_id: str, email: str) -> str | None:
        body = await self._call(
            token, cloud_id, "user_search", "GET", "/user/search", params={"query": email}
        )
        for user in body if isinstance(body, list) else []:
            if str(user.get("emailAddress", "")).lower() == email.lower() and user.get("accountId"):
                return str(user["accountId"])
        return None


# --- the container ---------------------------------------------------------------------------


@dataclass
class Clients:
    oauth: dict[str, OAuthClient]
    calendar: GoogleCalendarApi
    slack: SlackApi
    jira: JiraApi


def default_clients() -> Clients:
    return Clients(
        oauth={
            config.GOOGLE_CALENDAR: GoogleOAuth(),
            config.SLACK: SlackOAuth(),
            config.JIRA: JiraOAuth(),
        },
        calendar=GoogleCalendarApi(),
        slack=SlackApi(),
        jira=JiraApi(),
    )


_clients: Clients | None = None


def get_clients() -> Clients:
    global _clients
    if _clients is None:
        _clients = default_clients()
    return _clients


def set_clients(clients: Clients | None) -> None:
    """Tests install fakes here; None restores the real clients on next use."""
    global _clients
    _clients = clients
