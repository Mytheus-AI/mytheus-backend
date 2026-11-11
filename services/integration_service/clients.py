"""Provider HTTP clients: the only code in integration-service that talks to Google, Slack,
Atlassian (Jira, Confluence), HubSpot or Notion.

Each provider is two small classes: an OAuth client (authorize URL, code exchange, refresh,
revoke) and an API client (the handful of REST calls the connector needs). Everything above this
module depends on the `Clients` container, never on httpx directly, so tests swap the whole set
for in-memory fakes (`set_clients`) and nothing in the suite reaches a real provider.

No method logs a request or response body, and `ProviderError` carries only the provider, the
operation and a short machine-readable reason, never a payload: provider error bodies echo
codes and tokens back often enough that the safe default is to keep them out entirely.
"""

from __future__ import annotations

import base64
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

    async def file_owners(self, token: str, *, file_id: str) -> list[str]:
        """Lower-cased owner emails of a Drive file (empty for a shared-drive file)."""
        body = await self.json(
            self.provider,
            "file_owners",
            "GET",
            f"{GOOGLE_DRIVE_API}/files/{file_id}",
            headers=self._auth(token),
            params={"fields": "owners(emailAddress)", "supportsAllDrives": "true"},
        )
        return [
            str(o["emailAddress"]).lower()
            for o in body.get("owners") or []
            if isinstance(o, dict) and o.get("emailAddress")
        ]

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

    async def _web(
        self, token: str, operation: str, method: str, api: str, **kwargs: Any
    ) -> dict[str, Any]:
        """One Slack Web API call. Slack answers 200 with `ok: false` for every API error, so
        the error code (e.g. `not_in_channel`, `users_not_found`) becomes the reason."""
        body: dict[str, Any] = await self.json(
            self.provider,
            operation,
            method,
            f"{SLACK_API}/{api}",
            headers={"Authorization": f"Bearer {token}"},
            **kwargs,
        )
        if not body.get("ok"):
            raise ProviderError(self.provider, operation, str(body.get("error", "not_ok")))
        return body

    async def user_id_for_email(self, token: str, email: str) -> str | None:
        try:
            body = await self._web(
                token, "lookup_by_email", "GET", "users.lookupByEmail", params={"email": email}
            )
        except ProviderError as exc:
            if exc.reason == "users_not_found":
                return None
            raise
        user = body.get("user") or {}
        return str(user["id"]) if user.get("id") else None

    async def open_dm(self, token: str, user_id: str) -> str:
        body = await self._web(
            token, "conversations_open", "POST", "conversations.open", json={"users": user_id}
        )
        channel = (body.get("channel") or {}).get("id")
        if not channel:
            raise ProviderError(self.provider, "conversations_open", "no_channel")
        return str(channel)

    async def post_message(
        self, token: str, *, channel: str, text: str, blocks: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """-> {channel, ts}. `not_in_channel` / `channel_not_found` surface as the reason."""
        body = await self._web(
            token,
            "post_message",
            "POST",
            "chat.postMessage",
            json={"channel": channel, "text": text, "blocks": blocks, "unfurl_links": False},
        )
        return {"channel": str(body.get("channel") or channel), "ts": str(body.get("ts") or "")}

    async def permalink(self, token: str, *, channel: str, ts: str) -> str | None:
        try:
            body = await self._web(
                token,
                "permalink",
                "GET",
                "chat.getPermalink",
                params={"channel": channel, "message_ts": ts},
            )
        except ProviderError:
            return None
        link = body.get("permalink")
        return str(link) if link else None


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

    async def get_issue(self, token: str, cloud_id: str, key: str) -> dict[str, Any] | None:
        """The issue as Jira has it now (key, status, labels), or None if it is gone."""
        response = await self.request(
            self.provider,
            "get_issue",
            "GET",
            f"{JIRA_API}/{cloud_id}/rest/api/3/issue/{key}",
            headers=self._auth(token),
            params={"fields": "status,labels"},
        )
        if response.status_code == 404:
            return None
        if response.status_code >= 400:
            raise ProviderError(
                self.provider, "get_issue", f"http_{response.status_code}", response.status_code
            )
        body: dict[str, Any] = response.json()
        return body

    async def issue_for_watch(self, token: str, cloud_id: str, key: str) -> dict[str, Any] | None:
        """Status, status category, summary and due date of one issue (the assumption watch), or
        None when Jira no longer has it."""
        response = await self.request(
            self.provider,
            "get_issue_watch",
            "GET",
            f"{JIRA_API}/{cloud_id}/rest/api/3/issue/{key}",
            headers=self._auth(token),
            params={"fields": "status,summary,duedate"},
        )
        if response.status_code == 404:
            return None
        if response.status_code >= 400:
            raise ProviderError(
                self.provider,
                "get_issue_watch",
                f"http_{response.status_code}",
                response.status_code,
            )
        body: dict[str, Any] = response.json()
        return body

    async def account_id_for_email(self, token: str, cloud_id: str, email: str) -> str | None:
        body = await self._call(
            token, cloud_id, "user_search", "GET", "/user/search", params={"query": email}
        )
        for user in body if isinstance(body, list) else []:
            if str(user.get("emailAddress", "")).lower() == email.lower() and user.get("accountId"):
                return str(user["accountId"])
        return None


# --- HubSpot (Phase 4 CRM sync) -------------------------------------------------------------

HUBSPOT_AUTHORIZE = "https://app.hubspot.com/oauth/authorize"
HUBSPOT_API = "https://api.hubapi.com"
# HubSpot-defined association type ids (CRM v3/v4): note -> company, note -> contact.
HUBSPOT_NOTE_TO_COMPANY = 190
HUBSPOT_NOTE_TO_CONTACT = 202


class HubSpotOAuth(_Http):
    """HubSpot OAuth 2.0 (authorization code). HubSpot does not take a PKCE challenge, so the
    verifier the engine keeps is simply not sent; the signed, user-bound, single-use `state` is
    what binds the completion (ADR-0037 D4)."""

    provider = config.HUBSPOT
    scopes = (
        "oauth",
        "crm.objects.companies.read",
        "crm.objects.companies.write",
        "crm.objects.contacts.read",
        "crm.objects.contacts.write",
        # Beyond, Layer 1: the assumption watch reads deal amount, stage and close date, and the
        # blast radius lists customers' open deals. Read only.
        "crm.objects.deals.read",
    )

    def authorize_url(self, *, state: str, code_challenge: str, redirect_uri: str) -> str:
        query = {
            "client_id": _creds(self.provider).client_id,
            "redirect_uri": redirect_uri,
            "scope": " ".join(self.scopes),
            "state": state,
        }
        return f"{HUBSPOT_AUTHORIZE}?{urlencode(query)}"

    async def _token(self, operation: str, form: dict[str, str]) -> dict[str, Any]:
        creds = _creds(self.provider)
        form = {**form, "client_id": creds.client_id, "client_secret": creds.client_secret}
        body: dict[str, Any] = await self.json(
            self.provider, operation, "POST", f"{HUBSPOT_API}/oauth/v1/token", data=form
        )
        if not body.get("access_token"):
            raise ProviderError(self.provider, operation, "no_access_token")
        return body

    async def exchange(self, *, code: str, code_verifier: str, redirect_uri: str) -> TokenGrant:
        body = await self._token(
            "exchange",
            {"grant_type": "authorization_code", "code": code, "redirect_uri": redirect_uri},
        )
        info = await self.json(
            self.provider,
            "token_info",
            "GET",
            f"{HUBSPOT_API}/oauth/v1/access-tokens/{body['access_token']}",
        )
        hub_id = info.get("hub_id")
        if not hub_id:
            raise ProviderError(self.provider, "exchange", "no_hub_id")
        return TokenGrant(
            access_token=body["access_token"],
            refresh_token=body.get("refresh_token"),
            expires_in=body.get("expires_in"),
            scopes=[str(s) for s in info.get("scopes") or []],
            external_account_id=str(hub_id),
            extra={"hub_domain": info.get("hub_domain")},
        )

    async def refresh(self, refresh_token: str) -> TokenGrant:
        body = await self._token(
            "refresh", {"grant_type": "refresh_token", "refresh_token": refresh_token}
        )
        return TokenGrant(
            access_token=body["access_token"],
            refresh_token=body.get("refresh_token") or refresh_token,
            expires_in=body.get("expires_in"),
            scopes=list(self.scopes),
        )

    async def revoke(self, token: str) -> None:
        await self.request(
            self.provider, "revoke", "DELETE", f"{HUBSPOT_API}/oauth/v1/refresh_tokens/{token}"
        )


class HubSpotApi(_Http):
    provider = config.HUBSPOT

    @staticmethod
    def _auth(token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    async def account(self, token: str) -> dict[str, Any]:
        """Proves the token: the portal's account details."""
        body: dict[str, Any] = await self.json(
            self.provider,
            "account",
            "GET",
            f"{HUBSPOT_API}/account-info/v3/details",
            headers=self._auth(token),
        )
        return body

    async def _search(
        self, token: str, obj: str, prop: str, values: list[str], props: list[str]
    ) -> list[dict[str, Any]]:
        if not values:
            return []
        body = await self.json(
            self.provider,
            f"search_{obj}",
            "POST",
            f"{HUBSPOT_API}/crm/v3/objects/{obj}/search",
            headers=self._auth(token),
            json={
                "filterGroups": [
                    {"filters": [{"propertyName": prop, "operator": "IN", "values": values}]}
                ],
                "properties": props,
                "limit": 100,
            },
        )
        return [r for r in body.get("results") or [] if isinstance(r, dict) and r.get("id")]

    async def companies_by_domain(self, token: str, domains: list[str]) -> list[dict[str, Any]]:
        return await self._search(token, "companies", "domain", domains, ["name", "domain"])

    async def contacts_by_email(self, token: str, emails: list[str]) -> list[dict[str, Any]]:
        return await self._search(
            token, "contacts", "email", emails, ["email", "firstname", "lastname"]
        )

    async def create_note(
        self,
        token: str,
        *,
        body_html: str,
        timestamp: str,
        company_ids: list[str],
        contact_ids: list[str],
    ) -> str:
        associations = [
            {
                "to": {"id": cid},
                "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": type_id}],
            }
            for ids, type_id in (
                (company_ids, HUBSPOT_NOTE_TO_COMPANY),
                (contact_ids, HUBSPOT_NOTE_TO_CONTACT),
            )
            for cid in ids
        ]
        body = await self.json(
            self.provider,
            "create_note",
            "POST",
            f"{HUBSPOT_API}/crm/v3/objects/notes",
            headers=self._auth(token),
            json={
                "properties": {"hs_timestamp": timestamp, "hs_note_body": body_html},
                "associations": associations,
            },
        )
        if not body.get("id"):
            raise ProviderError(self.provider, "create_note", "no_id")
        return str(body["id"])

    async def update_note(
        self, token: str, note_id: str, *, body_html: str, timestamp: str
    ) -> bool:
        """PATCH the note's body. False when HubSpot no longer has it (deleted there)."""
        response = await self.request(
            self.provider,
            "update_note",
            "PATCH",
            f"{HUBSPOT_API}/crm/v3/objects/notes/{note_id}",
            headers=self._auth(token),
            json={"properties": {"hs_timestamp": timestamp, "hs_note_body": body_html}},
        )
        if response.status_code == 404:
            return False
        if response.status_code >= 400:
            raise ProviderError(
                self.provider, "update_note", f"http_{response.status_code}", response.status_code
            )
        return True

    DEAL_PROPS = (
        "dealname",
        "amount",
        "closedate",
        "dealstage",
        "hs_is_closed_won",
        "hs_is_closed",
    )

    async def deals_by_id(self, token: str, ids: list[str]) -> list[dict[str, Any]]:
        """Deals by id (batch read), with the properties the assumption watch compares."""
        if not ids:
            return []
        body = await self.json(
            self.provider,
            "read_deals",
            "POST",
            f"{HUBSPOT_API}/crm/v3/objects/deals/batch/read",
            headers=self._auth(token),
            json={"properties": list(self.DEAL_PROPS), "inputs": [{"id": i} for i in ids]},
        )
        return [r for r in body.get("results") or [] if isinstance(r, dict) and r.get("id")]

    async def deal_ids_for_companies(
        self, token: str, company_ids: list[str]
    ) -> dict[str, list[str]]:
        """company id -> its associated deal ids (v4 batch associations)."""
        if not company_ids:
            return {}
        body = await self.json(
            self.provider,
            "company_deals",
            "POST",
            f"{HUBSPOT_API}/crm/v4/associations/companies/deals/batch/read",
            headers=self._auth(token),
            json={"inputs": [{"id": c} for c in company_ids]},
        )
        out: dict[str, list[str]] = {}
        for r in body.get("results") or []:
            if not isinstance(r, dict):
                continue
            src = str((r.get("from") or {}).get("id") or "")
            ids = [str(t.get("toObjectId")) for t in r.get("to") or [] if t.get("toObjectId")]
            if src:
                out[src] = ids
        return out

    async def associate_note(self, token: str, note_id: str, to_object: str, to_id: str) -> None:
        """v4 default association (idempotent): note -> company | contact."""
        await self.json(
            self.provider,
            "associate_note",
            "PUT",
            f"{HUBSPOT_API}/crm/v4/objects/note/{note_id}/associations/default/{to_object}/{to_id}",
            headers=self._auth(token),
        )


# --- Notion (Phase 4 decision exports) --------------------------------------------------------

NOTION_AUTHORIZE = "https://api.notion.com/v1/oauth/authorize"
NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"


def _basic(creds: config.AppCredentials) -> dict[str, str]:
    raw = f"{creds.client_id}:{creds.client_secret}".encode()
    return {"Authorization": "Basic " + base64.b64encode(raw).decode("ascii")}


class NotionOAuth(_Http):
    """A public Notion integration. Notion access tokens do not expire; newer integrations
    also get a refresh token, which `refresh` uses when present."""

    provider = config.NOTION
    scopes: tuple[str, ...] = ()  # Notion has no OAuth scopes: access is per shared page

    def authorize_url(self, *, state: str, code_challenge: str, redirect_uri: str) -> str:
        query = {
            "client_id": _creds(self.provider).client_id,
            "response_type": "code",
            "owner": "user",
            "redirect_uri": redirect_uri,
            "state": state,
        }
        return f"{NOTION_AUTHORIZE}?{urlencode(query)}"

    async def _token(self, operation: str, payload: dict[str, str]) -> dict[str, Any]:
        body: dict[str, Any] = await self.json(
            self.provider,
            operation,
            "POST",
            f"{NOTION_API}/oauth/token",
            headers={**_basic(_creds(self.provider)), "Notion-Version": NOTION_VERSION},
            json=payload,
        )
        if not body.get("access_token"):
            raise ProviderError(self.provider, operation, "no_access_token")
        return body

    async def exchange(self, *, code: str, code_verifier: str, redirect_uri: str) -> TokenGrant:
        body = await self._token(
            "exchange",
            {"grant_type": "authorization_code", "code": code, "redirect_uri": redirect_uri},
        )
        workspace = body.get("workspace_id")
        if not workspace:
            raise ProviderError(self.provider, "exchange", "no_workspace")
        return TokenGrant(
            access_token=body["access_token"],
            refresh_token=body.get("refresh_token"),
            expires_in=body.get("expires_in"),
            scopes=[],
            external_account_id=str(workspace),
            extra={"workspace_name": body.get("workspace_name"), "bot_id": body.get("bot_id")},
        )

    async def refresh(self, refresh_token: str) -> TokenGrant:
        body = await self._token(
            "refresh", {"grant_type": "refresh_token", "refresh_token": refresh_token}
        )
        return TokenGrant(
            access_token=body["access_token"],
            refresh_token=body.get("refresh_token") or refresh_token,
            expires_in=body.get("expires_in"),
            scopes=[],
        )

    async def revoke(self, token: str) -> None:
        await self.request(
            self.provider,
            "revoke",
            "POST",
            f"{NOTION_API}/oauth/revoke",
            headers={**_basic(_creds(self.provider)), "Notion-Version": NOTION_VERSION},
            json={"token": token},
        )


def notion_title(page: dict[str, Any]) -> str:
    """A page's plain-text title (the one property of type `title`)."""
    for prop in (page.get("properties") or {}).values():
        if isinstance(prop, dict) and prop.get("type") == "title":
            text = "".join(str(t.get("plain_text", "")) for t in prop.get("title") or [])
            if text.strip():
                return text.strip()
    return "Untitled"


class NotionApi(_Http):
    provider = config.NOTION

    @staticmethod
    def _auth(token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {token}", "Notion-Version": NOTION_VERSION}

    async def me(self, token: str) -> dict[str, Any]:
        body: dict[str, Any] = await self.json(
            self.provider, "me", "GET", f"{NOTION_API}/users/me", headers=self._auth(token)
        )
        return body

    async def search_pages(self, token: str, limit: int = 50) -> list[dict[str, Any]]:
        """Pages shared with the integration -> [{id, title, url}]."""
        body = await self.json(
            self.provider,
            "search",
            "POST",
            f"{NOTION_API}/search",
            headers=self._auth(token),
            json={"filter": {"property": "object", "value": "page"}, "page_size": limit},
        )
        return [
            {"id": str(p["id"]), "title": notion_title(p), "url": p.get("url")}
            for p in body.get("results") or []
            if isinstance(p, dict) and p.get("id") and not p.get("archived")
        ]

    async def create_page(
        self, token: str, *, parent_page_id: str, title: str, blocks: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """-> {id, url}."""
        body = await self.json(
            self.provider,
            "create_page",
            "POST",
            f"{NOTION_API}/pages",
            headers=self._auth(token),
            json={
                "parent": {"page_id": parent_page_id},
                "properties": {"title": {"title": [{"text": {"content": title[:200]}}]}},
                "children": blocks[:100],
            },
        )
        if not body.get("id"):
            raise ProviderError(self.provider, "create_page", "no_id")
        return {"id": str(body["id"]), "url": body.get("url")}

    async def archive_page(self, token: str, page_id: str) -> None:
        response = await self.request(
            self.provider,
            "archive_page",
            "PATCH",
            f"{NOTION_API}/pages/{page_id}",
            headers=self._auth(token),
            json={"archived": True},
        )
        if response.status_code >= 400 and response.status_code != 404:
            raise ProviderError(
                self.provider, "archive_page", f"http_{response.status_code}", response.status_code
            )


# --- Confluence (Phase 4 decision exports; Atlassian OAuth 2.0 3LO) ---------------------------

CONFLUENCE_API = "https://api.atlassian.com/ex/confluence"


class ConfluenceOAuth(_Http):
    provider = config.CONFLUENCE
    scopes = (
        "read:confluence-space.summary",
        "read:confluence-content.summary",
        "write:confluence-content",
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
        site = next(
            (
                s
                for s in sites
                if isinstance(s, dict)
                and s.get("id")
                and any("confluence" in str(sc) for sc in s.get("scopes") or [])
            ),
            None,
        )
        if site is None:
            raise ProviderError(self.provider, "exchange", "no_confluence_site")
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
        # Atlassian 3LO has no revocation endpoint (see JiraOAuth.revoke).
        return None


class ConfluenceApi(_Http):
    provider = config.CONFLUENCE

    @staticmethod
    def _auth(token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    def _url(self, cloud_id: str, path: str) -> str:
        return f"{CONFLUENCE_API}/{cloud_id}/wiki/rest/api{path}"

    async def spaces(self, token: str, cloud_id: str) -> list[dict[str, Any]]:
        """Global spaces the user can see -> [{key, name}]."""
        body = await self.json(
            self.provider,
            "spaces",
            "GET",
            self._url(cloud_id, "/space"),
            headers=self._auth(token),
            params={"limit": "100", "type": "global"},
        )
        return [
            {"key": str(s["key"]), "name": str(s.get("name") or s["key"])}
            for s in body.get("results") or []
            if isinstance(s, dict) and s.get("key")
        ]

    async def create_page(
        self, token: str, cloud_id: str, *, space_key: str, title: str, storage: str
    ) -> dict[str, Any]:
        """-> {id, webui} (webui is relative to the site's /wiki)."""
        body = await self.json(
            self.provider,
            "create_page",
            "POST",
            self._url(cloud_id, "/content"),
            headers=self._auth(token),
            json={
                "type": "page",
                "title": title[:250],
                "space": {"key": space_key},
                "body": {"storage": {"value": storage, "representation": "storage"}},
            },
        )
        if not body.get("id"):
            raise ProviderError(self.provider, "create_page", "no_id")
        return {"id": str(body["id"]), "webui": (body.get("_links") or {}).get("webui")}

    async def update_page(
        self, token: str, cloud_id: str, page_id: str, *, title: str, storage: str
    ) -> dict[str, Any] | None:
        """New version of an existing page -> {id, webui}, or None when it is gone."""
        response = await self.request(
            self.provider,
            "read_page",
            "GET",
            self._url(cloud_id, f"/content/{page_id}"),
            headers=self._auth(token),
            params={"expand": "version"},
        )
        if response.status_code == 404:
            return None
        if response.status_code >= 400:
            raise ProviderError(
                self.provider, "read_page", f"http_{response.status_code}", response.status_code
            )
        version = int(((response.json().get("version") or {}).get("number")) or 1)
        body = await self.json(
            self.provider,
            "update_page",
            "PUT",
            self._url(cloud_id, f"/content/{page_id}"),
            headers=self._auth(token),
            json={
                "type": "page",
                "title": title[:250],
                "version": {"number": version + 1},
                "body": {"storage": {"value": storage, "representation": "storage"}},
            },
        )
        return {
            "id": str(body.get("id") or page_id),
            "webui": (body.get("_links") or {}).get("webui"),
        }


# --- the container ---------------------------------------------------------------------------


@dataclass
class Clients:
    oauth: dict[str, OAuthClient]
    calendar: GoogleCalendarApi
    slack: SlackApi
    jira: JiraApi
    # Phase 4 reach. Defaulted so a container built for the R8 providers alone stays valid.
    hubspot: HubSpotApi = field(default_factory=HubSpotApi)
    notion: NotionApi = field(default_factory=NotionApi)
    confluence: ConfluenceApi = field(default_factory=ConfluenceApi)


def default_clients() -> Clients:
    return Clients(
        oauth={
            config.GOOGLE_CALENDAR: GoogleOAuth(),
            config.SLACK: SlackOAuth(),
            config.JIRA: JiraOAuth(),
            config.HUBSPOT: HubSpotOAuth(),
            config.NOTION: NotionOAuth(),
            config.CONFLUENCE: ConfluenceOAuth(),
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
