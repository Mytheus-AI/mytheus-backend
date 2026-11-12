"""integration-service configuration: which providers exist, which are configured, and where.

A provider is "configured" only when its app credentials are set on the server (P12 contract
section 0). An unconfigured provider is listed as `configured: false` and cannot be connected.

Environment:

    OAUTH_REDIRECT_BASE         public origin of the API, e.g. https://api.mytheus.cloud.
                                Webhooks are `{OAUTH_REDIRECT_BASE}/webhooks/{provider}`.
    APP_BASE_URL                the web app origin. The provider OAuth redirect URI is the app
                                page `{APP_BASE_URL}/settings/integrations/callback/{provider}`;
                                also the base of links in Jira and Slack.
    OAUTH_STATE_SECRET          HMAC key for the OAuth `state` and the Google channel token;
                                32+ characters.
    GOOGLE_OAUTH_CLIENT_ID / GOOGLE_OAUTH_CLIENT_SECRET
    SLACK_CLIENT_ID / SLACK_CLIENT_SECRET / SLACK_SIGNING_SECRET
    JIRA_CLIENT_ID / JIRA_CLIENT_SECRET   (Atlassian OAuth 2.0 3LO)
    LINEAR_CLIENT_ID / LINEAR_CLIENT_SECRET / LINEAR_WEBHOOK_SECRET
                                          (a Linear OAuth application, actor=app; all three,
                                          or Linear is not configured; ADR-0044)
    HUBSPOT_CLIENT_ID / HUBSPOT_CLIENT_SECRET          (Phase 4 CRM sync)
    NOTION_CLIENT_ID / NOTION_CLIENT_SECRET            (Phase 4 decision exports; a public
                                                       Notion integration)
    CONFLUENCE_CLIENT_ID / CONFLUENCE_CLIENT_SECRET    (Phase 4 decision exports; an Atlassian
                                                       3LO app, may be the Jira app's values
                                                       when that app also has Confluence scopes)

Phase 4 "reach" providers (HubSpot, Notion, Confluence) sit outside guardrail R8's
Calendar + Slack + one-PM-tool set: they are not PM tools, so the single-PM rule does not apply
to them, and they receive no inbound webhooks (`/webhooks/{provider}` stays R8-only). Each is
"not configured" until its app credentials are set, exactly like the R8 providers (ADR-0037 D3).

Salesforce is deliberately not built. A Salesforce connected app needs a per-org My Domain
login host, a different note model (ContentNote + ContentDocumentLink, or Task) and an AppExchange
security review before customers can install it, so even a stub would not be "trivial": HubSpot
is the one CRM, and a second one is a separate piece of work.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final

from libs.shared.errors import ErrorCode, ProblemDetail

GOOGLE_CALENDAR: Final = "google_calendar"
SLACK: Final = "slack"
JIRA: Final = "jira"
LINEAR: Final = "linear"
HUBSPOT: Final = "hubspot"
NOTION: Final = "notion"
CONFLUENCE: Final = "confluence"

# R8: Calendar + Slack + exactly one PM tool, Jira or Linear (Linear built by ADR-0044). The
# one-tracker rule is `PM_PROVIDERS` below.
R8_PROVIDERS: Final = (GOOGLE_CALENDAR, SLACK, JIRA, LINEAR)
PM_PROVIDERS: Final = frozenset({JIRA, LINEAR})
# Phase 4 reach: the CRM and the two document tools a decision record can be exported to.
REACH_PROVIDERS: Final = (HUBSPOT, NOTION, CONFLUENCE)
# Everything the settings API lists and lets an admin connect.
CONNECTABLE_PROVIDERS: Final = (*R8_PROVIDERS, *REACH_PROVIDERS)
# Where a decision record can be exported (Slack is the existing R8 connector).
EXPORT_PROVIDERS: Final = (NOTION, CONFLUENCE, SLACK)

_CREDENTIAL_ENV = {
    GOOGLE_CALENDAR: ("GOOGLE_OAUTH_CLIENT_ID", "GOOGLE_OAUTH_CLIENT_SECRET"),
    SLACK: ("SLACK_CLIENT_ID", "SLACK_CLIENT_SECRET"),
    JIRA: ("JIRA_CLIENT_ID", "JIRA_CLIENT_SECRET"),
    LINEAR: ("LINEAR_CLIENT_ID", "LINEAR_CLIENT_SECRET"),
    HUBSPOT: ("HUBSPOT_CLIENT_ID", "HUBSPOT_CLIENT_SECRET"),
    NOTION: ("NOTION_CLIENT_ID", "NOTION_CLIENT_SECRET"),
    CONFLUENCE: ("CONFLUENCE_CLIENT_ID", "CONFLUENCE_CLIENT_SECRET"),
}

MIN_STATE_SECRET = 32


@dataclass(frozen=True)
class AppCredentials:
    client_id: str
    client_secret: str

    def __repr__(self) -> str:  # never render the secret
        return f"AppCredentials(client_id={self.client_id!r}, client_secret=<redacted>)"


def credentials(provider: str) -> AppCredentials | None:
    names = _CREDENTIAL_ENV.get(provider)
    if names is None:
        return None
    client_id = os.environ.get(names[0], "").strip()
    client_secret = os.environ.get(names[1], "").strip()
    if not client_id or not client_secret:
        return None
    if provider == SLACK and not os.environ.get("SLACK_SIGNING_SECRET", "").strip():
        return None  # an install we could never verify an event from is not configured
    if provider == LINEAR and not os.environ.get("LINEAR_WEBHOOK_SECRET", "").strip():
        return None  # same rule: no verifiable status updates, no Linear
    return AppCredentials(client_id, client_secret)


def is_configured(provider: str) -> bool:
    return credentials(provider) is not None


def drive_attachments_enabled() -> bool:
    """Whether Google Calendar also asks for Drive read access and ingests event attachments.

    Off by default: `drive.readonly` is a Google *restricted* scope, which needs a paid yearly
    security assessment before the app can be verified for external users. With it off, a
    meeting still lists its attachments (title and link); the agent just does not read them.
    Set GOOGLE_DRIVE_ATTACHMENTS=1 to turn it back on."""
    return os.environ.get("GOOGLE_DRIVE_ATTACHMENTS", "").strip().lower() in {"1", "true", "yes"}


def redirect_base() -> str:
    return os.environ.get("OAUTH_REDIRECT_BASE", "http://localhost:8000").rstrip("/")


def redirect_uri(provider: str) -> str:
    """The provider's OAuth redirect target: the web app's callback page, which (signed in)
    completes the flow with `POST /oauth/{provider}`. Register exactly this with each provider."""
    return f"{app_base_url()}/settings/integrations/callback/{provider}"


def webhook_url(provider: str) -> str:
    return f"{redirect_base()}/webhooks/{provider}"


def app_base_url() -> str:
    return os.environ.get("APP_BASE_URL", "http://localhost:3000").rstrip("/")


def state_secret() -> bytes:
    raw = os.environ.get("OAUTH_STATE_SECRET", "")
    if len(raw) < MIN_STATE_SECRET:
        raise ProblemDetail(
            503,
            ErrorCode.DEPENDENCY_UNAVAILABLE,
            "OAUTH_STATE_SECRET is not configured",
        )
    return raw.encode("utf-8")


def slack_signing_secret() -> bytes | None:
    raw = os.environ.get("SLACK_SIGNING_SECRET", "").strip()
    return raw.encode("utf-8") if raw else None


def linear_webhook_secret() -> bytes | None:
    raw = os.environ.get("LINEAR_WEBHOOK_SECRET", "").strip()
    return raw.encode("utf-8") if raw else None
