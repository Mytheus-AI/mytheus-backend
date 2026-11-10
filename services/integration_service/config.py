"""integration-service configuration: which providers exist, which are configured, and where.

A provider is "configured" only when its app credentials are set on the server (P12 contract
section 0). An unconfigured provider is listed as `configured: false` and cannot be connected.

Environment:

    OAUTH_REDIRECT_BASE         public origin of the API, e.g. https://api.mytheus.cloud. The
                                provider redirect URI is `{OAUTH_REDIRECT_BASE}/oauth/{provider}`
                                and webhooks are `{OAUTH_REDIRECT_BASE}/webhooks/{provider}`.
    APP_BASE_URL                the web app origin, for the post-callback redirect and links.
    OAUTH_STATE_SECRET          HMAC key for the OAuth `state` and the Google channel token;
                                32+ characters.
    GOOGLE_OAUTH_CLIENT_ID / GOOGLE_OAUTH_CLIENT_SECRET
    SLACK_CLIENT_ID / SLACK_CLIENT_SECRET / SLACK_SIGNING_SECRET
    JIRA_CLIENT_ID / JIRA_CLIENT_SECRET   (Atlassian OAuth 2.0 3LO)
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

# R8: Calendar + Slack + exactly one PM tool. Linear is in the closed enum (canon) but is not
# built, so it is not offered and cannot be connected.
R8_PROVIDERS: Final = (GOOGLE_CALENDAR, SLACK, JIRA)
PM_PROVIDERS: Final = frozenset({JIRA, LINEAR})

_CREDENTIAL_ENV = {
    GOOGLE_CALENDAR: ("GOOGLE_OAUTH_CLIENT_ID", "GOOGLE_OAUTH_CLIENT_SECRET"),
    SLACK: ("SLACK_CLIENT_ID", "SLACK_CLIENT_SECRET"),
    JIRA: ("JIRA_CLIENT_ID", "JIRA_CLIENT_SECRET"),
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
    return AppCredentials(client_id, client_secret)


def is_configured(provider: str) -> bool:
    return credentials(provider) is not None


def redirect_base() -> str:
    return os.environ.get("OAUTH_REDIRECT_BASE", "http://localhost:8000").rstrip("/")


def redirect_uri(provider: str) -> str:
    return f"{redirect_base()}/oauth/{provider}"


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
