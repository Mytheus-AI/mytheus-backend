"""Linear OAuth and GraphQL clients (ADR-0044 D1), tested at the HTTP boundary.

Nothing here reaches Linear: every call goes through an `httpx.MockTransport` that answers like
Linear's OAuth endpoints and GraphQL API, and records what was sent.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from services.integration_service import config
from services.integration_service.clients import LinearApi, LinearOAuth, ProviderError

LINEAR_ENV = {
    "LINEAR_CLIENT_ID": "linear-client",
    "LINEAR_CLIENT_SECRET": "linear-secret",
    "LINEAR_WEBHOOK_SECRET": "linear-webhook-secret",
}


@pytest.fixture(autouse=True)
def _linear_env() -> Iterator[None]:
    previous = {k: os.environ.get(k) for k in LINEAR_ENV}
    os.environ.update(LINEAR_ENV)
    yield
    for k, v in previous.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


class Recorder:
    def __init__(self, answers: list[tuple[int, Any]]) -> None:
        self.answers = answers
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        status, body = self.answers.pop(0)
        return httpx.Response(status, json=body)


def test_authorize_url_asks_for_app_actor_and_pkce() -> None:
    url = LinearOAuth().authorize_url(
        state="st", code_challenge="ch", redirect_uri="https://app.test/cb/linear"
    )
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    assert parsed.netloc == "linear.app"
    assert query["actor"] == ["app"]
    assert query["scope"] == ["read,write,issues:create"]
    assert query["code_challenge_method"] == ["S256"]
    assert query["client_id"] == ["linear-client"]


async def test_exchange_returns_organization_id() -> None:
    rec = Recorder(
        [
            (
                200,
                {
                    "access_token": "lin-AT",
                    "refresh_token": "lin-RT",
                    "expires_in": 86399,
                    "scope": "read write issues:create",
                },
            ),
            (200, {"data": {"organization": {"id": "org-1", "urlKey": "acme"}}}),
        ]
    )
    grant = await LinearOAuth(transport=httpx.MockTransport(rec.handler)).exchange(
        code="c", code_verifier="v", redirect_uri="https://app.test/cb/linear"
    )
    assert grant.external_account_id == "org-1"
    assert grant.extra["url_key"] == "acme"
    assert grant.refresh_token == "lin-RT"
    assert rec.requests[1].headers["authorization"] == "Bearer lin-AT"


async def test_refresh_keeps_old_refresh_token_when_none_returned() -> None:
    rec = Recorder([(200, {"access_token": "lin-AT2", "expires_in": 86399})])
    grant = await LinearOAuth(transport=httpx.MockTransport(rec.handler)).refresh("lin-RT")
    assert grant.access_token == "lin-AT2"
    assert grant.refresh_token == "lin-RT"


async def test_graphql_errors_raise_provider_error() -> None:
    rec = Recorder([(200, {"errors": [{"message": "boom"}]})])
    with pytest.raises(ProviderError) as info:
        await LinearApi(transport=httpx.MockTransport(rec.handler)).first_team("tok")
    assert info.value.provider == "linear"


async def test_get_issue_not_found_is_none() -> None:
    rec = Recorder(
        [
            (
                200,
                {
                    "errors": [
                        {"message": "Entity not found", "extensions": {"code": "ENTITY_NOT_FOUND"}}
                    ]
                },
            )
        ]
    )
    assert (
        await LinearApi(transport=httpx.MockTransport(rec.handler)).get_issue("t", "ENG-9") is None
    )


async def test_create_issue_sends_team_title_due_and_assignee() -> None:
    issue = {
        "id": "i1",
        "identifier": "ENG-1",
        "url": "https://linear.app/acme/issue/ENG-1",
        "state": {"type": "unstarted"},
    }
    rec = Recorder([(200, {"data": {"issueCreate": {"success": True, "issue": issue}}})])
    out = await LinearApi(transport=httpx.MockTransport(rec.handler)).create_issue(
        "t",
        team_id="team-1",
        title="Ship it",
        description="d",
        due_date="2026-11-20",
        assignee_id="user-1",
    )
    assert out["identifier"] == "ENG-1"
    sent = json.loads(rec.requests[0].content)["variables"]["input"]
    assert sent == {
        "teamId": "team-1",
        "title": "Ship it",
        "description": "d",
        "dueDate": "2026-11-20",
        "assigneeId": "user-1",
    }


def test_linear_not_configured_without_webhook_secret() -> None:
    os.environ.pop("LINEAR_WEBHOOK_SECRET")
    assert config.is_configured(config.LINEAR) is False
    os.environ["LINEAR_WEBHOOK_SECRET"] = "x"
    assert config.is_configured(config.LINEAR) is True
