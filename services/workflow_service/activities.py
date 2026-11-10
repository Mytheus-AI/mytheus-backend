"""MeetingLifecycleWorkflow activities (P10, plan 3.1). REST only, never a table write.

workflow-service owns no durable store, so every effect is a call to the service that owns the
data (one writer per store):

- `open_meeting_room`: meeting-service `POST /internal/meetings/{id}/room/open`
  (base `MEETING_SERVICE_URL`).
- `run_post_processing`: core-api `GET /internal/meetings/{id}/post-meeting`
  (base `CORE_API_URL`).
- `close_meeting_room`: meeting-service `POST /internal/meetings/{id}/room/close`.
- `send_prep_brief` / `send_meeting_summary` (P12): integration-service
  `POST /internal/slack/prep-brief` and `POST /internal/slack/meeting-summary` with body
  `{meeting_id}` (base `INTEGRATION_SERVICE_URL`, secret from
  `MYTHEUS_INTEGRATION_CALLER_TOKENS`). integration-service answers `{sent: 0}` when the tenant
  has no Slack, so "not connected" is a successful no-op, not an error.

`run_post_processing` is a read: it confirms the meeting is over in core-api and returns what it
produced (see `services/core_api/routes/internal_lifecycle.py` for why nothing is built yet).

Auth is the internal caller-token scheme both services already use: this service presents its
own `workflow-service=<secret>` entry from `MYTHEUS_CORE_CALLER_TOKENS` (core-api) and from
`MYTHEUS_MEETING_CALLER_TOKENS` (meeting-service), with `X-Mytheus-Org` set to the workflow's
`org_id`. The owning service binds RLS from that header, so an `org_id` that does not own the
meeting is a 404.

Failure policy: a 4xx other than 409/429 can never succeed on retry (unknown meeting, bad
token, bad request), so it is a non-retryable `ApplicationError`; 409, 429, 5xx and network
errors raise retryably and the workflow's `RetryPolicy` backs off.
"""

from __future__ import annotations

import os
from typing import Any

import httpx
from temporalio import activity
from temporalio.exceptions import ApplicationError

SERVICE_NAME = "workflow-service"
CORE_TOKENS_ENV = "MYTHEUS_CORE_CALLER_TOKENS"
MEETING_TOKENS_ENV = "MYTHEUS_MEETING_CALLER_TOKENS"
INTEGRATION_TOKENS_ENV = "MYTHEUS_INTEGRATION_CALLER_TOKENS"
_RETRYABLE_4XX = frozenset({408, 409, 425, 429})


def own_secret(raw: str) -> str | None:
    """This service's secret from a `service=secret,...` list, or None."""
    for entry in raw.split(","):
        name, _, secret = entry.strip().partition("=")
        if name.strip() == SERVICE_NAME and secret.strip():
            return secret.strip()
    return None


async def call_service(
    method: str,
    url: str,
    secret: str | None,
    org_id: str,
    env: str,
    *,
    json: dict[str, Any] | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    timeout: float = 20.0,
) -> dict[str, Any]:
    """One internal caller-token request; the failure policy in the module docstring."""
    if not secret:
        raise ApplicationError(
            f"{env} has no {SERVICE_NAME} entry; cannot call {url}",
            type="NotConfigured",
            non_retryable=True,
        )
    headers = {
        "X-Mytheus-Service": SERVICE_NAME,
        "X-Mytheus-Service-Token": secret,
        "X-Mytheus-Org": org_id,
    }
    async with httpx.AsyncClient(transport=transport, timeout=timeout) as client:
        response = await client.request(method, url, headers=headers, json=json)
    if response.status_code < 300:
        body: dict[str, Any] = response.json()
        return body
    retryable = response.status_code >= 500 or response.status_code in _RETRYABLE_4XX
    raise ApplicationError(
        f"{method} {url} -> HTTP {response.status_code}",
        type=f"HTTP{response.status_code}",
        non_retryable=not retryable,
    )


class LifecycleActivities:
    """Activities as bound methods, so the worker injects configuration (and tests inject an
    in-process transport that talks to the real FastAPI apps)."""

    def __init__(
        self,
        *,
        core_url: str,
        meeting_url: str,
        core_secret: str | None,
        meeting_secret: str | None,
        integration_url: str = "http://integration-service:8000",
        integration_secret: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: float = 20.0,
    ) -> None:
        self._core_url = core_url.rstrip("/")
        self._meeting_url = meeting_url.rstrip("/")
        self._integration_url = integration_url.rstrip("/")
        self._core_secret = core_secret
        self._meeting_secret = meeting_secret
        self._integration_secret = integration_secret
        self._transport = transport
        self._timeout = timeout

    @classmethod
    def from_env(cls) -> LifecycleActivities:
        return cls(
            core_url=os.environ.get("CORE_API_URL", "http://core-api:8000"),
            meeting_url=os.environ.get("MEETING_SERVICE_URL", "http://meeting-service:8000"),
            core_secret=own_secret(os.environ.get(CORE_TOKENS_ENV, "")),
            meeting_secret=own_secret(os.environ.get(MEETING_TOKENS_ENV, "")),
            integration_url=os.environ.get(
                "INTEGRATION_SERVICE_URL", "http://integration-service:8000"
            ),
            integration_secret=own_secret(os.environ.get(INTEGRATION_TOKENS_ENV, "")),
        )

    async def _call(
        self,
        method: str,
        url: str,
        secret: str | None,
        org_id: str,
        env: str,
        json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await call_service(
            method,
            url,
            secret,
            org_id,
            env,
            json=json,
            transport=self._transport,
            timeout=self._timeout,
        )

    @activity.defn(name="open_meeting_room")
    async def open_meeting_room(self, meeting_id: str, org_id: str) -> dict[str, Any]:
        return await self._call(
            "POST",
            f"{self._meeting_url}/internal/meetings/{meeting_id}/room/open",
            self._meeting_secret,
            org_id,
            MEETING_TOKENS_ENV,
        )

    @activity.defn(name="run_post_processing")
    async def run_post_processing(self, meeting_id: str, org_id: str) -> dict[str, Any]:
        return await self._call(
            "GET",
            f"{self._core_url}/internal/meetings/{meeting_id}/post-meeting",
            self._core_secret,
            org_id,
            CORE_TOKENS_ENV,
        )

    @activity.defn(name="close_meeting_room")
    async def close_meeting_room(self, meeting_id: str, org_id: str) -> dict[str, Any]:
        return await self._call(
            "POST",
            f"{self._meeting_url}/internal/meetings/{meeting_id}/room/close",
            self._meeting_secret,
            org_id,
            MEETING_TOKENS_ENV,
        )

    @activity.defn(name="send_prep_brief")
    async def send_prep_brief(self, meeting_id: str, org_id: str) -> dict[str, Any]:
        return await self._call(
            "POST",
            f"{self._integration_url}/internal/slack/prep-brief",
            self._integration_secret,
            org_id,
            INTEGRATION_TOKENS_ENV,
            json={"meeting_id": meeting_id},
        )

    @activity.defn(name="send_meeting_summary")
    async def send_meeting_summary(self, meeting_id: str, org_id: str) -> dict[str, Any]:
        return await self._call(
            "POST",
            f"{self._integration_url}/internal/slack/meeting-summary",
            self._integration_secret,
            org_id,
            INTEGRATION_TOKENS_ENV,
            json={"meeting_id": meeting_id},
        )

    def all(self) -> list[Any]:
        return [
            self.open_meeting_room,
            self.run_post_processing,
            self.close_meeting_room,
            self.send_prep_brief,
            self.send_meeting_summary,
        ]
