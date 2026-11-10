"""integration-service -> core-api, over the P10 caller-token scheme.

integration-service presents its own `integration-service=<secret>` entry from
`MYTHEUS_CORE_CALLER_TOKENS` (the variable core-api verifies against), base URL `CORE_API_URL`.

- `POST /internal/actions/{id}/external-status`: a Jira status reaches NEXUS here, never by
  publishing `actions` (P12 contract section 0).
- `GET /internal/meetings/{id}/slack-digest`: the meeting, its participants and its confirmed
  decisions and actions, for the Slack prep brief and summary.
- `GET /internal/meetings/{id}/crm-digest` (Phase 4): the meeting's confirmed outcome plus the
  email addresses the HubSpot matcher needs. 404 with `reason: confidential` for a confidential
  meeting, which is never pushed to a CRM.
- `GET /internal/decisions/{id}/record?viewer=` (Phase 4): the decision record as that user may
  see it, for the Notion / Confluence / Slack exports. 404 when they may not.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import httpx

SERVICE_NAME = "integration-service"
CORE_TOKENS_ENV = "MYTHEUS_CORE_CALLER_TOKENS"


class CoreApiError(Exception):
    def __init__(self, status: int | None, reason: str, problem_reason: str | None = None) -> None:
        super().__init__(f"core-api call failed: {reason}")
        self.status = status
        self.retryable = status is None or status >= 500 or status in (408, 409, 429)
        # The RFC 7807 `reason` extension core-api attached, when it sent one.
        self.problem_reason = problem_reason


def own_secret(raw: str) -> str | None:
    for entry in raw.split(","):
        name, _, secret = entry.strip().partition("=")
        if name.strip() == SERVICE_NAME and secret.strip():
            return secret.strip()
    return None


class CoreApiClient:
    def __init__(
        self,
        base_url: str | None = None,
        secret: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base = (base_url or os.environ.get("CORE_API_URL", "http://core-api:8000")).rstrip(
            "/"
        )
        self._secret = (
            secret if secret is not None else own_secret(os.environ.get(CORE_TOKENS_ENV, ""))
        )
        self._transport = transport

    async def _call(
        self,
        method: str,
        path: str,
        org_id: uuid.UUID,
        json: Any = None,
        params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        if not self._secret:
            raise CoreApiError(None, f"{CORE_TOKENS_ENV} has no {SERVICE_NAME} entry")
        headers = {
            "X-Mytheus-Service": SERVICE_NAME,
            "X-Mytheus-Service-Token": self._secret,
            "X-Mytheus-Org": str(org_id),
        }
        try:
            async with httpx.AsyncClient(transport=self._transport, timeout=15.0) as client:
                response = await client.request(
                    method, f"{self._base}{path}", headers=headers, json=json, params=params
                )
        except httpx.HTTPError as exc:
            raise CoreApiError(None, type(exc).__name__) from None
        if response.status_code >= 300:
            problem: str | None = None
            try:
                raw = response.json()
                problem = str(raw["reason"]) if isinstance(raw, dict) and "reason" in raw else None
            except ValueError:
                problem = None
            raise CoreApiError(response.status_code, f"HTTP {response.status_code}", problem)
        body: dict[str, Any] = response.json()
        return body

    async def external_status(
        self, org_id: uuid.UUID, action_id: uuid.UUID, status: str, external_key: str
    ) -> dict[str, Any]:
        return await self._call(
            "POST",
            f"/internal/actions/{action_id}/external-status",
            org_id,
            {"status": status, "source": "jira", "external_key": external_key},
        )

    async def slack_digest(self, org_id: uuid.UUID, meeting_id: uuid.UUID) -> dict[str, Any]:
        return await self._call("GET", f"/internal/meetings/{meeting_id}/slack-digest", org_id)

    async def crm_digest(self, org_id: uuid.UUID, meeting_id: uuid.UUID) -> dict[str, Any]:
        return await self._call("GET", f"/internal/meetings/{meeting_id}/crm-digest", org_id)

    async def decision_record(
        self, org_id: uuid.UUID, decision_id: uuid.UUID, viewer: uuid.UUID
    ) -> dict[str, Any]:
        return await self._call(
            "GET",
            f"/internal/decisions/{decision_id}/record",
            org_id,
            params={"viewer": str(viewer)},
        )


_core: CoreApiClient | None = None


def get_core() -> CoreApiClient:
    global _core
    if _core is None:
        _core = CoreApiClient()
    return _core


def set_core(client: CoreApiClient | None) -> None:
    global _core
    _core = client
