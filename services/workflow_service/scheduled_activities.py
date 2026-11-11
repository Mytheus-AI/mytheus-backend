"""Activities for the polish wave 3 schedules: promise drift (daily) and the weekly digest.

Both enumerate tenants the way billing's nightly job and integration-service's worker do
(`SELECT id FROM organizations WHERE deleted_at IS NULL`, a column the application role may
read) and then work per tenant over REST, never writing another service's table:

- `promise_drift.run(as_of)`: per tenant, core-api `GET /internal/promise-drift/due` ->
  integration-service `POST /internal/jira/issue-status` (only the still-open, Jira-linked ones
  come back; `connected: false` when the tenant has no Jira, the "not connected" state, and the
  tenant is skipped) -> core-api `POST /internal/promise-drift/nudges`.
- `weekly_digest.run(week_start)`: per tenant, core-api `POST /internal/digests/weekly`.
- `listening_evening.run(now)` (hourly, In every meeting at once): per tenant, core-api
  `POST /internal/listening/evening`.

Idempotent end to end (core-api's unique keys), so a retry or a manual re-run is harmless. One
tenant's failure is logged and counted, never fatal to the others.
"""

from __future__ import annotations

import logging
import os
from datetime import date
from typing import Any

import httpx
from sqlalchemy import text
from temporalio import activity

from libs.shared.db import get_sessionmaker
from services.workflow_service.activities import (
    CORE_TOKENS_ENV,
    INTEGRATION_TOKENS_ENV,
    call_service,
    own_secret,
)

logger = logging.getLogger(__name__)

PROMISE_DRIFT_ACTIVITY = "promise_drift.run"
WEEKLY_DIGEST_ACTIVITY = "weekly_digest.run"
LISTENING_EVENING_ACTIVITY = "listening_evening.run"


async def tenant_ids() -> list[str]:
    async with get_sessionmaker()() as session:
        rows = await session.execute(
            text("SELECT id FROM organizations WHERE deleted_at IS NULL ORDER BY id")
        )
        return [str(r[0]) for r in rows]


class ScheduledActivities:
    def __init__(
        self,
        *,
        core_url: str,
        core_secret: str | None,
        integration_url: str,
        integration_secret: str | None,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: float = 60.0,
    ) -> None:
        self._core_url = core_url.rstrip("/")
        self._core_secret = core_secret
        self._integration_url = integration_url.rstrip("/")
        self._integration_secret = integration_secret
        self._transport = transport
        self._timeout = timeout

    @classmethod
    def from_env(cls) -> ScheduledActivities:
        return cls(
            core_url=os.environ.get("CORE_API_URL", "http://core-api:8000"),
            core_secret=own_secret(os.environ.get(CORE_TOKENS_ENV, "")),
            integration_url=os.environ.get(
                "INTEGRATION_SERVICE_URL", "http://integration-service:8000"
            ),
            integration_secret=own_secret(os.environ.get(INTEGRATION_TOKENS_ENV, "")),
        )

    async def _core(
        self, method: str, path: str, org_id: str, json: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return await call_service(
            method,
            f"{self._core_url}{path}",
            self._core_secret,
            org_id,
            CORE_TOKENS_ENV,
            json=json,
            transport=self._transport,
            timeout=self._timeout,
        )

    async def _integration(self, path: str, org_id: str, json: dict[str, Any]) -> dict[str, Any]:
        return await call_service(
            "POST",
            f"{self._integration_url}{path}",
            self._integration_secret,
            org_id,
            INTEGRATION_TOKENS_ENV,
            json=json,
            transport=self._transport,
            timeout=self._timeout,
        )

    async def drift_for_tenant(self, org_id: str, as_of: date) -> dict[str, int]:
        due = await self._core("GET", f"/internal/promise-drift/due?as_of={as_of}", org_id)
        ids = [str(i["action_id"]) for i in due.get("items") or []]
        if not ids:
            return {"due": 0, "open": 0, "owner": 0, "host": 0, "jira": 1}
        status = await self._integration("/internal/jira/issue-status", org_id, {"action_ids": ids})
        if not status.get("connected"):
            return {"due": len(ids), "open": 0, "owner": 0, "host": 0, "jira": 0}
        still_open = [
            {
                "action_id": i["action_id"],
                "issue_key": i["issue_key"],
                "issue_url": i.get("issue_url"),
            }
            for i in status.get("issues") or []
            if i.get("open")
        ]
        result = {"owner": 0, "host": 0}
        if still_open:
            result = await self._core(
                "POST",
                "/internal/promise-drift/nudges",
                org_id,
                {"as_of": as_of.isoformat(), "issues": still_open},
            )
        return {
            "due": len(ids),
            "open": len(still_open),
            "owner": int(result.get("owner", 0)),
            "host": int(result.get("host", 0)),
            "jira": 1,
        }

    @activity.defn(name=PROMISE_DRIFT_ACTIVITY)
    async def promise_drift(self, as_of: str) -> dict[str, int]:
        day = date.fromisoformat(as_of)
        totals = {"tenants": 0, "failed": 0, "not_connected": 0, "owner": 0, "host": 0}
        for org_id in await tenant_ids():
            totals["tenants"] += 1
            try:
                r = await self.drift_for_tenant(org_id, day)
            except Exception:  # noqa: BLE001 - one tenant never stops the run
                logger.exception("promise drift failed for org %s", org_id)
                totals["failed"] += 1
                continue
            totals["not_connected"] += 1 - r["jira"]
            totals["owner"] += r["owner"]
            totals["host"] += r["host"]
        return totals

    @activity.defn(name=WEEKLY_DIGEST_ACTIVITY)
    async def weekly_digest(self, week_start: str) -> dict[str, int]:
        totals = {"tenants": 0, "failed": 0, "built": 0, "notified": 0, "emailed": 0}
        for org_id in await tenant_ids():
            totals["tenants"] += 1
            try:
                r = await self._core(
                    "POST", "/internal/digests/weekly", org_id, {"week_start": week_start}
                )
            except Exception:  # noqa: BLE001
                logger.exception("weekly digest failed for org %s", org_id)
                totals["failed"] += 1
                continue
            for key in ("built", "notified", "emailed"):
                totals[key] += int(r.get(key, 0))
        return totals

    @activity.defn(name=LISTENING_EVENING_ACTIVITY)
    async def listening_evening(self, now: str) -> dict[str, int]:
        """In every meeting at once: per tenant, core-api `POST /internal/listening/evening`."""
        totals = {"tenants": 0, "failed": 0, "built": 0, "notified": 0, "rules_applied": 0}
        for org_id in await tenant_ids():
            totals["tenants"] += 1
            try:
                r = await self._core("POST", "/internal/listening/evening", org_id, {"now": now})
            except Exception:  # noqa: BLE001
                logger.exception("evening digest failed for org %s", org_id)
                totals["failed"] += 1
                continue
            for key in ("built", "notified", "rules_applied"):
                totals[key] += int(r.get(key, 0))
        return totals

    def all(self) -> list[Any]:
        return [self.promise_drift, self.weekly_digest, self.listening_evening]
