"""Activity for the `assumption-watch` schedule (Beyond, Layer 1: decisions that know why).

`assumption_watch.run(as_of)`: per tenant (enumerated like the polish wave 3 schedules),
core-api `GET /internal/assumptions/watch` (the reasons watched against a Jira issue or a
HubSpot deal, and the guest domains of the customer meetings recent decisions touch) ->
integration-service `POST /internal/watch/sources` (what Jira, Linear and HubSpot say now, or
`connected: false`) -> core-api `POST /internal/assumptions/watch-results` (breaks what no
longer holds, once per evidence, and tells the owner; links customers' open deals; records
which sources answered so the screens can say "not connected").

A tenant with nothing to watch still posts the sources' state. Idempotent end to end (core-api
dedupes each piece of evidence per reason), so a retry or a manual run is harmless. One tenant's
failure is logged and counted, never fatal to the others. The LLM side of the watcher (later
meetings and decisions) runs in agent-runtime-service, event-driven.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from temporalio import activity

from services.workflow_service.scheduled_activities import ScheduledActivities, tenant_ids

logger = logging.getLogger(__name__)

ASSUMPTION_WATCH_ACTIVITY = "assumption_watch.run"


def _state(source: dict[str, Any] | None) -> str:
    return "connected" if (source or {}).get("connected") else "not_connected"


class AssumptionWatchActivities(ScheduledActivities):
    async def watch_tenant(self, org_id: str, as_of: date) -> dict[str, int]:
        watch = await self._core("GET", "/internal/assumptions/watch", org_id)
        items = watch.get("items") or []
        keys = sorted({str(i["jira_key"]) for i in items if i.get("jira_key")})
        deals = sorted({str(i["deal_id"]) for i in items if i.get("deal_id")})
        domains = sorted(
            {str(d) for g in watch.get("deal_domains") or [] for d in g.get("domains") or []}
        )
        found = await self._integration(
            "/internal/watch/sources",
            org_id,
            {"jira_keys": keys[:200], "deal_ids": deals[:200], "domains": domains[:50]},
        )
        jira, hubspot = found.get("jira") or {}, found.get("hubspot") or {}
        linear = found.get("linear") or {}
        applied = await self._core(
            "POST",
            "/internal/assumptions/watch-results",
            org_id,
            {
                "as_of": as_of.isoformat(),
                "sources": {
                    "jira": _state(jira),
                    "linear": _state(linear),
                    "hubspot": _state(hubspot),
                },
                # Only one tracker is ever connected; each issue carries its `provider`.
                "issues": (jira.get("issues") or []) + (linear.get("issues") or []),
                "deals": hubspot.get("deals") or [],
                "domain_deals": hubspot.get("domain_deals") or [],
            },
        )
        return {
            "checked": int(applied.get("checked", 0)),
            "broken": int(applied.get("broken", 0)),
            "deals_linked": int(applied.get("deals_linked", 0)),
        }

    @activity.defn(name=ASSUMPTION_WATCH_ACTIVITY)
    async def assumption_watch(self, as_of: str) -> dict[str, int]:
        day = date.fromisoformat(as_of)
        totals = {"tenants": 0, "failed": 0, "checked": 0, "broken": 0, "deals_linked": 0}
        for org_id in await tenant_ids():
            totals["tenants"] += 1
            try:
                r = await self.watch_tenant(org_id, day)
            except Exception:  # noqa: BLE001 - one tenant never stops the run
                logger.exception("assumption watch failed for org %s", org_id)
                totals["failed"] += 1
                continue
            for key in ("checked", "broken", "deals_linked"):
                totals[key] += r[key]
        return totals

    def all(self) -> list[Any]:
        return [self.assumption_watch]
