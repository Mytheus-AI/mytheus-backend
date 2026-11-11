"""Activity for decisions as bets: the daily check (`decision_bets.run(as_of)`).

Per tenant (the way `scheduled_activities.py` enumerates them), over REST only:

1. core-api `GET /internal/bets/due?as_of=`: open bets whose check date has come;
2. for each bet: integration-service `POST /internal/crm/metric` when the bet names a CRM
   metric, and `POST /internal/jira/issue-status` with the confirmed actions of the decision's
   meeting (each answers `connected: false` when that integration is not connected);
3. core-api `POST /internal/bets/{id}/check` with the answers: core-api adds its own evidence,
   suggests an outcome only where a connected metric is clear, marks the bet due and asks the
   owner to resolve it. Nothing is resolved here.

Idempotent end to end (core-api only checks `open` bets and keys evidence by (bet, kind, ref)),
so a retry is harmless. One bet's or one tenant's failure is counted, never fatal to the rest.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from temporalio import activity

from services.workflow_service.scheduled_activities import ScheduledActivities, tenant_ids

logger = logging.getLogger(__name__)

DECISION_BETS_ACTIVITY = "decision_bets.run"


class BetActivities(ScheduledActivities):
    """Reuses the scheduled activities' core-api and integration-service callers."""

    async def check_bet(self, org_id: str, bet: dict[str, Any], as_of: date) -> dict[str, Any]:
        metric: dict[str, Any] | None = None
        if bet.get("source") == "crm" and bet.get("metric_key"):
            metric = await self._integration(
                "/internal/crm/metric",
                org_id,
                {
                    "metric_key": bet["metric_key"],
                    "start": bet["decided_on"],
                    "end": min(as_of.isoformat(), str(bet["check_date"])),
                },
            )
            metric = {
                k: metric.get(k)
                for k in (
                    "connected",
                    "provider",
                    "label",
                    "unit",
                    "value",
                    "baseline_value",
                    "window_start",
                    "window_end",
                    "error",
                )
            }
        elif bet.get("source") in ("crm", "metrics"):
            metric = {"connected": False}
        jira: dict[str, Any] | None = None
        ids = [str(a) for a in bet.get("action_ids") or []]
        if ids or bet.get("source") == "jira":
            status = await self._integration(
                "/internal/jira/issue-status", org_id, {"action_ids": ids}
            )
            jira = {
                "connected": bool(status.get("connected")),
                "issues": [
                    {
                        "action_id": i["action_id"],
                        "issue_key": i["issue_key"],
                        "issue_url": i.get("issue_url"),
                        "open": bool(i.get("open")),
                    }
                    for i in status.get("issues") or []
                ],
            }
        return await self._core(
            "POST",
            f"/internal/bets/{bet['bet_id']}/check",
            org_id,
            {"as_of": as_of.isoformat(), "metric": metric, "jira": jira},
        )

    async def bets_for_tenant(self, org_id: str, as_of: date) -> dict[str, int]:
        due = await self._core("GET", f"/internal/bets/due?as_of={as_of}", org_id)
        out = {"due": 0, "checked": 0, "failed": 0, "notified": 0, "suggested": 0}
        for bet in due.get("items") or []:
            out["due"] += 1
            try:
                r = await self.check_bet(org_id, bet, as_of)
            except Exception:  # noqa: BLE001 - one bet never stops the others
                logger.exception("bet check failed for org %s bet %s", org_id, bet.get("bet_id"))
                out["failed"] += 1
                continue
            out["checked"] += 1
            out["notified"] += int(bool(r.get("notified")))
            out["suggested"] += int(bool(r.get("suggested_outcome")))
        return out

    @activity.defn(name=DECISION_BETS_ACTIVITY)
    async def decision_bets(self, as_of: str) -> dict[str, int]:
        day = date.fromisoformat(as_of)
        totals = {"tenants": 0, "failed": 0, "due": 0, "checked": 0, "notified": 0, "suggested": 0}
        for org_id in await tenant_ids():
            totals["tenants"] += 1
            try:
                r = await self.bets_for_tenant(org_id, day)
            except Exception:  # noqa: BLE001
                logger.exception("decision bets failed for org %s", org_id)
                totals["failed"] += 1
                continue
            for key in ("due", "checked", "notified", "suggested"):
                totals[key] += r[key]
            totals["failed"] += r["failed"]
        return totals

    def all(self) -> list[Any]:
        return [self.decision_bets]
