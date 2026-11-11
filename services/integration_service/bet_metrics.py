"""`POST /internal/crm/metric` (workflow-service): one CRM number for a decision's bet.

Decisions as bets ("Beyond the meeting", Layer 3). On a bet's check date the daily
`DecisionBetsWorkflow` asks for the metric the bet named (`metric_key`, the catalogue below)
over two spans of the same length: from the decision to the check date (`value`), and the
span just before the decision (`baseline_value`), so core-api can say whether the number
moved the way the decision expected. core-api only ever SUGGESTS an outcome from it; the
bet's owner resolves.

HubSpot is the one CRM (`config.py`). The metrics are counts from the CRM v3 search API,
`total` of a filtered search, and for revenue the sum of `amount` over the matching deals:

    hubspot.deals_won      deals in a closed-won stage with closedate in the span
    hubspot.revenue_won    the sum of their `amount` (first 1000 deals; `partial` beyond)
    hubspot.new_contacts   contacts with createdate in the span
    hubspot.new_companies  companies with createdate in the span

The deals metrics need the `crm.objects.deals.read` scope on the HubSpot app. Without a
HubSpot connection the answer is `{connected: false}` (the "not connected" state the bet's
evidence shows); a provider error is `{connected: true, value: null, error}`, never a 5xx.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, time, timedelta
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field

from libs.shared.db import get_sessionmaker
from libs.shared.tenancy import set_rls
from services.integration_service import config, store
from services.integration_service.clients import HUBSPOT_API, Clients, ProviderError, get_clients
from services.integration_service.internal_auth import workflow_caller
from services.integration_service.redaction import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["integration-internal"])

MetricKey = Literal[
    "hubspot.deals_won", "hubspot.revenue_won", "hubspot.new_contacts", "hubspot.new_companies"
]
CATALOGUE: dict[str, tuple[str, str, str, str]] = {
    # key: (label, unit, object, date property)
    "hubspot.deals_won": ("Deals won", "deals", "deals", "closedate"),
    "hubspot.revenue_won": ("Revenue won", "$", "deals", "closedate"),
    "hubspot.new_contacts": ("New contacts", "contacts", "contacts", "createdate"),
    "hubspot.new_companies": ("New companies", "companies", "companies", "createdate"),
}
REVENUE_PAGES = 10


class MetricRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    metric_key: MetricKey
    start: date
    end: date


class MetricReading(BaseModel):
    connected: bool
    provider: str = config.HUBSPOT
    label: str | None = None
    unit: str | None = None
    value: float | None = None
    baseline_value: float | None = None
    window_start: date | None = None
    window_end: date | None = None
    partial: bool = False
    error: str | None = Field(default=None, max_length=200)


def _ms(d: date, end: bool = False) -> str:
    moment = datetime.combine(d, time.max if end else time.min, tzinfo=UTC)
    return str(int(moment.timestamp() * 1000))


def _filters(obj: str, prop: str, start: date, end: date) -> list[dict[str, Any]]:
    filters: list[dict[str, Any]] = [
        {
            "propertyName": prop,
            "operator": "BETWEEN",
            "value": _ms(start),
            "highValue": _ms(end, True),
        }
    ]
    if obj == "deals":
        filters.append({"propertyName": "hs_is_closed_won", "operator": "EQ", "value": "true"})
    return filters


async def _search(
    clients: Clients,
    token: str,
    obj: str,
    filters: list[dict[str, Any]],
    *,
    after: str | None,
    props: list[str],
    limit: int,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "filterGroups": [{"filters": filters}],
        "properties": props,
        "limit": limit,
    }
    if after:
        body["after"] = after
    result: dict[str, Any] = await clients.hubspot.json(
        config.HUBSPOT,
        f"metric_{obj}",
        "POST",
        f"{HUBSPOT_API}/crm/v3/objects/{obj}/search",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=body,
    )
    return result


async def read_value(
    clients: Clients, token: str, key: str, start: date, end: date
) -> tuple[float, bool]:
    """(value, partial) for one span."""
    _label, _unit, obj, prop = CATALOGUE[key]
    filters = _filters(obj, prop, start, end)
    if key != "hubspot.revenue_won":
        page = await _search(
            clients, token, obj, filters, after=None, props=["hs_object_id"], limit=1
        )
        return float(page.get("total") or 0), False
    total, after, pages = 0.0, None, 0
    while True:
        page = await _search(clients, token, obj, filters, after=after, props=["amount"], limit=100)
        for r in page.get("results") or []:
            try:
                total += float((r.get("properties") or {}).get("amount") or 0)
            except (TypeError, ValueError):
                continue
        after = ((page.get("paging") or {}).get("next") or {}).get("after")
        pages += 1
        if not after:
            return total, False
        if pages >= REVENUE_PAGES:
            return total, True


async def crm_metric(
    org_id: uuid.UUID, body: MetricRequest, clients: Clients | None = None
) -> MetricReading:
    clients = clients or get_clients()
    label, unit, _obj, _prop = CATALOGUE[body.metric_key]
    start, end = min(body.start, body.end), max(body.start, body.end)
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        conn = await store.get_connection(session, config.HUBSPOT, active_only=True)
        if conn is None:
            return MetricReading(connected=False, label=label, unit=unit)
        try:
            tokens = await store.access_token(session, conn, clients.oauth[config.HUBSPOT])
        except Exception:  # noqa: BLE001 - a dead token reads as "could not read", not a 5xx
            logger.warning("hubspot token unavailable for a bet metric")
            return MetricReading(
                connected=True, label=label, unit=unit, error="The HubSpot token could not be used"
            )
    span = end - start
    before_end = start - timedelta(days=1)
    before_start = before_end - span
    try:
        value, partial = await read_value(clients, tokens.access_token, body.metric_key, start, end)
        baseline, partial_b = await read_value(
            clients, tokens.access_token, body.metric_key, before_start, before_end
        )
    except ProviderError as exc:
        logger.warning("hubspot metric read failed: %s", exc.reason)
        reason = (
            "The HubSpot app lacks the scope for this metric"
            if exc.status in (401, 403)
            else "HubSpot could not be read just now"
        )
        return MetricReading(connected=True, label=label, unit=unit, error=reason)
    return MetricReading(
        connected=True,
        label=label,
        unit=unit,
        value=value,
        baseline_value=baseline,
        window_start=start,
        window_end=end,
        partial=partial or partial_b,
    )


@router.post("/internal/crm/metric", response_model=MetricReading)
async def metric(
    body: MetricRequest,
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
) -> MetricReading:
    return await crm_metric(org_id, body)
