"""`POST /internal/watch/sources` (workflow-service): what Jira, Linear and HubSpot say now.

Beyond, Layer 1 (decisions that know why, blast radius). The `AssumptionWatchWorkflow` hands
over the Jira keys and HubSpot deal ids the tenant's decision reasons are watched against, and
the guest domains of the customer meetings recent decisions touch. This answers, with the
tenant's own tokens:

- `jira`: `{connected, issues: [{key, summary, status, status_category, due_date, url}]}`. An
  issue Jira no longer has is left out. Read with the same token and site as promise drift.
- `linear`: `{connected, issues: [...]}` when Linear is the tenant's tracker (ADR-0044): the
  same keys (`jira_keys`; Linear identifiers have the same `ENG-42` shape) read from Linear.
  Only one of `jira` and `linear` is ever connected.
- `hubspot`: `{connected, deals: [...], domain_deals: [{domain, deals: [...]}]}`. Deals by id
  (amount, stage, close date, won/lost), and the deals of the companies whose domain matches a
  guest domain. Needs the `crm.objects.deals.read` scope; a connection made before that scope
  existed answers `connected: true` with no deals (HubSpot refuses the read) until reconnected.

`connected: false` when the tenant has no active connection (the "not connected" state). A
provider failure never becomes a 5xx: that provider's list is just empty, so a hiccup cannot
break or un-break anything (core-api breaks a reason only on evidence it was given).
"""

from __future__ import annotations

import uuid
from datetime import date
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field

from libs.shared.db import get_sessionmaker
from libs.shared.tenancy import set_rls
from services.integration_service import config, store
from services.integration_service.clients import Clients, get_clients
from services.integration_service.connectors import jira
from services.integration_service.internal_auth import workflow_caller
from services.integration_service.redaction import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["integration-internal"])

MAX_KEYS = 200
MAX_DOMAINS = 50
MAX_DEALS_PER_DOMAIN = 10


class SourcesRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    jira_keys: list[Annotated[str, Field(max_length=40)]] = Field(
        default_factory=list, max_length=MAX_KEYS
    )
    deal_ids: list[Annotated[str, Field(max_length=40)]] = Field(
        default_factory=list, max_length=MAX_KEYS
    )
    domains: list[Annotated[str, Field(max_length=200)]] = Field(
        default_factory=list, max_length=MAX_DOMAINS
    )


class WatchIssue(BaseModel):
    key: str
    provider: Literal["jira", "linear"] = "jira"
    summary: str | None = None
    status: str | None = None
    status_category: str | None = None
    due_date: date | None = None
    url: str | None = None


class WatchDeal(BaseModel):
    id: str
    name: str | None = None
    amount: float | None = None
    close_date: date | None = None
    stage: str | None = None
    closed_won: bool = False
    closed_lost: bool = False
    url: str | None = None


class DomainDeals(BaseModel):
    domain: str
    deals: list[WatchDeal]


class JiraSource(BaseModel):
    connected: bool
    issues: list[WatchIssue] = Field(default_factory=list)


class LinearSource(BaseModel):
    connected: bool = False
    issues: list[WatchIssue] = Field(default_factory=list)


class HubSpotSource(BaseModel):
    connected: bool
    deals: list[WatchDeal] = Field(default_factory=list)
    domain_deals: list[DomainDeals] = Field(default_factory=list)


class WatchSources(BaseModel):
    jira: JiraSource
    linear: LinearSource
    hubspot: HubSpotSource


def _date(raw: Any) -> date | None:
    if not isinstance(raw, str) or not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _float(raw: Any) -> float | None:
    try:
        return float(raw) if raw not in (None, "") else None
    except (TypeError, ValueError):
        return None


def issue_from(raw: dict[str, Any], site: dict[str, Any]) -> WatchIssue:
    fields = raw.get("fields") or {}
    status = fields.get("status") or {}
    key = str(raw.get("key") or "")
    return WatchIssue(
        key=key,
        summary=fields.get("summary"),
        status=status.get("name"),
        status_category=(status.get("statusCategory") or {}).get("key"),
        due_date=_date(fields.get("duedate")),
        url=jira._browse_url(site, key),
    )


def deal_from(raw: dict[str, Any], portal: str | None) -> WatchDeal:
    p = raw.get("properties") or {}
    won = str(p.get("hs_is_closed_won") or "").lower() == "true"
    closed = str(p.get("hs_is_closed") or "").lower() == "true"
    deal_id = str(raw.get("id"))
    return WatchDeal(
        id=deal_id,
        name=p.get("dealname"),
        amount=_float(p.get("amount")),
        close_date=_date(p.get("closedate")),
        stage=p.get("dealstage"),
        closed_won=won,
        closed_lost=closed and not won,
        url=f"https://app.hubspot.com/contacts/{portal}/record/0-3/{deal_id}" if portal else None,
    )


async def _jira(org_id: uuid.UUID, keys: list[str], clients: Clients) -> JiraSource:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        conn = await store.get_connection(session, config.JIRA, active_only=True)
        if conn is None:
            return JiraSource(connected=False)
        if not keys:
            return JiraSource(connected=True)
        try:
            tokens = await store.access_token(session, conn, clients.oauth[config.JIRA])
            site = jira._site(await store.get_cursor(session, conn.id, jira.SITE_KIND), conn)
        except Exception:  # noqa: BLE001 - no usable token: nothing to read this run
            logger.warning("jira token unavailable for the assumption watch")
            return JiraSource(connected=True)
    issues: list[WatchIssue] = []
    for key in keys:
        try:
            raw = await clients.jira.issue_for_watch(
                tokens.access_token, str(site["cloud_id"]), key
            )
        except Exception:  # noqa: BLE001 - one issue's hiccup skips that issue
            logger.warning("jira read failed for one watched issue")
            continue
        if raw is not None:
            issues.append(issue_from(raw, site))
    return JiraSource(connected=True, issues=issues)


# Linear workflow state types, as Jira status categories (what core-api's watch reads).
_LINEAR_CATEGORY = {"completed": "done", "canceled": "done", "started": "indeterminate"}


def linear_issue_from(raw: dict[str, Any]) -> WatchIssue:
    state = raw.get("state") or {}
    return WatchIssue(
        key=str(raw.get("identifier") or ""),
        provider="linear",
        summary=raw.get("title"),
        status=state.get("name"),
        status_category=_LINEAR_CATEGORY.get(str(state.get("type")), "new"),
        due_date=_date(raw.get("dueDate")),
        url=raw.get("url"),
    )


async def _linear(org_id: uuid.UUID, keys: list[str], clients: Clients) -> LinearSource:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        conn = await store.get_connection(session, config.LINEAR, active_only=True)
        if conn is None:
            return LinearSource(connected=False)
        if not keys:
            return LinearSource(connected=True)
        try:
            tokens = await store.access_token(session, conn, clients.oauth[config.LINEAR])
        except Exception:  # noqa: BLE001 - no usable token: nothing to read this run
            logger.warning("linear token unavailable for the assumption watch")
            return LinearSource(connected=True)
    issues: list[WatchIssue] = []
    for key in keys:
        try:
            raw = await clients.linear.get_issue(tokens.access_token, key)
        except Exception:  # noqa: BLE001 - one issue's hiccup skips that issue
            logger.warning("linear read failed for one watched issue")
            continue
        if raw is not None:
            issues.append(linear_issue_from(raw))
    return LinearSource(connected=True, issues=issues)


async def _hubspot(
    org_id: uuid.UUID, deal_ids: list[str], domains: list[str], clients: Clients
) -> HubSpotSource:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        conn = await store.get_connection(session, config.HUBSPOT, active_only=True)
        if conn is None:
            return HubSpotSource(connected=False)
        if not deal_ids and not domains:
            return HubSpotSource(connected=True)
        try:
            tokens = await store.access_token(session, conn, clients.oauth[config.HUBSPOT])
        except Exception:  # noqa: BLE001
            logger.warning("hubspot token unavailable for the assumption watch")
            return HubSpotSource(connected=True)
    token, portal = tokens.access_token, conn.external_account_id
    deals: list[WatchDeal] = []
    by_domain: list[DomainDeals] = []
    try:
        deals = [deal_from(r, portal) for r in await clients.hubspot.deals_by_id(token, deal_ids)]
        if domains:
            companies = await clients.hubspot.companies_by_domain(token, domains)
            company_domain = {
                str(c["id"]): str((c.get("properties") or {}).get("domain") or "").lower()
                for c in companies
            }
            links = await clients.hubspot.deal_ids_for_companies(token, list(company_domain))
            wanted = sorted({d for ids in links.values() for d in ids})[:MAX_KEYS]
            read = {
                r["id"]: deal_from(r, portal)
                for r in await clients.hubspot.deals_by_id(token, wanted)
            }
            grouped: dict[str, list[WatchDeal]] = {}
            for company, ids in links.items():
                domain = company_domain.get(company) or ""
                for i in ids:
                    if domain and i in read:
                        grouped.setdefault(domain, []).append(read[i])
            by_domain = [
                DomainDeals(domain=d, deals=v[:MAX_DEALS_PER_DOMAIN]) for d, v in grouped.items()
            ]
    except Exception:  # noqa: BLE001 - a missing scope or an outage: nothing this run
        logger.warning("hubspot deal read failed for the assumption watch")
    return HubSpotSource(connected=True, deals=deals, domain_deals=by_domain)


async def watch_sources(
    org_id: uuid.UUID, body: SourcesRequest, clients: Clients | None = None
) -> WatchSources:
    clients = clients or get_clients()
    keys = list(dict.fromkeys(k.strip().upper() for k in body.jira_keys if k.strip()))
    deal_ids = list(dict.fromkeys(d.strip() for d in body.deal_ids if d.strip().isdigit()))
    domains = list(dict.fromkeys(d.strip().lower() for d in body.domains if d.strip()))
    return WatchSources(
        jira=await _jira(org_id, keys, clients),
        linear=await _linear(org_id, keys, clients),
        hubspot=await _hubspot(org_id, deal_ids, domains, clients),
    )


@router.post("/internal/watch/sources", response_model=WatchSources)
async def sources(
    body: SourcesRequest,
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
) -> WatchSources:
    return await watch_sources(org_id, body)
