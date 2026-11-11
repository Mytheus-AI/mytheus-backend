"""Internal routes for decisions that know why (Beyond, Layer 1). Not routed by api-gateway.

For `agent-runtime-service` (the `internal_agents.py` caller scheme):
- `GET /internal/decisions/{id}/why-source`: the confirmed decision, its moment, the transcript
  around it (finals 150 s before to 60 s after the quote, or the 40 finals before the decision
  was recorded), the meeting's participants and speaker map, up to 40 open actions from other
  meetings (`customer` when that meeting listed external guests) and `already_read`.
- `GET /internal/assumptions/open?exclude_meeting_id=&exclude_decision_id=`: watching reasons of
  confirmed decisions, newest first (60), for the watcher's `assumption_check@v1`.

For `workflow-service` (the `AssumptionWatchWorkflow` schedule):
- `GET  /internal/assumptions/watch`: reasons with a Jira key or a HubSpot deal to re-read, and
  the guest domains of the customer meetings each recent decision touches.
- `POST /internal/assumptions/watch-results`: what integration-service read. core-api applies
  the deterministic rules below, breaks what no longer holds (once per evidence, through
  `decision_why.record_break`, which tells the owner), links customers' open deals into the
  blast radius and records which sources answered (`watch_source_status`).

Rules (an assumption breaks only on evidence):
- Jira, with a watch date: the issue is not done and is now due after that date, or the date
  has passed and it is still not done.
- HubSpot, with a value and comparator: the deal amount no longer satisfies it ("under 40k" is
  `lt 40000`). Any HubSpot deal reason: the deal was lost. With a watch date: the deal is not won
  and now closes after it.
"""

from __future__ import annotations

import json
import uuid
from datetime import date
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text

from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.core_api import decision_why as why
from services.core_api.records import display_name
from services.core_api.routes.internal_agents import agent_runtime_caller
from services.core_api.routes.internal_lifecycle import workflow_caller

router = APIRouter(tags=["core-internal"])

BEFORE_MS = 150_000
AFTER_MS = 60_000
FALLBACK_TURNS = 40
MAX_CANDIDATE_ACTIONS = 40
CANDIDATE_DAYS = 120
MAX_OPEN = 60
MAX_WATCH = 500
DOMAIN_DAYS = 90

SourceState = Literal["connected", "not_connected"]


class WhyDecision(BaseModel):
    id: uuid.UUID
    meeting_id: uuid.UUID
    content: str
    rationale: str | None = None
    quote: str | None = None
    quote_speaker_id: uuid.UUID | None = None
    quote_start_ms: int | None = None
    confirmed_by: uuid.UUID | None = None
    confidential: bool
    status: str


class WhyParticipant(BaseModel):
    user_id: uuid.UUID
    name: str


class WhyTurn(BaseModel):
    speaker_id: str
    start_ms: int
    end_ms: int
    text: str


class CandidateAction(BaseModel):
    id: uuid.UUID
    content: str
    due_date: date | None = None
    meeting_id: uuid.UUID | None = None
    meeting_title: str | None = None
    customer: bool


class WhySource(BaseModel):
    decision: WhyDecision
    meeting_title: str | None = None
    participants: list[WhyParticipant]
    speakers: dict[str, uuid.UUID]
    turns: list[WhyTurn]
    candidate_actions: list[CandidateAction]
    already_read: bool


class OpenAssumption(BaseModel):
    id: uuid.UUID
    decision_id: uuid.UUID
    decision_content: str
    kind: str
    statement: str
    quote: str | None = None
    meeting_id: uuid.UUID


class OpenAssumptions(BaseModel):
    items: list[OpenAssumption]


class WatchItem(BaseModel):
    assumption_id: uuid.UUID
    decision_id: uuid.UUID
    kind: str
    statement: str
    jira_key: str | None = None
    deal_id: str | None = None
    watch_date: date | None = None
    value: float | None = None
    comparator: str | None = None
    unit: str | None = None


class DealDomains(BaseModel):
    decision_id: uuid.UUID
    domains: list[str]


class WatchList(BaseModel):
    items: list[WatchItem]
    deal_domains: list[DealDomains]


class Issue(BaseModel):
    model_config = ConfigDict(extra="ignore")

    key: str
    summary: str | None = None
    status: str | None = None
    status_category: str | None = None  # "done" | "indeterminate" | "new"
    due_date: date | None = None
    url: str | None = None


class Deal(BaseModel):
    model_config = ConfigDict(extra="ignore")

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
    deals: list[Deal] = Field(default_factory=list)


class Sources(BaseModel):
    jira: SourceState = "not_connected"
    linear: SourceState = "not_connected"
    hubspot: SourceState = "not_connected"


class WatchResults(BaseModel):
    model_config = ConfigDict(extra="forbid")

    as_of: date
    sources: Sources
    issues: list[Issue] = Field(default_factory=list, max_length=MAX_WATCH)
    deals: list[Deal] = Field(default_factory=list, max_length=MAX_WATCH)
    domain_deals: list[DomainDeals] = Field(default_factory=list, max_length=MAX_WATCH)


class WatchApplied(BaseModel):
    checked: int
    broken: int
    deals_linked: int


# --- agent-runtime-service -----------------------------------------------------------------


@router.get("/internal/decisions/{decision_id}/why-source", response_model=WhySource)
async def why_source(
    decision_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(agent_runtime_caller)],
) -> WhySource:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        d = await why.decision_row(session, decision_id)
        if d is None:
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        if d["status"] != "confirmed":
            raise ProblemDetail(409, ErrorCode.VERSION_CONFLICT, "The decision is not confirmed")
        m = {"m": str(d["meeting_id"])}
        people = (
            await session.execute(
                text(
                    "SELECT p.user_id, u.email FROM meeting_participants p JOIN users u "
                    "ON u.id = p.user_id AND u.deleted_at IS NULL WHERE p.meeting_id = :m "
                    "ORDER BY u.email, p.user_id"
                ),
                m,
            )
        ).all()
        speakers = {
            str(r[0]): r[1]
            for r in await session.execute(
                text(
                    "SELECT speaker_id, user_id FROM meeting_speakers "
                    "WHERE meeting_id = :m AND user_id IS NOT NULL"
                ),
                m,
            )
        }
        turns: list[WhyTurn] = []
        has_table = (
            await session.execute(
                text("SELECT to_regclass('public.transcript_chunks') IS NOT NULL")
            )
        ).scalar()
        if has_table:
            if d["quote_start_ms"] is not None:
                rows = await session.execute(
                    text(
                        "SELECT speaker_id, start_ms, end_ms, text FROM transcript_chunks "
                        "WHERE meeting_id = :m AND is_final = true AND start_ms >= :lo "
                        "AND start_ms <= :hi ORDER BY start_ms, id LIMIT 400"
                    ),
                    {
                        **m,
                        "lo": max(0, int(d["quote_start_ms"]) - BEFORE_MS),
                        "hi": int(d["quote_start_ms"]) + AFTER_MS,
                    },
                )
                turns = [WhyTurn(**dict(r._mapping)) for r in rows]
            else:
                rows = await session.execute(
                    text(
                        "SELECT speaker_id, start_ms, end_ms, text FROM transcript_chunks "
                        "WHERE meeting_id = :m AND is_final = true AND created_at <= :t "
                        "ORDER BY start_ms DESC, id DESC LIMIT :n"
                    ),
                    {**m, "t": d["created_at"], "n": FALLBACK_TURNS},
                )
                turns = list(reversed([WhyTurn(**dict(r._mapping)) for r in rows]))
        actions = (
            (
                await session.execute(
                    text(
                        "SELECT a.id, a.content, a.due_date, a.meeting_id, mt.title AS "
                        "meeting_title FROM action_items a LEFT JOIN meetings mt "
                        "ON mt.id = a.meeting_id AND mt.deleted_at IS NULL "
                        "WHERE a.status = 'confirmed' AND a.deleted_at IS NULL "
                        "AND (a.meeting_id IS NULL OR a.meeting_id <> :m) "
                        "AND a.created_at > now() - make_interval(days => :days) "
                        "AND (mt.id IS NULL OR mt.confidential = false) "
                        "ORDER BY a.created_at DESC, a.id LIMIT :n"
                    ),
                    {**m, "days": CANDIDATE_DAYS, "n": MAX_CANDIDATE_ACTIONS},
                )
            )
            .mappings()
            .all()
        )
        customers = await why.customer_meetings(
            session, [a["meeting_id"] for a in actions if a["meeting_id"] is not None]
        )
        read = (
            await session.execute(
                text("SELECT read_at IS NOT NULL FROM decision_why_state WHERE decision_id = :d"),
                {"d": str(decision_id)},
            )
        ).scalar()
    return WhySource(
        decision=WhyDecision(
            id=d["id"],
            meeting_id=d["meeting_id"],
            content=d["content"],
            rationale=d["rationale"],
            quote=d["quote"],
            quote_speaker_id=d["quote_speaker_id"],
            quote_start_ms=d["quote_start_ms"],
            confirmed_by=d["confirmed_by"],
            confidential=bool(d["confidential"]),
            status=d["status"],
        ),
        meeting_title=d["meeting_title"],
        participants=[
            WhyParticipant(user_id=p[0], name=display_name(p[1]) or "A colleague") for p in people
        ],
        speakers=speakers,
        turns=turns,
        candidate_actions=[
            CandidateAction(**dict(a), customer=a["meeting_id"] in customers) for a in actions
        ],
        already_read=bool(read),
    )


@router.get("/internal/assumptions/open", response_model=OpenAssumptions)
async def open_assumptions(
    org_id: Annotated[uuid.UUID, Depends(agent_runtime_caller)],
    exclude_meeting_id: Annotated[uuid.UUID | None, Query()] = None,
    exclude_decision_id: Annotated[uuid.UUID | None, Query()] = None,
) -> OpenAssumptions:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT a.id, a.decision_id, d.content AS decision_content, a.kind, "
                        "a.statement, a.quote, d.meeting_id FROM decision_assumptions a "
                        "JOIN decisions d ON d.id = a.decision_id AND d.deleted_at IS NULL "
                        "AND d.status = 'confirmed' "
                        "WHERE a.status = 'watching' "
                        "AND (CAST(:xm AS uuid) IS NULL OR d.meeting_id <> CAST(:xm AS uuid)) "
                        "AND (CAST(:xd AS uuid) IS NULL OR d.id <> CAST(:xd AS uuid)) "
                        "ORDER BY a.created_at DESC, a.id LIMIT :n"
                    ),
                    {
                        "xm": str(exclude_meeting_id) if exclude_meeting_id else None,
                        "xd": str(exclude_decision_id) if exclude_decision_id else None,
                        "n": MAX_OPEN,
                    },
                )
            )
            .mappings()
            .all()
        )
    return OpenAssumptions(items=[OpenAssumption(**dict(r)) for r in rows])


# --- workflow-service ------------------------------------------------------------------------


@router.get("/internal/assumptions/watch", response_model=WatchList)
async def watch_list(
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
) -> WatchList:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT a.id AS assumption_id, a.decision_id, a.kind, a.statement, "
                        "a.watch_jira_key AS jira_key, a.watch_deal_id AS deal_id, a.watch_date, "
                        "a.watch_value AS value, a.watch_comparator AS comparator, "
                        "a.watch_unit AS unit FROM decision_assumptions a JOIN decisions d "
                        "ON d.id = a.decision_id AND d.deleted_at IS NULL "
                        "AND d.status = 'confirmed' WHERE a.status = 'watching' "
                        "AND (a.watch_jira_key IS NOT NULL OR a.watch_deal_id IS NOT NULL) "
                        "ORDER BY a.last_checked_at NULLS FIRST, a.id LIMIT :n"
                    ),
                    {"n": MAX_WATCH},
                )
            )
            .mappings()
            .all()
        )
        domains = await watch_domains(session)
    return WatchList(
        items=[
            WatchItem(**{**dict(r), "value": float(r["value"]) if r["value"] is not None else None})
            for r in rows
        ],
        deal_domains=[
            DealDomains(decision_id=k, domains=sorted(set(v))[:20]) for k, v in domains.items()
        ],
    )


def _holds(amount: float, comparator: str, value: float) -> bool:
    return {
        "lt": amount < value,
        "lte": amount <= value,
        "gt": amount > value,
        "gte": amount >= value,
        "eq": abs(amount - value) < 0.005,
    }[comparator]


def _money(amount: float, unit: str | None) -> str:
    shown = f"{amount:,.0f}" if amount == int(amount) else f"{amount:,.2f}"
    return f"{shown} {unit}" if unit else shown


def jira_break(item: dict[str, Any], issue: Issue, as_of: date) -> tuple[str, str] | None:
    """(rationale, dedupe key) when the issue breaks the reason, else None."""
    done = (issue.status_category or "").lower() == "done"
    watch_date = item["watch_date"]
    if done or watch_date is None:
        return None
    status = issue.status or "open"
    if issue.due_date is not None and issue.due_date > watch_date:
        return (
            f"{issue.key} is now due {issue.due_date:%d %b %Y}, after {watch_date:%d %b %Y}.",
            f"jira:{issue.key}:due:{issue.due_date.isoformat()}",
        )
    if as_of > watch_date:
        return (
            f"{issue.key} is still {status} on {as_of:%d %b %Y}, past {watch_date:%d %b %Y}.",
            f"jira:{issue.key}:overdue:{watch_date.isoformat()}",
        )
    return None


def deal_break(item: dict[str, Any], deal: Deal) -> tuple[str, str] | None:
    name = deal.name or f"Deal {deal.id}"
    if deal.closed_lost:
        return (f"{name} was marked lost in HubSpot.", f"hubspot:{deal.id}:lost")
    value, comparator = item["value"], item["comparator"]
    if value is not None and comparator and deal.amount is not None:
        if not _holds(deal.amount, comparator, float(value)):
            return (
                f"{name} is now {_money(deal.amount, item['unit'])} in HubSpot.",
                f"hubspot:{deal.id}:amount:{deal.amount}",
            )
    watch_date = item["watch_date"]
    if watch_date is not None and deal.close_date is not None and not deal.closed_won:
        if deal.close_date > watch_date:
            return (
                f"{name} now closes {deal.close_date:%d %b %Y}, after {watch_date:%d %b %Y}.",
                f"hubspot:{deal.id}:close:{deal.close_date.isoformat()}",
            )
    return None


@router.post("/internal/assumptions/watch-results", response_model=WatchApplied)
async def watch_results(
    body: WatchResults,
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
) -> WatchApplied:
    issues = {i.key.upper(): i for i in body.issues}
    deals = {d.id: d for d in body.deals}
    broken = checked = linked = 0
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        await session.execute(
            text(
                "INSERT INTO watch_source_status (org_id, jira, linear, hubspot, checked_at) "
                "VALUES (:o, :j, :l, :h, now()) ON CONFLICT (org_id) DO UPDATE SET jira = :j, "
                "linear = :l, hubspot = :h, checked_at = now()"
            ),
            {
                "o": str(org_id),
                "j": body.sources.jira,
                "l": body.sources.linear,
                "h": body.sources.hubspot,
            },
        )
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT a.id, a.kind, a.watch_jira_key, a.watch_deal_id, a.watch_date, "
                        "a.watch_value AS value, a.watch_comparator AS comparator, "
                        "a.watch_unit AS unit FROM decision_assumptions a "
                        "WHERE a.status = 'watching' "
                        "AND (a.watch_jira_key = ANY(CAST(:keys AS text[])) "
                        "OR a.watch_deal_id = ANY(CAST(:deals AS text[])))"
                    ),
                    {"keys": list(issues), "deals": list(deals)},
                )
            )
            .mappings()
            .all()
        )
        for r in rows:
            item = dict(r)
            checked += 1
            found: tuple[str, str] | None = None
            evidence: dict[str, Any] = {}
            issue = issues.get(str(r["watch_jira_key"] or ""))
            if issue is not None and body.sources.jira == "connected":
                found = jira_break(item, issue, body.as_of)
                due = f", due {issue.due_date:%d %b %Y}" if issue.due_date else ""
                evidence = {
                    "source": "jira",
                    "source_ref": issue.key,
                    "source_url": issue.url,
                    "quote": why.clip(
                        f"{issue.key}: {issue.summary or 'Issue'} ({issue.status or 'open'}{due})",
                        600,
                    ),
                }
            deal = deals.get(str(r["watch_deal_id"] or ""))
            if found is None and deal is not None and body.sources.hubspot == "connected":
                found = deal_break(item, deal)
                parts = [deal.stage or "", _money(deal.amount, r["unit"]) if deal.amount else ""]
                evidence = {
                    "source": "hubspot",
                    "source_ref": deal.name or deal.id,
                    "source_url": deal.url,
                    "quote": why.clip(
                        f"{deal.name or 'Deal'}: " + ", ".join(p for p in parts if p), 600
                    ),
                }
            if found is not None:
                rationale, key = found
                made = await why.record_break(
                    session,
                    org_id,
                    r["id"],
                    rationale=rationale,
                    confidence=1.0,
                    dedupe_key=key,
                    **evidence,
                )
                broken += 1 if made else 0
        if rows:
            await session.execute(
                text(
                    "UPDATE decision_assumptions SET last_checked_at = now() "
                    "WHERE id = ANY(CAST(:ids AS uuid[]))"
                ),
                {"ids": [str(r["id"]) for r in rows]},
            )
        if body.sources.hubspot == "connected" and body.domain_deals:
            by_domain = {d.domain.lower(): d.deals for d in body.domain_deals}
            wanted = await watch_domains(session)
            for decision_id, domains in wanted.items():
                for domain in domains:
                    for deal in by_domain.get(domain, [])[:5]:
                        if deal.closed_won or deal.closed_lost:
                            continue
                        detail = {
                            "stage": deal.stage,
                            "amount": deal.amount,
                            "close_date": deal.close_date.isoformat() if deal.close_date else None,
                            "url": deal.url,
                            "domain": domain,
                        }
                        linked += int(
                            await why.link_deal(
                                session,
                                org_id,
                                decision_id,
                                deal_id=deal.id,
                                label=why.clip(deal.name or f"Deal with {domain}", 300),
                                detail=json.dumps(detail),
                            )
                        )
    return WatchApplied(checked=checked, broken=broken, deals_linked=linked)


async def watch_domains(session: Any) -> dict[uuid.UUID, list[str]]:
    rows = (
        await session.execute(
            text(
                "SELECT l.decision_id, array_agg(DISTINCT lower(split_part(g, '@', 2))) "
                "FROM decision_ripple_links l "
                "JOIN action_items a ON CAST(a.id AS text) = l.target_id "
                "JOIN meeting_followups f ON f.meeting_id = a.meeting_id "
                "CROSS JOIN LATERAL unnest(f.guest_emails) AS g "
                "WHERE l.target_kind = 'customer_action' "
                "AND l.created_at > now() - make_interval(days => :days) "
                "GROUP BY l.decision_id"
            ),
            {"days": DOMAIN_DAYS},
        )
    ).all()
    return {r[0]: [x for x in r[1] if x] for r in rows if r[1]}
