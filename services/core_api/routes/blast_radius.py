"""Blast radius: what a confirmed decision touches, by distance (Beyond, Layer 1).

- `GET /api/v1/decisions/{id}/blast-radius`: three rings.
  1. "Work it changes": the actions the agent found it changes, blocks or unblocks
     (`decision_ripple_links`, from `decision_ripple@v1` and the graph's
     `actions_blocked_on_decision`), plus the open tasks its reversals touched (a decision it
     replaced, or its own reopening).
  2. "Goals and other teams": the company goals it advances or risks, and the confirmed
     decisions it may contradict (`decision_clashes`, the existing ProactiveAgent conflict
     check over the graph and the decision embeddings), with the settle thread when one exists.
  3. "Customers": actions from meetings with external guests, and HubSpot deals of those
     customers when HubSpot is connected (the watch schedule adds them).
- `GET /api/v1/meetings/{id}/blast-radius`: the one-line summary for each confirmed decision of
  a meeting (the room and the after page).

Visible to the participants of the decision's meeting (404 otherwise). An item from a meeting
the viewer was not in keeps its shape but loses its words (`redacted`): the viewer learns that
the decision touches something, never what another meeting said.
"""

from __future__ import annotations

import json
import uuid
from datetime import date
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from services.core_api import decision_why as why
from services.core_api.decision_why import Person
from services.core_api.routes.decision_why import sources

router = APIRouter(prefix="/api/v1", tags=["blast-radius"])

RippleKind = Literal["action", "goal", "decision", "customer_action", "deal"]
Effect = Literal["changes", "blocks", "unblocks", "advances", "risks", "contradicts", "commits"]
SourceState = Literal["connected", "not_connected", "unknown"]
RING_LABELS = {1: "Work it changes", 2: "Goals and other teams", 3: "Customers"}


class RippleItem(BaseModel):
    id: str
    kind: RippleKind
    ring: Literal[1, 2, 3]
    label: str | None = None
    effect: Effect
    why: str | None = None
    href: str | None = None
    redacted: bool = False
    owner: Person | None = None
    team: str | None = None
    status: str | None = None
    due: str | None = None
    settlement_id: uuid.UUID | None = None


class Ring(BaseModel):
    ring: Literal[1, 2, 3]
    label: str
    items: list[RippleItem]


class Counts(BaseModel):
    actions: int
    goals: int
    decisions: int
    customers: int


class Crm(BaseModel):
    hubspot: SourceState


class BlastRadius(BaseModel):
    decision_id: uuid.UUID
    meeting_id: uuid.UUID
    content: str
    state: Literal["pending", "ready"]
    summary: str
    counts: Counts
    rings: list[Ring]
    crm: Crm


class MeetingBlastItem(BaseModel):
    decision_id: uuid.UUID
    content: str
    state: Literal["pending", "ready"]
    summary: str
    counts: Counts


class MeetingBlast(BaseModel):
    items: list[MeetingBlastItem]


def _iso(d: Any) -> str | None:
    return d.isoformat() if isinstance(d, date) else (str(d) if d else None)


async def _action_items(
    session: AsyncSession, decision: dict[str, Any], viewer: uuid.UUID | None
) -> list[RippleItem]:
    rows = (
        (
            await session.execute(
                text(
                    "SELECT l.target_kind, l.effect, l.why, l.ring, a.id, a.content, a.status, "
                    "a.due_date, a.meeting_id, a.assignee_id FROM decision_ripple_links l "
                    "JOIN action_items a ON CAST(a.id AS text) = l.target_id "
                    "AND a.deleted_at IS NULL "
                    "WHERE l.decision_id = :d AND l.target_kind IN ('action', 'customer_action') "
                    "ORDER BY l.ring, l.confidence DESC, l.created_at"
                ),
                {"d": str(decision["id"])},
            )
        )
        .mappings()
        .all()
    )
    touched = (
        (
            await session.execute(
                text(
                    "SELECT DISTINCT ON (a.id) 'action' AS target_kind, 'changes' AS effect, "
                    "CASE WHEN r.kind = 'reopened' THEN 'Its owner was asked to keep, re-scope "
                    "or close it when this decision was reopened.' ELSE 'Its owner was asked to "
                    "keep, re-scope or close it when this decision replaced the one behind it.' "
                    "END AS why, 1 AS ring, a.id, a.content, a.status, a.due_date, a.meeting_id, "
                    "a.assignee_id FROM decision_reversals r JOIN action_items a "
                    "ON a.id = r.action_id AND a.deleted_at IS NULL "
                    "WHERE r.new_decision_id = :d ORDER BY a.id, r.created_at"
                ),
                {"d": str(decision["id"])},
            )
        )
        .mappings()
        .all()
    )
    names = await why.people(session, [r["assignee_id"] for r in [*rows, *touched]])
    seen_meeting: dict[Any, bool] = {}
    out: list[RippleItem] = []
    ids: set[uuid.UUID] = set()
    for r in [*rows, *touched]:
        if r["id"] in ids:
            continue
        ids.add(r["id"])
        m = r["meeting_id"]
        if m not in seen_meeting:
            seen_meeting[m] = await why.in_meeting(session, m, viewer)
        visible = seen_meeting[m] or (viewer is not None and r["assignee_id"] == viewer)
        customer = r["target_kind"] == "customer_action"
        out.append(
            RippleItem(
                id=str(r["id"]),
                kind="customer_action" if customer else "action",
                ring=3 if customer else 1,
                label=r["content"] if visible else None,
                effect=r["effect"],
                why=r["why"] if visible else None,
                href=(f"/meetings/{m}" if visible and m is not None else None),
                redacted=not visible,
                owner=names.get(r["assignee_id"]),
                status=r["status"],
                due=_iso(r["due_date"]),
            )
        )
    return out


async def _goal_and_deal_items(session: AsyncSession, decision_id: uuid.UUID) -> list[RippleItem]:
    rows = (
        (
            await session.execute(
                text(
                    "SELECT target_kind, target_id, label, effect, why, ring, detail "
                    "FROM decision_ripple_links WHERE decision_id = :d "
                    "AND target_kind IN ('goal', 'deal') ORDER BY ring, confidence DESC, label"
                ),
                {"d": str(decision_id)},
            )
        )
        .mappings()
        .all()
    )
    out: list[RippleItem] = []
    for r in rows:
        detail = r["detail"] if isinstance(r["detail"], dict) else json.loads(r["detail"] or "{}")
        deal = r["target_kind"] == "deal"
        out.append(
            RippleItem(
                id=r["target_id"],
                kind="deal" if deal else "goal",
                ring=3 if deal else 2,
                label=r["label"],
                effect=r["effect"],
                why=r["why"],
                href=(
                    detail.get("url")
                    if deal and str(detail.get("url", "")).startswith("https://")
                    else None
                ),
                status=detail.get("stage") if deal else None,
                due=detail.get("close_date") if deal else None,
            )
        )
    return out


async def _clash_items(
    session: AsyncSession, decision: dict[str, Any], viewer: uuid.UUID | None
) -> tuple[list[RippleItem], list[str]]:
    rows = (
        (
            await session.execute(
                text(
                    "SELECT c.id AS clash_id, c.rationale, c.cross_team, "
                    "CASE WHEN c.decision_id = :d THEN c.other_decision_id ELSE c.decision_id "
                    "END AS other_id, s.id AS settlement_id, s.owner_a, s.owner_b "
                    "FROM decision_clashes c LEFT JOIN decision_settlements s "
                    "ON s.clash_id = c.id WHERE c.decision_id = :d OR c.other_decision_id = :d "
                    "ORDER BY c.confidence DESC, c.created_at"
                ),
                {"d": str(decision["id"])},
            )
        )
        .mappings()
        .all()
    )
    out: list[RippleItem] = []
    teams: list[str] = []
    for r in rows:
        other = await why.decision_row(session, r["other_id"])
        if other is None or other["status"] not in ("confirmed", "superseded"):
            continue
        visible = await why.in_meeting(session, other["meeting_id"], viewer)
        team = await why.team_name(session, other["confirmed_by"])
        if r["cross_team"] and team:
            teams.append(team)
        settlement = r["settlement_id"]
        in_thread = viewer is not None and viewer in (r["owner_a"], r["owner_b"])
        out.append(
            RippleItem(
                id=str(other["id"]),
                kind="decision",
                ring=2,
                label=other["content"] if visible else None,
                effect="contradicts",
                why=(r["rationale"] or None) if visible else None,
                href=(
                    f"/work/settle/{settlement}"
                    if settlement and in_thread
                    else why.decision_link(other["meeting_id"], other["id"]) if visible else None
                ),
                redacted=not visible,
                owner=await why.person(session, other["confirmed_by"]),
                team=team,
                status=other["status"],
                settlement_id=settlement if in_thread else None,
            )
        )
    return out, teams


async def compute(
    session: AsyncSession, decision: dict[str, Any], viewer: uuid.UUID | None
) -> BlastRadius:
    actions = await _action_items(session, decision, viewer)
    others = await _goal_and_deal_items(session, decision["id"])
    clashes, teams = await _clash_items(session, decision, viewer)
    items = [*actions, *others, *clashes]
    rings = [
        Ring(ring=n, label=RING_LABELS[n], items=[i for i in items if i.ring == n])  # type: ignore[arg-type]
        for n in (1, 2, 3)
    ]
    counts = Counts(
        actions=sum(1 for i in items if i.kind == "action"),
        goals=sum(1 for i in items if i.kind == "goal"),
        decisions=sum(1 for i in items if i.kind == "decision"),
        customers=sum(1 for i in items if i.kind in ("customer_action", "deal")),
    )
    read = (
        await session.execute(
            text("SELECT read_at IS NOT NULL FROM decision_why_state WHERE decision_id = :d"),
            {"d": str(decision["id"])},
        )
    ).scalar()
    state: Literal["pending", "ready"] = "ready" if read or items else "pending"
    src = await sources(session)
    return BlastRadius(
        decision_id=decision["id"],
        meeting_id=decision["meeting_id"],
        content=decision["content"],
        state=state,
        summary=(
            why.summary_line(
                counts.actions, counts.goals, counts.decisions, counts.customers, teams
            )
            if state == "ready"
            else "Working out what this decision touches"
        ),
        counts=counts,
        rings=rings,
        crm=Crm(hubspot=src.hubspot),
    )


@router.get("/decisions/{decision_id}/blast-radius", response_model=BlastRadius)
async def decision_blast_radius(
    decision_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> BlastRadius:
    decision = await why.visible_decision(session, decision_id, ctx.user_id)
    return await compute(session, decision, ctx.user_id)


@router.get("/meetings/{meeting_id}/blast-radius", response_model=MeetingBlast)
async def meeting_blast_radius(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MeetingBlast:
    if not await why.in_meeting(session, meeting_id, ctx.user_id):
        raise not_found()
    ids = [
        r[0]
        for r in await session.execute(
            text(
                "SELECT id FROM decisions WHERE meeting_id = :m AND deleted_at IS NULL "
                "AND status IN ('confirmed', 'superseded') ORDER BY created_at, id LIMIT 100"
            ),
            {"m": str(meeting_id)},
        )
    ]
    items: list[MeetingBlastItem] = []
    for decision_id in ids:
        decision = await why.decision_row(session, decision_id)
        if decision is None:
            continue
        b = await compute(session, decision, ctx.user_id)
        items.append(
            MeetingBlastItem(
                decision_id=b.decision_id,
                content=b.content,
                state=b.state,
                summary=b.summary,
                counts=b.counts,
            )
        )
    return MeetingBlast(items=items)
