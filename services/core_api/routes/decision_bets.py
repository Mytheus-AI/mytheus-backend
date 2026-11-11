"""Decisions as bets: the person-facing API.

- `GET  /api/v1/decisions/{id}/bet`            the decision's bet (or none), whether you may
                                                change it, owner and team choices, and hints
                                                from earlier calls of the same kind.
- `PUT  /api/v1/decisions/{id}/bet`            add or edit the expectation (a proposed one is
                                                accepted by saving it).
- `POST /api/v1/decisions/{id}/bet/accept`     accept the agent's proposal as it is.
- `POST /api/v1/decisions/{id}/bet/dismiss`    not a bet (proposed or open); undoable.
- `POST /api/v1/decisions/{id}/bet/restore`    undo a dismiss.
- `POST /api/v1/decisions/{id}/bet/resolve`    hit, partly or missed, with the lesson.
- `POST /api/v1/decisions/{id}/bet/reopen`     undo a resolution (back to due).
- `GET  /api/v1/bets/mine`                     bets you own that wait for you, then the rest.
- `GET  /api/v1/bets/calibration`              "How good are our calls": aggregates only.
- `GET  /api/v1/bets/hints?q=&category=`       lessons for a decision being written now.

Visibility is the decision's: participants of its meeting read it, anyone else gets a 404.
Changing it is for the bet's owner, the meeting's host or the decision's confirmer
(`bets.can_edit`). Every change is audited.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import audit
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api import bets
from services.core_api.records import display_name

router = APIRouter(prefix="/api/v1", tags=["decision-bets"])

ACTION_BET_SAVED = "decision_bet.saved"
ACTION_BET_ACCEPTED = "decision_bet.accepted"
ACTION_BET_DISMISSED = "decision_bet.dismissed"
ACTION_BET_RESTORED = "decision_bet.restored"
ACTION_BET_RESOLVED = "decision_bet.resolved"
ACTION_BET_REOPENED = "decision_bet.reopened"
MAX_MINE = 100


def _conflict(detail: str) -> ProblemDetail:
    return ProblemDetail(409, ErrorCode.VERSION_CONFLICT, detail)


def _forbidden() -> ProblemDetail:
    return ProblemDetail(
        403,
        ErrorCode.FORBIDDEN_ROLE,
        "Only the bet's owner, the meeting's host or the person who confirmed the decision "
        "can change it",
    )


async def _facts(
    session: AsyncSession, ctx: RequestContext, decision_id: uuid.UUID
) -> tuple[bets.DecisionFacts, uuid.UUID]:
    if ctx.user_id is None:
        raise not_found()
    facts = await bets.decision_facts(session, decision_id, ctx.user_id)
    if facts is None or not facts.is_participant or facts.status == "dismissed":
        raise not_found()
    return facts, ctx.user_id


async def _view(
    session: AsyncSession, facts: bets.DecisionFacts, viewer: uuid.UUID
) -> bets.DecisionBetView:
    row = await bets.bet_row_for_decision(session, facts.decision_id)
    bet = None
    owner = None
    if row is not None:
        owner = row["owner_user_id"]
        bet = bets.bet_from_row(row, await bets.evidence_for(session, row["id"], viewer))
    people = (
        (
            await session.execute(
                text(
                    "SELECT p.user_id, u.email FROM meeting_participants p JOIN users u "
                    "ON u.id = p.user_id AND u.deleted_at IS NULL WHERE p.meeting_id = :m "
                    "ORDER BY u.email, p.user_id"
                ),
                {"m": str(facts.meeting_id)},
            )
        )
        .mappings()
        .all()
    )
    teams = (
        (
            await session.execute(
                text("SELECT id, name FROM teams WHERE deleted_at IS NULL ORDER BY name, id")
            )
        )
        .mappings()
        .all()
    )
    hints = await bets.hints_for(
        session,
        category=bet.category if bet is not None else None,
        content=(bet.statement + " " + bet.metric) if bet is not None else facts.content,
        viewer=viewer,
        exclude_decision=facts.decision_id,
    )
    return bets.DecisionBetView(
        decision_id=facts.decision_id,
        meeting_id=facts.meeting_id,
        decision_status=facts.status,
        bet=bet,
        can_edit=bets.can_edit(facts, owner, viewer),
        owner_candidates=[
            bets.OwnerOption(user_id=p["user_id"], name=display_name(p["email"]) or "Teammate")
            for p in people
        ],
        teams=[bets.TeamOption(id=t["id"], name=t["name"]) for t in teams],
        hints=hints,
    )


async def _locked(session: AsyncSession, decision_id: uuid.UUID) -> Any:
    return (
        (
            await session.execute(
                text(
                    "SELECT id, status, owner_user_id, proposed_by, created_by FROM decision_bets "
                    "WHERE decision_id = :d AND deleted_at IS NULL FOR UPDATE"
                ),
                {"d": str(decision_id)},
            )
        )
        .mappings()
        .first()
    )


async def _audit(
    session: AsyncSession,
    ctx: RequestContext,
    action: str,
    bet_id: Any,
    facts: bets.DecisionFacts,
    **meta: Any,
) -> None:
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=action,
        target_type="decision_bet",
        target_id=bet_id,
        metadata={"decision_id": str(facts.decision_id), **meta},
        ip=ctx.ip,
    )


@router.get("/decisions/{decision_id}/bet", response_model=bets.DecisionBetView)
async def get_bet(
    decision_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> bets.DecisionBetView:
    facts, viewer = await _facts(session, ctx, decision_id)
    return await _view(session, facts, viewer)


@router.put("/decisions/{decision_id}/bet", response_model=bets.DecisionBetView)
async def save_bet(
    decision_id: uuid.UUID,
    body: bets.BetUpsert,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> bets.DecisionBetView:
    facts, viewer = await _facts(session, ctx, decision_id)
    existing = await _locked(session, decision_id)
    owner_now = existing["owner_user_id"] if existing else None
    if not bets.can_edit(facts, owner_now, viewer):
        raise _forbidden()
    if existing is not None and existing["status"] == "resolved":
        raise _conflict("A resolved bet cannot be edited; reopen it first")
    owner = body.owner_user_id or owner_now or viewer
    if body.owner_user_id is not None:
        is_participant = (
            await session.execute(
                text(
                    "SELECT 1 FROM meeting_participants p JOIN users u ON u.id = p.user_id "
                    "AND u.deleted_at IS NULL WHERE p.meeting_id = :m AND p.user_id = :u"
                ),
                {"m": str(facts.meeting_id), "u": str(body.owner_user_id)},
            )
        ).first()
        if is_participant is None:
            raise ProblemDetail(
                422, ErrorCode.VALIDATION_FAILED, "The owner must be someone from the meeting"
            )
    team = body.team_id
    if team is not None:
        if (
            await session.execute(
                text("SELECT 1 FROM teams WHERE id = :t AND deleted_at IS NULL"), {"t": str(team)}
            )
        ).first() is None:
            raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "No such team")
    else:
        team = await bets.first_team(session, owner)
    values = {
        "statement": body.statement,
        "kind": body.kind,
        "metric": body.metric,
        "direction": body.direction,
        "magnitude": body.magnitude,
        "unit": body.unit,
        "baseline": body.baseline,
        "check_date": body.check_date,
        "source": body.source,
        "metric_key": body.metric_key,
        "category": body.category,
        "confidence_pct": body.confidence_pct,
        "assumption": body.assumption,
        "owner": str(owner),
        "team": str(team) if team else None,
        "by": str(viewer),
    }
    if existing is None:
        bet_id = (
            await session.execute(
                text(
                    "INSERT INTO decision_bets (org_id, decision_id, meeting_id, owner_user_id, "
                    "team_id, status, statement, kind, metric, direction, magnitude, unit, "
                    "baseline, check_date, source, metric_key, category, confidence_pct, "
                    "assumption, proposed_by, created_by) VALUES (:o, :d, :m, :owner, :team, "
                    "'open', :statement, :kind, :metric, :direction, :magnitude, :unit, "
                    ":baseline, :check_date, :source, :metric_key, :category, :confidence_pct, "
                    ":assumption, 'person', :by) RETURNING id"
                ),
                {
                    **values,
                    "o": str(ctx.org_id),
                    "d": str(decision_id),
                    "m": str(facts.meeting_id),
                },
            )
        ).scalar_one()
        await _audit(session, ctx, ACTION_BET_SAVED, bet_id, facts, created=True)
    else:
        bet_id = existing["id"]
        # Editing a bet that was due or dismissed puts it back on the clock; a new check
        # date after a check re-runs the check on that date.
        await session.execute(
            text(
                "UPDATE decision_bets SET statement = :statement, kind = :kind, "
                "metric = :metric, direction = :direction, magnitude = :magnitude, unit = :unit, "
                "baseline = :baseline, source = :source, metric_key = :metric_key, "
                "category = :category, confidence_pct = :confidence_pct, "
                "assumption = :assumption, owner_user_id = :owner, team_id = :team, "
                "created_by = COALESCE(created_by, :by), "
                "status = CASE WHEN status = 'due' AND check_date = :check_date THEN 'due' "
                "  ELSE 'open' END, "
                "evidence_status = CASE WHEN check_date = :check_date THEN evidence_status "
                "  ELSE 'pending' END, "
                "check_date = :check_date, updated_at = now() WHERE id = :id"
            ),
            {**values, "id": str(bet_id)},
        )
        await _audit(
            session, ctx, ACTION_BET_SAVED, bet_id, facts, created=False, was=existing["status"]
        )
    return await _view(session, facts, viewer)


async def _transition(
    session: AsyncSession,
    ctx: RequestContext,
    decision_id: uuid.UUID,
    *,
    allowed: tuple[str, ...],
    sql: str,
    action: str,
    params: dict[str, Any] | None = None,
    idempotent_from: str | None = None,
) -> bets.DecisionBetView:
    facts, viewer = await _facts(session, ctx, decision_id)
    existing = await _locked(session, decision_id)
    if existing is None:
        raise not_found()
    if not bets.can_edit(facts, existing["owner_user_id"], viewer):
        raise _forbidden()
    if idempotent_from is not None and existing["status"] == idempotent_from:
        return await _view(session, facts, viewer)
    if existing["status"] not in allowed:
        raise _conflict(f"A {existing['status']} bet cannot do that")
    await session.execute(
        text(sql), {"id": str(existing["id"]), "by": str(viewer), **(params or {})}
    )
    await _audit(session, ctx, action, existing["id"], facts, was=existing["status"])
    return await _view(session, facts, viewer)


@router.post("/decisions/{decision_id}/bet/accept", response_model=bets.DecisionBetView)
async def accept_bet(
    decision_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> bets.DecisionBetView:
    return await _transition(
        session,
        ctx,
        decision_id,
        allowed=("proposed",),
        idempotent_from="open",
        sql=(
            "UPDATE decision_bets SET status = 'open', created_by = COALESCE(created_by, :by), "
            "owner_user_id = COALESCE(owner_user_id, :by), updated_at = now() WHERE id = :id"
        ),
        action=ACTION_BET_ACCEPTED,
    )


@router.post("/decisions/{decision_id}/bet/dismiss", response_model=bets.DecisionBetView)
async def dismiss_bet(
    decision_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> bets.DecisionBetView:
    return await _transition(
        session,
        ctx,
        decision_id,
        allowed=("proposed", "open", "due"),
        idempotent_from="dismissed",
        sql="UPDATE decision_bets SET status = 'dismissed', updated_at = now() WHERE id = :id",
        action=ACTION_BET_DISMISSED,
    )


@router.post("/decisions/{decision_id}/bet/restore", response_model=bets.DecisionBetView)
async def restore_bet(
    decision_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> bets.DecisionBetView:
    return await _transition(
        session,
        ctx,
        decision_id,
        allowed=("dismissed",),
        sql=(
            "UPDATE decision_bets SET status = CASE "
            "  WHEN proposed_by = 'agent' AND created_by IS NULL THEN 'proposed' "
            "  WHEN evidence_status = 'gathered' THEN 'due' ELSE 'open' END, "
            "updated_at = now() WHERE id = :id"
        ),
        action=ACTION_BET_RESTORED,
    )


@router.post("/decisions/{decision_id}/bet/resolve", response_model=bets.DecisionBetView)
async def resolve_bet(
    decision_id: uuid.UUID,
    body: bets.BetResolve,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> bets.DecisionBetView:
    """The person's call, always: hit, partly or missed. A resolved bet may be resolved again
    (a changed mind or a later lesson); `reopen` takes it back to due."""
    return await _transition(
        session,
        ctx,
        decision_id,
        allowed=("open", "due", "resolved"),
        sql=(
            "UPDATE decision_bets SET status = 'resolved', outcome = :outcome, "
            "assumption_held = :held, lesson = :lesson, resolved_by = :by, "
            "resolved_at = now(), updated_at = now() WHERE id = :id"
        ),
        params={"outcome": body.outcome, "held": body.assumption_held, "lesson": body.lesson},
        action=ACTION_BET_RESOLVED,
    )


@router.post("/decisions/{decision_id}/bet/reopen", response_model=bets.DecisionBetView)
async def reopen_bet(
    decision_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> bets.DecisionBetView:
    return await _transition(
        session,
        ctx,
        decision_id,
        allowed=("resolved",),
        sql=(
            "UPDATE decision_bets SET status = CASE WHEN evidence_status = 'gathered' "
            "  OR check_date <= current_date THEN 'due' ELSE 'open' END, outcome = NULL, "
            "resolved_by = NULL, resolved_at = NULL, updated_at = now() WHERE id = :id"
        ),
        action=ACTION_BET_REOPENED,
    )


# --- lists ----------------------------------------------------------------------------------


class MyBet(BaseModel):
    bet: bets.Bet
    decision_content: str
    meeting_title: str | None = None
    needs_you: bool


class MyBets(BaseModel):
    items: list[MyBet]


@router.get("/bets/mine", response_model=MyBets)
async def my_bets(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MyBets:
    """Bets you own: the ones waiting for you (proposed, due) first, then open, then the
    latest resolved. Private to you: nobody else's bets are listed."""
    if ctx.user_id is None:
        return MyBets(items=[])
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {bets.BET_COLS}, d.content AS decision_content, "  # noqa: S608
                    "m.title AS meeting_title FROM decision_bets b "
                    "JOIN decisions d ON d.id = b.decision_id AND d.deleted_at IS NULL "
                    "JOIN meetings m ON m.id = b.meeting_id AND m.deleted_at IS NULL "
                    "WHERE b.deleted_at IS NULL AND b.owner_user_id = :u "
                    "AND b.status <> 'dismissed' AND d.status <> 'dismissed' "
                    "AND EXISTS (SELECT 1 FROM meeting_participants p "
                    "  WHERE p.meeting_id = b.meeting_id AND p.user_id = :u) "
                    "ORDER BY CASE b.status WHEN 'due' THEN 0 WHEN 'proposed' THEN 1 "
                    "WHEN 'open' THEN 2 ELSE 3 END, "
                    "CASE WHEN b.status = 'resolved' THEN NULL ELSE b.check_date END, "
                    "b.resolved_at DESC NULLS LAST, b.id LIMIT :lim"
                ),
                {"u": str(ctx.user_id), "lim": MAX_MINE},
            )
        )
        .mappings()
        .all()
    )
    return MyBets(
        items=[
            MyBet(
                bet=bets.bet_from_row(r),
                decision_content=r["decision_content"],
                meeting_title=r["meeting_title"],
                needs_you=r["status"] in ("due", "proposed"),
            )
            for r in rows
        ]
    )


@router.get("/bets/calibration", response_model=bets.Calibration)
async def calibration(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> bets.Calibration:
    """How good are our calls: the workspace's record by decision type and by team. Aggregate
    only; a team appears once it has at least three resolved bets."""
    return await bets.calibration(session)


@router.get("/bets/hints", response_model=bets.BetHints)
async def hints(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    q: Annotated[str, Query(min_length=1, max_length=500)],
    category: Annotated[bets.Category | None, Query()] = None,
) -> bets.BetHints:
    if ctx.user_id is None:
        return bets.BetHints(category=category)
    return await bets.hints_for(session, category=category, content=q, viewer=ctx.user_id)
