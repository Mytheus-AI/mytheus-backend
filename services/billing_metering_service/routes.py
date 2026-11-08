"""`GET /usage` (tenant admin Usage console) and `GET /plans` (the catalog).

Plan 7-9 section 3.1 step 8. `/usage` reads the live `agent_budgets` row for the current
period (the number the agent tier is gated on) plus the nightly `usage_aggregates`; it never
scans the raw `usage_events` firehose. Stripe webhook and dunning land in their own band.

Reading `/usage` provisions the current month's budget row if it does not exist yet (step 9's
"lazy create on first use"), so an admin opening the console on the 1st sees this month's
allowance rather than an empty page.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, require_tier, tenant_session
from services.billing_metering_service.budgets import (
    current_budget,
    enforcement_mode,
    period_of,
    plan_for_tenant,
)

router = APIRouter(tags=["billing"])


class PlanOut(BaseModel):
    code: str
    name: str
    seat_price_cents: int
    included_meeting_minutes: int
    included_agent_tokens: int
    features: dict[str, Any]


class BudgetOut(BaseModel):
    token_quota: int
    tokens_consumed: int
    hard_cap: int
    state: str
    percent_used: float
    alert_80_sent: bool
    alert_95_sent: bool
    enforcement: str


class DimensionOut(BaseModel):
    dimension: str
    quantity: float
    tier_breakdown: dict[str, float]
    updated_at: datetime


class UsageOut(BaseModel):
    period: str
    plan: PlanOut
    budget: BudgetOut
    dimensions: list[DimensionOut]


_PLAN_FIELDS = (
    "code",
    "name",
    "seat_price_cents",
    "included_meeting_minutes",
    "included_agent_tokens",
    "features",
)
_PLAN_COLUMNS = ", ".join(_PLAN_FIELDS)
_TENANT_PLAN_COLUMNS = ", ".join(f"p.{f}" for f in _PLAN_FIELDS)


@router.get("/plans", response_model=list[PlanOut])
async def list_plans(
    _ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> list[PlanOut]:
    """The latest version of each plan. Global catalog; no tenant data."""
    rows = (
        await session.execute(
            text(
                f"SELECT DISTINCT ON (code) {_PLAN_COLUMNS} FROM plans "
                "ORDER BY code, version DESC"
            )
        )
    ).mappings()
    order = {"free": 0, "pro": 1, "enterprise": 2}
    return sorted((PlanOut(**r) for r in rows), key=lambda p: order.get(p.code, 99))


@router.get("/usage", response_model=UsageOut)
async def get_usage(
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> UsageOut:
    period = period_of(datetime.now(UTC))
    budget = await current_budget(session, ctx.org_id, period)
    await plan_for_tenant(session, ctx.org_id)  # ensures the subscription exists
    plan = (
        (
            await session.execute(
                text(
                    f"SELECT {_TENANT_PLAN_COLUMNS} "
                    "FROM subscriptions s JOIN plans p ON p.id = s.plan_id"
                )
            )
        )
        .mappings()
        .one()
    )
    dims = (
        await session.execute(
            text(
                "SELECT dimension, quantity, tier_breakdown, updated_at FROM usage_aggregates "
                "WHERE billing_period = :p ORDER BY dimension"
            ),
            {"p": period},
        )
    ).mappings()
    return UsageOut(
        period=period,
        plan=PlanOut(**plan),
        budget=BudgetOut(
            token_quota=budget.token_quota,
            tokens_consumed=budget.tokens_consumed,
            hard_cap=budget.hard_cap,
            state=budget.state,
            percent_used=budget.percent_used,
            alert_80_sent=budget.alert_80_sent,
            alert_95_sent=budget.alert_95_sent,
            enforcement=enforcement_mode(),
        ),
        dimensions=[
            DimensionOut(
                dimension=d["dimension"],
                quantity=float(d["quantity"]),
                tier_breakdown={k: float(v) for k, v in (d["tier_breakdown"] or {}).items()},
                updated_at=d["updated_at"],
            )
            for d in dims
        ],
    )
