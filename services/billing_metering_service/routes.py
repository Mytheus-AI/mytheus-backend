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
from services.billing_metering_service import payg
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
    alert_75_sent: bool = False
    alert_90_sent: bool = False
    credit_tokens: int = 0


class DimensionOut(BaseModel):
    dimension: str
    quantity: float
    tier_breakdown: dict[str, float]
    updated_at: datetime


class PaygWindowOut(BaseModel):
    """The running Pay as you go window: usage so far, priced, against the deposit."""

    window_start: datetime
    window_end: datetime
    tokens: int
    cost_cents: int
    deposit_cents: int
    cents_per_million: int


class SettlementOut(BaseModel):
    id: str
    window_start: datetime
    window_end: datetime
    tokens: int
    cost_cents: int
    deposit_cents: int
    # > 0 owed by the workspace, < 0 refunded to it
    balance_cents: int
    status: str


class UsageOut(BaseModel):
    period: str
    plan: PlanOut
    budget: BudgetOut
    dimensions: list[DimensionOut]
    # When a bought month of Pro, Ultra or Pay as you go ends (it then lapses to Free).
    paid_until: datetime | None = None
    payg: PaygWindowOut | None = None
    balance_due: SettlementOut | None = None
    last_settlement: SettlementOut | None = None


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


def _settlement(row: dict[str, Any] | None) -> SettlementOut | None:
    if row is None:
        return None
    return SettlementOut(**{**row, "id": str(row["id"]), "status": str(row.get("status") or "due")})


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
    order = {"free": 0, "pro": 1, "ultra": 2, "payg": 3, "unlimited": 4, "enterprise": 5}
    return sorted((PlanOut(**r) for r in rows), key=lambda p: order.get(p.code, 99))


@router.get("/usage", response_model=UsageOut)
async def get_usage(
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> UsageOut:
    period = period_of(datetime.now(UTC))
    budget = await current_budget(session, ctx.org_id, period)
    tenant_plan = await plan_for_tenant(session, ctx.org_id)  # ensures the subscription exists
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
            alert_75_sent=budget.alert_75_sent,
            alert_90_sent=budget.alert_90_sent,
            credit_tokens=budget.credit_tokens,
        ),
        paid_until=tenant_plan.get("current_period_end"),
        payg=(
            PaygWindowOut(**window)
            if tenant_plan["code"] == "payg"
            and (
                window := await payg.window_view(
                    session,
                    tenant_plan.get("current_period_start"),
                    tenant_plan.get("current_period_end"),
                )
            )
            else None
        ),
        balance_due=_settlement(await payg.open_balance(session)),
        last_settlement=_settlement(await payg.last_settlement(session)),
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
