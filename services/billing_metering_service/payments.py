"""Razorpay checkout: a month of Pro or Ultra, a Pay as you go deposit, AI credits, or the
balance a Pay as you go window ran over its deposit.

Two calls from the web app, both workspace admin (RBAC `admin`):

1. `POST /billing/checkout` `{kind: "plan", plan_code}`, `{kind: "credits", packs}` or
   `{kind: "settlement", settlement_id}`: prices the purchase from the catalog or the recorded
   balance (never from the client), creates a Razorpay order with the server's
   key pair, records it in `payment_orders` (`created`), and returns what Razorpay Checkout needs
   in the browser: the order id, amount, currency and the PUBLIC key id.
2. `POST /billing/checkout/verify` `{order_id, payment_id, signature}`: checks Razorpay's
   signature, `HMAC_SHA256(order_id + "|" + payment_id, key_secret)`, against our own row for that
   order in this tenant. Only then is the order marked `paid` and the purchase applied, in the same
   transaction: a plan for 30 days (`subscriptions.current_period_end`; for Pay as you go that is
   the window its deposit covers, see `payg.py`), credits on this month's budget, or a balance
   marked paid. Verifying an order twice is a no-op that answers the same result.

After commit: the agent-tier Redis mirror is rewritten (so AI unblocks at once) and, for a plan
change, the superadmins get an email (`libs/shared/staff_mail.py`).

Prices. The catalog is in US dollars (`plans.seat_price_cents`, `CREDIT_PACK_CENTS`). Razorpay
charges in `RAZORPAY_CURRENCY` (default `INR`, which every Razorpay account accepts) at
`RAZORPAY_INR_PER_USD` (default 84); set `RAZORPAY_CURRENCY=USD` on an account with international
payments enabled to charge dollars directly. `RAZORPAY_KEY_ID` / `RAZORPAY_KEY_SECRET` unset means
checkout answers 409 PROVIDER_NOT_CONFIGURED and nothing is created.

`GET /billing/status` (any member) is the small read every screen needs to know whether AI is
paused: the plan code and name, the budget state and, for admins only, how much is used.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal

import httpx
from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, require_tier, tenant_session
from libs.shared.cache import get_redis
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.staff_mail import notify_staff_soon
from libs.shared.tenancy import set_rls
from services.billing_metering_service.budgets import (
    PAID_PLANS,
    BudgetSnapshot,
    add_credit_tokens,
    current_budget,
    period_of,
    plan_for_tenant,
    set_subscription_plan,
    sync_budget_to_plan,
    write_mirror,
)
from services.billing_metering_service.payg import open_balance

logger = logging.getLogger(__name__)

router = APIRouter(tags=["billing-checkout"])

RAZORPAY_ORDERS_URL = "https://api.razorpay.com/v1/orders"
CREDIT_PACK_TOKENS = 100_000
CREDIT_PACK_CENTS = 1_000  # $10 per 100,000 tokens
MAX_PACKS = 50
PLAN_DAYS = 30
PLAN_LABELS = {
    "free": "Free",
    "pro": "Pro",
    "ultra": "Ultra",
    "payg": "Pay as you go",
    "unlimited": "Unlimited",
}


def razorpay_keys() -> tuple[str, str] | None:
    key_id = os.environ.get("RAZORPAY_KEY_ID", "").strip()
    secret = os.environ.get("RAZORPAY_KEY_SECRET", "").strip()
    return (key_id, secret) if key_id and secret else None


def charge_currency() -> str:
    return (os.environ.get("RAZORPAY_CURRENCY", "INR").strip() or "INR").upper()


def to_minor(usd_cents: int) -> int:
    """US cents to the minor unit of the charge currency (paise for INR)."""
    if charge_currency() == "USD":
        return usd_cents
    try:
        rate = float(os.environ.get("RAZORPAY_INR_PER_USD", "84"))
    except ValueError:
        rate = 84.0
    return int(round(usd_cents * rate))


def expected_signature(order_id: str, payment_id: str, secret: str) -> str:
    return hmac.new(
        secret.encode(), f"{order_id}|{payment_id}".encode(), hashlib.sha256
    ).hexdigest()


class CheckoutRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["plan", "credits", "settlement"]
    plan_code: Literal["pro", "ultra", "payg"] | None = None
    packs: int | None = Field(default=None, ge=1, le=MAX_PACKS)
    settlement_id: uuid.UUID | None = None


class CheckoutOrder(BaseModel):
    order_id: str
    key_id: str
    amount: int  # minor units of `currency`
    currency: str
    usd_cents: int
    description: str


class VerifyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    order_id: str = Field(min_length=1, max_length=64)
    payment_id: str = Field(min_length=1, max_length=64)
    signature: str = Field(min_length=1, max_length=128)


class CheckoutResult(BaseModel):
    kind: Literal["plan", "credits", "settlement"]
    plan_code: str
    tokens_added: int
    token_quota: int
    state: str


class BillingStatus(BaseModel):
    plan_code: str
    plan_name: str
    state: str
    blocked: bool
    # Admins only; None for members (ADR-0029: usage figures are an admin read).
    percent_used: float | None = None
    tokens_consumed: int | None = None
    token_quota: int | None = None
    paid_until: datetime | None = None


async def _create_razorpay_order(
    keys: tuple[str, str], amount: int, currency: str, receipt: str, notes: dict[str, str]
) -> str:
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0, connect=5.0)) as client:
            resp = await client.post(
                RAZORPAY_ORDERS_URL,
                auth=keys,
                json={"amount": amount, "currency": currency, "receipt": receipt, "notes": notes},
            )
    except httpx.HTTPError as exc:
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "The payment provider could not be reached"
        ) from exc
    if resp.status_code >= 400:
        logger.warning("razorpay order create answered %s", resp.status_code)
        raise ProblemDetail(
            502, ErrorCode.DEPENDENCY_UNAVAILABLE, "The payment provider refused the order"
        )
    order_id = resp.json().get("id")
    if not isinstance(order_id, str) or not order_id:
        raise ProblemDetail(502, ErrorCode.DEPENDENCY_UNAVAILABLE, "No order id from provider")
    return order_id


@router.post("/billing/checkout", response_model=CheckoutOrder)
async def create_checkout(
    body: CheckoutRequest,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> CheckoutOrder:
    """authz: admin. Price the purchase server-side and open a Razorpay order for it."""
    keys = razorpay_keys()
    if keys is None:
        raise ProblemDetail(
            409, ErrorCode.PROVIDER_NOT_CONFIGURED, "Payments are not set up on this server"
        )
    settlement_id: str | None = None
    if body.kind != "settlement" and await open_balance(session) is not None:
        raise ProblemDetail(
            409,
            ErrorCode.VERSION_CONFLICT,
            "Pay the balance from the last Pay as you go month first",
        )
    if body.kind == "plan":
        if body.plan_code is None:
            raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "plan_code is required")
        price = (
            await session.execute(
                text(
                    "SELECT name, seat_price_cents FROM plans WHERE code = :c "
                    "ORDER BY version DESC LIMIT 1"
                ),
                {"c": body.plan_code},
            )
        ).one()
        usd_cents = int(price[1])
        description = (
            "Mytheus Pay as you go deposit, 30 days"
            if body.plan_code == "payg"
            else f"Mytheus {price[0]}, one month"
        )
        tokens = None
    elif body.kind == "settlement":
        due = await open_balance(session)
        if due is None or body.settlement_id is None or str(due["id"]) != str(body.settlement_id):
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        usd_cents = int(due["balance_cents"])
        settlement_id = str(due["id"])
        description = "Mytheus Pay as you go, usage over the deposit"
        tokens = None
    else:
        if body.packs is None:
            raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "packs is required")
        usd_cents = body.packs * CREDIT_PACK_CENTS
        tokens = body.packs * CREDIT_PACK_TOKENS
        description = f"Mytheus AI credits, {tokens:,} tokens"

    currency = charge_currency()
    amount = to_minor(usd_cents)
    receipt = f"mx-{uuid.uuid4().hex[:20]}"
    order_id = await _create_razorpay_order(
        keys,
        amount,
        currency,
        receipt,
        {"org_id": str(ctx.org_id), "kind": body.kind, "plan": body.plan_code or ""},
    )
    await session.execute(
        text(
            "INSERT INTO payment_orders (org_id, provider_order_id, kind, plan_code, tokens, "
            "settlement_id, amount_minor, currency, usd_cents, created_by) "
            "VALUES (:o, :oid, :k, :p, :t, :sid, :a, :c, :usd, :u)"
        ),
        {
            "o": str(ctx.org_id),
            "oid": order_id,
            "k": body.kind,
            "p": body.plan_code if body.kind == "plan" else None,
            "t": tokens,
            "sid": settlement_id,
            "a": amount,
            "c": currency,
            "usd": usd_cents,
            "u": str(ctx.user_id) if ctx.user_id else None,
        },
    )
    return CheckoutOrder(
        order_id=order_id,
        key_id=keys[0],
        amount=amount,
        currency=currency,
        usd_cents=usd_cents,
        description=description,
    )


async def _org_name(session: AsyncSession, org_id: uuid.UUID) -> str:
    # `organizations` carries no RLS (it is the tenant root): the id filter is what scopes it.
    name = (
        await session.execute(
            text("SELECT name FROM organizations WHERE id = :o"), {"o": str(org_id)}
        )
    ).scalar()
    return str(name or "a workspace")


@router.post("/billing/checkout/verify", response_model=CheckoutResult)
async def verify_checkout(
    body: VerifyRequest,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
) -> CheckoutResult:
    """authz: admin. Check Razorpay's signature for one of this tenant's orders, then apply it.

    Its own transaction rather than `tenant_session`, so the Redis mirror and the staff email
    happen strictly after the purchase has committed."""
    keys = razorpay_keys()
    if keys is None:
        raise ProblemDetail(
            409, ErrorCode.PROVIDER_NOT_CONFIGURED, "Payments are not set up on this server"
        )
    period = period_of(datetime.now(UTC))
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, ctx.org_id)
        order = (
            (
                await session.execute(
                    text(
                        "SELECT id, kind, plan_code, tokens, status, amount_minor, "
                        "currency, settlement_id "
                        "FROM payment_orders WHERE provider = 'razorpay' "
                        "AND provider_order_id = :o FOR UPDATE"
                    ),
                    {"o": body.order_id},
                )
            )
            .mappings()
            .first()
        )
        if order is None:  # unknown, or another tenant's (RLS): same answer
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        good = hmac.compare_digest(
            expected_signature(body.order_id, body.payment_id, keys[1]), body.signature
        )
        if good:
            first_time = order["status"] != "paid"
            snap: BudgetSnapshot
            if first_time:
                await session.execute(
                    text(
                        "UPDATE payment_orders SET status = 'paid', provider_payment_id = :p, "
                        "paid_at = now() WHERE id = :i"
                    ),
                    {"p": body.payment_id, "i": str(order["id"])},
                )
                if order["kind"] == "plan":
                    await set_subscription_plan(
                        session,
                        ctx.org_id,
                        str(order["plan_code"]),
                        paid_until=datetime.now(UTC) + timedelta(days=PLAN_DAYS),
                    )
                    snap = await sync_budget_to_plan(session, ctx.org_id, period)
                elif order["kind"] == "settlement":
                    await session.execute(
                        text(
                            "UPDATE payg_settlements SET status = 'paid', settled_at = now() "
                            "WHERE id = :i AND status = 'due'"
                        ),
                        {"i": str(order["settlement_id"])},
                    )
                    snap = await current_budget(session, ctx.org_id, period)
                else:
                    snap = await add_credit_tokens(
                        session, ctx.org_id, int(order["tokens"]), period
                    )
            else:
                snap = await current_budget(session, ctx.org_id, period)
            plan = await plan_for_tenant(session, ctx.org_id)
            org_name = await _org_name(session, ctx.org_id)
        elif order["status"] == "created":
            await session.execute(
                text("UPDATE payment_orders SET status = 'failed' WHERE id = :i"),
                {"i": str(order["id"])},
            )
    if not good:
        raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "The payment could not be verified")

    try:
        await write_mirror(get_redis(), snap)
    except Exception:  # noqa: BLE001 - the next charge or read-through republishes it
        logger.warning("budget mirror write after payment failed org=%s", ctx.org_id)
    if first_time:
        amount = f"{int(order['amount_minor']) / 100:,.2f} {order['currency']}"
        if order["kind"] == "plan":
            new_plan = PLAN_LABELS.get(str(order["plan_code"]), str(order["plan_code"]))
            notify_staff_soon(
                f"{org_name} moved to {new_plan}",
                [
                    ("Workspace", org_name),
                    ("New plan", new_plan),
                    ("Paid", amount),
                    ("Razorpay payment", body.payment_id),
                    ("Workspace id", str(ctx.org_id)),
                ],
                "Paid through Razorpay. The new allowance applies now and the plan runs 30 days.",
                kind="plan",
            )
        elif order["kind"] == "settlement":
            notify_staff_soon(
                f"{org_name} paid its Pay as you go balance",
                [
                    ("Workspace", org_name),
                    ("Paid", amount),
                    ("Razorpay payment", body.payment_id),
                ],
                kind="payment",
            )
        else:
            notify_staff_soon(
                f"{org_name} bought AI credits",
                [
                    ("Workspace", org_name),
                    ("Tokens", f"{int(order['tokens']):,}"),
                    ("Paid", amount),
                    ("Razorpay payment", body.payment_id),
                ],
                "Added to this month's allowance; they end with the month.",
                kind="credits",
            )
    return CheckoutResult(
        kind=order["kind"],
        plan_code=str(plan["code"]),
        tokens_added=int(order["tokens"] or 0) if order["kind"] == "credits" else 0,
        token_quota=snap.token_quota,
        state=snap.state,
    )


@router.get("/billing/status", response_model=BillingStatus)
async def billing_status(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> BillingStatus:
    """authz: member. Whether AI is paused for this workspace, and its plan; usage for admins."""
    period = period_of(datetime.now(UTC))
    snap = await current_budget(session, ctx.org_id, period)
    plan = await plan_for_tenant(session, ctx.org_id)
    name = (
        await session.execute(
            text("SELECT name FROM plans WHERE code = :c ORDER BY version DESC LIMIT 1"),
            {"c": plan["code"]},
        )
    ).scalar()
    if snap.state == "blocked":
        # The agent tier's gate reads the Redis mirror, which a migration or a lapse can leave
        # behind the row (it is only rewritten on the next charge or plan change). Republishing
        # here, where every member's page load already reads the row, keeps the gate and the
        # banner saying the same thing. Best effort: the banner answers either way.
        try:
            await write_mirror(get_redis(), snap)
        except Exception:  # noqa: BLE001 - Redis down must not fail the status read
            logger.warning("billing status: budget mirror republish failed org=%s", ctx.org_id)
    admin = ctx.rbac_tier in ("admin", "owner")
    return BillingStatus(
        plan_code=str(plan["code"]),
        plan_name=str(name or plan["code"]).strip(),
        state=snap.state,
        blocked=snap.state == "blocked",
        percent_used=snap.percent_used if admin else None,
        tokens_consumed=snap.tokens_consumed if admin else None,
        token_quota=snap.token_quota if admin else None,
        paid_until=plan.get("current_period_end") if admin and plan["code"] in PAID_PLANS else None,
    )
