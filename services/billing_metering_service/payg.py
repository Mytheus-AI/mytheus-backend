"""Pay as you go: a $500 deposit, metered usage, settled when its 30-day window ends.

A workspace on `payg` has no token cap. Its window is the subscription's
`current_period_start .. current_period_end`, set when the deposit was paid. Usage in the window
is the sum of its `usage_events` `agent_token` rows, priced at `PAYG_CENTS_PER_MILLION`.

When the window ends (noticed lazily by `plan_for_tenant`, and by the nightly job):

- the usage is priced and one `payg_settlements` row is written for that deposit;
- cost below the deposit: `refund_pending`, then `refund_remainder` asks Razorpay to refund the
  difference (`refunded`, or `refund_failed` to retry from the nightly job);
- cost above the deposit: `due`, and the admin pays the balance from Usage and billing (a
  Razorpay checkout of kind `settlement`, which marks it `paid`);
- exactly the deposit: `settled`;
- the workspace goes back to Free. Paying a new deposit starts a new window.

A Pay as you go plan set by Mytheus staff has no window (`current_period_end` NULL) and is never
settled: it is for testing.
"""

from __future__ import annotations

import logging
import math
import os
import uuid
from datetime import UTC, datetime
from typing import Any

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

PAYG_CODE = "payg"
PAYG_CENTS_PER_MILLION = 10_000  # $100 per 1,000,000 tokens
RAZORPAY_REFUND_URL = "https://api.razorpay.com/v1/payments/{payment_id}/refund"


def payg_cost_cents(tokens: int) -> int:
    return math.ceil(max(0, tokens) * PAYG_CENTS_PER_MILLION / 1_000_000)


async def window_tokens(session: AsyncSession, start: datetime, end: datetime) -> int:
    """Agent tokens metered in `[start, end)`. Caller has set RLS."""
    return int(
        (
            await session.execute(
                text(
                    "SELECT COALESCE(sum(quantity), 0) FROM usage_events "
                    "WHERE event_type = 'agent_token' AND occurred_at >= :s AND occurred_at < :e"
                ),
                {"s": start, "e": end},
            )
        ).scalar_one()
    )


async def latest_deposit(session: AsyncSession) -> dict[str, Any] | None:
    row = (
        (
            await session.execute(
                text(
                    "SELECT id, provider_payment_id, amount_minor, currency, usd_cents, paid_at "
                    "FROM payment_orders WHERE kind = 'plan' AND plan_code = 'payg' "
                    "AND status = 'paid' ORDER BY paid_at DESC LIMIT 1"
                )
            )
        )
        .mappings()
        .first()
    )
    return dict(row) if row else None


async def window_view(
    session: AsyncSession, start: datetime | None, end: datetime | None
) -> dict[str, Any] | None:
    """The running window for `/usage`: tokens and cost so far against the deposit."""
    if start is None or end is None:
        return None
    deposit = await latest_deposit(session)
    tokens = await window_tokens(session, start, min(end, datetime.now(UTC)))
    return {
        "window_start": start,
        "window_end": end,
        "tokens": tokens,
        "cost_cents": payg_cost_cents(tokens),
        "deposit_cents": int(deposit["usd_cents"]) if deposit else 0,
        "cents_per_million": PAYG_CENTS_PER_MILLION,
    }


async def settle_window(
    session: AsyncSession, org_id: uuid.UUID, start: datetime | None, end: datetime
) -> str | None:
    """Write the settlement for the window that ended at `end`. Returns its status, or None when
    there is no deposit to settle (a staff-set plan). Idempotent per deposit."""
    deposit = await latest_deposit(session)
    if deposit is None:
        return None
    window_start = start or deposit["paid_at"]
    tokens = await window_tokens(session, window_start, end)
    cost = payg_cost_cents(tokens)
    paid = int(deposit["usd_cents"])
    balance = cost - paid
    status = "due" if balance > 0 else "refund_pending" if balance < 0 else "settled"
    await session.execute(
        text(
            "INSERT INTO payg_settlements (org_id, deposit_order_id, window_start, window_end, "
            "tokens, cost_cents, deposit_cents, balance_cents, status, settled_at) "
            "VALUES (:o, :d, :s, :e, :t, :c, :p, :b, :st, "
            "CASE WHEN :st = 'settled' THEN now() END) "
            "ON CONFLICT (deposit_order_id) DO NOTHING"
        ),
        {
            "o": str(org_id),
            "d": str(deposit["id"]),
            "s": window_start,
            "e": end,
            "t": tokens,
            "c": cost,
            "p": paid,
            "b": balance,
            "st": status,
        },
    )
    return status


async def refund_remainder(session: AsyncSession) -> int:
    """Ask Razorpay to refund every `refund_pending` / `refund_failed` settlement of the tenant
    in `app.current_org`. Returns how many were refunded. Network calls: run it outside a request
    transaction (the nightly job, or after a request has committed)."""
    key_id = os.environ.get("RAZORPAY_KEY_ID", "").strip()
    secret = os.environ.get("RAZORPAY_KEY_SECRET", "").strip()
    if not key_id or not secret:
        return 0
    rows = (
        (
            await session.execute(
                text(
                    "SELECT s.id, s.balance_cents, s.deposit_cents, o.provider_payment_id, "
                    "o.amount_minor FROM payg_settlements s "
                    "JOIN payment_orders o ON o.id = s.deposit_order_id "
                    "WHERE s.status IN ('refund_pending','refund_failed')"
                )
            )
        )
        .mappings()
        .all()
    )
    done = 0
    for row in rows:
        refund_cents = -int(row["balance_cents"])
        # The refund is in the currency the deposit was charged in, pro rata.
        minor = int(row["amount_minor"]) * refund_cents // max(1, int(row["deposit_cents"]))
        status, refund_id = "refund_failed", None
        if minor > 0 and row["provider_payment_id"]:
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(15.0, connect=5.0)) as client:
                    resp = await client.post(
                        RAZORPAY_REFUND_URL.format(payment_id=row["provider_payment_id"]),
                        auth=(key_id, secret),
                        json={"amount": minor, "notes": {"reason": "Pay as you go remainder"}},
                    )
                if resp.status_code < 400:
                    status, refund_id = "refunded", str(resp.json().get("id") or "")
                else:
                    logger.warning("razorpay refund answered %s", resp.status_code)
            except httpx.HTTPError:
                logger.warning("razorpay refund could not be reached", exc_info=True)
        await session.execute(
            text(
                "UPDATE payg_settlements SET status = :s, refund_id = :r, "
                "settled_at = CASE WHEN :s = 'refunded' THEN now() END WHERE id = :i"
            ),
            {"s": status, "r": refund_id, "i": str(row["id"])},
        )
        done += status == "refunded"
    return done


async def open_balance(session: AsyncSession) -> dict[str, Any] | None:
    """The oldest unpaid balance of the tenant in `app.current_org`, for Usage and billing."""
    row = (
        (
            await session.execute(
                text(
                    "SELECT id, window_start, window_end, tokens, cost_cents, deposit_cents, "
                    "balance_cents FROM payg_settlements WHERE status = 'due' "
                    "ORDER BY created_at LIMIT 1"
                )
            )
        )
        .mappings()
        .first()
    )
    return dict(row) if row else None


async def last_settlement(session: AsyncSession) -> dict[str, Any] | None:
    row = (
        (
            await session.execute(
                text(
                    "SELECT id, window_start, window_end, tokens, cost_cents, deposit_cents, "
                    "balance_cents, status FROM payg_settlements ORDER BY created_at DESC LIMIT 1"
                )
            )
        )
        .mappings()
        .first()
    )
    return dict(row) if row else None
