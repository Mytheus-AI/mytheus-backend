"""Per-tenant token budgets: provisioning, the ok -> throttled -> blocked state machine, alerts,
and the Redis hot mirror the agent tier reads before every LLM call.

Plan 7-9 section 3.1 steps 6, 9 and 10. Semantics from mytheus-docs ADR-0029 D1:

- Allowances are per-tenant monthly pools: `token_quota = plans.included_agent_tokens` plus
  any credits bought this period (`credit_tokens`). `subscriptions.seats` never scales them.
- `hard_cap = token_quota` on every plan (migration a7c3e9f1b2d4): reaching the allowance stops
  AI work outright (`ok` goes straight to `blocked`); the way on is a credit top-up or a bigger
  plan. "Unlimited" is a cap of 10^15, too large to reach.

The 75%, 90% and 95% alerts are FLAGS, not states: `state` stays `ok` through all three. Each
fires once per period, independently, so one large charge that crosses several fires each.

Enforcement is a separate switch (`BUDGET_ENFORCEMENT_MODE`, `enforce` by default since the
paid tiers shipped; `alert_only` keeps the old record-but-allow behaviour). This module always
records the true state in `agent_budgets` and in the mirror, and the mirror carries the mode, so
`alert_only` means the agent tier reads `blocked` and still does not stop. Flipping the mode
changes no stored data.

Side effects of a crossing go through the transactional outbox, in the same transaction as the
`agent_budgets` update, so a crossing is never recorded without its event or vice versa:

- `billing-events` `quota_exceeded` when the tenant becomes `throttled` or `blocked`. The
  schema's `BillingEventType` has no alert or throttle symbol; `detail_json.state` says which.
- `notifications` (in-app) to every owner and admin for the 80% and 95% alerts and for
  `blocked`, which is what the plan's "alert fires" demo means a person actually sees.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.cache import redis_key
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue

PRODUCER = "billing-metering-service"
DEFAULT_PLAN_CODE = "free"
ALERT_THRESHOLDS = (75, 90, 95)
ENFORCEMENT_MODES = ("alert_only", "enforce")


def enforcement_mode() -> str:
    """`enforce` unless explicitly set to `alert_only`. An unknown value is refused, not
    guessed: a typo must not silently turn enforcement off or on."""
    mode = os.environ.get("BUDGET_ENFORCEMENT_MODE", "enforce")
    if mode not in ENFORCEMENT_MODES:
        raise ValueError(f"BUDGET_ENFORCEMENT_MODE={mode!r}; expected one of {ENFORCEMENT_MODES}")
    return mode


def period_of(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m")


def period_end(period: str) -> datetime:
    year, month = (int(p) for p in period.split("-"))
    return datetime(year + (month == 12), 1 if month == 12 else month + 1, 1, tzinfo=UTC)


def budget_limits(
    plan_code: str, included_agent_tokens: int, credit_tokens: int = 0
) -> tuple[int, int]:
    """`(token_quota, hard_cap)`: the plan's allowance plus credits, and a hard stop at it."""
    quota = int(included_agent_tokens) + int(credit_tokens)
    return quota, quota


def next_state(tokens_consumed: int, token_quota: int, hard_cap: int) -> str:
    if tokens_consumed >= hard_cap:
        return "blocked"
    if tokens_consumed >= token_quota:
        return "throttled"
    return "ok"


@dataclass(frozen=True)
class BudgetSnapshot:
    org_id: str
    period: str
    token_quota: int
    tokens_consumed: int
    hard_cap: int
    state: str
    alert_75_sent: bool
    alert_90_sent: bool
    alert_95_sent: bool
    credit_tokens: int = 0

    @property
    def alert_80_sent(self) -> bool:
        """Kept for the `/usage` contract: true once the 75% or a later alert has fired."""
        return self.alert_75_sent or self.alert_90_sent or self.alert_95_sent

    @property
    def percent_used(self) -> float:
        if self.token_quota <= 0:
            return 100.0 if self.tokens_consumed > 0 else 0.0
        return round(100.0 * self.tokens_consumed / self.token_quota, 2)


_SELECT_PLAN = text(
    "SELECT p.code, p.included_agent_tokens, p.included_meeting_minutes, "
    "s.current_period_start, s.current_period_end "
    "FROM subscriptions s JOIN plans p ON p.id = s.plan_id"
)
# Plans a workspace buys (and which lapse back to Free after their paid month) and every plan
# Mytheus staff may set from the ops console (no payment, no lapse).
PAID_PLANS = ("pro", "ultra", "payg")
STAFF_PLANS = ("free", "pro", "ultra", "payg", "unlimited")
_SET_PLAN = text(
    "INSERT INTO subscriptions (org_id, plan_id, status, current_period_start, current_period_end) "
    "SELECT :org_id, id, 'active', now(), :until FROM plans WHERE code = :code "
    "ORDER BY version DESC LIMIT 1 "
    "ON CONFLICT (org_id) DO UPDATE SET plan_id = EXCLUDED.plan_id, status = 'active', "
    "current_period_start = EXCLUDED.current_period_start, "
    "current_period_end = EXCLUDED.current_period_end, updated_at = now() "
    "RETURNING id"
)
_INSERT_DEFAULT_SUBSCRIPTION = text(
    "INSERT INTO subscriptions (org_id, plan_id, status) "
    "SELECT :org_id, id, 'active' FROM plans WHERE code = :code "
    "ORDER BY version DESC LIMIT 1 "
    "ON CONFLICT (org_id) DO NOTHING"
)
_SELECT_BUDGET = (
    "SELECT org_id, period, token_quota, tokens_consumed, hard_cap, state, "
    "alert_75_sent, alert_90_sent, alert_95_sent, credit_tokens "
    "FROM agent_budgets WHERE period = :period"
)


async def plan_for_tenant(session: AsyncSession, org_id: uuid.UUID) -> dict[str, Any]:
    """The tenant's plan, creating the default Free subscription on first use (step 9's lazy
    provisioning). The caller has already set `app.current_org`; RLS scopes both queries."""
    row = (await session.execute(_SELECT_PLAN)).mappings().first()
    if row is None:
        await session.execute(
            _INSERT_DEFAULT_SUBSCRIPTION, {"org_id": str(org_id), "code": DEFAULT_PLAN_CODE}
        )
        row = (await session.execute(_SELECT_PLAN)).mappings().one()
    end = row["current_period_end"]
    if row["code"] in PAID_PLANS and end is not None and end <= datetime.now(UTC):
        # The paid month is over and was not renewed: back to Free, and this month's allowance
        # with it. Lazy, like the rest of provisioning: the next read notices. A Pay as you go
        # window is priced and settled against its deposit first (`payg.py`).
        if row["code"] == "payg":
            from services.billing_metering_service.payg import settle_window

            await settle_window(session, org_id, row["current_period_start"], end)
        await set_subscription_plan(session, org_id, DEFAULT_PLAN_CODE, paid_until=None)
        await sync_budget_to_plan(session, org_id, period_of(datetime.now(UTC)))
        row = (await session.execute(_SELECT_PLAN)).mappings().one()
    return dict(row)


async def set_subscription_plan(
    session: AsyncSession, org_id: uuid.UUID, code: str, *, paid_until: datetime | None
) -> None:
    """Point the tenant's subscription at the newest version of `code`. Caller has set RLS and
    must follow with `sync_budget_to_plan` so this month's allowance moves with it."""
    done = (
        await session.execute(_SET_PLAN, {"org_id": str(org_id), "code": code, "until": paid_until})
    ).first()
    if done is None:
        raise ValueError(f"no plan with code {code!r}")


async def sync_budget_to_plan(
    session: AsyncSession, org_id: uuid.UUID, period: str
) -> BudgetSnapshot:
    """Recompute this period's allowance from the current plan plus credits bought, and the
    state that follows. An alert flag whose threshold is no longer crossed is cleared, so a
    workspace that upgrades is warned again when it nears the new allowance."""
    await ensure_budget(session, org_id, period)
    row = (
        (await session.execute(text(_SELECT_BUDGET + " FOR UPDATE"), {"period": period}))
        .mappings()
        .one()
    )
    before = _snapshot(row)
    plan = await plan_for_tenant(session, org_id)
    quota, cap = budget_limits(plan["code"], plan["included_agent_tokens"], before.credit_tokens)
    consumed = before.tokens_consumed
    state = next_state(consumed, quota, cap)
    flags = {pct: consumed >= quota * pct / 100 for pct in ALERT_THRESHOLDS}
    after = BudgetSnapshot(
        org_id=before.org_id,
        period=period,
        token_quota=quota,
        tokens_consumed=consumed,
        hard_cap=cap,
        state=state,
        alert_75_sent=before.alert_75_sent and flags[75],
        alert_90_sent=before.alert_90_sent and flags[90],
        alert_95_sent=before.alert_95_sent and flags[95],
        credit_tokens=before.credit_tokens,
    )
    await session.execute(
        text(
            "UPDATE agent_budgets SET token_quota = :quota, hard_cap = :cap, state = :state, "
            "alert_75_sent = :a75, alert_90_sent = :a90, alert_95_sent = :a95, "
            "updated_at = now() WHERE period = :period"
        ),
        {
            "quota": quota,
            "cap": cap,
            "state": state,
            "a75": after.alert_75_sent,
            "a90": after.alert_90_sent,
            "a95": after.alert_95_sent,
            "period": period,
        },
    )
    if state == "blocked" and before.state != "blocked":
        await _notify_admins(session, after, kind="budget_blocked", causation_id=None)
    return after


async def add_credit_tokens(
    session: AsyncSession, org_id: uuid.UUID, tokens: int, period: str
) -> BudgetSnapshot:
    """Add bought tokens on top of this period's plan allowance (they end with the period)."""
    await ensure_budget(session, org_id, period)
    await session.execute(
        text(
            "UPDATE agent_budgets SET credit_tokens = credit_tokens + :t, updated_at = now() "
            "WHERE period = :period"
        ),
        {"t": int(tokens), "period": period},
    )
    return await sync_budget_to_plan(session, org_id, period)


async def ensure_budget(session: AsyncSession, org_id: uuid.UUID, period: str) -> None:
    """Create this tenant's `agent_budgets` row for `period` if it does not exist yet."""
    exists = (
        await session.execute(text("SELECT 1 FROM agent_budgets WHERE period = :p"), {"p": period})
    ).first()
    if exists is not None:
        return
    plan = await plan_for_tenant(session, org_id)
    quota, cap = budget_limits(plan["code"], plan["included_agent_tokens"])
    await session.execute(
        text(
            "INSERT INTO agent_budgets (org_id, period, token_quota, hard_cap) "
            "VALUES (:org_id, :period, :quota, :cap) ON CONFLICT (org_id, period) DO NOTHING"
        ),
        {"org_id": str(org_id), "period": period, "quota": quota, "cap": cap},
    )


def _snapshot(row: Any) -> BudgetSnapshot:
    return BudgetSnapshot(
        org_id=str(row["org_id"]),
        period=row["period"],
        token_quota=int(row["token_quota"]),
        tokens_consumed=int(row["tokens_consumed"]),
        hard_cap=int(row["hard_cap"]),
        state=row["state"],
        alert_75_sent=bool(row["alert_75_sent"]),
        alert_90_sent=bool(row["alert_90_sent"]),
        alert_95_sent=bool(row["alert_95_sent"]),
        credit_tokens=int(row["credit_tokens"]),
    )


async def current_budget(session: AsyncSession, org_id: uuid.UUID, period: str) -> BudgetSnapshot:
    await ensure_budget(session, org_id, period)
    row = (await session.execute(text(_SELECT_BUDGET), {"period": period})).mappings().one()
    return _snapshot(row)


async def apply_tokens(
    session: AsyncSession,
    org_id: uuid.UUID,
    tokens: int,
    occurred_at: datetime,
    *,
    causation_id: str | None = None,
) -> BudgetSnapshot:
    """Add `tokens` to the tenant's budget for the period `occurred_at` falls in, and record
    every threshold it crosses. Must run inside the caller's transaction, after `set_rls`.

    `FOR UPDATE` serializes concurrent charges against one tenant-month, so two consumers
    cannot both read 79% and both skip the 80% alert, or both miss the cap.
    """
    period = period_of(occurred_at)
    await ensure_budget(session, org_id, period)
    row = (
        (await session.execute(text(_SELECT_BUDGET + " FOR UPDATE"), {"period": period}))
        .mappings()
        .one()
    )
    before = _snapshot(row)
    consumed = before.tokens_consumed + int(tokens)
    state = next_state(consumed, before.token_quota, before.hard_cap)
    alerts = {
        pct: (not getattr(before, f"alert_{pct}_sent"))
        and consumed >= before.token_quota * pct / 100
        for pct in ALERT_THRESHOLDS
    }
    after = BudgetSnapshot(
        org_id=before.org_id,
        period=period,
        token_quota=before.token_quota,
        tokens_consumed=consumed,
        hard_cap=before.hard_cap,
        state=state,
        alert_75_sent=before.alert_75_sent or alerts[75],
        alert_90_sent=before.alert_90_sent or alerts[90],
        alert_95_sent=before.alert_95_sent or alerts[95],
        credit_tokens=before.credit_tokens,
    )
    await session.execute(
        text(
            "UPDATE agent_budgets SET tokens_consumed = :consumed, state = :state, "
            "alert_75_sent = :a75, alert_90_sent = :a90, alert_95_sent = :a95, "
            "updated_at = now() WHERE period = :period"
        ),
        {
            "consumed": consumed,
            "state": state,
            "a75": after.alert_75_sent,
            "a90": after.alert_90_sent,
            "a95": after.alert_95_sent,
            "period": period,
        },
    )

    for pct, crossed in alerts.items():
        if crossed:
            await _notify_admins(
                session, after, kind=f"budget_alert_{pct}", causation_id=causation_id
            )
    if state != before.state and state in ("throttled", "blocked"):
        await _emit_quota_exceeded(session, after, causation_id=causation_id)
        if state == "blocked":
            await _notify_admins(session, after, kind="budget_blocked", causation_id=causation_id)
    return after


async def _emit_quota_exceeded(
    session: AsyncSession, snap: BudgetSnapshot, *, causation_id: str | None
) -> None:
    # One event per (tenant, period, state): the key makes a replayed crossing a no-op at the
    # outbox's UNIQUE (topic, idempotency_key).
    await enqueue(
        session,
        EventEnvelope(
            topic="billing-events",
            org_id=snap.org_id,
            producer=PRODUCER,
            idempotency_key=f"billing:{snap.org_id}:{snap.period}:{snap.state}",
            causation_id=causation_id,
            payload={
                "event_type": "quota_exceeded",
                "subscription_id": None,
                "detail_json": json.dumps({"kind": "agent_budget", **asdict(snap)}),
            },
        ),
    )


_MESSAGES = {
    "budget_alert_75": (
        "AI budget 75% used",
        "Your workspace has used 75% of this month's AI token allowance.",
    ),
    "budget_alert_90": (
        "AI budget 90% used",
        "Your workspace has used 90% of this month's AI token allowance. Buy credits or "
        "upgrade before it runs out.",
    ),
    "budget_alert_95": (
        "AI budget 95% used",
        "Your workspace has used 95% of this month's AI token allowance. AI features stop "
        "when it runs out.",
    ),
    "budget_blocked": (
        "AI budget used up",
        "This month's AI token allowance is used up, so AI features are paused for everyone. "
        "Buy credits or upgrade to turn them back on; captions and transcription continue.",
    ),
}


async def _notify_admins(
    session: AsyncSession, snap: BudgetSnapshot, *, kind: str, causation_id: str | None
) -> None:
    """One in-app notification per owner/admin. Reads `rbac_grants` (identity-owned) the same
    way `libs/shared/authz.py` does on every request; it writes nothing there."""
    admins = (
        await session.execute(
            text("SELECT user_id FROM rbac_grants WHERE tier IN ('owner','admin')")
        )
    ).scalars()
    title, body = _MESSAGES[kind]
    source_event_id = f"budget:{snap.org_id}:{snap.period}:{kind}"
    for user_id in admins:
        await enqueue(
            session,
            EventEnvelope(
                topic="notifications",
                org_id=snap.org_id,
                producer=PRODUCER,
                idempotency_key=f"notif:{source_event_id}:in_app:{user_id}",
                causation_id=causation_id,
                payload={
                    "source_event_id": source_event_id,
                    "channel": "in_app",
                    "user_id": str(user_id),
                    "priority": 1 if kind == "budget_blocked" else 2,
                    "title": title,
                    "body": body,
                    "link": "/workspace/billing",
                },
            ),
        )


def mirror_key(org_id: uuid.UUID | str, period: str) -> str:
    return redis_key(org_id, "budget", period)


async def write_mirror(redis: Redis[bytes], snap: BudgetSnapshot) -> None:
    """Publish the snapshot to `nexus:{org_id}:budget:{period}`. Call AFTER the transaction
    that produced it commits: a mirror written before a rolled-back commit would show tokens
    that were never recorded. The key expires at period end, so a new month starts cold and
    the agent tier's read-through provisions the new row."""
    key = mirror_key(snap.org_id, snap.period)
    await redis.hset(
        key,
        mapping={
            "tokens_consumed": snap.tokens_consumed,
            "token_quota": snap.token_quota,
            "hard_cap": snap.hard_cap,
            "state": snap.state,
            "enforcement": enforcement_mode(),
            "period": snap.period,
        },
    )
    await redis.expireat(key, int(period_end(snap.period).timestamp()))
