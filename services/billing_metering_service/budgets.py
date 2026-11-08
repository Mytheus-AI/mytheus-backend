"""Per-tenant token budgets: provisioning, the ok -> throttled -> blocked state machine, alerts,
and the Redis hot mirror the agent tier reads before every LLM call.

Plan 7-9 section 3.1 steps 6, 9 and 10. Semantics from mytheus-docs ADR-0029 D1:

- Allowances are per-tenant monthly pools: `token_quota = plans.included_agent_tokens`.
  `subscriptions.seats` never scales them.
- `hard_cap = token_quota` on Free (no throttle band: `ok` goes straight to `blocked`),
  `2 x token_quota` on Pro (the band between is Haiku-only `throttled`), and the tenant's own
  row on Enterprise (catalog value 0, set per contract).

The 80% and 95% alerts are FLAGS, not states: `state` stays `ok` through both. Each fires
once per period, independently, so one large charge that crosses both fires both.

Enforcement is a separate switch (`BUDGET_ENFORCEMENT_MODE`, `alert_only` by default, per
step 10). This module always records the true state in `agent_budgets` and in the mirror, and
the mirror carries the mode, so `alert_only` means the agent tier reads `blocked` and still
does not stop. Flipping to `enforce` changes no stored data.

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
PRO_HARD_CAP_FACTOR = 2  # ADR-0029 D1: a working value, not derived from the capacity model
ALERT_THRESHOLDS = (80, 95)
ENFORCEMENT_MODES = ("alert_only", "enforce")


def enforcement_mode() -> str:
    """`alert_only` unless explicitly set to `enforce`. An unknown value is refused, not
    guessed: a typo must not silently turn enforcement off or on."""
    mode = os.environ.get("BUDGET_ENFORCEMENT_MODE", "alert_only")
    if mode not in ENFORCEMENT_MODES:
        raise ValueError(f"BUDGET_ENFORCEMENT_MODE={mode!r}; expected one of {ENFORCEMENT_MODES}")
    return mode


def period_of(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m")


def period_end(period: str) -> datetime:
    year, month = (int(p) for p in period.split("-"))
    return datetime(year + (month == 12), 1 if month == 12 else month + 1, 1, tzinfo=UTC)


def budget_limits(plan_code: str, included_agent_tokens: int) -> tuple[int, int]:
    """`(token_quota, hard_cap)` for a newly provisioned period (ADR-0029 D1)."""
    quota = int(included_agent_tokens)
    if plan_code == "pro":
        return quota, quota * PRO_HARD_CAP_FACTOR
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
    alert_80_sent: bool
    alert_95_sent: bool

    @property
    def percent_used(self) -> float:
        if self.token_quota <= 0:
            return 100.0 if self.tokens_consumed > 0 else 0.0
        return round(100.0 * self.tokens_consumed / self.token_quota, 2)


_SELECT_PLAN = text(
    "SELECT p.code, p.included_agent_tokens, p.included_meeting_minutes "
    "FROM subscriptions s JOIN plans p ON p.id = s.plan_id"
)
_INSERT_DEFAULT_SUBSCRIPTION = text(
    "INSERT INTO subscriptions (org_id, plan_id, status) "
    "SELECT :org_id, id, 'active' FROM plans WHERE code = :code "
    "ORDER BY version DESC LIMIT 1 "
    "ON CONFLICT (org_id) DO NOTHING"
)
_SELECT_BUDGET = (
    "SELECT org_id, period, token_quota, tokens_consumed, hard_cap, state, "
    "alert_80_sent, alert_95_sent FROM agent_budgets WHERE period = :period"
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
    return dict(row)


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
        alert_80_sent=bool(row["alert_80_sent"]),
        alert_95_sent=bool(row["alert_95_sent"]),
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
        alert_80_sent=before.alert_80_sent or alerts[80],
        alert_95_sent=before.alert_95_sent or alerts[95],
    )
    await session.execute(
        text(
            "UPDATE agent_budgets SET tokens_consumed = :consumed, state = :state, "
            "alert_80_sent = :a80, alert_95_sent = :a95, updated_at = now() "
            "WHERE period = :period"
        ),
        {
            "consumed": consumed,
            "state": state,
            "a80": after.alert_80_sent,
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
    "budget_alert_80": (
        "AI budget 80% used",
        "Your organization has used 80% of this month's AI token allowance.",
    ),
    "budget_alert_95": (
        "AI budget 95% used",
        "Your organization has used 95% of this month's AI token allowance.",
    ),
    "budget_blocked": (
        "AI budget exhausted",
        "This month's AI token allowance is used up. Live notes and suggestions are paused; "
        "captions and transcription continue.",
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
                    "link": "/settings/usage",
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
