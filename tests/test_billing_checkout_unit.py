"""Pure parts of Razorpay checkout and Pay as you go pricing (no database, no network)."""

from __future__ import annotations

import hashlib
import hmac

from services.billing_metering_service import payg, payments


def test_signature_is_hmac_of_order_and_payment():
    want = hmac.new(b"sek", b"order_1|pay_2", hashlib.sha256).hexdigest()
    assert payments.expected_signature("order_1", "pay_2", "sek") == want
    assert payments.expected_signature("order_1", "pay_3", "sek") != want


def test_charges_inr_by_default_and_usd_when_set(monkeypatch):
    monkeypatch.delenv("RAZORPAY_CURRENCY", raising=False)
    monkeypatch.setenv("RAZORPAY_INR_PER_USD", "84")
    assert payments.charge_currency() == "INR"
    assert payments.to_minor(20_000) == 1_680_000  # $200 -> 16,800.00 INR in paise
    monkeypatch.setenv("RAZORPAY_CURRENCY", "usd")
    assert payments.to_minor(20_000) == 20_000


def test_no_keys_means_not_configured(monkeypatch):
    monkeypatch.delenv("RAZORPAY_KEY_ID", raising=False)
    monkeypatch.setenv("RAZORPAY_KEY_SECRET", "x")
    assert payments.razorpay_keys() is None


def test_payg_is_priced_per_million_and_rounds_up():
    assert payg.payg_cost_cents(0) == 0
    assert payg.payg_cost_cents(5_000_000) == 50_000  # $500, exactly the deposit
    assert payg.payg_cost_cents(1) == 1


# --- GET /billing/status republishes a blocked budget's mirror --------------------------------


def _status_call(monkeypatch, state: str, write_mirror):
    import asyncio
    import uuid

    from libs.shared.authz import RequestContext
    from services.billing_metering_service.budgets import BudgetSnapshot

    org = uuid.uuid4()
    snap = BudgetSnapshot(str(org), "2026-11", 10_000, 12_000, 10_000, state, True, True, True)

    async def current_budget(_session, _org, _period):
        return snap

    async def plan_for_tenant(_session, _org):
        return {"code": "free"}

    class _Result:
        def scalar(self):
            return "Free"

    class _Session:
        async def execute(self, *_a, **_k):
            return _Result()

    monkeypatch.setattr(payments, "current_budget", current_budget)
    monkeypatch.setattr(payments, "plan_for_tenant", plan_for_tenant)
    monkeypatch.setattr(payments, "get_redis", lambda: object())
    monkeypatch.setattr(payments, "write_mirror", write_mirror)
    ctx = RequestContext(org_id=org, user_id=None, clerk_user_id="u", rbac_tier="member", ip=None)
    return asyncio.run(payments.billing_status(ctx, _Session())), snap  # type: ignore[arg-type]


def test_status_republishes_the_mirror_when_blocked(monkeypatch):
    """The agent tier's gate reads the Redis mirror; a stale one let Mytes answer while the
    banner said AI was paused. A blocked status read rewrites it from the row."""
    written = []

    async def write_mirror(_redis, s):
        written.append(s)

    status, snap = _status_call(monkeypatch, "blocked", write_mirror)
    assert status.blocked is True
    assert written == [snap]


def test_status_leaves_the_mirror_alone_when_not_blocked(monkeypatch):
    written = []

    async def write_mirror(_redis, s):
        written.append(s)

    status, _ = _status_call(monkeypatch, "ok", write_mirror)
    assert status.blocked is False
    assert written == []


def test_status_answers_when_redis_is_down(monkeypatch):
    async def write_mirror(_redis, _s):
        raise ConnectionError("redis down")

    status, _ = _status_call(monkeypatch, "blocked", write_mirror)
    assert status.blocked is True
