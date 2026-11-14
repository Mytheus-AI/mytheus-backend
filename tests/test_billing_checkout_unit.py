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
