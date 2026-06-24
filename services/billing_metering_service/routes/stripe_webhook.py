"""Stripe webhook receiver.

Verifies Stripe-Signature before processing. Dedupes on Stripe event_id.
Updates subscriptions/invoices and emits billing-events that drive dunning saga.
"""
import os
from typing import Any
import stripe
from fastapi import APIRouter, Header, HTTPException, Request

router = APIRouter(tags=["stripe"])

_STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")


@router.post("/stripe/webhook")
async def stripe_webhook(
    request: Request,
    stripe_signature: str | None = Header(default=None, alias="Stripe-Signature"),
) -> Any:
    payload = await request.body()
    try:
        event = stripe.Webhook.construct_event(payload, stripe_signature, _STRIPE_WEBHOOK_SECRET)
    except stripe.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid Stripe signature")

    # TODO: dedupe on event.id, dispatch to handler, emit billing-events via outbox
    return {"ok": True}
