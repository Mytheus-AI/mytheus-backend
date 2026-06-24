"""Provider webhook receivers.

Each provider webhook MUST be signature-verified before any work:
verify → dedupe on provider event_id → normalize → return 2xx fast.
"""
from typing import Any
from fastapi import APIRouter, Request

router = APIRouter(tags=["webhooks"])


@router.post("/{provider}")
async def receive_webhook(provider: str, request: Request) -> Any:
    # TODO: verify per-provider HMAC/token, dedupe, emit integration-events
    payload = await request.json()
    return {"ok": True}
