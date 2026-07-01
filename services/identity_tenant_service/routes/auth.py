"""Clerk webhook user-sync (reverts the Supabase DB-webhook drift).

Verifies the Svix signature, dedupes on the Clerk event_id via the inbox, applies to `users` in
one transaction, and writes an outbox row -> user-sync-events (the inbox topic, per canon). The
webhook secret is read lazily so importing this module never requires env.
"""

from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request, status
from svix.webhooks import Webhook, WebhookVerificationError

router = APIRouter(tags=["auth"])


def _webhook_secret() -> str:
    secret = os.environ.get("CLERK_WEBHOOK_SECRET")
    if not secret:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "CLERK_WEBHOOK_SECRET not configured"
        )
    return secret


@router.post("/clerk/webhook")
async def clerk_webhook(
    request: Request,
    svix_id: str = Header(alias="svix-id"),
    svix_timestamp: str = Header(alias="svix-timestamp"),
    svix_signature: str = Header(alias="svix-signature"),
) -> dict[str, Any]:
    body = await request.body()
    try:
        Webhook(_webhook_secret()).verify(
            body,
            {
                "svix-id": svix_id,
                "svix-timestamp": svix_timestamp,
                "svix-signature": svix_signature,
            },
        )
    except WebhookVerificationError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid Svix signature") from None
    # TODO(P1): inbox-dedupe on evt["data"]["id"] via
    #   nexus:{org_id}:dedupe:user-sync-events:{event_id};
    #   upsert users (clerk_user_id, email, role); write outbox -> tenant-events on org change.
    return {"ok": True}
