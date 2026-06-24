"""Supabase Auth webhook handler (replaces Clerk's /clerk/webhook).

Supabase triggers this via a Database Webhook configured in the Supabase Dashboard:
  Database → Webhooks → Create webhook on table auth.users, events: INSERT / UPDATE / DELETE
  URL: POST /supabase/webhook
  Secret: SUPABASE_WEBHOOK_SECRET (sent as X-Webhook-Secret header)

Note: A Postgres trigger (migrations/sql/user_sync_trigger.sql) also syncs
auth.users → public.users immediately on every row change, so this webhook
is a belt-and-suspenders path for cross-service notification rather than the
primary sync mechanism.
"""
from __future__ import annotations

import hmac
import os
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request, status

router = APIRouter(tags=["auth"])

_WEBHOOK_SECRET = os.environ.get("SUPABASE_WEBHOOK_SECRET", "")


@router.post("/supabase/webhook")
async def supabase_auth_webhook(
    request: Request,
    x_webhook_secret: str | None = Header(default=None),
) -> dict[str, Any]:
    """Receive Supabase DB webhook events for auth.users changes."""
    if not hmac.compare_digest(x_webhook_secret or "", _WEBHOOK_SECRET):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid webhook secret")

    payload = await request.json()
    event_type: str = payload.get("type", "")  # INSERT | UPDATE | DELETE
    record: dict = payload.get("record", {})
    old_record: dict = payload.get("old_record", {})

    if event_type in ("INSERT", "UPDATE"):
        await _handle_user_upsert(record)
    elif event_type == "DELETE":
        await _handle_user_delete(old_record)

    return {"ok": True}


async def _handle_user_upsert(record: dict) -> None:
    """Sync new/updated auth.users row into public.users and emit tenant-events if needed."""
    pass  # TODO: upsert public.users; if new org → write outbox row → tenant-events


async def _handle_user_delete(old_record: dict) -> None:
    """Soft-delete user from public.users on Supabase auth deletion."""
    pass  # TODO: soft-delete (deleted_at = now()); trigger RTBF workflow if needed
