"""Clerk webhook user-sync. Plan section 3.5.

This is how a `users` row is born, updated and soft-deleted. It replaces both the Plan 0 `pass`
handler and the reverted Supabase `auth.users` trigger.

The order of operations is the security property, and it is not interchangeable:

1. **Verify the Svix signature FIRST**, on the raw body, before parsing anything. An unsigned
   request is spoofing, and parsing attacker-controlled JSON before authenticating it widens
   the attack surface for no benefit.
2. **Resolve the tenant** from the payload's Clerk organization id through the same
   `org_resolver` every other path uses. A webhook naming an organization we do not have is
   not an error to retry, it is a message for somebody else.
3. **Dedupe on the Svix message id**, which is stable across Clerk's retries. Redis first for
   speed, then the durable `inbox` table, which is the authority once the cache evicts.
4. **Apply and record in ONE transaction.** The `users` write and the `inbox` row commit
   together, so a crash between them cannot double-apply on redelivery.

Per-user changes write NO outbox row. There is no per-user propagation topic in canon
(ADR-0010 D4): seat counts and downstream state are derived, not pushed. Only org-lifecycle
changes produce `tenant-events`, and those are born in the provisioning path, not here.
"""

from __future__ import annotations

import json
import os
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Header, Request, status
from redis.asyncio import from_url
from sqlalchemy import text
from svix.webhooks import Webhook, WebhookVerificationError

from libs.shared.audit import ACTION_USER_SYNCED, audit
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.inbox import is_duplicate, record_consumed
from libs.shared.org_resolver import resolve_org_id
from libs.shared.tenancy import set_rls
from services.identity_tenant_service.provisioning import (
    ensure_member,
    remove_member,
    tier_for_clerk_role,
)

router = APIRouter(tags=["auth"])

CONSUMER_GROUP = "identity-tenant-service.clerk-webhook"
TOPIC = "user-sync-events"


def _webhook_secret() -> str:
    secret = os.environ.get("CLERK_WEBHOOK_SECRET")
    if not secret:
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "CLERK_WEBHOOK_SECRET not configured"
        )
    return secret


def _primary_email(data: dict[str, Any]) -> str:
    """Clerk sends every address plus a pointer at the primary one."""
    primary_id = data.get("primary_email_address_id")
    for entry in data.get("email_addresses") or []:
        if entry.get("id") == primary_id:
            return str(entry.get("email_address", ""))
    addresses = data.get("email_addresses") or []
    return str(addresses[0].get("email_address", "")) if addresses else ""


_MEMBERSHIP_EVENTS = (
    "organizationMembership.created",
    "organizationMembership.updated",
    "organizationMembership.deleted",
)


def _clerk_org_of(data: dict[str, Any]) -> str:
    """The organization this user event belongs to.

    Clerk puts it in `organization_memberships` on user events. A user in no organization is
    a personal account with no tenant to sync into, which is not an error.
    """
    for membership in data.get("organization_memberships") or []:
        org = membership.get("organization") or {}
        if org.get("id"):
            return str(org["id"])
    return ""


@router.post("/clerk/webhook", status_code=status.HTTP_200_OK)
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
        raise ProblemDetail(401, ErrorCode.AUTH_REQUIRED, "Invalid Svix signature") from None

    event = json.loads(body)
    event_type = str(event.get("type", ""))
    data = event.get("data") or {}

    clerk_org = (
        str((data.get("organization") or {}).get("id") or "")
        if event_type in _MEMBERSHIP_EVENTS
        else _clerk_org_of(data)
    )
    if not clerk_org:
        # A personal-account user with no organization. 200, because retrying will never
        # produce an organization and Clerk would redeliver forever.
        return {"status": "ignored", "reason": "no organization membership"}

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session, session.begin():
        org_id = await resolve_org_id(session, clerk_org)

    redis = from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"), decode_responses=False)
    try:
        # Dedupe on the SVIX MESSAGE id, not on the Clerk user id. The message id is what is
        # stable across Clerk's retries of one delivery; the user id is the same for every
        # event about that user, so keying on it would drop every legitimate later update.
        if await is_duplicate(redis, str(org_id), TOPIC, svix_id):
            return {"status": "duplicate"}
    finally:
        # `aclose` is the correct async close since redis 5.0.1; `close` is deprecated. The
        # bundled types-redis stubs predate the rename, hence the ignore rather than a
        # downgrade to the deprecated call.
        await redis.aclose()  # type: ignore[attr-defined]

    async with sessionmaker() as session, session.begin():
        await set_rls(session, org_id)

        already = (
            await session.execute(
                text(
                    "SELECT 1 FROM inbox WHERE consumer_group = :cg AND org_id = :o "
                    "AND idempotency_key = :k"
                ),
                {"cg": CONSUMER_GROUP, "o": str(org_id), "k": svix_id},
            )
        ).first()
        if already:
            # The durable authority, consulted when the Redis key has evicted or when two
            # deliveries race past the SET NX at the same moment.
            return {"status": "duplicate"}

        applied = await _apply(session, org_id, event_type, data)
        await record_consumed(session, CONSUMER_GROUP, str(org_id), svix_id)

        if applied:
            await audit(
                session,
                org_id=org_id,
                action=ACTION_USER_SYNCED,
                actor_type="system",
                target_type="user",
                target_id=data.get("id"),
                metadata={"event_type": event_type},
            )

    return {"status": "ok", "event_type": event_type}


async def _apply(session: Any, org_id: UUID, event_type: str, data: dict[str, Any]) -> bool:
    """Apply one Clerk event to the local mirror. Returns whether anything changed."""
    if event_type in _MEMBERSHIP_EVENTS:
        # Someone joined, changed role in, or left THIS organization. The user events below
        # only ever sync a user into their first organization, so without these a teammate
        # who accepts an invitation never becomes a member of the inviting tenant.
        member = data.get("public_user_data") or {}
        clerk_user = str(member.get("user_id") or "")
        if not clerk_user:
            return False
        if event_type == "organizationMembership.deleted":
            return await remove_member(session, org_id=org_id, clerk_user_id=clerk_user)
        await ensure_member(
            session,
            org_id=org_id,
            clerk_user_id=clerk_user,
            email=str(member.get("identifier") or ""),
            avatar_url=member.get("image_url"),
            tier=tier_for_clerk_role(str(data.get("role") or "")),
        )
        return True

    clerk_user_id = str(data.get("id", ""))
    if not clerk_user_id:
        return False

    if event_type in ("user.created", "user.updated"):
        await session.execute(
            text(
                "INSERT INTO users (org_id, clerk_user_id, email, avatar_url) "
                "VALUES (:org, :cuid, :email, :avatar) "
                # The conflict target is (org_id, clerk_user_id), which only became possible
                # once the global unique on clerk_user_id was dropped. One person can belong
                # to several organizations, and each membership is its own mirror row.
                "ON CONFLICT (org_id, clerk_user_id) DO UPDATE SET "
                "  email = EXCLUDED.email, "
                "  avatar_url = EXCLUDED.avatar_url, "
                "  updated_at = now(), "
                # An update for a previously soft-deleted user is a re-invite: clear the
                # tombstone rather than leaving them invisible with fresh data.
                "  deleted_at = NULL"
            ),
            {
                "org": str(org_id),
                "cuid": clerk_user_id,
                "email": _primary_email(data),
                "avatar": data.get("image_url"),
            },
        )
        return True

    if event_type == "user.deleted":
        # Soft delete. A hard delete would break `meetings.created_by` and every audit row
        # pointing at this actor; erasure is the RTBF saga's job, which is ordered and audited.
        await session.execute(
            text(
                "UPDATE users SET deleted_at = now(), updated_at = now() "
                "WHERE clerk_user_id = :cuid AND deleted_at IS NULL"
            ),
            {"cuid": clerk_user_id},
        )
        return True

    return False  # organization events and anything else are not this handler's business
