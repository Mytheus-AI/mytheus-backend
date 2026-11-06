"""`/rbac/grants`: list, grant and revoke the caller's tenant's `rbac_grants`.

`rbac_grants.tier` is the ONLY thing authorization reads (`libs/shared/authz.py`), so this module
is the write path for the security boundary itself. Every rule below is deny-by-default and is
implemented once, in `check_grant_change`, a pure function the route calls and the tests
exercise directly.

The rules, and why each one exists:

1. **`admin` or above to call at all** (`require_tier("admin")`). A member managing grants is
   self-service privilege escalation.
2. **No escalation beyond the caller's own tier.** An admin can grant `member` or `admin`,
   never `owner`. Without this, rule 1 collapses: any admin mints an owner.
3. **No acting on a peer or superior.** A non-owner may change only a grant strictly BELOW
   their own tier, so two admins cannot demote each other and an admin cannot touch an owner.
   An owner may change any grant, subject to rule 5.
4. **No changing your own grant.** Self-promotion is already blocked by rule 2; self-demotion
   and self-revocation are blocked so an operator cannot lock themselves out by accident. Another
   owner can always do it for them.
5. **The last owner cannot be demoted or revoked.** A tenant with no owner cannot be
   administered at all: nothing can ever grant `owner` again (rule 2). The owner count is read
   `FOR UPDATE`, so two concurrent demotions cannot both see "two owners" and leave zero.

Ordering mirrors `routes/users.py`: the target user's existence is checked FIRST, under RLS, so
a user id in another tenant is a 404 before any 403 could confirm anything about it.
"""

from __future__ import annotations

import base64
import binascii
import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, Response, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import ACTION_RBAC_GRANTED, ACTION_RBAC_REVOKED, audit
from libs.shared.authz import TIER_RANK, RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.identity_tenant_service.schemas import RbacGrant, RbacGrantPage, RbacGrantUpdate

router = APIRouter(tags=["rbac"])

DEFAULT_LIMIT = 50
MAX_LIMIT = 200

_GRANT_COLUMNS = "user_id, tier, granted_by, created_at"


def _rank(tier: str | None) -> int:
    return TIER_RANK.get(tier or "", 0)


def check_grant_change(
    *,
    caller_tier: str,
    caller_user_id: uuid.UUID | None,
    target_user_id: uuid.UUID,
    current_tier: str | None,
    new_tier: str | None,
    owner_count: int,
) -> None:
    """Raise `ProblemDetail` unless this grant change is permitted. `new_tier=None` = revoke.

    Pure: every input is already read, so the whole policy is unit-testable without a database.
    """
    caller_rank = _rank(caller_tier)
    if caller_rank < TIER_RANK["admin"]:
        # The route's gate already enforces this; restated so the function is safe on its own.
        raise ProblemDetail(403, ErrorCode.FORBIDDEN_ROLE, "Managing grants requires admin")
    if caller_user_id is not None and caller_user_id == target_user_id:
        raise ProblemDetail(403, ErrorCode.FORBIDDEN_ROLE, "You cannot change your own grant")
    if new_tier is not None and _rank(new_tier) > caller_rank:
        raise ProblemDetail(403, ErrorCode.FORBIDDEN_ROLE, "You cannot grant a tier above your own")
    if caller_tier != "owner" and _rank(current_tier) >= caller_rank:
        raise ProblemDetail(
            403,
            ErrorCode.FORBIDDEN_ROLE,
            "You can only change grants below your own tier",
        )
    demoting_owner = current_tier == "owner" and new_tier != "owner"
    if demoting_owner and owner_count <= 1:
        raise ProblemDetail(
            409, ErrorCode.VERSION_CONFLICT, "The last owner cannot be demoted or revoked"
        )


def _encode_cursor(user_id: uuid.UUID) -> str:
    return base64.urlsafe_b64encode(str(user_id).encode()).decode().rstrip("=")


def _decode_cursor(cursor: str) -> uuid.UUID:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        return uuid.UUID(base64.urlsafe_b64decode(padded.encode()).decode())
    except (ValueError, binascii.Error, UnicodeDecodeError):
        raise ProblemDetail(400, ErrorCode.INVALID_CURSOR, "Malformed cursor") from None


async def _require_user(session: AsyncSession, user_id: uuid.UUID) -> None:
    """404 unless the user exists in the caller's tenant. No `org_id` predicate: RLS scopes it."""
    row = (
        await session.execute(
            text("SELECT 1 FROM users WHERE id = :id AND deleted_at IS NULL"),
            {"id": str(user_id)},
        )
    ).first()
    if row is None:
        raise not_found()


async def _current_tier(session: AsyncSession, user_id: uuid.UUID) -> str | None:
    row = (
        await session.execute(
            text("SELECT tier FROM rbac_grants WHERE user_id = :u FOR UPDATE"),
            {"u": str(user_id)},
        )
    ).first()
    return str(row[0]) if row else None


async def _owner_count(session: AsyncSession) -> int:
    # FOR UPDATE on the owner rows themselves: a concurrent demotion blocks here until this
    # transaction commits, then re-reads the reduced count (rule 5).
    rows = (
        await session.execute(text("SELECT id FROM rbac_grants WHERE tier = 'owner' FOR UPDATE"))
    ).all()
    return len(rows)


def _row_to_grant(row: Any) -> RbacGrant:
    return RbacGrant(**dict(row))


@router.get("/rbac/grants", response_model=RbacGrantPage)
async def list_grants(
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    cursor: str | None = None,
) -> RbacGrantPage:
    """Keyset page of the caller's tenant's grants, by `user_id`. RLS scopes it to the tenant."""
    params: dict[str, Any] = {"limit": limit + 1}
    where = ""
    if cursor:
        where = "WHERE user_id > :after"
        params["after"] = str(_decode_cursor(cursor))
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_GRANT_COLUMNS} FROM rbac_grants {where} "
                    "ORDER BY user_id LIMIT :limit"
                ),
                params,
            )
        )
        .mappings()
        .all()
    )
    has_more = len(rows) > limit
    page = list(rows[:limit])
    return RbacGrantPage(
        items=[_row_to_grant(r) for r in page],
        next_cursor=_encode_cursor(page[-1]["user_id"]) if has_more and page else None,
    )


@router.put("/rbac/grants/{user_id}", response_model=RbacGrant)
async def set_grant(
    user_id: uuid.UUID,
    body: RbacGrantUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> RbacGrant:
    """Set `user_id`'s tier, creating the grant if absent. Idempotent in the value written."""
    await _require_user(session, user_id)
    current = await _current_tier(session, user_id)
    check_grant_change(
        caller_tier=ctx.rbac_tier,
        caller_user_id=ctx.user_id,
        target_user_id=user_id,
        current_tier=current,
        new_tier=body.tier,
        owner_count=await _owner_count(session) if current == "owner" else 0,
    )
    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO rbac_grants (org_id, user_id, tier, granted_by) "
                    "VALUES (:o, :u, :t, :by) "
                    "ON CONFLICT (org_id, user_id) DO UPDATE "
                    "SET tier = EXCLUDED.tier, granted_by = EXCLUDED.granted_by "
                    f"RETURNING {_GRANT_COLUMNS}"
                ),
                {
                    "o": str(ctx.org_id),
                    "u": str(user_id),
                    "t": body.tier,
                    "by": str(ctx.user_id) if ctx.user_id else None,
                },
            )
        )
        .mappings()
        .one()
    )
    if current != body.tier:
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=ctx.user_id,
            action=ACTION_RBAC_GRANTED,
            target_type="user",
            target_id=user_id,
            metadata={"from": current, "to": body.tier},
            ip=ctx.ip,
        )
    return _row_to_grant(row)


@router.delete("/rbac/grants/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_grant(
    user_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    """Remove `user_id`'s grant. A user with no grant is a 204 no-op (and writes no audit row).

    The user row is untouched: they stay a tenant member with no tier, which the authz chain
    treats as satisfying nothing.
    """
    await _require_user(session, user_id)
    current = await _current_tier(session, user_id)
    if current is None:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    check_grant_change(
        caller_tier=ctx.rbac_tier,
        caller_user_id=ctx.user_id,
        target_user_id=user_id,
        current_tier=current,
        new_tier=None,
        owner_count=await _owner_count(session) if current == "owner" else 0,
    )
    await session.execute(text("DELETE FROM rbac_grants WHERE user_id = :u"), {"u": str(user_id)})
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_RBAC_REVOKED,
        target_type="user",
        target_id=user_id,
        metadata={"from": current},
        ip=ctx.ip,
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
