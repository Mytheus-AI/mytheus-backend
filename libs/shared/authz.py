"""Tenant context, the RLS-bound session, and the deny-by-default authorization gate.

Plan section 3.8 step 3. Mounted per route by every service, because the gateway is a DDoS
shield and not a trust delegation: the service tier re-verifies the token itself and makes its
own permit/deny decision.

The chain, in order, and the order is the point:

1. `verify_token`      the Clerk JWT is valid and carries an organization claim.
2. `resolve_org_id`    that opaque claim maps to a live, non-suspended tenant root.
3. `tenant_session`    `app.current_org` is set TRANSACTION-LOCALLY, so RLS now applies.
4. `require_tier`      the caller's `rbac_grants.tier` permits this route.

Every step fails closed, and steps 3 and 4 are belt and braces for each other. RLS alone would
let a `member` call an admin route and simply see their own tenant's data; `require_tier` alone
would be one forgotten decorator away from a cross-tenant read. Together, a route that forgets
the tier check still cannot cross a tenant boundary, and a route that somehow escapes RLS still
cannot be called by the wrong tier.

`users.role` is NEVER consulted for permissions (resolved decision, plan section 8 item 2).
Authorization reads `rbac_grants.tier` and nothing else.
"""

from __future__ import annotations

import ipaddress
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from fastapi import Depends, Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared import pat
from libs.shared.audit import ACTION_AUTHZ_DENY, audit
from libs.shared.auth import clerk_org_id_from, user_id_from, verify_token
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.org_resolver import resolve_org_id
from libs.shared.tenancy import set_org_id, set_rls

# Ordered, so "admin or above" is expressible without enumerating tiers at every call site.
# An unknown or absent tier ranks below `member` and therefore satisfies nothing.
TIER_RANK: dict[str, int] = {"member": 1, "admin": 2, "owner": 3}


@dataclass(frozen=True)
class RequestContext:
    """Everything downstream is allowed to know about who is calling.

    Frozen on purpose: a handler that could mutate `org_id` mid-request is a handler that can
    be made to write into another tenant.
    """

    org_id: UUID
    user_id: UUID | None  # our users.id; None if the Clerk user has no local mirror row yet
    clerk_user_id: str
    rbac_tier: str  # "" when the user holds no grant, which satisfies no tier
    ip: str | None
    # How the caller authenticated: a Clerk session, or a personal access token (Phase 4).
    # A token resolves to its owner's context, so every permission check is the owner's.
    auth_method: str = "session"
    token_id: UUID | None = None
    token_scope: str | None = None  # "read" | "read_write" for a token, None for a session

    @property
    def can_write(self) -> bool:
        """False only for a read-only API token. Sessions and read_write tokens may write."""
        return self.token_scope != "read"

    def satisfies(self, minimum: str) -> bool:
        return TIER_RANK.get(self.rbac_tier, 0) >= TIER_RANK[minimum]


_SELECT_PRINCIPAL = text(
    "SELECT u.id, g.tier FROM users u "
    "LEFT JOIN rbac_grants g ON g.user_id = u.id AND g.org_id = u.org_id "
    "WHERE u.clerk_user_id = :cuid AND u.deleted_at IS NULL"
)


def _client_ip(request: Request) -> str | None:
    """The audit trail's `ip`, validated as an actual IP address or dropped.

    `audit_logs.ip` is `inet`, so a value Postgres cannot parse raises inside the audit write
    - and audited operations fail closed, which means the operation being audited fails too.
    A caller-supplied `X-Forwarded-For` of `not-an-ip` would therefore have been enough to
    stop meeting creation. Provenance is worth less than availability here: an unparseable
    address is recorded as NULL rather than allowed to break the write path.

    `X-Forwarded-For` is trusted only because Cloudflare and then the single ingress sit in
    front of every deployment. A directly-exposed service would be spoofable here.
    """
    candidate = request.headers.get("x-forwarded-for", "").split(",")[0].strip()
    if not candidate:
        candidate = request.client.host if request.client else ""
    try:
        return str(ipaddress.ip_address(candidate)) if candidate else None
    except ValueError:
        return None


async def tenant_context(
    request: Request,
    claims: dict[str, Any] = Depends(verify_token),  # noqa: B008
) -> RequestContext:
    """Resolve the caller to a tenant and a tier. Raises 401 before any tenant is established.

    The `users` lookup runs with `app.current_org` already set, so a Clerk user id that exists
    in another tenant resolves to nothing here rather than to that tenant's row.

    A personal access token (`libs/shared/pat.py`) arrives already resolved by this request's
    `PatAuthMiddleware`; it becomes the same context, re-checked against the scope rules here
    as well so a route can never be reached by a token the middleware would have refused.
    """
    if pat.PAT_CLAIM in claims:
        return _token_context(request, claims)
    clerk_org = clerk_org_id_from(claims)
    clerk_user = user_id_from(claims)

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session, session.begin():
        org_id = await resolve_org_id(session, clerk_org)
        await set_rls(session, org_id)
        row = (await session.execute(_SELECT_PRINCIPAL, {"cuid": clerk_user})).mappings().first()

    # A verified Clerk user with no local mirror row is a real state: the webhook may not have
    # arrived yet. They get a context with no tier, so they can be authenticated and still
    # permitted nothing, rather than being rejected as if their token were bad.
    set_org_id(org_id)
    return RequestContext(
        org_id=org_id,
        user_id=row["id"] if row else None,
        clerk_user_id=clerk_user,
        rbac_tier=(row["tier"] if row and row["tier"] else ""),
        ip=_client_ip(request),
    )


async def tenant_session(
    ctx: RequestContext = Depends(tenant_context),  # noqa: B008
) -> AsyncIterator[AsyncSession]:
    """A session inside one transaction, with `app.current_org` bound for the whole request.

    Transaction-local (`set_config(..., true)`) is not a detail. With a pooled connection a
    session-level GUC outlives the request and leaks one tenant's `app.current_org` into the
    next tenant that reuses the connection, which is a cross-tenant read with no bug in any
    handler.

    Yielding inside `session.begin()` means the route body, its outbox row and its audit row
    all commit together or not at all.
    """
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        async with session.begin():
            await set_rls(session, ctx.org_id)
            yield session


def _token_context(request: Request, claims: dict[str, Any]) -> RequestContext:
    principal = pat.current_principal()
    marker = claims.get(pat.PAT_CLAIM) or {}
    if principal is None or str(principal.token_id) != str(marker.get("token_id")):
        raise pat.invalid()
    verdict = pat.allows(request.method, request.url.path, principal.scope)
    if verdict != "ok":
        raise pat.refused(verdict)
    set_org_id(principal.org_id)
    return RequestContext(
        org_id=principal.org_id,
        user_id=principal.user_id,
        clerk_user_id=principal.clerk_user_id,
        rbac_tier=principal.rbac_tier,
        ip=_client_ip(request),
        auth_method="api_token",
        token_id=principal.token_id,
        token_scope=principal.scope,
    )


def require_tier(minimum: str) -> Callable[..., Any]:
    """Route dependency: deny unless the caller's grant is `minimum` or above.

    A denial is audited. The audit write uses its OWN session rather than the request's,
    because the request transaction is about to be discarded by the 403 and the denial is
    exactly the fact worth keeping. Writing it into the doomed transaction would mean the
    audit trail records every denial except the ones that happened.
    """
    if minimum not in TIER_RANK:
        raise ValueError(f"unknown tier {minimum!r}; expected one of {sorted(TIER_RANK)}")

    async def _dep(
        request: Request,
        ctx: RequestContext = Depends(tenant_context),  # noqa: B008
    ) -> RequestContext:
        if ctx.satisfies(minimum):
            return ctx
        await _audit_denial(ctx, route=request.url.path, required=minimum)
        raise ProblemDetail(
            403,
            ErrorCode.FORBIDDEN_ROLE,
            f"This action requires the {minimum} role or above",
        )

    return _dep


async def _audit_denial(ctx: RequestContext, *, route: str, required: str) -> None:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session, session.begin():
        await set_rls(session, ctx.org_id)
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=ctx.user_id,
            action=ACTION_AUTHZ_DENY,
            target_type="route",
            target_id=route,
            metadata={"reason": "rbac_tier", "required": required, "held": ctx.rbac_tier or None},
            ip=ctx.ip,
        )


def not_found() -> ProblemDetail:
    """The cross-tenant answer. 404, never 403.

    A resource in another tenant is invisible to RLS, so the handler genuinely cannot tell
    "does not exist" from "exists elsewhere" - and must not, because a 403 would confirm the
    resource exists and turn any id into a probe. `FORBIDDEN_ROLE` above is a different case:
    it says nothing about whether any particular resource exists.
    """
    return ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
