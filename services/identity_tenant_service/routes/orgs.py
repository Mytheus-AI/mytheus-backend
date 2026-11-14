"""`/orgs`: read the caller's tenant, and provision a new one. Plan 1-2 section 3.4 step 3.

**Why provisioning is an operator action, not self-serve.** Plan 1-2 deferred `POST /orgs`
because of a design hole (`work-done-till-now/02-plan-1-2-foundation-and-tenancy.md` section 5
item 7): the edge has two states, public or org-required, and a caller creating their FIRST
tenant has no org claim. "Public" would let anyone create tenants; "org-required" lets nobody.
The missing third state, authenticated-but-org-exempt, is still an open decision, and this
module does not make it.

What it does instead needs no third state: the caller is an `owner` of a designated
PLATFORM-OPERATOR tenant, authenticated through the unchanged gate chain (`verify_token` ->
`resolve_org_id` -> `require_tier("owner")`), and provisions a customer tenant on its behalf.
The operator tenants are listed in `PLATFORM_OPERATOR_ORG_IDS` (comma-separated internal
`org_id` UUIDs). Deny-by-default: unset or empty means NOBODY may provision through this route,
which is the same reachable surface as before the route existed. Self-serve signup remains the
deferred decision it was.

**Why the Clerk org id is in the body, and why that is safe here.** The new tenant is
reachable only once `organizations.clerk_org_id` names its Clerk organization. The operator
supplies it; `provision_tenant` refuses one that already names a tenant and never attaches it
to an existing row (`link_existing=False`), so this route cannot repoint a live tenant.

**Why this route manages its own session.** `tenant_session` commits AFTER the route returns,
but the Clerk `public_metadata` push (`sync_internal_org_id_to_clerk`) must run after COMMIT:
publishing an `org_id` a rollback then erases would leave Clerk pointing at nothing. Background
tasks do not help (Starlette runs them before a yield-dependency's cleanup; see the FIX ROUND 1
note in `services/meeting_service/routes.py`). So the route takes the gate (`require_tier`, via
`_require_platform_operator`) and opens one explicit transaction of its own.

**Deployment prerequisite.** Migration `a1c4e7f20b18` grants `mytheus_app` column-level SELECT
on `organizations` and nothing else ("a tenant is born through the operator seam, never through
a request handler"). Until a migration grants `INSERT` on the tenant root to the request-path
role, the INSERT here is refused by Postgres, and the route answers `501 NOT_IMPLEMENTED` with
nothing written, rather than a `500`.
"""

from __future__ import annotations

import logging
import os
import re
import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Request, Response, status
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError

from libs.shared.audit import ACTION_AUTHZ_DENY, ACTION_ORG_PROVISIONED, audit
from libs.shared.auth import clerk_org_id_from, user_id_from, verify_token
from libs.shared.authz import RequestContext, _client_ip, not_found, require_tier
from libs.shared.clerk_admin import ClerkUnavailable, membership_of, org_for_admin
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.identity_tenant_service.branding import routes as branding_routes
from services.identity_tenant_service.provisioning import (
    PRODUCER,
    ProvisionResult,
    TenantConflict,
    TenantOwner,
    ensure_member,
    provision_tenant,
    sync_internal_org_id_to_clerk,
    tier_for_clerk_role,
)
from services.identity_tenant_service.schemas import (
    Organization,
    OrgCreate,
    ProvisionedOrganization,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["orgs"])

OPERATOR_ORGS_ENV = "PLATFORM_OPERATOR_ORG_IDS"

_SQLSTATE_INSUFFICIENT_PRIVILEGE = "42501"
_SQLSTATE_UNIQUE_VIOLATION = "23505"

_SELECT_OWN_ORG = text(
    "SELECT id, name, slug, status, clerk_org_id FROM organizations "
    "WHERE id = :id AND deleted_at IS NULL"
)


def operator_org_ids() -> frozenset[uuid.UUID]:
    """The tenants whose owners may provision. Unparseable entries are dropped, not fatal:
    a typo must narrow the set (fail closed), never widen it or take the service down."""
    out: set[uuid.UUID] = set()
    for raw in os.environ.get(OPERATOR_ORGS_ENV, "").split(","):
        raw = raw.strip()
        if not raw:
            continue
        try:
            out.add(uuid.UUID(raw))
        except ValueError:
            logger.warning("%s: ignoring unparseable entry", OPERATOR_ORGS_ENV)
    return frozenset(out)


async def _audit_operator_denial(ctx: RequestContext, route: str) -> None:
    """Own session, same reason as `libs/shared/authz._audit_denial`: the request is about to
    be rejected, and the denial is exactly the fact worth keeping."""
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, ctx.org_id)
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=ctx.user_id,
            action=ACTION_AUTHZ_DENY,
            target_type="route",
            target_id=route,
            metadata={"reason": "not_platform_operator", "held": ctx.rbac_tier or None},
            ip=ctx.ip,
        )


async def _require_platform_operator(
    request: Request,
    ctx: Annotated[RequestContext, Depends(require_tier("owner"))],
) -> RequestContext:
    """`owner` of an operator tenant, or 403. Layered ON the tier gate, never instead of it."""
    if ctx.org_id in operator_org_ids():
        return ctx
    await _audit_operator_denial(ctx, request.url.path)
    raise ProblemDetail(
        403, ErrorCode.FORBIDDEN_ROLE, "Provisioning a tenant requires a platform operator"
    )


def _sqlstate(exc: DBAPIError) -> str | None:
    orig = exc.orig
    for candidate in (orig, getattr(orig, "__cause__", None)):
        code = getattr(candidate, "sqlstate", None) or getattr(candidate, "pgcode", None)
        if code:
            return str(code)
    return None


@router.get("/orgs", response_model=Organization)
async def read_own_org(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
) -> Organization:
    """The organization the caller's token is scoped to, and only that one.

    A collection path returning one object, deliberately: `/orgs/{id}` would put a tenant
    identifier in the path (`org-id-never-in-path` in `design/.spectral.yaml`), and a list
    would only ever have one visible member anyway.
    """
    async with get_sessionmaker()() as session, session.begin():
        row = (await session.execute(_SELECT_OWN_ORG, {"id": str(ctx.org_id)})).mappings().first()
        if row is None:
            # Resolved a moment ago, gone now (soft-deleted between the two reads).
            raise not_found()
        # The logo lives on the RLS-policied `tenant_settings`, so the tenant is bound first.
        await set_rls(session, ctx.org_id)
        logo = branding_routes.to_model(await branding_routes.read_logo_row(session, ctx.org_id))
    return Organization(**dict(row), logo_url=logo.logo_url, logo_updated_at=logo.logo_updated_at)


@router.post("/orgs", status_code=status.HTTP_201_CREATED, response_model=ProvisionedOrganization)
async def provision_org(
    body: OrgCreate,
    ctx: Annotated[RequestContext, Depends(_require_platform_operator)],
) -> ProvisionedOrganization:
    """Create a complete tenant: root, settings, residency, owner + grant, outbox, audit.

    `Idempotency-Key` is honoured by the shared middleware (scoped to the OPERATOR's tenant),
    so a retried request replays the stored 201. Without a key, a repeat is a 409.
    """
    new_org_id = uuid.uuid4()
    try:
        async with get_sessionmaker()() as session, session.begin():
            # The operator tenant's own trail first, while `app.current_org` is still the
            # operator's: `provision_tenant` moves it to the new tenant.
            await set_rls(session, ctx.org_id)
            await audit(
                session,
                org_id=ctx.org_id,
                actor_id=ctx.user_id,
                action=ACTION_ORG_PROVISIONED,
                target_type="organization",
                target_id=new_org_id,
                metadata={"slug": body.slug, "side": "operator"},
                ip=ctx.ip,
            )
            created = await provision_tenant(
                session,
                org_id=new_org_id,
                name=body.name,
                slug=body.slug,
                clerk_org_id=body.clerk_org_id,
                owner=TenantOwner(clerk_user_id=body.owner.clerk_user_id, email=body.owner.email),
            )
            # And the new tenant's own trail, so its compliance export shows how it was born.
            await audit(
                session,
                org_id=created.org_id,
                actor_id=ctx.user_id,
                action=ACTION_ORG_PROVISIONED,
                target_type="organization",
                target_id=created.org_id,
                metadata={
                    "slug": body.slug,
                    "side": "tenant",
                    "operator_org_id": str(ctx.org_id),
                    "producer": PRODUCER,
                },
                ip=ctx.ip,
            )
    except TenantConflict:
        raise ProblemDetail(
            409, ErrorCode.VERSION_CONFLICT, "Organization already exists"
        ) from None
    except IntegrityError as exc:
        if _sqlstate(exc) == _SQLSTATE_UNIQUE_VIOLATION:
            # A concurrent provision won the race on a unique index. Same non-echoing body.
            raise ProblemDetail(
                409, ErrorCode.VERSION_CONFLICT, "Organization already exists"
            ) from None
        raise
    except DBAPIError as exc:
        if _sqlstate(exc) == _SQLSTATE_INSUFFICIENT_PRIVILEGE:
            logger.error(
                "POST /orgs: request-path role lacks INSERT on organizations; "
                "provisioning is not enabled on this deployment"
            )
            raise ProblemDetail(
                501,
                ErrorCode.NOT_IMPLEMENTED,
                "Tenant provisioning is not enabled on this deployment",
            ) from None
        raise

    # After COMMIT, best-effort, never fails the request (see the module docstring).
    await sync_internal_org_id_to_clerk(
        ProvisionResult(org_id=created.org_id, created=True), body.clerk_org_id
    )

    return ProvisionedOrganization(
        id=created.org_id,
        name=body.name,
        slug=body.slug,
        status="active",
        clerk_org_id=body.clerk_org_id,
        owner_user_id=created.owner_user_id,
    )


# --- self-serve: the org's own Clerk admin provisions its first tenant -----------------------
#
# The third edge state the module docstring calls missing, authenticated-but-org-exempt, now
# exists for exactly this one path. `api-gateway` verifies the token on it but skips tenant
# resolution (there is no tenant yet); this route then re-verifies the token itself and asks
# CLERK, not the caller, whether the caller is an `org:admin` of the org in their token. The
# name, slug and owner email are read from Clerk too, so the body carries nothing at all.
#
# Idempotent: a second call for an org that already has a tenant returns it with 200. That is
# what lets onboarding call this unconditionally after every org it creates or selects.

SELF_SERVE_PATH = "/orgs/self-serve"

_SELECT_BY_CLERK_ORG = text(
    "SELECT id, name, slug, status, clerk_org_id FROM organizations "
    "WHERE clerk_org_id = :c AND deleted_at IS NULL"
)
_SLUG_INVALID = re.compile(r"[^a-z0-9-]+")


def self_serve_slug(clerk_slug: str, clerk_org_id: str) -> str:
    """Clerk's slug, normalized to `OrgCreate`'s pattern `^[a-z0-9][a-z0-9-]{1,62}$`.

    Clerk slugs may hold `_` and run past 63 characters; both are folded. An empty result
    falls back to the opaque org id, which is unique by construction.
    """
    slug = _SLUG_INVALID.sub("-", clerk_slug.lower()).strip("-")
    if len(slug) < 2:
        slug = _SLUG_INVALID.sub("-", clerk_org_id.lower()).strip("-")
    return slug[:63].rstrip("-")


_SELECT_CALLER = text("SELECT 1 FROM users WHERE clerk_user_id = :c AND deleted_at IS NULL")


async def _ensure_caller_is_member(org_id: uuid.UUID, clerk_org: str, clerk_user: str) -> None:
    """The join path. Someone who accepted an invitation to an existing workspace reaches it
    before (or without) Clerk's membership webhook, so they have no mirror row or grant yet and
    every tenant route would refuse them. Clerk is asked whether they are a member, and if so
    they are added with the tier their Clerk role maps to. A caller already here costs one
    indexed read and no Clerk call."""
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        if (await session.execute(_SELECT_CALLER, {"c": clerk_user})).first():
            return
    try:
        membership = await membership_of(clerk_org, clerk_user)
    except ClerkUnavailable:
        logger.exception("self-serve join: Clerk unavailable for %s", clerk_org)
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "Identity provider unavailable"
        ) from None
    if membership is None:
        raise ProblemDetail(
            403, ErrorCode.FORBIDDEN_ROLE, "You are not a member of this organization"
        )
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        await ensure_member(
            session,
            org_id=org_id,
            clerk_user_id=clerk_user,
            email=membership.email,
            avatar_url=membership.avatar_url,
            tier=tier_for_clerk_role(membership.role),
        )


@router.post(
    SELF_SERVE_PATH,
    status_code=status.HTTP_201_CREATED,
    response_model=Organization,
    responses={200: {"model": Organization, "description": "Already provisioned"}},
)
async def self_serve_provision(
    request: Request,
    response: Response,
    claims: Annotated[dict[str, Any], Depends(verify_token)],
) -> Organization:
    """authz: Clerk `org:admin` of the token's active organization (checked against Clerk).

    Creates the tenant for the caller's active Clerk organization: root, settings, residency,
    the caller as `owner`, outbox and audit, then publishes `internal_org_id` to Clerk.
    """
    clerk_org = clerk_org_id_from(claims)
    clerk_user = user_id_from(claims)

    async with get_sessionmaker()() as session, session.begin():
        row = (await session.execute(_SELECT_BY_CLERK_ORG, {"c": clerk_org})).mappings().first()
    if row is not None:
        await _ensure_caller_is_member(uuid.UUID(str(row["id"])), clerk_org, clerk_user)
        response.status_code = status.HTTP_200_OK
        return Organization(**dict(row))

    try:
        clerk = await org_for_admin(clerk_org, clerk_user)
    except ClerkUnavailable:
        logger.exception("self-serve provisioning: Clerk unavailable for %s", clerk_org)
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "Identity provider unavailable"
        ) from None
    if clerk is None:
        raise ProblemDetail(
            403, ErrorCode.FORBIDDEN_ROLE, "Only an admin of this organization can set it up"
        )

    slug = self_serve_slug(clerk.slug, clerk_org)
    try:
        async with get_sessionmaker()() as session, session.begin():
            created = await provision_tenant(
                session,
                name=clerk.name[:200],
                slug=slug,
                clerk_org_id=clerk_org,
                owner=TenantOwner(clerk_user_id=clerk_user, email=clerk.owner_email),
            )
            await audit(
                session,
                org_id=created.org_id,
                actor_id=created.owner_user_id,
                action=ACTION_ORG_PROVISIONED,
                target_type="organization",
                target_id=created.org_id,
                metadata={
                    "slug": slug,
                    "side": "tenant",
                    "mode": "self_serve",
                    "producer": PRODUCER,
                },
                ip=_client_ip(request),
            )
    except (TenantConflict, IntegrityError):
        # Lost a race with a concurrent call for the same org (or the slug is taken): answer
        # with whatever now holds this Clerk org, or 409 when nothing does.
        async with get_sessionmaker()() as session, session.begin():
            row = (await session.execute(_SELECT_BY_CLERK_ORG, {"c": clerk_org})).mappings().first()
        if row is not None:
            response.status_code = status.HTTP_200_OK
            return Organization(**dict(row))
        raise ProblemDetail(
            409, ErrorCode.VERSION_CONFLICT, "Organization already exists"
        ) from None

    await sync_internal_org_id_to_clerk(
        ProvisionResult(org_id=created.org_id, created=True), clerk_org
    )
    return Organization(
        id=created.org_id, name=clerk.name[:200], slug=slug, status="active", clerk_org_id=clerk_org
    )
