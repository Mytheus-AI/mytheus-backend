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
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Request, status
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError

from libs.shared.audit import ACTION_AUTHZ_DENY, ACTION_ORG_PROVISIONED, audit
from libs.shared.authz import RequestContext, not_found, require_tier
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.identity_tenant_service.provisioning import (
    PRODUCER,
    ProvisionResult,
    TenantConflict,
    TenantOwner,
    provision_tenant,
    sync_internal_org_id_to_clerk,
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
    return Organization(**dict(row))


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
