"""SCIM 2.0 provisioning endpoints (stubbed).

Enabled for Enterprise tenants (features.scim_provisioning = true in tenant_settings).
SSO/SAML/SCIM are Clerk-fronted (Clerk Enterprise connections); SCIM writes flow through the same
Clerk user-sync path as the /clerk/webhook handler. Full SCIM lands in a later band.
"""

from typing import Any

from fastapi import APIRouter, Depends

from libs.shared.auth import verify_token
from libs.shared.errors import NotImplementedProblem

# SCIM is authenticated (IdP bearer token). Guard at the router level so every current and future
# route inherits auth — a route here can never silently ship unauthenticated.
router = APIRouter(tags=["scim"], dependencies=[Depends(verify_token)])

# DEFERRED to the Enterprise band, per the resolved decision on ADR-0010 D2: SSO/SAML/SCIM
# are out of scope for MVP, these routes stay present and clearly stubbed, and
# `tenant_settings.sso_config` exists unused. Each returns 501 with an RFC 7807 body.
#
# They previously raised NotImplementedError, which is unhandled and therefore a 500. To an
# IdP integrator 500 means "this is broken, retry" and 501 means "this is not offered" -
# and SCIM clients retry 5xx on a schedule, so the wrong one manufactures load and a
# support ticket. When SCIM lands, these bodies flow through the same user-sync path as
# POST /clerk/webhook rather than becoming a second write path into `users`.
_DEFERRED = "SCIM provisioning is deferred to the Enterprise band."


@router.get("/Users")
async def list_users() -> Any:
    raise NotImplementedProblem(_DEFERRED)


@router.post("/Users")
async def provision_user() -> Any:
    raise NotImplementedProblem(_DEFERRED)


@router.put("/Users/{user_id}")
async def update_user(user_id: str) -> Any:
    raise NotImplementedProblem(_DEFERRED)


@router.delete("/Users/{user_id}")
async def deprovision_user(user_id: str) -> Any:
    """Near-real-time deactivation — applied immediately, not deferred to reconciliation."""
    raise NotImplementedProblem(_DEFERRED)


@router.get("/Groups")
async def list_groups() -> Any:
    raise NotImplementedProblem(_DEFERRED)
