"""SCIM 2.0 provisioning endpoints (stubbed).

Enabled for Enterprise tenants (features.scim_provisioning = true in tenant_settings).
SSO/SAML/SCIM are Clerk-fronted (Clerk Enterprise connections); SCIM writes flow through the same
Clerk user-sync path as the /clerk/webhook handler. Full SCIM lands in a later band.
"""

from typing import Any

from fastapi import APIRouter, Depends

from libs.shared.auth import verify_token

# SCIM is authenticated (IdP bearer token). Guard at the router level so every current and future
# route inherits auth — a route here can never silently ship unauthenticated.
router = APIRouter(tags=["scim"], dependencies=[Depends(verify_token)])


@router.get("/Users")
async def list_users() -> Any:
    raise NotImplementedError


@router.post("/Users")
async def provision_user() -> Any:
    raise NotImplementedError


@router.put("/Users/{user_id}")
async def update_user(user_id: str) -> Any:
    raise NotImplementedError


@router.delete("/Users/{user_id}")
async def deprovision_user(user_id: str) -> Any:
    """Near-real-time deactivation — applied immediately, not deferred to reconciliation."""
    raise NotImplementedError


@router.get("/Groups")
async def list_groups() -> Any:
    raise NotImplementedError
