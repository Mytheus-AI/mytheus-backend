"""SCIM 2.0 provisioning endpoints (stubbed).

Enabled for Enterprise tenants (features.scim_provisioning = true in tenant_settings).
Supabase SSO/SAML is configured per-tenant in the Supabase Dashboard (Auth → SSO).
SCIM writes flow through the same user sync path as DB webhooks.
"""
from typing import Any
from fastapi import APIRouter

router = APIRouter(tags=["scim"])


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
