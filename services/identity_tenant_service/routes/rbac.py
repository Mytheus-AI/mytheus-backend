"""RBAC grant management.

Permission tiers: owner / admin / member.
These are separate from users.role (domain/skills attribute for agent relevance).
"""
from typing import Any
from fastapi import APIRouter, Depends
from libs.shared.auth import verify_token

router = APIRouter(tags=["rbac"])


@router.get("/members")
async def list_members(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.post("/grants")
async def create_grant(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.delete("/grants/{grant_id}")
async def revoke_grant(grant_id: str, claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError
