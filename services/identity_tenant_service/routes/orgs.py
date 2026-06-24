from typing import Any
from fastapi import APIRouter, Depends
from libs.shared.auth import verify_token

router = APIRouter(tags=["orgs"])


@router.get("")
async def list_orgs(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.post("")
async def create_org(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.get("/{org_id}")
async def get_org(org_id: str, claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.patch("/{org_id}")
async def update_org(org_id: str, claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError
