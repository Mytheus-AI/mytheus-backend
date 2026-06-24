from typing import Any
from fastapi import APIRouter, Depends
from libs.shared.auth import verify_token

router = APIRouter(tags=["admin:tenant"])


@router.get("/members")
async def list_members(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.post("/export")
async def request_export(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.post("/erasure")
async def request_erasure(claims: dict = Depends(verify_token)) -> Any:
    """GDPR right-to-be-forgotten request. Triggers RTBF Temporal workflow."""
    raise NotImplementedError
