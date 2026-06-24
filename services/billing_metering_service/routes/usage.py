from typing import Any
from fastapi import APIRouter, Depends
from libs.shared.auth import verify_token

router = APIRouter(tags=["usage"])


@router.get("")
async def get_usage(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.get("/quota")
async def get_quota(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError
