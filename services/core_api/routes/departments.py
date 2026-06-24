from typing import Any
from fastapi import APIRouter, Depends
from libs.shared.auth import verify_token

router = APIRouter(tags=["departments"])


@router.get("")
async def list_departments(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.post("")
async def create_department(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError
