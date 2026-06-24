from typing import Any
from fastapi import APIRouter, Depends
from libs.shared.auth import verify_token

router = APIRouter(tags=["teams"])


@router.get("")
async def list_teams(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.post("")
async def create_team(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError
