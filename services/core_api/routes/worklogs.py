from typing import Any
from fastapi import APIRouter, Depends
from libs.shared.auth import verify_token

router = APIRouter(tags=["worklogs"])


@router.get("")
async def list_worklogs(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.post("")
async def create_worklog(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError
