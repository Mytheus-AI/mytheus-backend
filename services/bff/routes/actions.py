from typing import Any
from fastapi import APIRouter, Depends
from libs.shared.auth import verify_token

router = APIRouter(tags=["bff:actions"])


@router.get("/actions")
async def list_actions(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError
