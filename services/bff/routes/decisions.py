from typing import Any
from fastapi import APIRouter, Depends
from libs.shared.auth import verify_token

router = APIRouter(tags=["bff:decisions"])


@router.get("/decisions")
async def list_decisions(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError
