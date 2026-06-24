from typing import Any
from fastapi import APIRouter, Depends
from libs.shared.auth import verify_token

router = APIRouter(tags=["bff:notifications"])


@router.get("/notifications")
async def list_notifications(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError
