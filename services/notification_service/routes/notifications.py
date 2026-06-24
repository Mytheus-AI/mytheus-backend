from typing import Any
from fastapi import APIRouter, Depends
from libs.shared.auth import verify_token

router = APIRouter(tags=["notifications"])


@router.get("")
async def list_notifications(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.patch("/{notification_id}")
async def update_notification(notification_id: str, claims: dict = Depends(verify_token)) -> Any:
    """Mark as read or dismissed."""
    raise NotImplementedError
