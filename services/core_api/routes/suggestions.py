from typing import Any
from fastapi import APIRouter, Depends
from libs.shared.auth import verify_token

router = APIRouter(tags=["suggestions"])


@router.get("")
async def list_suggestions(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.patch("/{suggestion_id}")
async def update_suggestion(suggestion_id: str, claims: dict = Depends(verify_token)) -> Any:
    """Ask / Dismiss / Save-for-later."""
    raise NotImplementedError
