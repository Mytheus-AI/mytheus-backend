"""Room token endpoint. p95 < 250 ms SLO."""
from typing import Any
from fastapi import APIRouter, Depends
from libs.shared.auth import verify_token

router = APIRouter(tags=["rooms"])


@router.post("/{meeting_id}/token")
async def mint_room_token(meeting_id: str, claims: dict = Depends(verify_token)) -> Any:
    """Mint a LiveKit participant token for the requesting user."""
    raise NotImplementedError
