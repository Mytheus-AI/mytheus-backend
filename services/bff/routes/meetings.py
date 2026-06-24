from typing import Any
from fastapi import APIRouter, Depends
from libs.shared.auth import verify_token

router = APIRouter(tags=["bff:meetings"])


@router.get("/meetings/{meeting_id}/room")
async def get_meeting_room(
    meeting_id: str,
    claims: dict = Depends(verify_token),
) -> Any:
    """Aggregate: meeting + participants + transcript summary + agent suggestions."""
    raise NotImplementedError
