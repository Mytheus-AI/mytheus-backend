from typing import Any
from fastapi import APIRouter, Depends, Header
from libs.shared.auth import verify_token

router = APIRouter(tags=["meetings"])


@router.get("")
async def list_meetings(claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.post("")
async def create_meeting(
    claims: dict = Depends(verify_token),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> Any:
    raise NotImplementedError


@router.get("/{meeting_id}")
async def get_meeting(meeting_id: str, claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.patch("/{meeting_id}")
async def update_meeting(meeting_id: str, claims: dict = Depends(verify_token)) -> Any:
    raise NotImplementedError


@router.get("/{meeting_id}/transcript")
async def get_transcript(meeting_id: str, claims: dict = Depends(verify_token)) -> Any:
    """Served via FTS over transcript_chunks (owned by transcription-service)."""
    raise NotImplementedError
